#!/usr/bin/env python3
# CephFS pool/layout migration tool ("transcoder")
#
# Loosely inspired by:
# https://git.sr.ht/~pjjw/cephfs-layout-tool/tree/master/item/cephfs_layout_tool/migrate_pools.py
# https://gist.github.com/ervwalter/5ff6632c930c27a1eb6b07c986d7439b
#
# MIT license (https://opensource.org/license/mit)

import errno, shlex
import os, re, stat, time, signal, shutil, logging, sys, fcntl, dataclasses
from concurrent.futures import ThreadPoolExecutor
import threading, uuid, argparse

_VERSION = "1701"

# Replacing a file must be serialized against another worker replacing the
# SAME file -- that is the only invariant here. A single global lock also
# serialized every *unrelated* file, which capped effective concurrency at one
# regardless of --threads (measured 0.96 on a 15-thread job). Stripe by a hash
# of the path: equal paths always map to the same stripe, so the per-path
# guarantee holds, while unrelated paths proceed in parallel.
_REPLACE_LOCK_STRIPES = 64
_replace_locks = [threading.Lock() for _ in range(_REPLACE_LOCK_STRIPES)]


def _replace_lock_for(path):
    return _replace_locks[hash(path) % _REPLACE_LOCK_STRIPES]
do_exit = threading.Event()
thread_count = None
file_delay_ms = 0
min_age_days = 1

# --- multiplicative delay stepping -------------------------------------------
# The delay knob is hyperbolic: +100ms at 2000ms is a 5% rate change, at 100ms
# it is a 100x change. A fixed additive step therefore cannot serve both ends of
# the range. A constant ratio gives even ~25% granularity everywhere, so
# 2100ms -> 0 is 30 steps instead of being unreachable below 100ms.
# Stays INTEGER milliseconds: 1ms is already ~1000 files/s, at or above what the
# walker can stat at, so sub-ms would control nothing -- and keeping it integral
# leaves the log line format unchanged for anything already parsing it.
# Multiplicative step for the delay signals, separately settable per direction
# so backing off can be coarser than recovering. "Back off fast, speed up
# slowly" then lives in the step size as well as in whatever cadence an external
# controller uses. Both default to 1.25 (a ~25%/20% rate change per step);
# raise delay_step_up alone to make SIGRTMIN retreat harder.
DELAY_STEP_UP = 1.25
DELAY_STEP_DOWN = 1.25
DELAY_MAX_MS = 600000
# Floor for the DOWN direction. 0 keeps the historical behavior of allowing a
# step to unthrottled; set it in the config to guarantee the signal path can
# never produce an unbounded stat rate on a live filesystem.
DELAY_MIN_MS = 0

# Where per-volume config files conventionally live: local disk, never in the
# CephFS volume being walked.
CONFIG_DIR = os.path.expanduser("~")

# Directory names pruned from the walk unless --prune-dir-regex says otherwise.
#
# Every entry is machine-generated, reproducible from a manifest, and composed
# almost entirely of files below any plausible --min-size, so walking one costs
# MDS stats and returns nothing. This is a DEFAULT, not a policy: pass
# --prune-dir-regex to replace the set, or --prune-dir-regex '' to disable
# pruning entirely. The effective pattern is logged at startup either way.
#
# Measured on one production volume: 86.5% of files sat inside virtualenvs, and
# the largest of 585,309 of them is 503,423 bytes -- below the 512,000 byte
# threshold, so nothing eligible is lost. Another volume spent 22 days at 1.17%
# of its files grinding a Bazel .runfiles tree carrying a pip site-packages copy
# of the awscli examples directory. A third's zero-yield stretch was inside
# renv/packrat R package libraries.
#
# Anchored to whole names: 'venv' prunes a directory called venv, not one
# called conventions.
DEFAULT_PRUNE_DIRS = (
    ".runfiles",        # Bazel symlink farms
    "site-packages",    # pip
    "node_modules",     # npm
    "__pycache__",      # cpython bytecode
    ".venv", "venv", "penv",
    "renv", "packrat",  # R package libraries
    ".git",
    ".tox", ".mypy_cache", ".pytest_cache",
    ".cargo", ".gradle",
)
DEFAULT_PRUNE_REGEX = "^(%s)$" % "|".join(re.escape(d) for d in DEFAULT_PRUNE_DIRS)

# Set in main() when --config is given; polled from the walker loop.
runtime_config = None
apply_config = None


def config_example(volume="VOLUME"):
    """A complete, fully-populated --config file with every key at its default.

    Printed by --help so the set of live tunables is discoverable without
    reading the source, and so an operator can redirect it straight to a file.
    Values shown ARE the running defaults, interpolated from the constants
    above rather than retyped, so this cannot drift.
    """
    return "\n".join((
        "# vcephfs_transcoder runtime config -- %s" % volume,
        "# Conventional path: %s" % config_path_for(volume),
        "#",
        "# Re-read when this file's mtime changes (--config-poll-seconds,",
        "# default 10s). Only keys whose value CHANGED in the file are applied,",
        "# so editing one line never re-asserts the others -- a delay set by",
        "# signal survives an unrelated edit here.",
        "#",
        "# Keep this on LOCAL disk, never inside the CephFS volume: a stat there",
        "# is an MDS round trip, which is the load being bounded.",
        "#",
        "# Every key is optional. Commenting one out means this file does not",
        "# assert it at all, leaving it to the command line or to signals.",
        "",
        "# --- pace -------------------------------------------------------------",
        "# Sleep per file ENCOUNTERED in the walk, before the stat. Scan rate is",
        "# 1000/file_delay_ms files/s REGARDLESS of threads -- the walker is",
        "# single-threaded and is what gates a pass. 0 = unthrottled.",
        "file_delay_ms   = 0",
        "",
        "# Concurrent copies. Bounds the copier, not the walker. 0 pauses the job",
        "# (in-flight copies finish); set > 0 to resume.",
        "threads         = 1",
        "",
        "# --- how the delay signals move ---------------------------------------",
        "# SIGRTMIN multiplies the delay, SIGRTMIN+1 divides it. Separate rates",
        "# let backing off be coarser than recovering: 2.0 up with 1.1 down",
        "# retreats in doublings and probes back in 10% increments.",
        "delay_step_up   = %s" % DELAY_STEP_UP,
        "delay_step_down = %s" % DELAY_STEP_DOWN,
        "",
        "# Floor for the down direction. 0 permits a step to fully unthrottled;",
        "# raise it to guarantee the signal path can never do that on a live",
        "# filesystem.",
        "delay_min_ms    = %d" % DELAY_MIN_MS,
        "",
        "# --- what counts as eligible ------------------------------------------",
        "# Skip files modified within this many days.",
        "min_age_days    = 1",
        "",
        "# Skip files smaller than this many bytes. Pre-Tentacle, EC pads small",
        "# objects to a whole stripe, which is why this is not lower.",
        "min_size        = 512000",
        "",
        "# --- what to skip entirely --------------------------------------------",
        "# Matched against directory NAMES during the walk; matches are never",
        "# descended into or statted. Cost is one regex match per directory, not",
        "# per file. Set empty to disable pruning entirely.",
        "prune_dir_regex = %s" % DEFAULT_PRUNE_REGEX,
        "",
    ))


def config_path_for(volume):
    """Conventional config path for a volume.

    One file per volume rather than sections in one file: jobs run on different
    hosts, so a shared file would need syncing and would invite cross-host
    read-modify-write races.
    """
    return "%s/tc_%s.conf" % (CONFIG_DIR, volume)


def _delay_up(old):
    """One step slower. Always moves by >=1ms; a multiplicative step at small
    values would otherwise round back to where it started and stick."""
    if old <= 0:
        return max(1, DELAY_MIN_MS)
    return min(max(int(round(old * DELAY_STEP_UP)), old + 1), DELAY_MAX_MS)


def _delay_down(old):
    """One step faster. Stops at DELAY_MIN_MS, which is 0 (unthrottled) unless
    the config raises it. Same minimum-movement guard."""
    if old <= DELAY_MIN_MS:
        return DELAY_MIN_MS
    if old <= max(1, DELAY_MIN_MS):
        return DELAY_MIN_MS
    return max(min(int(old / DELAY_STEP_DOWN), old - 1), DELAY_MIN_MS)


class RuntimeConfig:
    """Live tunables from a key=value file, re-read when its mtime changes.

    Applied on mtime change only, so a signal-driven adjustment persists until
    the file is next edited -- otherwise the two interfaces fight every poll.

    A bad value is rejected and the previous one kept, loudly: a typo must never
    stop a running job or silently set the delay to zero.

    Keep the file on LOCAL disk, not in the CephFS volume. A stat there is an
    MDS round trip, which is exactly the load this whole mechanism exists to
    bound.
    """

    # Live tunables are read from two different places at runtime, so a new key
    # must be applied to whichever one its reader uses or it is silently
    # ignored. Current split, as applied by _apply_config():
    #     file_delay_ms   -> module global   (read by the walker loop)
    #     min_age_days    -> module global   (read by the per-file age test)
    #     threads         -> thread_count.set_limit()
    #     min_size        -> args.min_size
    #     prune_dir_regex -> args.prune_re   (compiled, not the raw string)
    #     prune_subtree_max_bytes -> args.prune_subtree_max_bytes
    #     prune_budget_bytes      -> args.prune_budget_bytes
    #     delay_step_up   -> module global   (read by _delay_up)
    #     delay_step_down -> module global   (read by _delay_down)
    #     delay_min_ms    -> module global   (read by _delay_up/_delay_down)
    KEYS = ('file_delay_ms', 'threads', 'min_age_days', 'min_size',
            'prune_dir_regex', 'delay_step_up', 'delay_step_down',
            'delay_min_ms', 'prune_subtree_max_bytes', 'prune_budget_bytes')

    def __init__(self, path, poll_seconds=10.0):
        self.path = path
        self.poll_seconds = poll_seconds
        self._next = 0.0
        self._mtime = None
        # Last values seen IN THE FILE, so an edit only re-applies the keys it
        # actually changed. Without this, editing any one key re-asserts every
        # other key in the file and silently reverts whatever a signal had set
        # in the meantime. Observed 2026-08-30: adding prune_dir_regex to a
        # running job reset file_delay_ms from 20ms back to the 100ms still
        # written in the file, discarding four hours of adaptive easing.
        self._seen = {}

    @staticmethod
    def _comparable(v):
        """Regex objects compare by identity, so compare the pattern instead."""
        return v.pattern if hasattr(v, 'pattern') else v

    def poll(self, apply_cb):
        now = time.monotonic()
        if now < self._next:
            return
        self._next = now + self.poll_seconds
        try:
            st = os.stat(self.path)
        except FileNotFoundError:
            if self._mtime is not None:
                logging.warning("Config %s disappeared; keeping current tunables",
                                self.path)
                self._mtime = None
            return
        except OSError as e:
            logging.warning("Config %s unreadable (%s); keeping current tunables",
                            self.path, e)
            return
        if st.st_mtime == self._mtime:
            return
        self._mtime = st.st_mtime
        try:
            with open(self.path) as f:
                raw = f.read()
        except OSError as e:
            logging.warning("Config %s read failed (%s)", self.path, e)
            return
        parsed, errs = self._parse(raw)
        for e in errs:
            logging.error("Config %s: %s (ignored, previous value kept)",
                          self.path, e)

        # Apply only what changed in the file since the last read. On the very
        # first read everything is new, which is what we want at startup.
        if self._seen:
            changed = {k: v for k, v in parsed.items()
                       if self._comparable(v) != self._comparable(self._seen.get(k))}
        else:
            changed = dict(parsed)
        self._seen = dict(parsed)

        if changed:
            apply_cb(changed)
        elif parsed:
            logging.info("Config %s changed on disk but no tunable differs; "
                         "nothing re-applied", self.path)

    @staticmethod
    def _parse(raw):
        # Forward reference: _EXECUTOR_MAX_WORKERS is defined below this class.
        # Safe because _parse only runs at call time, well after import.
        out, errs = {}, []
        for n, line in enumerate(raw.splitlines(), 1):
            line = line.split('#', 1)[0].strip()
            if not line:
                continue
            if '=' not in line:
                errs.append("line %d: no '='" % n)
                continue
            k, v = (x.strip() for x in line.split('=', 1))
            try:
                if k == 'file_delay_ms':
                    iv = int(v)
                    if not 0 <= iv <= DELAY_MAX_MS:
                        raise ValueError("out of range 0..%d" % DELAY_MAX_MS)
                    out[k] = iv
                elif k == 'threads':
                    iv = int(v)
                    if not 0 <= iv <= _EXECUTOR_MAX_WORKERS:
                        raise ValueError("out of range 0..%d" % _EXECUTOR_MAX_WORKERS)
                    out[k] = iv
                elif k == 'min_age_days':
                    iv = int(v)
                    if iv < 1:
                        raise ValueError("must be >= 1")
                    out[k] = iv
                elif k == 'min_size':
                    iv = int(v)
                    if iv < 0:
                        raise ValueError("must be >= 0")
                    out[k] = iv
                elif k in ('delay_step_up', 'delay_step_down'):
                    fv = float(v)
                    # A step of exactly 1.0 would never move; below 1.0 it moves
                    # the wrong way. Cap well below the point where one step
                    # spans the whole useful range.
                    if not 1.01 <= fv <= 10.0:
                        raise ValueError("must be between 1.01 and 10.0")
                    out[k] = fv
                elif k == 'delay_min_ms':
                    iv = int(v)
                    if not 0 <= iv <= DELAY_MAX_MS:
                        raise ValueError("out of range 0..%d" % DELAY_MAX_MS)
                    out[k] = iv
                elif k in ('prune_subtree_max_bytes', 'prune_budget_bytes'):
                    iv = int(v)
                    if iv < 0:
                        raise ValueError("must be >= 0")
                    out[k] = iv
                elif k == 'prune_dir_regex':
                    out[k] = re.compile(v) if v else None
                else:
                    errs.append("line %d: unknown key %r" % (n, k))
            except (ValueError, re.error) as e:
                errs.append("line %d: %s=%r: %s" % (n, k, v, e))
        return out, errs


# setproctitle is optional: when present, the command line shown by `ps` is
# rewritten live as tunables change. Without it, that refresh is a no-op.
try:
    import setproctitle as _setproctitle
except ImportError:
    _setproctitle = None

# Upper bound for ThreadPoolExecutor max_workers.  The DynamicSemaphore is the
# real concurrency gate; this just ensures the executor has enough worker
# threads available when the operator increases concurrency at runtime via
# SIGUSR1.  Idle threads are cheap (just a stack), so a generous ceiling is
# fine for I/O-bound work.
_EXECUTOR_MAX_WORKERS = 128


class DynamicSemaphore:
    """A semaphore whose permit count can be changed at runtime.

    Unlike threading.BoundedSemaphore, the limit can be raised or lowered
    while the semaphore is in use.  Lowering the limit below the number of
    currently-held permits is safe — it just means no new acquires will
    succeed until enough releases bring usage below the new limit.
    """

    def __init__(self, value=1):
        # RLock (not the default-overriding plain Lock) so a signal handler may
        # re-enter .limit / .set_limit while the main thread already holds it
        # (same thread) without deadlocking.
        self._cond = threading.Condition(threading.RLock())
        self._limit = value
        self._value = value  # available permits

    def acquire(self, cancel=None):
        """Acquire a permit, blocking until one is available.

        If *cancel* is a callable, it is checked each iteration; when it
        returns True the acquire is abandoned and this method returns False.
        A bounded wait (0.5 s) guarantees that signals (SIGINT, etc.) and the
        cancel callback are always serviced promptly, even when no other
        thread calls release() or set_limit().
        """
        with self._cond:
            while self._value <= 0:
                self._cond.wait(timeout=0.5)
                if cancel is not None and cancel():
                    return False
            self._value -= 1
            return True

    def release(self):
        with self._cond:
            self._value += 1
            self._cond.notify()

    @property
    def limit(self):
        with self._cond:
            return self._limit

    def set_limit(self, new_limit):
        """Change the permit count.  If raised, blocked acquires may wake."""
        with self._cond:
            delta = new_limit - self._limit
            self._limit = new_limit
            self._value += delta
            # Wake waiters if we added permits
            if delta > 0:
                self._cond.notify_all()

# errno for "no data available" — ENODATA on Linux.
# We check explicitly rather than hardcoding 61, which means ECONNREFUSED on
# macOS/BSD.
ENODATA = getattr(errno, "ENODATA", 61)

# ---------------------------------------------------------------------------
# copy_file_range support
# ---------------------------------------------------------------------------
# On CephFS the kernel client can turn copy_file_range into OSD-to-OSD object
# copies, so data never transits the client.  We try three strategies:
#
#  1. os.copy_file_range  (Python >= 3.12)
#  2. glibc wrapper via ctypes  (glibc >= 2.27, i.e. any distro from ~2018+)
#  3. shutil.copyfileobj  (universal fallback)

import ctypes
import ctypes.util

def _probe_copy_file_range():
    """Return a (cfr_func, label) tuple or (None, None)."""
    # Strategy 1 – native Python (3.12+)
    if hasattr(os, "copy_file_range"):
        return os.copy_file_range, "os.copy_file_range"

    # Strategy 2 – ctypes into glibc
    libc_name = ctypes.util.find_library("c")
    if libc_name:
        try:
            libc = ctypes.CDLL(libc_name, use_errno=True)
            _cfr = libc.copy_file_range
            # ssize_t copy_file_range(int fd_in, off64_t *off_in,
            #                         int fd_out, off64_t *off_out,
            #                         size_t len, unsigned int flags)
            _cfr.argtypes = [
                ctypes.c_int,                        # fd_in
                ctypes.POINTER(ctypes.c_int64),      # off_in  (NULL → use fd offset)
                ctypes.c_int,                        # fd_out
                ctypes.POINTER(ctypes.c_int64),      # off_out (NULL → use fd offset)
                ctypes.c_size_t,                     # len
                ctypes.c_uint,                       # flags
            ]
            _cfr.restype = ctypes.c_ssize_t

            def _ctypes_cfr(fd_in, fd_out, count):
                n = _cfr(fd_in, None, fd_out, None, count, 0)
                if n < 0:
                    err = ctypes.get_errno()
                    raise OSError(err, os.strerror(err))
                return n

            return _ctypes_cfr, "ctypes/glibc"
        except (OSError, AttributeError):
            pass

    return None, None


_cfr_func, _cfr_label = _probe_copy_file_range()

# Errors that mean copy_file_range can't handle this particular fd pair and we
# should fall back to a userspace copy.
_CFR_FALLBACK_ERRNOS = frozenset({
    getattr(errno, "ENOSYS", None),     # syscall not available
    getattr(errno, "EXDEV", None),      # cross-device
    getattr(errno, "EOPNOTSUPP", None), # FS doesn't implement it
    getattr(errno, "EINVAL", None),     # layout incompatibility / bad range
    getattr(errno, "EBADF", None),      # fd type not supported
} - {None})


def _copy_file_data(ifd, ofd, file_size, buf_size):
    """Copy file data, preferring copy_file_range for potential server-side
    copies on CephFS, with automatic fallback to shutil.copyfileobj.
    Returns a short string describing the strategy used."""
    if _cfr_func is None or file_size == 0:
        shutil.copyfileobj(ifd, ofd, buf_size)
        return "userspace"

    copied = 0
    try:
        while copied < file_size:
            chunk = min(file_size - copied, buf_size)
            n = _cfr_func(ifd.fileno(), ofd.fileno(), chunk)
            if n == 0:
                # EOF earlier than expected (file may have been truncated)
                break
            copied += n
        return "copy_file_range"
    except OSError as e:
        if e.errno not in _CFR_FALLBACK_ERRNOS:
            raise
        # Partial data may already have been written; seek both fds to the
        # same offset and finish with a userspace copy.
        logging.debug(
            f"copy_file_range fell back after {copied} bytes "
            f"(errno {e.errno}: {os.strerror(e.errno)}), "
            f"finishing with userspace copy"
        )
        ofd.seek(copied)
        ifd.seek(copied)
        shutil.copyfileobj(ifd, ofd, buf_size)
        if copied > 0:
            return f"copy_file_range+userspace (fallback at {copied} bytes)"
        return "userspace (copy_file_range unsupported)"


def parse_byte_size(s):
    """Parse a size string: decimal digits plus optional B/K/M/G suffix (binary units)."""
    if isinstance(s, int):
        if s < 0:
            raise argparse.ArgumentTypeError("size must be non-negative")
        return s
    t = str(s).strip()
    if not t:
        raise argparse.ArgumentTypeError("empty size")
    m = re.fullmatch(r"(?i)(\d+)\s*([bkmg])?", t)
    if not m:
        raise argparse.ArgumentTypeError(
            f"invalid size {s!r} (expected e.g. 1024, 1K, 512M, 2G)"
        )
    n = int(m.group(1))
    suf = (m.group(2) or "").lower()
    mult = {"": 1, "b": 1, "k": 1024, "m": 1024**2, "g": 1024**3}[suf]
    return n * mult


def parse_optional_max_size(s):
    """Like parse_byte_size for --max-size; argparse passes None when the flag is omitted."""
    if s is None:
        return None
    return parse_byte_size(s)


def validate_size_bounds(min_size, max_size):
    if max_size is not None and max_size < min_size:
        raise ValueError("--max-size must be greater than or equal to --min-size")


def validate_age_bounds(min_age):
    if min_age <= 0:
        raise ValueError("--min-age must be greater than 0")


def positive_int(value):
    """Argparse type for a strictly positive integer."""
    try:
        n = int(value)
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError(f"invalid positive integer: {value!r}")
    if n <= 0:
        raise argparse.ArgumentTypeError(f"must be a positive integer, got {n}")
    return n


def parse_duration(s):
    """Parse a duration string: digits plus optional s/m/h/d suffix.

    Returns seconds as a float.  Examples: '30s', '5m', '2h', '1d', '3600'.
    """
    t = str(s).strip()
    if not t:
        raise argparse.ArgumentTypeError("empty duration")
    m = re.fullmatch(r"(?i)(\d+(?:\.\d+)?)\s*([smhd])?", t)
    if not m:
        raise argparse.ArgumentTypeError(
            f"invalid duration {s!r} (expected e.g. 60, 30s, 5m, 2h, 1d)"
        )
    n = float(m.group(1))
    suffix = (m.group(2) or "s").lower()
    mult = {"s": 1, "m": 60, "h": 3600, "d": 86400}[suffix]
    result = n * mult
    if result <= 0:
        raise argparse.ArgumentTypeError("duration must be positive")
    return result


class RotatingLogHandler(logging.Handler):
    """A file logging handler that rotates after a line count, time interval,
    or file size.

    File naming: given base path ``app.log``, successive files are named
    ``app.1.log``, ``app.2.log``, etc.  Without an extension (``app``),
    they become ``app.1``, ``app.2``, etc.
    """

    def __init__(self, base_path, max_lines=None, max_seconds=None,
                 max_bytes=None, level=logging.NOTSET):
        super().__init__(level)
        self._base_path = base_path
        stem, ext = os.path.splitext(base_path)
        self._stem = stem
        self._ext = ext  # e.g. ".log" or ""
        self._max_lines = max_lines
        self._max_seconds = max_seconds
        self._max_bytes = max_bytes
        self._file_index = 0
        self._line_count = 0
        self._byte_count = 0
        # RLock so a signal handler that logs while the main thread is inside
        # emit() holding this lock re-enters on the same thread instead of
        # deadlocking (reachable with --log-file + --log-rotate-*).
        self._rotate_lock = threading.RLock()
        self._stream = None
        self._open_time = None
        self._open_file(base_path)

    def _open_file(self, path):
        self._stream = open(path, "a")
        self._line_count = 0
        self._byte_count = 0
        self._open_time = time.monotonic()
        self._current_path = path

    def _make_path(self, index):
        if index == 0:
            return self._base_path
        return f"{self._stem}.{index}{self._ext}"

    def _should_rotate(self):
        if self._max_lines is not None and self._line_count >= self._max_lines:
            return True
        if self._max_seconds is not None:
            elapsed = time.monotonic() - self._open_time
            if elapsed >= self._max_seconds:
                return True
        if self._max_bytes is not None and self._byte_count >= self._max_bytes:
            return True
        return False

    def emit(self, record):
        try:
            msg = self.format(record)
            with self._rotate_lock:
                if self._should_rotate():
                    self._stream.close()
                    self._file_index += 1
                    new_path = self._make_path(self._file_index)
                    self._open_file(new_path)
                data = msg + "\n"
                self._stream.write(data)
                self._stream.flush()
                self._line_count += 1
                self._byte_count += len(data.encode("utf-8"))
        except Exception:
            self.handleError(record)

    def close(self):
        with self._rotate_lock:
            if self._stream:
                self._stream.close()
                self._stream = None
        super().close()


@dataclasses.dataclass
class Stats:
    files_submitted: int = 0
    files_transcoded: int = 0
    files_skipped_recent: int = 0
    files_skipped_changed: int = 0
    files_skipped_layout_match: int = 0
    files_skipped_hardlink: int = 0
    files_skipped_open: int = 0
    files_skipped_small: int = 0
    files_skipped_symlink: int = 0
    files_skipped_large: int = 0
    files_skipped_source_pool: int = 0
    dirs_pruned: int = 0
    subtrees_pruned: int = 0
    bytes_pruned: int = 0
    files_failed: int = 0
    bytes_copied: int = 0
    copy_seconds: float = 0.0
    _symlink_batch: int = 0
    _dirs_pruned_batch: int = 0
    _lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)

    def prune_budget_left(self, budget):
        with self._lock:
            return budget - self.bytes_pruned

    def note_pruned_subtree(self, nbytes):
        with self._lock:
            self.subtrees_pruned += 1
            self.bytes_pruned += nbytes
            return ""

    def log_progress(self):
        avg = self._avg_throughput_str()
        logging.info(
            f"Progress: {self.files_transcoded} transcoded, "
            f"{self.files_failed} failed, "
            f"{self.bytes_copied / (1024**3):.1f} GiB copied"
            f"{avg}"
        )

    def _avg_throughput_str(self):
        """Return a formatted aggregate throughput suffix, or '' if no data."""
        if self.copy_seconds > 0:
            mbps = (self.bytes_copied / (1024**2)) / self.copy_seconds
            return f", avg {mbps:.1f} MiB/s"
        return ""

    def note_skipped_symlink(self, batch_size=1000):
        """Count a skipped non-regular (symlink/etc.) file. Return a log
        message once a full batch of *batch_size* has accumulated (else None),
        so these are logged in batches instead of one line per file."""
        with self._lock:
            self.files_skipped_symlink += 1
            self._symlink_batch += 1
            if self._symlink_batch >= batch_size:
                n = self._symlink_batch
                self._symlink_batch = 0
                return f"Skipped {n} symlinks/non-regular files (total {self.files_skipped_symlink})"
        return None

    def flush_skipped_symlinks(self):
        """Return a log message for any partial (<batch_size) symlink batch."""
        with self._lock:
            if self._symlink_batch > 0:
                n = self._symlink_batch
                self._symlink_batch = 0
                return f"Skipped {n} symlinks/non-regular files (total {self.files_skipped_symlink})"
        return None

    def note_pruned_dir(self, batch_size=100):
        """Count a directory pruned via --prune-dir-regex. Return a log message
        once a full batch of *batch_size* has accumulated (else None), so the
        pruning is observable at INFO level like the symlink-skip batches."""
        with self._lock:
            self.dirs_pruned += 1
            self._dirs_pruned_batch += 1
            if self._dirs_pruned_batch >= batch_size:
                n = self._dirs_pruned_batch
                self._dirs_pruned_batch = 0
                return f"Pruned {n} directories via --prune-dir-regex (total {self.dirs_pruned})"
        return None

    def flush_pruned_dirs(self):
        """Return a log message for any partial (<batch_size) prune batch."""
        with self._lock:
            if self._dirs_pruned_batch > 0:
                n = self._dirs_pruned_batch
                self._dirs_pruned_batch = 0
                return f"Pruned {n} directories via --prune-dir-regex (total {self.dirs_pruned})"
        return None


stats = Stats()


class CephLayout:
    def __init__(self, layout):
        vals = {}
        for s in layout.split():
            k, v = s.split("=", 1)
            vals[k] = v
        self.stripe_unit = int(vals["stripe_unit"])
        self.stripe_count = int(vals["stripe_count"])
        self.object_size = int(vals["object_size"])
        self.pool = vals["pool"]
        self.layout = layout

    @classmethod
    def from_dir(cls, path):
        try:
            return CephLayout(
                os.getxattr(path, "ceph.dir.layout", follow_symlinks=False).decode(
                    "utf-8"
                )
            )
        except OSError as e:
            if e.errno == ENODATA:
                return None
            raise  # Re-raise unexpected errors (EACCES, EIO, etc.)

    @classmethod
    def from_file(cls, path):
        try:
            return CephLayout(
                os.getxattr(path, "ceph.file.layout", follow_symlinks=False).decode(
                    "utf-8"
                )
            )
        except OSError as e:
            if e.errno == ENODATA:
                return None
            raise

    def apply_file(self, path):
        # Set layout fields individually for compatibility with el9 kernel client
        for attr in ("stripe_unit", "stripe_count", "object_size", "pool"):
            os.setxattr(
                path,
                f"ceph.file.layout.{attr}",
                str(getattr(self, attr)).encode("utf-8"),
                follow_symlinks=False,
            )

    def __str__(self):
        return self.layout

    def __eq__(self, other):
        if not isinstance(other, CephLayout):
            return NotImplemented
        return self.layout == other.layout

    def __hash__(self):
        return hash(self.layout)

    def diff(self, other):
        diff = []
        for i in ("stripe_unit", "stripe_count", "object_size", "pool"):
            a = getattr(self, i)
            b = getattr(other, i)
            if a != b:
                diff.append(f"{i}=[{a} -> {b}]")
        return " ".join(diff)


def get_layout_walking_up(path):
    layout = CephLayout.from_dir(path)
    parent = path
    while layout is None and parent != "/":
        parent = os.path.split(parent)[0]
        layout = CephLayout.from_dir(parent)
    return layout


def _prune_inert_warning(args):
    """Message if subtree pruning is configured but cannot fire, else None.

    The mean-size gate compares against min_size/8, so at min_size 0 it can
    never be satisfied. min_size is runtime-mutable via --config, so this is
    re-checked whenever it changes rather than only at startup.
    """
    if getattr(args, "prune_small_subtrees", False) and args.min_size <= 0:
        return (
            "--prune-small-subtrees is set with min-size 0, so the mean-size "
            "test (mean < min-size/8) can never be satisfied and NOTHING will "
            "be pruned. Set min-size to make pruning effective."
        )
    return None


def _recursive_stats(path):
    """(rbytes, rfiles) for a directory from CephFS recursive stats, or (None, None).

    These are maintained by the MDS, so this is one getfattr rather than a walk.
    """
    try:
        rb = int(os.getxattr(path, b"ceph.dir.rbytes"))
        rf = int(os.getxattr(path, b"ceph.dir.rfiles"))
        return rb, rf
    except (OSError, ValueError):
        return None, None


def process_file(args, filepaths, st, layout, file_layout):
    if do_exit.is_set():
        return

    tmp_file = os.path.join(args.tmpdir, uuid.uuid4().hex)

    if len(filepaths) == 1:
        logging.info(
            f"Transcoding {filepaths[0]} [{st.st_size} bytes]: {file_layout.diff(layout)}"
        )
    else:
        logging.info(
            f"Transcoding {filepaths[0]} [{st.st_size} bytes] (+ {len(filepaths) - 1} hardlink(s)): {file_layout.diff(layout)} [{tmp_file}]"
        )

    try:
        with open(tmp_file, "wb") as ofd:
            layout.apply_file(tmp_file)
            with open(filepaths[0], "rb") as ifd:
                with stats._lock:
                    stats.files_submitted += 1

                # Take a shared (read) lock on the source file to prevent
                # concurrent writers from modifying it while we copy.
                try:
                    fcntl.flock(ifd, fcntl.LOCK_SH | fcntl.LOCK_NB)
                except OSError:
                    logging.warning(
                        f"Could not obtain shared lock on {filepaths[0]}, file may be in use — skipping"
                    )
                    with stats._lock:
                        stats.files_skipped_open += 1
                    os.unlink(tmp_file)
                    return
                copy_start = time.monotonic()
                copy_method = _copy_file_data(ifd, ofd, st.st_size, layout.object_size)
                # Flush to disk before we compare stats
                ofd.flush()
                os.fsync(ofd.fileno())
                copy_elapsed = time.monotonic() - copy_start
                # Lock released when ifd is closed

        shutil.copystat(filepaths[0], tmp_file, follow_symlinks=False)
        os.chown(tmp_file, st.st_uid, st.st_gid)

        # The per-copy rate and copy method were here to show whether
        # copy_file_range (server-side copy) was actually faster in practice.
        # It is not measurably so, and the numbers swing 30x on identically
        # sized files because they track OSD and MDS contention rather than
        # the copy path. Dropped as noise.
        #
        # The "[N bytes] in Xs" shape is deliberately unchanged: log mining
        # keys on it, and the archives span years of prior runs. Only the
        # "(Y MiB/s) via <method>" suffix is gone, so old and new logs parse
        # identically.
        if copy_elapsed > 0:
            logging.info(
                f"Copied {filepaths[0]} [{st.st_size} bytes] in {copy_elapsed:.2f}s"
            )
        else:
            logging.info(
                f"Copied {filepaths[0]} [{st.st_size} bytes] in <1ms"
            )

    except Exception:
        # Clean up temp file on any failure during copy
        try:
            os.unlink(tmp_file)
        except OSError:
            pass
        raise

    if args.dry_run or do_exit.is_set():
        os.unlink(tmp_file)
        return

    with _replace_lock_for(filepaths[0]):
        try:
            # Block SIGINT in this thread to reduce the chance of EINTR during
            # the rename sequence.  Note: Python's signal handler runs on the
            # main thread regardless, so this is primarily belt-and-suspenders.
            signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGINT])
            st2 = os.stat(filepaths[0], follow_symlinks=False)
            # Check mtime, ctime, and size for a more robust change-detection
            if (
                st2.st_mtime_ns != st.st_mtime_ns
                or st2.st_ctime_ns != st.st_ctime_ns
                or st2.st_size != st.st_size
            ):
                if st2.st_mtime_ns != st.st_mtime_ns:
                    logging.error(f"... mtime changed")
                elif st2.st_ctime_ns != st.st_ctime_ns:
                    logging.error(f"... ctime changed (metadata-only change?)")
                elif st2.st_size != st.st_size:
                    logging.error(f"... size changed")
                logging.error(
                    f"Failed to replace {filepaths[0]} (+ {len(filepaths) - 1} hardlink(s)): Source file changed"
                )
                os.unlink(tmp_file)
                with stats._lock:
                    stats.files_skipped_changed += 1
                return

            for i, path in enumerate(filepaths):
                parent_path = os.path.split(path)[0]
                parent_st = os.stat(parent_path, follow_symlinks=False)

                if i == 0:
                    logging.info(f"Renaming {tmp_file} -> {path}")
                    os.rename(tmp_file, path)
                else:
                    logging.info(f"Linking {filepaths[0]} -> {path}")
                    os.link(filepaths[0], tmp_file, follow_symlinks=False)
                    os.rename(tmp_file, path)
                os.utime(
                    parent_path,
                    ns=(parent_st.st_atime_ns, parent_st.st_mtime_ns),
                    follow_symlinks=False,
                )

            with stats._lock:
                stats.files_transcoded += 1
                stats.bytes_copied += st.st_size
                stats.copy_seconds += copy_elapsed

        except Exception:
            # If we fail mid-rename, attempt to clean up the temp file
            try:
                os.unlink(tmp_file)
            except OSError:
                pass
            raise
        finally:
            signal.pthread_sigmask(signal.SIG_UNBLOCK, [signal.SIGINT])


def handler(future):
    try:
        future.result()
    except Exception:
        logging.exception("Error processing file in worker thread")
        with stats._lock:
            stats.files_failed += 1
    finally:
        thread_count.release()


def process_dir(args, start_dir, hard_links, executor, mountpoints, dir_layouts):
    def _limit_reached():
        return args.max_files is not None and stats.files_submitted >= args.max_files

    for dirpath, dirnames, filenames in os.walk(start_dir, topdown=True):
        if do_exit.is_set() or _limit_reached():
            return

        # Also poll here, not only in the per-file loop below. A subtree of
        # pure directories -- or one where every file is pruned -- never
        # reaches the per-file poll, so a config edit would go unobserved
        # while the walker descends it. Still mtime-gated, so this is a
        # clock check per directory, not a stat.
        if runtime_config is not None:
            runtime_config.poll(apply_config)

        if dirpath in mountpoints:
            logging.warning(f"Skipping {dirpath}: path is a mountpoint")
            del dirnames[:]
            continue
        if dirpath == args.tmpdir:
            logging.info(f"Skipping {dirpath}: path is the temporary dir")
            del dirnames[:]
            continue

        layout = dir_layouts.get(dirpath, None)
        if layout is None:
            layout = CephLayout.from_dir(dirpath)
            if layout is None:
                layout = dir_layouts.get(os.path.split(dirpath)[0])

        if layout is None:
            layout = get_layout_walking_up(dirpath)

        if layout is None:
            logging.error(f"Could not determine layout for {dirpath}, skipping")
            del dirnames[:]
            continue

        dirnames.sort()
        filenames.sort()
        # Prune subtrees that are not worth walking, using the recursive stats
        # the MDS already maintains. One getfattr answers what would otherwise
        # be a full walk. The rbytes test is the safety property -- it bounds
        # the loss regardless of how the bytes are distributed inside -- and the
        # mean-size test is only an efficiency signal on top of it.
        if getattr(args, "prune_small_subtrees", False) and dirnames:
            keep = []
            for d in dirnames:
                full = os.path.join(dirpath, d)
                rb, rf = _recursive_stats(full)
                if (
                    rb is not None
                    and rf
                    and rb < args.prune_subtree_max_bytes
                    and rb / rf < args.min_size / 8
                    and stats.prune_budget_left(args.prune_budget_bytes) > rb
                ):
                    msg = stats.note_pruned_subtree(rb)
                    logging.info(
                        f"Pruning {full}/: {rf} files / {rb} bytes recursive "
                        f"(mean {rb // rf} B < min-size/8){msg}"
                    )
                else:
                    keep.append(d)
            dirnames[:] = keep
        # Prune (do not descend into or stat) directories whose name matches
        # --prune-dir-regex.  Mutating dirnames in place controls os.walk.
        if getattr(args, "prune_re", None) is not None and dirnames:
            keep = []
            for d in dirnames:
                if args.prune_re.search(d):
                    logging.debug(f"Pruning {os.path.join(dirpath, d)}/ (--prune-dir-regex)")
                    msg = stats.note_pruned_dir()
                    if msg:
                        logging.info(msg)
                else:
                    keep.append(d)
            dirnames[:] = keep
        logging.debug(
            f"Scanning {dirpath} ({layout}): {len(dirnames)} dirs and {len(filenames)} files"
        )
        dir_layouts[dirpath] = layout

        def submit(filepaths, st, file_layout, _layout=layout):
            if do_exit.is_set() or _limit_reached():
                return
            if not thread_count.acquire(cancel=lambda: do_exit.is_set() or _limit_reached()):
                return
            try:
                future = executor.submit(
                    process_file, args, filepaths, st, _layout, file_layout
                )
                future.add_done_callback(handler)
            except Exception:
                thread_count.release()
                raise

        last_progress = time.monotonic()

        for filename in filenames:
            if do_exit.is_set() or _limit_reached():
                return

            # Time-gated mtime check (default 10s). Must not stat per iteration:
            # at full speed the walker runs thousands of iterations a second.
            if runtime_config is not None:
                runtime_config.poll(apply_config)

            delay = file_delay_ms
            if delay > 0:
                time.sleep(delay / 1000.0)

            if time.monotonic() - last_progress > 60:
                stats.log_progress()
                last_progress = time.monotonic()

            filepath = os.path.join(dirpath, filename)
            st = os.stat(filepath, follow_symlinks=False)
            if not stat.S_ISREG(st.st_mode):
                msg = stats.note_skipped_symlink()
                if msg:
                    logging.info(msg)
                continue
            if st.st_nlink == 1 and st.st_size < args.min_size:
                logging.info(
                    f"Skipping {filepath}: size {st.st_size} below --min-size {args.min_size}"
                )
                with stats._lock:
                    stats.files_skipped_small += 1
                continue
            if (
                st.st_nlink == 1
                and args.max_size is not None
                and st.st_size > args.max_size
            ):
                logging.info(
                    f"Skipping {filepath}: size {st.st_size} above --max-size {args.max_size}"
                )
                with stats._lock:
                    stats.files_skipped_large += 1
                continue
            if st.st_mtime > (time.time() - 86400 * min_age_days):
                logging.info(f"Skipping {filepath}: modified too recently")
                with stats._lock:
                    stats.files_skipped_recent += 1
                continue
            file_layout = CephLayout.from_file(filepath)
            if file_layout is None:
                logging.error(f"Could not read layout for {filepath}, skipping")
                with stats._lock:
                    stats.files_failed += 1
                continue
            # if there is a layout match, don't count skipping as a failure
            if file_layout == layout:
                with stats._lock:
                    stats.files_skipped_layout_match += 1
                continue
            # --source-pool restricts the run to files currently in one pool,
            # so a drain (e.g. ec6.3 -> ec4.2) does not also sweep up every
            # file still sitting on the default replicated pool.
            if args.source_pool is not None and file_layout.pool != args.source_pool:
                with stats._lock:
                    stats.files_skipped_source_pool += 1
                continue
            if st.st_nlink == 1:
                submit([filepath], st, file_layout)
            elif not args.process_hardlinks:
                logging.info(
                    f"Skipping {filepath}: has {st.st_nlink} hard links (--skip-hardlinks)"
                )
                with stats._lock:
                    stats.files_skipped_hardlink += 1
                continue
            else:
                file_id = (st.st_dev, st.st_ino)
                if file_id not in hard_links:
                    hard_links[file_id] = ([filepath], [layout])
                else:
                    hard_links[file_id][0].append(filepath)
                    hard_links[file_id][1].append(layout)

                if len(hard_links[file_id][0]) == st.st_nlink:
                    filepaths = hard_links[file_id][0]
                    layouts = hard_links[file_id][1]
                    del hard_links[file_id]
                    if not all(i == layouts[0] for i in layouts[1:]):
                        logging.error(
                            "Hardlinked file has inconsistent directory layouts:"
                        )
                        with stats._lock:
                            stats.files_failed += 1
                        for fp, ly in zip(filepaths, layouts):
                            logging.error(f"  [{ly}]: {fp}")
                    elif st.st_size < args.min_size:
                        logging.info(
                            f"Skipping {filepaths[0]} (+ {len(filepaths) - 1} hardlink(s)): "
                            f"size {st.st_size} below --min-size {args.min_size}"
                        )
                        with stats._lock:
                            stats.files_skipped_small += 1
                    elif args.max_size is not None and st.st_size > args.max_size:
                        logging.info(
                            f"Skipping {filepaths[0]} (+ {len(filepaths) - 1} hardlink(s)): "
                            f"size {st.st_size} above --max-size {args.max_size}"
                        )
                        with stats._lock:
                            stats.files_skipped_large += 1
                    else:
                        submit(filepaths, st, file_layout)
                else:
                    logging.info(
                        f"Deferring {filepath} due to hardlinks ({st.st_nlink - len(hard_links[file_id][0])} link(s) left)"
                    )


def cleanup_tmpdir(tmpdir):
    """Remove any orphaned temp files left by previous interrupted runs."""
    if not os.path.isdir(tmpdir):
        return
    count = 0
    for entry in os.scandir(tmpdir):
        if entry.is_file(follow_symlinks=False):
            try:
                # Only remove files that look like our UUID hex temp files
                uuid.UUID(entry.name)
                os.unlink(entry.path)
                count += 1
            except (ValueError, OSError):
                pass
    if count:
        logging.info(f"Cleaned up {count} orphaned temp file(s) from {tmpdir}")


def process_files(args):
    args.tmpdir = os.path.abspath(args.tmpdir)

    if not os.path.exists(args.tmpdir):
        os.makedirs(args.tmpdir)

    cleanup_tmpdir(args.tmpdir)

    hard_links = {}
    dir_layouts = {}

    mountpoints = set()
    with open("/proc/self/mounts", "r") as f:
        for line in f:
            mountpoints.add(line.split()[1])

    with ThreadPoolExecutor(max_workers=_EXECUTOR_MAX_WORKERS) as executor:
        tmpdir_dev = os.stat(args.tmpdir).st_dev
        for start_dir in args.dirs:
            start_dir = os.path.abspath(start_dir)
            if os.stat(start_dir).st_dev != tmpdir_dev:
                logging.error(
                    f"tmpdir {args.tmpdir} is on a different filesystem than {start_dir}. "
                    f"os.rename() will fail with EXDEV. Aborting."
                )
                sys.exit(1)

            if start_dir in mountpoints:
                mountpoints.remove(start_dir)

            layout = get_layout_walking_up(start_dir)

            if layout is None:
                logging.error(f"Could not determine layout for {start_dir}, skipping")
                continue
            dir_layouts[start_dir] = layout

            logging.info(f"Starting at {start_dir} ({layout})")
            process_dir(args, start_dir, hard_links, executor, mountpoints, dir_layouts)
            if do_exit.is_set():
                break

    if hard_links and not do_exit.is_set():
        logging.warning(
            f"Some hard links could not be located. Refusing to transcode these inodes:"
        )
        for file_id, v in hard_links.items():
            dev, inode = file_id
            try:
                st = os.stat(v[0][0], follow_symlinks=False)
                nlink = st.st_nlink
            except OSError:
                nlink = "?"
            logging.warning(f"  Inode {dev}:{inode} ({len(v[0])}/{nlink} links):")
            for path in v[0]:
                logging.warning(f"    - {path}")


def _amended_cmdline():
    """sys.argv with the live tunables (threads / min-age / file-delay)
    substituted in — i.e. the current effective command line."""
    argv = list(sys.argv)

    def _set(names, val):
        for nm in names:
            # separate form: --flag value
            if nm in argv:
                i = argv.index(nm)
                if i + 1 < len(argv):
                    argv[i + 1] = str(val)
                return
            # joined form: --flag=value
            pref = nm + "="
            for i, tok in enumerate(argv):
                if tok.startswith(pref):
                    argv[i] = f"{nm}={val}"
                    return
        argv.extend([names[0], str(val)])

    if thread_count is not None:
        _set(["--threads"], thread_count.limit)
    _set(["--min-age"], min_age_days)
    _set(["--file-delay"], file_delay_ms)
    return shlex.join(argv)


def _update_proctitle():
    """Reflect the current live tunables in the command line shown by `ps`."""
    if _setproctitle is not None:
        _setproctitle.setproctitle(_amended_cmdline())


def _report_state(prefix="State"):
    """Log the current state/tunables (with the amended command line) and
    refresh the `ps` command line."""
    tc = thread_count.limit if thread_count is not None else "?"
    logging.info(
        f"{prefix}: threads(limit)={tc}, file_delay={file_delay_ms}ms, "
        f"min_age={min_age_days}d | cmdline: {_amended_cmdline()}"
    )
    _update_proctitle()


def main():
    global thread_count
    parser = argparse.ArgumentParser(
        description="Transcode cephfs files to their directory layout",
        epilog=(
            "runtime signals:\n"
            "  SIGUSR1  (10)  increase thread count by 1 (resumes from pause)\n"
            "  SIGUSR2  (12)  decrease thread count by 1 (0 = pause)\n"
            "  SIGTSTP  (20)  throttle to 1 thread (Ctrl+Z)\n"
            f"  SIGRTMIN (34)  increase file delay x{DELAY_STEP_UP}"
            f" (min +1ms, cap {DELAY_MAX_MS}ms)\n"
            f"  SIGRTMIN+1(35) decrease file delay /{DELAY_STEP_DOWN}"
            f" (min -1ms, floor {DELAY_MIN_MS}ms)\n"
            "  SIGRTMIN+2(36) increase min-age by 3 days\n"
            "  SIGRTMIN+3(37) decrease min-age by 3 days (min 1)\n"
            "  SIGRTMIN+4(38) dump current state/tunables to the log"
            "\n\n"
            "example --config file (every key at its default):\n\n"
            # argparse runs the epilog through `text % dict(prog=...)`, so any
            # literal % arriving from config_example() is read as a format spec
            # and raises. Double them here rather than banning % from the
            # config comments; our own %(prog)s below is added after this and
            # is meant to be substituted.
            + "\n".join("    " + l for l in config_example().splitlines()).replace("%", "%%")
            + "\n\n    redirect it to disk with:  %(prog)s --print-config-example > "
            + config_path_for("VOLUME")
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {_VERSION}"
    )
    parser.add_argument("dirs", help="Directories to scan", nargs="*")
    parser.add_argument(
        "--tmpdir",
        default="/data/tmp",
        help="Temporary directory to which to copy files.\nImportant: This directory should have its layout set to\nthe *default* data pool for the FS, to avoid excess backtrace objects.",
    )
    parser.add_argument(
        "--process-hardlinks",
        action="store_true",
        default=False,
        help="Process files with nlink > 1, which is potentially dangerous",
    )
    parser.add_argument("--debug", "-d", action="store_true")
    parser.add_argument(
        "--min-age",
        default=1,
        type=int,
        help="Minimum age of file before transcoding, in days (adjustable at runtime via SIGRTMIN+2/SIGRTMIN+3)",
    )
    parser.add_argument(
        "--min-size",
        default=parse_byte_size("0"),
        type=parse_byte_size,
        metavar="SIZE",
        help="Skip files smaller than this size. Suffix B/K/M/G (binary); plain number means bytes. 0 disables.",
    )
    parser.add_argument(
        "--max-size",
        default=None,
        type=parse_optional_max_size,
        metavar="SIZE",
        help="Skip files larger than this size (same format as --min-size). Omit for no upper limit.",
    )
    parser.add_argument(
        "--threads",
        default=4,
        type=int,
        help="Number of threads for data copying",
    )
    parser.add_argument(
        "--dry-run",
        "-n",
        action="store_true",
        help="Perform transcode but do not replace files",
    )
    parser.add_argument(
        "--config",
        default=None,
        metavar="PATH",
        help="key=value file of live tunables (file_delay_ms, threads, min_age_days, "
             "min_size, prune_dir_regex), re-read when its mtime changes. Keep it on "
             "local disk, NOT in the CephFS volume: a stat there is an MDS round trip.",
    )
    parser.add_argument(
        "--print-config-example",
        metavar="VOLUME",
        nargs="?",
        const="VOLUME",
        help="print a fully-populated --config file for VOLUME and exit",
    )
    parser.add_argument(
        "--config-poll-seconds",
        type=float,
        default=10.0,
        metavar="SEC",
        help="how often to stat --config for changes (default: 10)",
    )
    parser.add_argument(
        "--force-tmpdir-pool",
        action="store_true",
        help="proceed even if the tmpdir is in a target pool. Disables the only "
             "check that makes a failed layout application visible; do not use "
             "routinely.",
    )
    parser.add_argument("--log-file", help="Also log to this file")
    parser.add_argument(
        "--log-rotate-lines",
        type=positive_int,
        default=None,
        help="Rotate the log file after this many lines (requires --log-file)",
    )
    parser.add_argument(
        "--log-rotate-time",
        type=parse_duration,
        default=None,
        help="Rotate the log file after this duration, e.g. 30m, 2h, 1d (requires --log-file)",
    )
    parser.add_argument(
        "--log-rotate-size",
        type=float,
        default=None,
        metavar="GIB",
        help="Rotate the log file when it reaches this size in GiB (requires --log-file)",
    )
    parser.add_argument(
        "--no-copy-file-range",
        dest="no_copy_file_range",
        action="store_true",
        default=True,
        help="Disable use of copy_file_range and always use userspace copy. "
             "DEFAULT since 2026-09-05: a 15-thread A/B on a production "
             "volume measured "
             "copy_file_range no faster overall (154.0 vs 170.1 MiB/s) and "
             "1.3-2.1x SLOWER for files under 1 MiB, which is the size range "
             "these jobs now work in.",
    )
    parser.add_argument(
        "--copy-file-range",
        dest="no_copy_file_range",
        action="store_false",
        help="Opt back in to copy_file_range (server-side copy when CephFS "
             "supports it). Off by default; see --no-copy-file-range.",
    )
    parser.add_argument(
        "--source-pool",
        default=None,
        metavar="POOL",
        help="Only transcode files whose CURRENT data pool is POOL. Without it, "
             "every file not already on the target pool is eligible. Use this to "
             "drain one pool into another without also sweeping the default pool.",
    )
    parser.add_argument(
        "--prune-small-subtrees",
        action="store_true",
        default=False,
        help="Skip whole subtrees whose recursive size makes them not worth "
             "walking, using the MDS's own ceph.dir.rbytes/rfiles (one getfattr "
             "per directory, no walk). OFF by default: it changes what the pass "
             "covers, which should always be deliberate. INERT without --min-size, "
             "since the mean-size test compares against min-size/8; a warning is "
             "logged at startup if you pass this with --min-size 0.",
    )
    parser.add_argument(
        "--prune-subtree-max-bytes",
        type=int,
        default=1 << 30,
        metavar="BYTES",
        help="With --prune-small-subtrees, only prune a subtree whose TOTAL "
             "recursive bytes are below this (default 1 GiB). This is a bound on "
             "what pruning can cost you: whatever the size distribution inside, "
             "skipping the subtree forgoes at most this many bytes.",
    )
    parser.add_argument(
        "--prune-budget-bytes",
        type=int,
        default=100 << 30,
        metavar="BYTES",
        help="With --prune-small-subtrees, stop pruning once this many bytes have "
             "been skipped in total (default 100 GiB). Deliberately finite: the "
             "per-subtree bound above says nothing about the aggregate, so without "
             "this a pass could skip unbounded data a gigabyte at a time.",
    )
    parser.add_argument(
        "--max-files",
        type=positive_int,
        default=None,
        help="Stop after submitting this many files for transcoding",
    )
    parser.add_argument(
        "--file-delay",
        type=int,
        default=0,
        metavar="MS",
        help="Delay in milliseconds before statting each new file (adjustable at runtime via SIGRTMIN/SIGRTMIN+1)",
    )
    parser.add_argument(
        "--prune-dir-regex",
        default=None,
        metavar="REGEX",
        help="Regular expression (unanchored, via re.search) matched against "
        "directory NAMES; any name containing a match is pruned from the walk "
        "entirely (not descended into or statted). Anchor with ^ / $ for exact "
        "names, e.g. '\\.runfiles$' to skip Bazel runfiles symlink farms. "
        "Overrides the built-in default set (see DEFAULT_PRUNE_DIRS); pass an "
        "empty string to disable pruning entirely.",
    )

    args = parser.parse_args()

    # Emit an example config and exit, before any argument that only matters to
    # a real run is validated -- this is a documentation command, not a run.
    if args.print_config_example:
        print(config_example(args.print_config_example))
        return 0

    if not args.dirs:
        parser.error("the following arguments are required: dirs")

    try:
        validate_size_bounds(args.min_size, args.max_size)
    except ValueError as e:
        parser.error(str(e))

    try:
        validate_age_bounds(args.min_age)
    except ValueError as e:
        parser.error(str(e))

    thread_count = DynamicSemaphore(args.threads)

    # None means "not given" -> apply the default set. An explicitly empty
    # string means "no pruning", and must stay distinguishable from not-given.
    _prune_defaulted = args.prune_dir_regex is None
    if _prune_defaulted:
        args.prune_dir_regex = DEFAULT_PRUNE_REGEX
    args.prune_re = None
    if args.prune_dir_regex:
        try:
            args.prune_re = re.compile(args.prune_dir_regex)
        except re.error as e:
            parser.error(f"--prune-dir-regex invalid regex: {e}")

    has_rotation = (
        args.log_rotate_lines is not None
        or args.log_rotate_time is not None
        or args.log_rotate_size is not None
    )
    if has_rotation and not args.log_file:
        parser.error("--log-rotate-lines, --log-rotate-time, and --log-rotate-size require --log-file")
    if args.log_rotate_size is not None and args.log_rotate_size <= 0:
        parser.error("--log-rotate-size must be a positive number")

    log_level = logging.DEBUG if args.debug else logging.INFO
    log_handlers = [logging.StreamHandler()]
    if args.log_file:
        if has_rotation:
            max_bytes = int(args.log_rotate_size * 1024**3) if args.log_rotate_size is not None else None
            log_handlers.append(
                RotatingLogHandler(
                    args.log_file,
                    max_lines=args.log_rotate_lines,
                    max_seconds=args.log_rotate_time,
                    max_bytes=max_bytes,
                )
            )
        else:
            log_handlers.append(logging.FileHandler(args.log_file))
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=log_handlers,
    )
    cmdline = shlex.join(sys.argv)
    logging.info(f"Starting: {cmdline}")

    if has_rotation:
        parts = []
        if args.log_rotate_lines is not None:
            parts.append(f"{args.log_rotate_lines} lines")
        if args.log_rotate_time is not None:
            parts.append(f"{args.log_rotate_time:.0f}s")
        if args.log_rotate_size is not None:
            parts.append(f"{args.log_rotate_size} GiB")
        logging.info(f"Log rotation enabled: every {' or '.join(parts)}")

    if os.geteuid() != 0:
        logging.error("This tool must be run as root (requires chown).")
        sys.exit(1)

    global _cfr_func, _cfr_label
    if args.no_copy_file_range:
        _cfr_func, _cfr_label = None, None

    _inert = _prune_inert_warning(args)
    if _inert:
        logging.warning(_inert)

    if _cfr_label:
        logging.info(f"Using copy_file_range via {_cfr_label} (server-side copy when supported by CephFS)")
    elif args.no_copy_file_range:
        logging.info(
            "copy_file_range disabled (default; pass --copy-file-range to enable), "
            "using userspace copy"
        )
    else:
        logging.info("copy_file_range not available, using userspace copy")

    layout = get_layout_walking_up(args.tmpdir)

    if args.max_files is not None:
        logging.info(f"Will stop after transcoding {args.max_files} file(s)")

    if layout is None:
        logging.error(
            f"Could not determine layout for tmpdir {args.tmpdir}. Is this a CephFS mount?"
        )
        sys.exit(1)

    logging.info(f"Temporary directory is {args.tmpdir} with pool {layout.pool}")

    # State the effective prune set before any walking happens. When it is the
    # built-in default the operator has not chosen it, so name every directory
    # rather than printing an opaque regex -- this silently skips whole
    # subtrees, and "why did it not transcode X" is otherwise hard to answer
    # from the log.
    if args.prune_re is None:
        logging.warning("Directory pruning DISABLED; every directory will be walked")
    elif _prune_defaulted:
        logging.warning(
            "Pruning these directory names by default (override with "
            "--prune-dir-regex, disable with --prune-dir-regex ''): %s",
            " ".join(DEFAULT_PRUNE_DIRS))
    else:
        logging.warning("Pruning directory names matching: %s", args.prune_dir_regex)

    # The tmpdir must not sit in a pool we are transcoding *into*.
    #
    # Two distinct failures if it does. First, every file whose target is that
    # pool becomes a no-op that still pays a full data rewrite -- the exact
    # churn that burned 41 TiB of already-EC data in one incident. Second, and
    # worse, a silently failed apply_file() stops being detectable: the temp file
    # inherits the target pool from the tmpdir, so a broken layout call still
    # produces a correct-looking result and the bug ships.
    #
    # This was previously an interactive "Proceed? [y/N]" prompt. A prompt
    # cannot be answered by a detached or scripted start -- input() raises
    # EOFError and the job aborts -- so the check is done programmatically
    # instead. That works headless AND is stronger, because it cannot be
    # waved through by a human who is not reading carefully.
    conflicts = []
    for d in args.dirs:
        target = get_layout_walking_up(d)
        if target is not None and target.pool == layout.pool:
            conflicts.append((d, target.pool))

    if conflicts:
        for d, pool in conflicts:
            logging.error(
                f"tmpdir {args.tmpdir} is in pool {pool}, which is also the "
                f"target pool for {d}."
            )
        logging.error(
            "The tmpdir must be the filesystem's DEFAULT data pool, never a "
            "target pool. Fix with: setfattr -n ceph.dir.layout.pool "
            f"-v <default_data_pool> {args.tmpdir}"
        )
        if not args.force_tmpdir_pool:
            logging.error("Aborting. Pass --force-tmpdir-pool if this is deliberate.")
            sys.exit(1)
        logging.warning(
            "--force-tmpdir-pool given; continuing despite the pool conflict. "
            "Layout-application failures will NOT be detectable in this run."
        )

    def signal_handler(sig, frame):
        name = signal.Signals(sig).name
        logging.error(f"{name} received, exiting cleanly...")
        do_exit.set()

    def sigtstp_handler(sig, frame):
        old = thread_count.limit
        if old != 1:
            thread_count.set_limit(1)
            logging.info(f"SIGTSTP received, thread limit: {old} -> 1")
        else:
            logging.info(f"SIGTSTP received, already at 1")
        _report_state()

    def sigusr1_handler(sig, frame):
        old = thread_count.limit
        new = min(old + 1, _EXECUTOR_MAX_WORKERS)
        if new != old:
            thread_count.set_limit(new)
            if old == 0:
                logging.info(f"SIGUSR1 received, processing resumed (thread limit: 0 -> {new})")
            else:
                logging.info(f"SIGUSR1 received, thread limit: {old} -> {new}")
        else:
            logging.warning(f"SIGUSR1 received, already at maximum ({_EXECUTOR_MAX_WORKERS})")
        _report_state()

    def sigusr2_handler(sig, frame):
        old = thread_count.limit
        new = max(old - 1, 0)
        if new != old:
            thread_count.set_limit(new)
            if new == 0:
                logging.info(
                    f"SIGUSR2 received, thread limit: {old} -> 0 — "
                    f"processing paused (in-flight copies will complete; send SIGUSR1 to resume)"
                )
            else:
                logging.info(f"SIGUSR2 received, thread limit: {old} -> {new}")
        else:
            logging.warning(f"SIGUSR2 received, already paused (thread limit 0; send SIGUSR1 to resume)")
        _report_state()

    global file_delay_ms, min_age_days
    if args.file_delay < 0:
        parser.error("--file-delay must be non-negative")
    file_delay_ms = args.file_delay
    min_age_days = args.min_age

    def sigrtmin_handler(sig, frame):
        global file_delay_ms
        old = file_delay_ms
        file_delay_ms = _delay_up(old)
        logging.info(f"SIGRTMIN received, file delay: {old}ms -> {file_delay_ms}ms")
        _report_state()

    def sigrtmin1_handler(sig, frame):
        global file_delay_ms
        old = file_delay_ms
        file_delay_ms = _delay_down(old)
        logging.info(f"SIGRTMIN+1 received, file delay: {old}ms -> {file_delay_ms}ms")
        _report_state()

    def sigrtmin2_handler(sig, frame):
        global min_age_days
        old = min_age_days
        min_age_days = old + 3
        logging.info(f"SIGRTMIN+2 received, min-age: {old}d -> {min_age_days}d")
        _report_state()

    def sigrtmin3_handler(sig, frame):
        global min_age_days
        old = min_age_days
        min_age_days = max(old - 3, 1)
        logging.info(f"SIGRTMIN+3 received, min-age: {old}d -> {min_age_days}d")
        _report_state()

    def sigrtmin4_handler(sig, frame):
        # On-demand full dump of current runtime state / tunables to the log.
        _report_state("State dump (SIGRTMIN+4)")

    # Signal-handler safety: the handlers below log and adjust tunables, and
    # Python delivers signals on the main thread — the same thread that, on its
    # hot path, holds DynamicSemaphore._cond (in acquire/set_limit) and the log
    # handler's _rotate_lock (in emit()). Both are RLocks (see each class's
    # __init__), so a handler that interrupts such a critical section re-enters
    # the lock on the same thread instead of deadlocking; setproctitle is a
    # bounded argv write.
    def _apply_config(cfg):
        """Apply a parsed config dict to live state, logging only what changes.

        Every applied change is logged with old and new values so the question
        "what was this job doing at 14:02?" is answerable from this log alone.
        """
        global file_delay_ms, min_age_days
        if 'file_delay_ms' in cfg and cfg['file_delay_ms'] != file_delay_ms:
            old_v = file_delay_ms
            file_delay_ms = cfg['file_delay_ms']
            logging.info("Config: file delay %dms -> %dms", old_v, file_delay_ms)
        if 'threads' in cfg and cfg['threads'] != thread_count.limit:
            old_v = thread_count.limit
            thread_count.set_limit(cfg['threads'])
            # Match the SIGUSR2/SIGUSR1 wording so a log scan finds a pause the
            # same way regardless of which interface caused it.
            if cfg['threads'] == 0:
                logging.info(
                    "Config: thread limit %d -> 0 — processing paused "
                    "(in-flight copies will complete; set threads > 0 to resume)",
                    old_v)
            elif old_v == 0:
                logging.info(
                    "Config: processing resumed (thread limit: 0 -> %d)",
                    cfg['threads'])
            else:
                logging.info("Config: thread limit %d -> %d", old_v, cfg['threads'])
        global DELAY_STEP_UP, DELAY_STEP_DOWN, DELAY_MIN_MS
        if 'delay_step_up' in cfg and cfg['delay_step_up'] != DELAY_STEP_UP:
            old_v = DELAY_STEP_UP
            DELAY_STEP_UP = cfg['delay_step_up']
            logging.info("Config: delay step up x%.3f -> x%.3f", old_v, DELAY_STEP_UP)
        if 'delay_step_down' in cfg and cfg['delay_step_down'] != DELAY_STEP_DOWN:
            old_v = DELAY_STEP_DOWN
            DELAY_STEP_DOWN = cfg['delay_step_down']
            logging.info("Config: delay step down /%.3f -> /%.3f", old_v, DELAY_STEP_DOWN)
        if 'delay_min_ms' in cfg and cfg['delay_min_ms'] != DELAY_MIN_MS:
            old_v = DELAY_MIN_MS
            DELAY_MIN_MS = cfg['delay_min_ms']
            logging.info("Config: delay floor %dms -> %dms", old_v, DELAY_MIN_MS)
        if 'min_age_days' in cfg and cfg['min_age_days'] != min_age_days:
            old_v = min_age_days
            min_age_days = cfg['min_age_days']
            logging.info("Config: min-age %dd -> %dd", old_v, min_age_days)
        if 'min_size' in cfg and cfg['min_size'] != args.min_size:
            old_v = args.min_size
            args.min_size = cfg['min_size']
            logging.info("Config: min-size %d -> %d bytes", old_v, args.min_size)
            # min_size feeds the subtree-prune gate, so a runtime change can
            # switch pruning between inert and active. Say so, or the startup
            # warning silently stops being true mid-run.
            if getattr(args, "prune_small_subtrees", False):
                msg = _prune_inert_warning(args)
                if msg:
                    logging.warning("Config: %s", msg)
                elif old_v <= 0:
                    logging.info(
                        "Config: subtree pruning is now ACTIVE (min-size is no "
                        "longer 0); subtrees under %d bytes averaging below %d "
                        "bytes/file may be skipped, budget %d bytes",
                        args.prune_subtree_max_bytes,
                        args.min_size // 8,
                        args.prune_budget_bytes,
                    )
        for _k, _label in (('prune_subtree_max_bytes', 'prune subtree ceiling'),
                           ('prune_budget_bytes', 'prune budget')):
            if _k in cfg and cfg[_k] != getattr(args, _k):
                _old = getattr(args, _k)
                setattr(args, _k, cfg[_k])
                logging.info("Config: %s %d -> %d bytes", _label, _old, cfg[_k])
        if 'prune_dir_regex' in cfg:
            old_p = args.prune_re.pattern if args.prune_re else None
            new_p = cfg['prune_dir_regex'].pattern if cfg['prune_dir_regex'] else None
            if old_p != new_p:
                # os.walk(topdown=True) lets dirnames be mutated, so a new
                # pattern takes effect at the next directory descent.
                args.prune_re = cfg['prune_dir_regex']
                logging.info("Config: prune-dir-regex %r -> %r", old_p, new_p)
        _report_state("Config reloaded")

    global runtime_config, apply_config
    if args.config:
        apply_config = _apply_config
        runtime_config = RuntimeConfig(args.config, args.config_poll_seconds)
        logging.info("Runtime config: %s (polled every %.0fs)",
                     args.config, args.config_poll_seconds)
        runtime_config.poll(apply_config)     # apply once at startup

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTSTP, sigtstp_handler)
    signal.signal(signal.SIGUSR1, sigusr1_handler)
    signal.signal(signal.SIGUSR2, sigusr2_handler)
    signal.signal(signal.SIGRTMIN, sigrtmin_handler)
    signal.signal(signal.SIGRTMIN + 1, sigrtmin1_handler)
    signal.signal(signal.SIGRTMIN + 2, sigrtmin2_handler)
    signal.signal(signal.SIGRTMIN + 3, sigrtmin3_handler)
    signal.signal(signal.SIGRTMIN + 4, sigrtmin4_handler)

    logging.info(
        f"PID {os.getpid()}: "
        f"SIGUSR1/{signal.SIGUSR1} +1 thread, "
        f"SIGUSR2/{signal.SIGUSR2} -1 thread (0 = pause), "
        f"SIGTSTP (Ctrl+Z) throttle to 1, "
        f"SIGRTMIN/{signal.SIGRTMIN} / SIGRTMIN+1/{signal.SIGRTMIN + 1} adjust file delay x{DELAY_STEP_UP} up / /{DELAY_STEP_DOWN} down (floor {DELAY_MIN_MS}ms), "
        f"SIGRTMIN+2/{signal.SIGRTMIN + 2} / SIGRTMIN+3/{signal.SIGRTMIN + 3} adjust min-age ±3d (min 1), "
        f"SIGRTMIN+4/{signal.SIGRTMIN + 4} dump state to log"
    )
    if file_delay_ms > 0:
        logging.info(f"File delay: {file_delay_ms}ms")

    # Make the degraded (no-op) mode observable rather than a silent surprise.
    logging.info(
        "ps command-line live-update: "
        + ("enabled" if _setproctitle is not None
           else "disabled (install python3-setproctitle to enable)")
    )
    # Prime the ps command line with the launch-time tunables.
    _update_proctitle()

    process_files(args)

    if args.max_files is not None and stats.files_submitted >= args.max_files:
        logging.info(f"Stopped early: --max-files limit of {args.max_files} reached")

    msg = stats.flush_skipped_symlinks()
    if msg:
        logging.info(msg)
    msg = stats.flush_pruned_dirs()
    if msg:
        logging.info(msg)

    logging.info(
        f"Complete: {stats.files_transcoded} transcoded, "
        f"{stats.files_failed} failed, "
        f"{stats.files_skipped_layout_match} already matched, "
        f"{stats.files_skipped_source_pool} wrong source pool, "
        f"{stats.subtrees_pruned} subtrees pruned ({stats.bytes_pruned} bytes), "
        f"{stats.files_skipped_recent} too recent, "
        f"{stats.files_skipped_changed} changed during processing, "
        f"{stats.files_skipped_hardlink} hardlinks skipped, "
        f"{stats.files_skipped_open} open/locked, "
        f"{stats.files_skipped_small} below min-size, "
        f"{stats.files_skipped_symlink} symlinks/non-regular, "
        f"{stats.files_skipped_large} above max-size, "
        f"{stats.dirs_pruned} dirs pruned, "
        f"{stats.bytes_copied / (1024**3):.1f} GiB copied"
        f"{stats._avg_throughput_str()}"
    )
    logging.info(f"Finished: {cmdline}")


if __name__ == "__main__":
    main()
