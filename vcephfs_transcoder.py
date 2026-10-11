#!/usr/bin/env python3
# CephFS pool/layout migration tool ("transcoder")
#
# Loosely inspired by:
# https://git.sr.ht/~pjjw/cephfs-layout-tool/tree/master/item/cephfs_layout_tool/migrate_pools.py
# https://gist.github.com/ervwalter/5ff6632c930c27a1eb6b07c986d7439b
#
# MIT license (https://opensource.org/license/mit)

import errno, json, shlex, urllib.parse, urllib.request
import os, re, stat, time, signal, shutil, logging, sys, fcntl, dataclasses
from concurrent.futures import ThreadPoolExecutor
import threading, uuid, argparse, heapq, tempfile, hashlib, select, subprocess

_VERSION = "2112"

# Replacing a file must be serialized against another worker replacing the
# SAME file -- that is the only invariant here. A single global lock would
# serialize every *unrelated* file too, capping effective concurrency at one
# regardless of --threads (0.96 on a 15-thread job). Stripe by a hash
# of the path: equal paths always map to the same stripe, so the per-path
# guarantee holds, while unrelated paths proceed in parallel.
_REPLACE_LOCK_STRIPES = 64
_replace_locks = [threading.Lock() for _ in range(_REPLACE_LOCK_STRIPES)]


def _replace_lock_for(path):
    return _replace_locks[hash(path) % _REPLACE_LOCK_STRIPES]
do_exit = threading.Event()
run_journal = None
thread_count = None
file_delay_ms = 0
min_age_days = 1

# --- staging: the temp copy goes beside its target, not in a shared tmpdir ---
#
# Where the temp is made sets the cost of the final rename (one client, same
# file size, 12 trials each):
#
#     A. same directory (deep in the tree)     mean    0.3 ms  p50 0.3  max     0.4
#     B. tmpdir -> deep dir                    mean 1266.7 ms  p50 8.2  max 14914.7
#     C. sibling dir -> deep dir               mean  387.9 ms  p50 0.3  max  4637.0
#
# B's tail is a cross-rank rename: with distributed=1 export pinning the tmpdir
# and the destination hash to different MDS ranks, so every replace is a
# two-phase commit. So the temp goes in its target's own directory (case A);
# since it then inherits the target's layout, _apply_and_verify_layout() reads
# the layout back to catch a failed apply_file(). --stage-in-tmpdir is case B.

# --- what transcoding costs in objects, and where it lands -------------------
#
# The FIRST data pool (data_pools[0]) is fixed when the fs is created; a
# directory's DEFAULT pool is its ceph.dir.layout.pool, settable anywhere.
# Pointing the root at an EC pool changes where new files land, not the first
# pool. --target-pool overrides the target per run (see below).
#
# A backtrace object lives in the first pool, the file's current pool, and every
# pool in its old_pools, so switching a file's layout after creation leaves one
# behind in the old pool for good. A file outside the first pool costs two
# objects: its data, and a zero-byte object in the first pool (a ~300-byte
# `parent` xattr, no omap). A transcode makes a new inode and renames it over
# the old one, so a non-first source pool can be emptied; the first never can.
#
# The cost is object count, not capacity (BlueStore metadata is 0.36% of used
# space across 980 OSDs): it drives scrub, recovery, peering and onode cache
# pressure. Widening EC from 4+2 to 8+2 takes shards per object from 6 to 10.

# --- multiplicative delay stepping -------------------------------------------
# The delay knob is hyperbolic (+100ms is 5% at 2000ms, 100x at 100ms), so the
# signals step it by a ratio, set separately per direction so backing off can
# be coarser than recovering. Integer ms: 1ms is already ~1000 files/s.
DELAY_STEP_UP = 1.25
DELAY_STEP_DOWN = 1.25
DELAY_MAX_MS = 600000

# Regulator defaults. Defined here, above config_example(), so the example
# file can interpolate them rather than retyping values that would drift.
REG_PAUSE_MS_DEFAULT = 150.0
REG_SLO_MS_DEFAULT = 75.0
REG_PERIOD_S_DEFAULT = 30
REG_FLOOR_MS_DEFAULT = 0
REG_QUIET_TICKS_DEFAULT = 10
# Thread adaptivity is OPT-IN: 0 means the regulator never changes the thread
# count except for the existing pause/resume. Set it to the most threads the
# volume may use and the regulator will climb toward it one step at a time.
REG_MAX_THREADS_DEFAULT = 0
# How long a REGULATOR pause may go without a usable sample before it resumes
# blind at one thread. 0 holds the pause until a sample arrives, which on a
# Prometheus outage is forever.
REG_BLIND_RESUME_S_DEFAULT = 1800
# How long the regulator may run a job (not paused) with no usable sample
# before it throttles the job itself. Holding the last settings is right for a
# short monitoring blip, but a Prometheus outage can last hours, and a job left
# at whatever it had earned -- as many threads as its ceiling -- runs with
# nothing watching MDS latency. 0 holds the last settings forever.
REG_BLIND_THROTTLE_S_DEFAULT = 600
# The file delay a blind throttle raises the job to (threads drop to 1). The
# walker sleeps this per file encountered, so it bounds the stat rate too.
REG_BLIND_DELAY_MS_DEFAULT = 20
# Floor for the DOWN direction. 0 allows a step all the way to unthrottled;
# set it in the config to guarantee the signal path can
# never produce an unbounded stat rate on a live filesystem.
DELAY_MIN_MS = 0

# Where per-volume config files conventionally live: local disk, never in the
# CephFS volume being walked.
CONFIG_DIR = os.path.expanduser("~")

# Directory names pruned from the walk unless --prune-dir-regex says otherwise
# ('' disables). Each is machine-generated and almost all files below any
# plausible --min-size, so walking one costs MDS stats and finds nothing (on
# production the largest of 585,309 virtualenv files is 503,423 bytes).
# Anchored to whole names: 'venv' prunes venv, not conventions.
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
regulator = None
# True once start_regulator() has run, whether or not it started a regulator.
# The regulator is decided once, so a later change to any regulate_* key is
# only meaningful to warn about after that point -- and it matters most when the
# run came up unregulated, where regulator stays None.
regulator_started = False


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
        "# --- self-regulation ----------------------------------------------------",
        "# Throttle against what the filesystem's OTHER clients experience, which",
        "# this job cannot observe from its own copy latency. Entirely optional:",
        "# leave the URL empty and the regulator never starts, though the job",
        "# then logs that at WARNING unless it was run with --no-regulate.",
        "#",
        "# The query is a complete PromQL expression and must evaluate to exactly",
        "# one series whose value is MILLISECONDS. Nothing here assumes Ceph, or",
        "# any particular exporter. {volume} is substituted with this filesystem's",
        "# name (regex-escaped); omit it and the query is used verbatim.",
        "#",
        "# Units are checked once at startup and called out, because seconds means",
        "# it never triggers and microseconds means it never stops -- both silent.",
        "#",
        "# Backslashes must be DOUBLED. PromQL string literals use Go escaping, so",
        "# a regex dot is \\\\. inside the quotes; a single backslash is a parse",
        "# error (HTTP 400: unknown escape sequence), not a wrong match. Example:",
        "#   A window SHORTER than this volume's request interval yields 0/0",
        "#   = nan: a quiet filesystem has no requests in the window and there",
        "#   is nothing to average. The regulator now starts anyway and retries,",
        "#   but it cannot regulate until the query returns a number. 5m copes",
        "#   with a volume served a few requests a minute; 1m does not.",
        "#   regulate_query = 1e3 * sum(increase(mds_lat_sum{n=~\"mds\\\\.{volume}\\\\..*\"}[5m]))",
        "#                        / sum(increase(mds_lat_count{n=~\"mds\\\\.{volume}\\\\..*\"}[5m]))",
        "regulate_prometheus_url =",
        "regulate_query  =",
        "",
        "# Pause above this. The soft target is what it eases back toward; it does",
        "# not gate the pause.",
        "regulate_pause_ms   = %s" % REG_PAUSE_MS_DEFAULT,
        "regulate_slo_ms     = %s" % REG_SLO_MS_DEFAULT,
        "",
        "# Poll period, and the delay floor the regulator decays back down to",
        "# after a quiet spell. Repeated pauses ratchet the floor UP; quiet time",
        "# releases it, but never below this baseline.",
        "regulate_period_s   = %d" % REG_PERIOD_S_DEFAULT,
        "regulate_floor_ms   = %d" % REG_FLOOR_MS_DEFAULT,
        "",
        "# Clean samples in a row before easing the delay. Also how many nan",
        "# samples a regulator pause needs before it resumes at 1 thread: nan",
        "# means no MDS request completed, which a quiet volume and a stalled",
        "# MDS look the same.",
        "regulate_quiet_ticks = %d" % REG_QUIET_TICKS_DEFAULT,
        "",
        "# Thread adaptivity, opt-in. 0 leaves the thread count exactly where",
        "# --threads put it. Set it to a ceiling and the regulator adds one",
        "# thread per quiet interval, but only once the delay has already",
        "# decayed to its floor -- delay is the cheaper knob, so it is spent",
        "# first. A pause lowers an internal ceiling below the level that",
        "# caused it, so the climb does not simply repeat.",
        "regulate_max_threads = %d" % REG_MAX_THREADS_DEFAULT,
        "",
        "# A regulator pause with no usable sample (Prometheus down, empty",
        "# result, nan, or a mix) for this long resumes at 1 thread instead",
        "# of holding 0 forever; the first usable sample restores the rest.",
        "# 0 holds until a sample arrives.",
        "regulate_blind_resume_s = %d" % REG_BLIND_RESUME_S_DEFAULT,
        "",
        "# A running job with no usable sample for this long throttles",
        "# itself to 1 thread and at least regulate_blind_delay_ms per file,",
        "# instead of holding whatever it had earned while nothing watches the",
        "# MDS. A usable sample hands control back. 0 holds forever.",
        "regulate_blind_throttle_s = %d" % REG_BLIND_THROTTLE_S_DEFAULT,
        "regulate_blind_delay_ms = %d" % REG_BLIND_DELAY_MS_DEFAULT,
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

    Applied only on a change, so a signal's adjustment lasts until the next
    edit. A bad value is rejected and the previous one kept. Keep the file on
    local disk: a stat on CephFS is the MDS load this exists to bound.
    """

    # Where _apply_config() puts each live tunable; a new key must go where its
    # reader looks, or it is silently ignored:
    #     file_delay_ms, min_age_days, delay_step_up, delay_step_down,
    #     delay_min_ms           -> module globals
    #     threads                -> thread_count.set_limit()
    #     min_size, prune_subtree_max_bytes, prune_budget_bytes -> args
    #     prune_dir_regex        -> args.prune_re (compiled)
    KEYS = ('file_delay_ms', 'threads', 'min_age_days', 'min_size',
            'prune_dir_regex', 'delay_step_up', 'delay_step_down',
            'delay_min_ms', 'prune_subtree_max_bytes', 'prune_budget_bytes',
            'regulate_prometheus_url', 'regulate_query', 'regulate_pause_ms',
            'regulate_slo_ms', 'regulate_period_s', 'regulate_quiet_ticks',
            'regulate_floor_ms', 'regulate_max_threads',
            'regulate_blind_resume_s', 'regulate_blind_throttle_s',
            'regulate_blind_delay_ms')

    def __init__(self, path, poll_seconds=10.0):
        self.path = path
        self.poll_seconds = poll_seconds
        self._next = 0.0
        self._mtime = None
        # Last values seen IN THE FILE, so an edit only re-applies the keys it
        # actually changed. Without this, editing any one key re-asserts every
        # other key in the file and silently reverts whatever a signal had set
        # in the meantime: adding prune_dir_regex to a running job would reset
        # a file_delay_ms eased to 20ms back to the 100ms still written in the
        # file.
        self._seen = {}
        # The walker polls between files and a config-poller thread polls on
        # its own clock (see _start_config_poller); this keeps them from
        # applying one edit twice.
        self._poll_lock = threading.Lock()

    @staticmethod
    def _comparable(v):
        """Regex objects compare by identity, so compare the pattern instead."""
        return v.pattern if hasattr(v, 'pattern') else v

    def poll(self, apply_cb):
        if not self._poll_lock.acquire(blocking=False):
            return
        try:
            self._poll(apply_cb)
        finally:
            self._poll_lock.release()

    def _poll(self, apply_cb):
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
                elif k in ('regulate_prometheus_url', 'regulate_query'):
                    out[k] = v or None
                elif k in ('regulate_pause_ms', 'regulate_slo_ms'):
                    fv = float(v)
                    if fv <= 0:
                        raise ValueError("must be > 0")
                    out[k] = fv
                elif k in ('regulate_period_s', 'regulate_quiet_ticks'):
                    iv = int(v)
                    if iv < 1:
                        raise ValueError("must be >= 1")
                    out[k] = iv
                elif k in ('regulate_max_threads', 'regulate_blind_resume_s',
                           'regulate_blind_throttle_s', 'regulate_blind_delay_ms'):
                    iv = int(v)
                    if iv < 0:
                        raise ValueError("must be >= 0 (0 disables)")
                    out[k] = iv
                elif k == 'regulate_floor_ms':
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
        # Bumped by every limit write, including one that repeats the current
        # value: an operator asking for 0 while the regulator already holds 0,
        # or for 1 during its 1-thread probe, is intent the number cannot show.
        # The regulator compares generations, not values, to tell its own
        # pause or probe from the operator's.
        self._gen = 0

    def acquire(self, cancel=None, tick=None):
        """Acquire a permit, blocking until one is available.

        cancel, if given, is checked every 0.5 s wait; True abandons the
        acquire and returns False. tick runs between waits with the lock
        released: the walker's config poll, so a threads=0 pause can still read
        the edit that ends it.
        """
        while True:
            with self._cond:
                if self._value > 0:
                    self._value -= 1
                    return True
                self._cond.wait(timeout=0.5)
                if cancel is not None and cancel():
                    return False
                if self._value > 0:
                    self._value -= 1
                    return True
            if tick is not None:
                tick()

    def release(self):
        with self._cond:
            self._value += 1
            self._cond.notify()

    @property
    def limit(self):
        with self._cond:
            return self._limit

    @property
    def gen(self):
        with self._cond:
            return self._gen

    @property
    def in_use(self):
        """Permits held right now: the copies in flight."""
        with self._cond:
            return self._limit - self._value

    def snapshot(self):
        """(limit, generation), read together."""
        with self._cond:
            return self._limit, self._gen

    def set_limit(self, new_limit):
        """Change the permit count.  If raised, blocked acquires may wake.

        Returns the new generation.
        """
        with self._cond:
            delta = new_limit - self._limit
            self._limit = new_limit
            self._value += delta
            self._gen += 1
            # Wake waiters if we added permits
            if delta > 0:
                self._cond.notify_all()
            return self._gen

    def set_limit_if(self, gen, new_limit):
        """set_limit(), unless something wrote the limit since generation gen.

        Returns the new generation, or None when it lost that race. The check
        and the write are one step under the lock, so an operator change that
        lands between a caller's read and its write is never overwritten.
        """
        with self._cond:
            if self._gen != gen:
                return None
            return self.set_limit(new_limit)

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


def validate_path_source(paths_from, paths_from_pool):
    if paths_from and paths_from_pool:
        raise ValueError("--paths-from and --paths-from-pool are mutually exclusive")


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


def non_negative_int(value):
    """Argparse type for an integer >= 0, matching the --config parser.

    A plain int let --regulate-blind-resume-s -1 through: negative is truthy,
    so "blind >= limit" held and a pause resumed blind on its second blind tick.
    """
    try:
        n = int(value)
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError(f"invalid non-negative integer: {value!r}")
    if n < 0:
        raise argparse.ArgumentTypeError(f"must be >= 0, got {n}")
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
    files_skipped_layout_invalid: int = 0
    files_vanished: int = 0
    files_outside_root: int = 0
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


class InvalidLayout(RuntimeError):
    """A layout no file or directory can have was read, or was about to be
    applied. The file is skipped, not failed: a later pass retries it."""


# A kernel client has been seen to answer a ceph.dir.layout read with
# "stripe_unit=0 stripe_count=0 object_size=0 pool=-1" instead of ENODATA.
# Taken at its word, --target-pool keeps the zeros and swaps in a pool, and
# setting stripe_unit=0 fails with EINVAL; walking past it to the parent
# could pick an ancestor's pool for a directory that has one of its own. So
# such a read is retried after these pauses, then refused.
_LAYOUT_REREAD_DELAYS_S = (0.1, 0.5, 2.0)


def _read_layout(path, xattr):
    """path's layout from xattr; None if it has none (ENODATA)."""
    for delay in _LAYOUT_REREAD_DELAYS_S + (None,):
        try:
            layout = CephLayout(
                os.getxattr(path, xattr, follow_symlinks=False).decode("utf-8"))
        except OSError as e:
            if e.errno == ENODATA:
                return None
            raise  # Re-raise unexpected errors (EACCES, EIO, etc.)
        if layout.valid:
            return layout
        if delay is None:
            raise InvalidLayout(f"{xattr} of {path} reads back as {layout}")
        time.sleep(delay)


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
        self.pool_namespace = vals.get("pool_namespace", "")
        self.layout = layout

    def _fields(self):
        return (self.stripe_unit, self.stripe_count, self.object_size,
                self.pool, self.pool_namespace)

    def with_pool(self, pool):
        """This layout with only the pool changed: what --target-pool asks for.

        Built as a layout string so __str__ and log mining see the same shape
        a real xattr read returns.
        """
        if pool == self.pool:
            return self
        s = (f"stripe_unit={self.stripe_unit} stripe_count={self.stripe_count} "
             f"object_size={self.object_size} pool={pool}")
        if self.pool_namespace:
            s += f" pool_namespace={self.pool_namespace}"
        return CephLayout(s)

    @property
    def valid(self):
        return (self.stripe_unit > 0 and self.stripe_count > 0
                and self.object_size > 0 and self.pool not in ("", "-1"))

    @classmethod
    def from_dir(cls, path):
        return _read_layout(path, "ceph.dir.layout")

    @classmethod
    def from_file(cls, path):
        return _read_layout(path, "ceph.file.layout")

    def apply_file(self, path):
        if not self.valid:
            raise InvalidLayout(f"refusing to apply {self} to {path}")
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

    # Compared field by field, not as the raw xattr string. A layout built by
    # with_pool() for --target-pool must match the layout a file read back after
    # it landed there, however the kernel happens to format or order the string.
    def __eq__(self, other):
        if not isinstance(other, CephLayout):
            return NotImplemented
        return self._fields() == other._fields()

    def __hash__(self):
        return hash(self._fields())

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


def target_layout(args, dir_layout):
    """The layout a file under a directory with dir_layout should end up with.

    Normally that is the directory's own layout. --target-pool replaces only the
    pool, so a small-file pass can land on the replicated pool while the volume
    root stays on EC and every other file keeps landing where it should.
    """
    pool = getattr(args, "target_pool", None)
    if dir_layout is None or not pool:
        return dir_layout
    return dir_layout.with_pool(pool)


# Shard-object yield floor. Every file moved to EC 4+2 goes from 3 pieces to
# k+m+3 = 9 whatever its size; below ~600 KiB the bytes saved per extra piece
# collapse (0.10-0.15 TB raw per million added pieces, against 13.2 above
# it). So a replicated target takes only files below this, and
# an EC target only files at or above it.
YIELD_FLOOR_BYTES = 614400

# Pool names on these clusters carry their profile, e.g. cephfs.x.ec4.2.qlc.data
# for EC. Replicated is matched only on its plain shape, cephfs.<fs>.data or
# cephfs.<fs>.rdata with nothing in between: EC pools end in .data too, so a
# bare suffix match would read cephfs.x.qlc.data as replicated.
_EC_POOL_NAME_RE = re.compile(r"\.ec\d+\.\d+(\.|$)")
_REP_POOL_NAME_RE = re.compile(r"^cephfs\.([^.]+)\.r?data$")


def target_pool_kind(pool, declared=None, fs_names=None):
    """("rep" | "ec", how) for a --target-pool, or (None, why not).

    The cluster decides when the ceph CLI is usable; a --target-pool-kind that
    contradicts it is refused. Without the CLI -- the transcode hosts have none
    -- the operator's --target-pool-kind decides, and failing that the pool's
    name, but only on a POSITIVE match either way. Guessing is not safe in
    either direction: an EC pool read as replicated would be sent exactly the
    small files the yield floor exists to keep off EC. fs_names, the
    filesystems the roots are on, pins the replicated shape to the actual
    filesystem, so cephfs.qlc.data does not pass on its shape alone.
    """
    scheme = _pool_scheme(pool)
    if scheme is not None:
        kind = "rep" if scheme[0] == "rep" else "ec"
        if declared and declared != kind:
            return None, (f"--target-pool-kind {declared}, but the cluster "
                          f"says {pool} is {kind}")
        return kind, "cluster"
    if declared:
        return declared, "--target-pool-kind"
    if _EC_POOL_NAME_RE.search(pool):
        return "ec", "name"
    if fs_names:
        # Exact names, not the regex: a filesystem name may contain a dot.
        if pool in {f"cephfs.{n}.{s}" for n in fs_names for s in ("data", "rdata")}:
            return "rep", "name, pinned to filesystem %s" % ",".join(sorted(fs_names))
    elif _REP_POOL_NAME_RE.match(pool):
        # ceph-fuse, or a kernel mount of the default fs with no mds_namespace:
        # nothing to pin to, and the startup line says so.
        return "rep", "name, no filesystem pin (no mds_namespace on the mount)"
    return None, (f"cannot tell whether {pool} is replicated or EC without the "
                  f"ceph CLI; pass --target-pool-kind rep|ec")


def target_pool_floor(src_pool, dst_pool):
    """--min-size floor for an EC --target-pool: max(space crossover, yield)."""
    src, dst = _pool_scheme(src_pool), _pool_scheme(dst_pool)
    x = None
    if src and dst:
        x = size_crossover(src, dst, _min_alloc_hint() or 4096)
    return max(x or 0, YIELD_FLOOR_BYTES)


def target_pool_refusal(kind, floor, min_size, max_size, pool):
    """Why a --target-pool run must not start with these bounds, or None.

    The override sends every eligible file to one pool regardless of where its
    directory points, so the size band is the only thing keeping it from moving
    the whole volume to the wrong tier.
    """
    if kind == "rep":
        if max_size is None:
            return (f"--target-pool {pool} is replicated: give --max-size below "
                    f"{YIELD_FLOOR_BYTES} so only small files move to it")
        if max_size >= YIELD_FLOOR_BYTES:
            return (f"--target-pool {pool} is replicated: --max-size {max_size} "
                    f"must be below {YIELD_FLOOR_BYTES}")
        return None
    if min_size < floor:
        return (f"--target-pool {pool} is EC: --min-size {min_size} is below "
                f"the {floor}-byte floor")
    return None


def alloc_bytes(size, scheme, min_alloc):
    """Allocated bytes for one file under a redundancy scheme on given media.

    scheme is ("rep", n) or ("ec", k, m, stripe_unit). Two roundings apply and
    both matter for small files: EC pads to whole stripe rows, and every shard
    (or replica) then rounds up to min_alloc. The effective granularity is
    therefore max(stripe_unit, min_alloc), not either one alone.
    """
    if scheme[0] == "rep":
        return scheme[1] * (-(-size // min_alloc)) * min_alloc
    _, k, m, su = scheme
    rows = -(-size // (k * su))
    per_shard = rows * su
    return (k + m) * (-(-per_shard // min_alloc)) * min_alloc


def size_crossover(src_scheme, dst_scheme, min_alloc, step=4096, limit=64 << 20):
    """Smallest file size at which dst allocates strictly less than src.

    Returns None if dst never wins below `limit`. Deliberately general in the
    SOURCE scheme: comparing against 3x replication only is wrong whenever the
    source is R2 or another EC profile, and the answer moves a long way. With
    4k min_alloc, R3 -> EC6+3 crosses at 16 KiB but R2 -> EC6+3 at 20 KiB, and
    at 16-32 KiB EC6+3 beats R3 while losing to R2.
    """
    for size in range(step, limit + 1, step):
        if alloc_bytes(size, dst_scheme, min_alloc) < alloc_bytes(size, src_scheme, min_alloc):
            return size
    return None


def _pool_scheme(pool):
    """Best-effort ("rep", n) / ("ec", k, m, su) for a pool name, else None.

    Needs the ceph CLI and a usable keyring. The transcoder runs on clients that
    often have neither, so every failure path returns None and the caller simply
    skips the advisory. This must never be load-bearing.
    """
    try:
        import subprocess
        out = subprocess.run(
            ["ceph", "osd", "pool", "ls", "detail", "-f", "json"],
            capture_output=True, text=True, timeout=15)
        if out.returncode != 0:
            return None
        for pd in json.loads(out.stdout):
            if pd.get("pool_name") != pool:
                continue
            if pd.get("type") == 1 or pd.get("erasure_code_profile") in (None, "", "replicated_rule"):
                return ("rep", int(pd.get("size", 3)))
            sw = int(pd.get("stripe_width", 0))
            prof = subprocess.run(
                ["ceph", "osd", "erasure-code-profile", "get",
                 pd["erasure_code_profile"], "-f", "json"],
                capture_output=True, text=True, timeout=15)
            if prof.returncode != 0:
                return None
            pj = json.loads(prof.stdout)
            k, m = int(pj["k"]), int(pj["m"])
            su = (sw // k) if (sw and k) else 4096
            return ("ec", k, m, su)
    except Exception:
        return None
    return None


def _min_alloc_hint():
    """Cluster-configured bluestore_min_alloc_size_ssd, or None."""
    try:
        import subprocess
        out = subprocess.run(["ceph", "config", "get", "osd",
                              "bluestore_min_alloc_size_ssd"],
                             capture_output=True, text=True, timeout=15)
        return int(out.stdout.strip()) if out.returncode == 0 else None
    except Exception:
        return None


def crossover_warning(src_pool, dst_pool, min_size):
    """Advisory if min_size sits below the size where the move starts paying.

    Returns a message or None. Advisory only: below the crossover a transcode
    consumes MORE raw space than it frees, which no other check would catch.
    """
    src = _pool_scheme(src_pool)
    dst = _pool_scheme(dst_pool)
    if not src or not dst:
        return None
    ma = _min_alloc_hint() or 4096
    x = size_crossover(src, dst, ma)
    if x is None:
        return ("%s -> %s never saves space at min_alloc %d: every size costs at "
                "least as much on the target. Check the target profile."
                % (src_pool, dst_pool, ma))
    if min_size and min_size >= x:
        return None
    return ("--min-size %d is below the %d-byte crossover for %s -> %s at "
            "min_alloc %d. Files under %d bytes cost MORE raw space after "
            "transcoding than before."
            % (min_size, x, src_pool, dst_pool, ma, x))


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



# --- self-regulation ---------------------------------------------------------
# Throttle against what the filesystem's other clients see (MDS latency, from
# Prometheus), not this job's own stat latency. A thread in the job, so its
# failures log here. Optional: with no regulate_prometheus_url it never starts.
# Every regulate_* key in RuntimeConfig.KEYS must be listed here, or it is
# parsed and then discarded (a test holds this).
REGULATE_APPLY = (
    ('regulate_prometheus_url', 'regulate Prometheus URL'),
    ('regulate_query', 'regulate query'),
    ('regulate_pause_ms', 'regulate pause threshold'),
    ('regulate_slo_ms', 'regulate soft target'),
    ('regulate_period_s', 'regulate poll period'),
    ('regulate_quiet_ticks', 'regulate quiet ticks'),
    ('regulate_floor_ms', 'regulate delay floor'),
    ('regulate_max_threads', 'regulate max threads'),
    ('regulate_blind_resume_s', 'regulate blind resume'),
    ('regulate_blind_throttle_s', 'regulate blind throttle'),
    ('regulate_blind_delay_ms', 'regulate blind delay'),
)

REG_DEFAULTS = {
    'regulate_prometheus_url': None,
    'regulate_query': None,
    'regulate_pause_ms': REG_PAUSE_MS_DEFAULT,
    'regulate_slo_ms': REG_SLO_MS_DEFAULT,
    'regulate_period_s': REG_PERIOD_S_DEFAULT,
    'regulate_floor_ms': REG_FLOOR_MS_DEFAULT,
    'regulate_quiet_ticks': REG_QUIET_TICKS_DEFAULT,
    'regulate_max_threads': REG_MAX_THREADS_DEFAULT,
    'regulate_blind_resume_s': REG_BLIND_RESUME_S_DEFAULT,
    'regulate_blind_throttle_s': REG_BLIND_THROTTLE_S_DEFAULT,
    'regulate_blind_delay_ms': REG_BLIND_DELAY_MS_DEFAULT,
}
# Raise the delay floor after repeated pauses: pausing repeatedly means the delay
# is set lower than this filesystem will sustain, and recovering within a tick is
# not good enough if external alerting has already fired.
REG_PAUSE_WINDOW_S = 6 * 3600
REG_PAUSE_MAX = 3
REG_FLOOR_BACKOFF = 1.5
REG_FLOOR_CAP_MS = 2000
# ... and release it again after sustained quiet. A ratchet with no release is a
# trap: one bad night walks the floor up step by step and it never comes back, so
# the job stays throttled long after the cause has gone.
REG_FLOOR_DECAY_S = 1800
REG_ERR_QUIET_S = 600          # rate-limit repeated query failures to one line
REG_TIGHTEN_CAP_MS = 200       # bound on the soft-band file-delay climb


def _mds_namespace_for(path):
    """CephFS filesystem name for the mount containing `path`, or None.

    Read from mds_namespace in /proc/mounts rather than guessed from the path.
    The path is not a safe substitute. A directory's last path component often
    differs from the filesystem it lives on -- two similarly named directories
    can sit on entirely different filesystems -- and
    querying the wrong one would regulate against a volume this job is not
    touching, with nothing to indicate it.
    """
    try:
        path = os.path.abspath(path)
        # Touch the path first. These trees are commonly autofs, and an autofs
        # mount point has no ceph entry in /proc/self/mounts until something
        # triggers it -- so reading the table cold reports "not a ceph mount" for
        # a path that is one. The mounts also carry a timeout and unmount when
        # idle, so this is not only a first-run concern.
        try:
            # os.stat is NOT enough: autofs answers a stat of the mount point
            # itself without mounting anything. Opening the directory is what
            # triggers it. Read at most one entry -- these are volume roots and
            # we only need the mount, not a listing.
            with os.scandir(path) as _it:
                next(_it, None)
        except OSError:
            pass
        best, best_ns = "", None
        with open("/proc/self/mounts") as fh:
            for line in fh:
                f = line.split()
                if len(f) < 4 or f[2] != "ceph":
                    continue
                mp = f[1]
                if (path == mp or path.startswith(mp.rstrip("/") + "/")) and len(mp) > len(best):
                    for opt in f[3].split(","):
                        if opt.startswith("mds_namespace="):
                            best, best_ns = mp, opt.split("=", 1)[1]
        return best_ns
    except OSError:
        return None


def _promql_regex_literal(name):
    """Escape a name for use as a regex INSIDE a PromQL double-quoted string.

    re.escape() gives \\. , which PromQL's Go string escaping rejects (HTTP
    400, "unknown escape sequence"), so every backslash it produces is doubled.
    """
    return re.escape(name).replace("\\", "\\\\")


def _resolve_query(args):
    """(query, why_disabled). Substitutes {volume} if the query asks for it."""
    q = getattr(args, "regulate_query", None)
    if not q:
        return None, "no regulate_query configured"
    if "{volume}" not in q:
        return q, None
    names = {_mds_namespace_for(d) for d in args.dirs}
    names.discard(None)
    if len(names) != 1:
        return None, (
            "regulate_query uses {volume} but the filesystem name could not be "
            "resolved to exactly one value (found %s). Either run one volume per "
            "job, or write regulate_query without {volume}." % (sorted(names) or "none"))
    return q.replace("{volume}", _promql_regex_literal(names.pop())), None


class NanSample(ValueError):
    """The query returned NaN: a latency ratio over zero requests, 0/0."""


class Regulator(threading.Thread):
    """Poll a latency query and throttle this job to keep it under a threshold."""

    daemon = True

    def __init__(self, args, query):
        super().__init__(name="regulator")
        self.args = args
        self.query = query
        self.floor_ms = int(args.regulate_floor_ms)
        self._floor_base = self.floor_ms
        self._pauses = []
        self._last_decay = 0.0
        # Separate clocks for the ceiling's release. _pauses is bookkeeping the
        # floor's own decay clears, so the ceiling cannot share it without the
        # two decays quietly resetting each other.
        self._last_ceiling_decay = 0.0
        self._last_pressure_at = 0.0
        self._quiet = 0
        # Monotonic, like _blind_since. -inf so the first warning is not held
        # back on a host whose monotonic clock is still small.
        self._last_err = float("-inf")
        self._paused_threads = None
        # Monotonic time a regulator pause last had a usable sample; None
        # while it has one. nan counts as unusable here too, so nan and
        # errors cannot keep resetting each other's clocks.
        self._blind_since = None
        # Monotonic time a RUNNING job last had a usable sample (None while it
        # has one), and whether _throttle_blind() has acted on it since.
        self._blind_run_since = None
        self._blind_throttled = False
        # stats.files_transcoded at a running job's last nan sample, and how
        # many nan samples in a row it kept finishing files; see _nan_running().
        self._nan_run_done = None
        self._nan_run_ticks = 0
        # nan samples during a regulator pause since the last usable sample;
        # see _nan_while_paused(). Errors do not reset it.
        self._nan_ticks = 0
        # What a 1-thread resume on no real evidence still owes; see _probing().
        self._probe_owed = None
        # thread_count generation of the regulator's own last write while it
        # holds a pause or probe. Any other write, even one repeating the
        # value, is the operator's, and ends the regulator's claim.
        self._own_gen = None
        # Learned ceiling for regulator-driven thread increases. A pause
        # lowers it and it never falls below the operator's own --threads
        # value. It moves UP two ways: immediately when the operator raises
        # regulate_max_threads, which _thread_ceiling treats as an explicit
        # instruction -- see there for why that matters -- and one step at a
        # time under sustained quiet, see _maybe_decay_ceiling().
        self._ceiling = None
        # The regulate_max_threads value this ceiling was last reconciled
        # against, so a live change to it can be told from a steady state.
        self._cfg_max = None

    def set_floor_base(self, ms):
        """Move the baseline the floor decays back to.

        The baseline is captured at construction, so without this a config
        change to regulate_floor_ms would be accepted and have no effect.
        """
        self._floor_base = int(ms)
        if self.floor_ms < self._floor_base:
            self.floor_ms = self._floor_base

    # -- data -----------------------------------------------------------------
    def sample(self):
        """Latency in ms, or None when there is no usable answer."""
        base = self.args.regulate_prometheus_url
        if not base:
            # regulate_prometheus_url is a live tunable, so it can be cleared
            # under a running regulator. Concatenating None would raise a bare
            # "unsupported operand type(s) for +: 'NoneType' and 'str'" --
            # nothing pointing at a config the operator had just emptied.
            # Raise the specific reason and let the rate-limited hold path
            # report it.
            raise ValueError(
                "regulate_prometheus_url is empty -- it was cleared in "
                "--config after this regulator started")
        url = base + "?" + urllib.parse.urlencode(
            {"query": self.query})
        with urllib.request.urlopen(url, timeout=45) as r:
            d = json.load(r)
        if d.get("status") != "success":
            raise ValueError("query status %r" % d.get("status"))
        res = d.get("data", {}).get("result", [])
        if len(res) != 1:
            raise ValueError("query returned %d series, need exactly 1" % len(res))
        v = float(res[0]["value"][1])
        if v != v:
            raise NanSample("query returned nan")
        if v in (float("inf"), float("-inf")) or v < 0:
            raise ValueError("query returned %r" % v)
        return v

    # -- thread adaptivity ----------------------------------------------------
    def _thread_base(self):
        """Never drop the ceiling below what the operator asked for."""
        return max(1, int(getattr(self.args, "threads", 1) or 1))

    def _thread_ceiling(self):
        """Current ceiling, or 0 when thread adaptivity is switched off.

        A change to regulate_max_threads, up or down, wins over a ceiling
        earned by pausing; pause evidence still applies between changes. The
        ceiling only moves here, so a live change is applied exactly once.
        """
        mx = int(getattr(self.args, "regulate_max_threads", 0) or 0)
        if mx <= 0:
            return 0
        if self._cfg_max is None:
            self._cfg_max = mx
        elif mx != self._cfg_max:
            # _ceiling is None only if the very first call here already sees a
            # changed mx, which the seeding above normally prevents; report the
            # effective ceiling rather than the word "None".
            logging.info(
                "Regulator: configured max threads %d -> %d, ceiling %d -> %d",
                self._cfg_max, mx,
                self._ceiling if self._ceiling is not None else mx, mx)
            self._cfg_max = mx
            self._ceiling = mx
        if self._ceiling is None or self._ceiling > mx:
            self._ceiling = mx
        return max(self._ceiling, self._thread_base())

    def _lower_ceiling(self, level):
        """A pause at `level` threads is evidence `level` is too many here.

        Without this the regulator would climb straight back to the count that
        just caused a pause, pause again, and oscillate. Lowering the ceiling
        below that level makes each pause cost one step of future ambition.
        """
        if int(getattr(self.args, "regulate_max_threads", 0) or 0) <= 0:
            return
        base = self._thread_base()
        new = max(level - 1, base)
        cur = self._thread_ceiling()
        if new < cur:
            self._ceiling = new
            logging.warning(
                "Regulator: thread ceiling %d -> %d after pausing at %d threads",
                cur, new, level)

    def _maybe_raise_threads(self, lat):
        """Add one thread, but only once the file delay is at its floor.

        One knob per interval keeps the next sample attributable. Raising is
        slower to decide than shedding: a new permit takes effect at once, a
        reduction only as in-flight copies finish.
        """
        ceil = self._thread_ceiling()
        if not ceil:
            return
        cur, gen = thread_count.snapshot()
        if cur <= 0 or cur >= ceil:
            return
        if file_delay_ms > self.floor_ms:
            return
        # "Not paused" is not the same as "has headroom": climb only while the
        # measurement is under the soft target, not merely under the pause line.
        if lat >= self.args.regulate_slo_ms:
            return
        if thread_count.set_limit_if(gen, cur + 1) is None:
            return      # the operator wrote the limit meanwhile; theirs stands
        _update_proctitle()
        logging.info(
            "Regulator: %.1f ms is under the %.0f ms target and the delay is at "
            "its %dms floor -- threads %d -> %d (ceiling %d)",
            lat, self.args.regulate_slo_ms, self.floor_ms, cur, cur + 1, ceil)

    # -- actions --------------------------------------------------------------
    def _pause(self, lat):
        global file_delay_ms
        # Every pause tick is evidence, including the ones that change nothing.
        # This is deliberately above the limit > 0 test: once the job is paused
        # the limit IS 0, so a clock kept below it stops advancing for the whole
        # duration of a sustained pause, and a pause lasting longer than
        # REG_FLOOR_DECAY_S then hands back a ceiling step on the very tick it
        # ends. _note_pause() gets this right for the floor's clock by recording
        # every tick; this is the same property for the ceiling's.
        self._last_pressure_at = time.time()
        extra = self._note_pause()
        # Pausing a 1-thread probe must not shrink what it still owes.
        owed = self._probe_owed if self._probing() else None
        for _ in range(3):
            cur, gen = thread_count.snapshot()
            if cur <= 0:
                return
            if gen != self._own_gen:
                owed = None     # the operator replaced the probe; not ours
            g = thread_count.set_limit_if(gen, 0)
            if g is not None:
                break
            # The operator wrote the limit between the read and the write:
            # pause from their value, so a resume gives back theirs.
        else:
            return
        self._own_gen = g
        self._probe_owed = None
        self._paused_threads = max(cur, owed or 0)
        self._lower_ceiling(cur)
        logging.warning(
            "Regulator: PAUSE at %.1f ms (%.0f%% of the %.0f ms target) -- "
            "threads %d -> 0, in-flight copies will finish%s",
            lat, 100.0 * lat / self.args.regulate_slo_ms,
            self.args.regulate_slo_ms, cur, extra)

    def _operator_took_over(self):
        """True, and the claim dropped, once anyone else wrote thread_count."""
        if self._own_gen is not None and thread_count.gen != self._own_gen:
            self._paused_threads = self._probe_owed = self._own_gen = None
            return True
        return False

    def _paused_by_me(self):
        """True while threads sit at 0 because THIS regulator paused them.

        The operator can take over in the meantime: SIGUSR1/SIGUSR2/SIGTSTP,
        or a threads edit in --config, including one to 0 while the regulator
        already holds 0. Forget the pause then, or the next quiet sample
        resumes over the operator's own pause. Compared by generation, not
        value: since the walker reads --config during a pause, "threads = 0"
        arrives while the limit is already 0, and a value test cannot see it.
        """
        self._operator_took_over()
        return bool(self._paused_threads)

    def _probing(self):
        """True while a no-evidence resume's probe still owes threads.

        A resume on nan or a Prometheus outage brings back 1 thread (a
        soft-band sample may raise it to --threads); the rest is owed until the
        first usable sample under the pause line pays it through _resume(). An
        operator write to the thread count forgets the debt.
        """
        self._operator_took_over()
        return bool(self._probe_owed)

    def holding(self):
        """True while the regulator holds a pause or a probe it will undo.

        Read-only, for other threads: the regulator's own thread does the
        forgetting.
        """
        gen = self._own_gen
        return bool((self._paused_threads or self._probe_owed)
                    and gen is not None and thread_count.gen == gen)

    def _resume_one(self):
        """Take a regulator pause to 1 thread, owing the rest.

        Returns the paused count, or None when the operator wrote the limit
        after the caller's _paused_by_me(): theirs stands, and the pause is no
        longer the regulator's to end.
        """
        was = self._paused_threads
        self._paused_threads = None
        self._blind_since = None
        self._nan_ticks = 0
        g = thread_count.set_limit_if(self._own_gen, 1)
        if g is None:
            self._probe_owed = self._own_gen = None
            return None
        self._own_gen = g
        self._probe_owed = was if was > 1 else None
        _update_proctitle()
        return was

    def _resume(self, pay_probe=True, why=None):
        """End a regulator pause, or pay back what a 1-thread probe owes.

        pay_probe=False is a soft-band sample: evidence the filesystem answers,
        not that it has room for the full count, and the probe exists because
        nan or an outage resumed with no evidence. It pays back only as far as
        the operator's --threads, the floor _tighten() never sheds below
        either; the rest waits for a sample under the target. Holding the
        probe at 1 left a volume whose latency sits in the soft band below
        --threads indefinitely, with nothing else to move it.
        """
        if self._paused_by_me():
            want, cur = self._paused_threads, 0
        elif self._probing():
            want, cur = self._probe_owed, thread_count.limit
            if not pay_probe:
                base = self._thread_base()
                if cur >= base:
                    return
                if want > base:
                    self._pay_to_base(cur, base, why)
                    return
                # Owed no more than --threads: the whole debt, as below.
        else:
            return
        gen = self._own_gen
        self._paused_threads = self._probe_owed = self._own_gen = None
        ceil = self._thread_ceiling()
        new = ceil if ceil and want > ceil else want
        if thread_count.set_limit_if(gen, new) is None:
            return      # the operator wrote the limit meanwhile; theirs stands
        if new != want:
            logging.info(
                "Regulator: resumed, threads %d -> %d (held under the %d it "
                "paused at by the learned ceiling)", cur, ceil, want)
        else:
            logging.info("Regulator: resumed, threads %d -> %d", cur, want)

    def _pay_to_base(self, cur, base, why=None):
        """Raise a probe to --threads, keeping the rest of its debt owed."""
        g = thread_count.set_limit_if(self._own_gen, base)
        if g is None:
            self._probe_owed = self._own_gen = None
            return      # the operator wrote the limit meanwhile; theirs stands
        self._own_gen = g
        _update_proctitle()
        logging.info(
            "Regulator: %s, threads %d -> %d (--threads); the other %d wait "
            "for a sample under the target",
            why or "over the target but under the pause line", cur, base,
            self._probe_owed - base)

    def _ease(self):
        global file_delay_ms
        if thread_count.limit == 0 or file_delay_ms <= self.floor_ms:
            return
        old = file_delay_ms
        new = max(_delay_down(old), self.floor_ms)
        if new != old:
            file_delay_ms = new
            _update_proctitle()
            logging.info("Regulator: quiet, file delay %dms -> %dms (floor %dms)",
                         old, new, self.floor_ms)

    def _tighten(self, lat):
        """Give a step back while latency sits above the soft target.

        Mirror of _ease(): raise the file delay first, and shed a thread only
        once the delay is back at the configured value or REG_TIGHTEN_CAP_MS,
        whichever is lower. With no delay and no floor, or a floor already at
        the cap, there is no cheap knob left and the first soft-band tick
        sheds a thread.
        """
        global file_delay_ms
        # Soft-band ticks are evidence too, including the ones that shed
        # nothing -- _tighten returns early at cur <= base, which is every
        # soft-band tick of a --threads 1 job, exactly the shape this whole
        # change exists to protect.
        self._last_pressure_at = time.time()
        want = max(int(getattr(self.args, "file_delay", 0) or 0), self.floor_ms)
        old = file_delay_ms
        if old < min(want, REG_TIGHTEN_CAP_MS):
            new = min(_delay_up(old), want, REG_TIGHTEN_CAP_MS)
            if new != old:
                file_delay_ms = new
                _update_proctitle()
                logging.info(
                    "Regulator: %.1f ms is over the %.0f ms target -- file "
                    "delay %dms -> %dms", lat, self.args.regulate_slo_ms,
                    old, new)
                return
        cur, gen = thread_count.snapshot()
        if cur <= self._thread_base():
            return
        if thread_count.set_limit_if(gen, cur - 1) is None:
            return      # the operator wrote the limit meanwhile; theirs stands
        # Treat this the same as a pause for ceiling purposes, so the next
        # quiet spell does not climb straight back into the same latency.
        self._lower_ceiling(cur)
        _update_proctitle()
        logging.warning(
            "Regulator: %.1f ms is over the %.0f ms target with the delay at "
            "%dms -- threads %d -> %d", lat, self.args.regulate_slo_ms,
            file_delay_ms, cur, cur - 1)

    def _note_pause(self):
        now = time.time()
        self._pauses = [t for t in self._pauses if now - t < REG_PAUSE_WINDOW_S] + [now]
        if len(self._pauses) <= REG_PAUSE_MAX:
            return " [%d pause(s) in %dh]" % (len(self._pauses), REG_PAUSE_WINDOW_S // 3600)
        old = self.floor_ms
        new = min(int(old * REG_FLOOR_BACKOFF) + 1, REG_FLOOR_CAP_MS)
        if new <= old:
            return " [floor already at cap %dms]" % REG_FLOOR_CAP_MS
        self.floor_ms = new
        self._pauses = []
        return " [floor raised %dms -> %dms after repeated pauses]" % (old, new)

    def _maybe_decay_floor(self):
        if self.floor_ms <= self._floor_base:
            return
        now = time.time()
        last = max([self._last_decay] + self._pauses) if self._pauses else self._last_decay
        if now - last < REG_FLOOR_DECAY_S:
            return
        old = self.floor_ms
        self.floor_ms = max(self._floor_base, int(old / REG_FLOOR_BACKOFF))
        self._last_decay = now
        self._pauses = []
        logging.info("Regulator: quiet %dmin, floor %dms -> %dms (baseline %dms)",
                     REG_FLOOR_DECAY_S // 60, old, self.floor_ms, self._floor_base)

    def _maybe_decay_ceiling(self):
        """Give back one step of learned thread ceiling after sustained quiet.

        Like the floor's decay: without it, one pause at two threads leaves a
        --threads 1 job at a ceiling of 1 for good. One step per
        REG_FLOOR_DECAY_S of quiet; a tick with pressure resets the clock
        (_pause(), _tighten()).
        """
        mx = int(getattr(self.args, "regulate_max_threads", 0) or 0)
        if mx <= 0 or self._ceiling is None or self._ceiling >= mx:
            return
        now = time.time()
        if now - max(self._last_ceiling_decay, self._last_pressure_at) < REG_FLOOR_DECAY_S:
            return
        old = self._ceiling
        self._ceiling = min(mx, old + 1)
        self._last_ceiling_decay = now
        logging.info(
            "Regulator: quiet %dmin, thread ceiling %d -> %d (configured max %d)",
            REG_FLOOR_DECAY_S // 60, old, self._ceiling, mx)

    # -- loop -----------------------------------------------------------------
    def _hold(self, why):
        """No usable sample: hold, unless holding means paused forever.

        Absent data is no evidence, so hold -- except in a pause of our own,
        which only a usable sample ends. That gets regulate_blind_resume_s
        (nan counts; monotonic clock), then 1 thread back, the rest waiting for
        a usable sample. The running-job clock: see _not_running().
        """
        now = time.monotonic()
        if not self._paused_by_me():
            self._blind_since = None
            if thread_count.limit <= 0:
                self._not_running(why)
                return
            if self._blind_run_since is None:
                self._blind_run_since = now
            lim = int(getattr(self.args, "regulate_blind_throttle_s", 0) or 0)
            if (lim and not self._blind_throttled
                    and now - self._blind_run_since >= lim):
                self._throttle_blind(now - self._blind_run_since, why)
                self._last_err = now
            elif now - self._last_err > REG_ERR_QUIET_S:
                logging.warning(
                    "Regulator: no usable sample (%s) -- holding current "
                    "settings, not adjusting%s", why,
                    " (throttled blind)" if self._blind_throttled else
                    " (throttles in %ds if it persists)"
                    % max(0, lim - (now - self._blind_run_since)) if lim else "")
                self._last_err = now
            return
        limit = int(getattr(self.args, "regulate_blind_resume_s", 0) or 0)
        if self._blind_since is None:
            self._blind_since = now
            self._last_err = now
            logging.warning(
                "Regulator: no usable sample (%s) while PAUSED by the "
                "regulator -- %s", why,
                "resuming at 1 thread in %ds if it persists" % limit if limit
                else "regulate_blind_resume_s is 0, staying paused until a "
                     "sample arrives")
            return
        blind = now - self._blind_since
        if limit and blind >= limit:
            self._resume_blind(blind, why)
        elif now - self._last_err > REG_ERR_QUIET_S:
            logging.warning("Regulator: still no usable sample (%s), paused "
                            "blind for %ds", why, blind)
            self._last_err = now

    def _not_running(self, why):
        """No usable sample while someone else holds the job at 0 threads.

        An operator pause (threads = 0, SIGUSR2) leaves no threads to take,
        and a paused job sends the MDS nothing, so a quiet volume reads nan for
        as long as it stays paused. Counted as running blind, that would log a
        false "running BLIND" ERROR and raise the file delay the operator's
        resume starts from.
        The clock restarts when the job runs again.
        """
        self._blind_run_since = None
        self._blind_throttled = False
        self._nan_run_done = None
        self._nan_run_ticks = 0
        now = time.monotonic()
        if now - self._last_err > REG_ERR_QUIET_S:
            logging.info("Regulator: no usable sample (%s) while the job is "
                         "paused -- nothing to throttle", why)
            self._last_err = now

    def _nan_running(self):
        """nan while the job runs: blind only if the job has stalled as well.

        nan means no MDS request completed: a stalled MDS, or a query window
        too short for a slow job. Every finished file is MDS work done, so nan
        counts toward the blind throttle only when no file finished since the
        previous nan sample (an error resets the baseline). While files keep
        finishing, regulate_quiet_ticks such samples pay a throttle's debt
        back as far as --threads.
        """
        if thread_count.limit <= 0:
            self._not_running("query returned nan")
            return
        done = stats.files_transcoded
        moved = self._nan_run_done is not None and done > self._nan_run_done
        self._nan_run_done = done
        if not moved:
            self._nan_run_ticks = 0
            self._hold("query returned nan")
            return
        self._blind_run_since = None
        self._blind_throttled = False
        self._nan_run_ticks += 1
        if self._nan_run_ticks >= max(1, int(self.args.regulate_quiet_ticks)):
            self._nan_run_ticks = 0
            self._resume(pay_probe=False,
                         why="query returned nan, but the job keeps finishing "
                             "files")

    def _throttle_blind(self, blind, why):
        """Running blind too long: drop to 1 thread and a safe file delay.

        After regulate_blind_throttle_s with no usable sample, take the job to
        one thread and at least regulate_blind_delay_ms. The threads are owed
        like a blind resume's probe and paid back by the first usable sample (a
        soft-band one as far as --threads); an operator write in between wins
        and forgets the debt.
        """
        global file_delay_ms
        self._blind_throttled = True
        want = int(getattr(self.args, "regulate_blind_delay_ms", 0) or 0)
        old_d = file_delay_ms
        if want and file_delay_ms < want:
            file_delay_ms = min(want, DELAY_MAX_MS)
        self._operator_took_over()          # a stale claim must not be re-owed
        cur, gen = thread_count.snapshot()
        new_t = cur
        if cur > 1:
            g = thread_count.set_limit_if(gen, 1)
            if g is not None:
                new_t = 1
                self._own_gen = g
                self._probe_owed = max(cur, self._probe_owed or 0)
        _update_proctitle()
        logging.error(
            "Regulator: no usable sample for %ds (%s) -- running BLIND, so "
            "throttling: threads %d -> %d, file delay %dms -> %dms until a "
            "usable sample arrives (regulate_blind_throttle_s)",
            blind, why, cur, new_t, old_d, file_delay_ms)

    def _resume_blind(self, blind, why):
        was = self._resume_one()
        if was is None:
            logging.info("Regulator: the operator set threads during the "
                         "pause; not resuming blind over it")
            return
        logging.error(
            "Regulator: no usable sample for %ds while paused (%s) -- "
            "resuming BLIND, threads 0 -> 1 (paused at %d); the rest waits "
            "for a usable sample", blind, why, was)

    def _nan_while_paused(self):
        """nan during our own pause: quiet, once it has lasted.

        Our pause makes nan likely on a quiet volume, but a stalled or
        recovering MDS reads nan too, so it takes regulate_quiet_ticks nan
        samples to bring back 1 thread. An error neither counts nor resets the
        streak, and nan does not reset the blind clock, so alternating nan and
        errors still reaches a bound.
        """
        now = time.monotonic()
        if self._blind_since is None:
            self._blind_since = now
        self._nan_ticks += 1
        need = max(1, int(self.args.regulate_quiet_ticks))
        if self._nan_ticks == 1:
            logging.info(
                "Regulator: query returned nan while paused -- no MDS request "
                "completed in the query window; resuming at 1 thread if that "
                "lasts %d samples", need)
        if self._nan_ticks < need:
            # The blind bound stays a hard upper limit, even with
            # regulate_quiet_ticks set longer than it.
            limit = int(getattr(self.args, "regulate_blind_resume_s", 0) or 0)
            blind = now - self._blind_since
            if limit and blind >= limit:
                self._resume_blind(blind, "query returned nan")
            return
        was = self._resume_one()
        if was is None:
            logging.info("Regulator: the operator set threads during the "
                         "pause; not resuming over it")
            return
        logging.warning(
            "Regulator: nan for %d samples while paused -- threads 0 -> 1 "
            "(paused at %d); the rest waits for a usable sample", need, was)

    def run(self):
        while not do_exit.is_set():
            # Re-read every tick: regulate_period_s is a --config tunable, and
            # reading it once before the loop froze it while the INFO line for
            # a change still read like success.
            period = max(5, int(self.args.regulate_period_s))
            try:
                lat = self.sample()
            except NanSample:
                if self._paused_by_me():
                    self._nan_while_paused()
                else:
                    self._nan_ticks = 0
                    self._nan_running()
                do_exit.wait(period)
                continue
            except Exception as e:
                # No reset of _nan_ticks: an error is no evidence either way.
                # The running-job progress baseline does reset: "files
                # finished since the previous sample" must not span an error
                # stretch, or one nan with progress after hours of errors
                # would restart the blind clock and the throttle never fire.
                self._nan_run_done = None
                self._nan_run_ticks = 0
                self._hold(_redact_text(e, self.args.regulate_prometheus_url))
                do_exit.wait(period)
                continue
            self._blind_since = None
            self._nan_ticks = 0
            self._nan_run_done = None
            self._nan_run_ticks = 0
            if self._blind_run_since is not None:
                if self._blind_throttled:
                    logging.warning(
                        "Regulator: usable sample again (%.1f ms) after %ds "
                        "blind -- regulating normally from threads %d, file "
                        "delay %dms", lat,
                        time.monotonic() - self._blind_run_since,
                        thread_count.limit, file_delay_ms)
                self._blind_run_since = None
                self._blind_throttled = False
            if lat >= self.args.regulate_pause_ms:
                self._quiet = 0
                self._pause(lat)
            elif lat >= self.args.regulate_slo_ms:
                # Between the soft target and the pause line. Not quiet: that
                # would ease the delay DOWN while latency is already past the
                # target the regulator defends, leaving only the pause cliff.
                # Back off a step instead.
                self._quiet = 0
                self._resume(pay_probe=False)
                self._tighten(lat)
            else:
                self._resume()
                self._maybe_decay_floor()
                self._maybe_decay_ceiling()
                self._quiet += 1
                if self._quiet >= int(self.args.regulate_quiet_ticks):
                    self._quiet = 0
                    self._ease()
                    # _ease() is a no-op once the delay is at its floor, which
                    # is exactly when this can act; they never both fire.
                    self._maybe_raise_threads(lat)
            do_exit.wait(period)


def _url_userinfo(url):
    """The raw "user:password" substring of url, or "" -- WITHOUT urlsplit().

    By hand, because urlsplit() raises on netlocs urllib.request still sends
    (an unbalanced "["), which would fail open. Scheme-less values count
    ("bob:pw@host"), as does a percent-encoded "@" with no literal one. Any
    non-empty userinfo is credentialed.
    """
    return _url_userinfo_split(url)[1]


def _url_userinfo_split(url):
    """(scheme_sep, userinfo, remainder) from the ORIGINAL string.

    The one boundary _url_userinfo() and _loggable_url() share, so a URL is
    never detected as credentialed by one and logged intact by the other.
    """
    s = str(url or "")
    _, sep, rest = s.partition("://")
    if not sep:
        rest = s
    # The LAST "@" after "://", not urllib's host boundary: cutting at the first
    # "/?#" would miss the "@" in "http://bob:pa/ss@prom/..." and fail open. A
    # Prometheus URL has no reason to carry an "@", so failing closed is free.
    idx = rest.rfind("@")
    width = 1
    if idx < 0:
        # No literal "@" -- fall back to a percent-ENCODED one. Request._parse()
        # runs unquote() on the host, so "http://bob:pw%40prom/api/v1/query"
        # reaches http.client as "bob:pw@prom" and dies with
        # InvalidURL("nonnumeric port: 'pw@prom'"). Matching only a literal "@"
        # meant neither refusal fired, the URL was logged whole, and "pw" was
        # not scrubbed from that exception. Only consulted when there is no
        # literal "@", so a URL that already has one keeps its old boundary and
        # a later "%40" in the path cannot move it.
        idx = rest.lower().rfind("%40")
        width = 3
    if idx < 0:
        return sep, "", rest
    return sep, rest[:idx], rest[idx + width:]


def _url_credentials(url):
    """Every substring of url's userinfo worth scrubbing from error text.

    Both the raw and the percent-DECODED form of each part. Request._parse()
    runs unquote() on the host, so a password written "p%40ss" surfaces as
    "p@ss" in the exception text while the URL holds the escaped form --
    scrubbing only one of the two misses the other. Longest first, so a
    password that merely contains the username is not left half-redacted.
    """
    ui = _url_userinfo(url)
    if not ui:
        return ()
    user, _, pw = ui.partition(":")
    out = set()
    for v in (user, pw):
        if not v:
            continue
        out.add(v)
        try:
            dec = urllib.parse.unquote(v)
        except Exception:
            dec = v
        if dec:
            out.add(dec)
    return tuple(sorted(out, key=len, reverse=True))


def _loggable_url(url):
    """url with any user:password@ userinfo replaced, for logging.

    All string work, no urlsplit(): this function must never be the thing
    that raises. urlsplit().port raises ValueError on a non-numeric port, and
    a raise here propagates through _apply_regulate_keys -> _apply_config ->
    RuntimeConfig.poll() and kills the job at startup or mid-walk. Keeping it
    purely textual also preserves IPv6 brackets and the original host case.
    """
    if not url:
        return url
    s = str(url)
    # The SAME split as _url_userinfo(), from the SAME helper -- not a second
    # implementation of the same rule. Had they disagreed, a password containing
    # "/", "?", "#" or a percent-encoded "@" would be detected as a credential
    # and then logged intact. "after" comes from the operator's own string, so
    # this redacts their URL rather than emitting a decoded rewrite of it.
    sep, userinfo, after = _url_userinfo_split(s)
    if not userinfo:
        return url
    head = s.partition("://")[0] if sep else ""
    return (head + "://" if sep else "") + "<redacted>@" + after


def _redact_text(text, url):
    """text with every credential substring from url replaced.

    urllib hands "user:pw@host" to http.client as the hostname, so the error
    text carries the bare password, not a "user:pw@" a regex would find:
    scrub the substrings the URL is known to hold.
    """
    # LONGEST FIRST. Replacing the username before the password leaves part
    # of the password behind when one contains the other: user "bob" with
    # password "bobSecret" would log "<redacted>Secret". _url_credentials()
    # already sorts, but this loop is where the ordering actually matters, so
    # it does not rely on that contract holding.
    s = str(text)
    for v in sorted(_url_credentials(url), key=len, reverse=True):
        s = s.replace(v, "<redacted>")
    return s


def _url_has_userinfo(url):
    """True if url carries userinfo, which can never work here (see above).

    Accepts scheme-less values and a percent-encoded "@", so it is wider than
    a "://" test. _redact_argv() has to accept the same inputs or a value this
    refuses still reaches the log. It matches this exactly for tokens holding
    "://". For scheme-less tokens it is deliberately NARROWER: it also
    requires the token to look like a URL authority (so PromQL is not
    rewritten) and to carry a ":" in the decoded userinfo (so ordinary
    arguments are not mangled). A scheme-less username-only value such as
    "tok@prom:9090/api" is therefore refused here but NOT redacted in argv.
    """
    return bool(_url_userinfo(url))


def _redact_argv(argv):
    """argv with any credentialed URL token replaced, BEFORE shlex.join().

    The command line reaches Starting:/Finished:, every _report_state() line
    and the process title. Per token, not on the joined string, so an
    abbreviated flag ("--regulate-prom"), another token sharing the
    credential's text, and shlex quoting cannot defeat it.

    A token with "://" is redacted whenever it has userinfo, the inputs
    _url_has_userinfo() refuses. A scheme-less one only with a ":" before the
    "@" (in the decoded userinfo), so "/data/x@y" and "bob@example.com" pass
    and "/a:b@c" is over-redacted.
    """
    def _split_flag(tok):
        """("--flag=", value) for a joined flag assignment, else ("", tok).

        A scheme-less value loses its flag name otherwise: _loggable_url()
        keeps the text before "://" as its head, so
        "--regulate-prometheus-url=http://u:pw@h" survives intact, but
        "--regulate-prometheus-url=bob:pw@prom:9090/api" has no "://" for the
        head to come from and was logged as "<redacted>@prom:9090/api" --
        the flag name simply gone from the amended command line.
        """
        if tok.startswith("-") and "=" in tok:
            name, _, value = tok.partition("=")
            return name + "=", value
        return "", tok

    def _authority_like(user, host):
        """Whether a SCHEME-LESS token looks like a real URL authority.

        Keeps PromQL ("x[1h:5m] @ end()") from being redacted: a character
        illegal in an RFC 3986 username or host means it is not one. Brackets
        stay legal in the host, for IPv6. The password is not tested; it may
        hold anything.
        """
        bad_user = set(' \t\n()[]{}"\',<>')
        bad_host = set(' \t\n(){}"\',<>')
        return not (set(user) & bad_user or set(host) & bad_host)

    def _credentialed(tok):
        _, value = _split_flag(tok)
        sep, ui, after = _url_userinfo_split(value)
        if not ui:
            return False
        if sep:
            # Unambiguously a URL, so match _url_has_userinfo() exactly. The
            # ":" test below was never needed here, and being narrower than
            # detection is how a refused credential still reached the log.
            # Deliberately NO _authority_like() test on this branch: a real
            # URL's query string may hold anything ("?q=(x)"), and rejecting
            # on that would stop redacting a genuine credential.
            return True
        # Scheme-less: keep the guard that protects ordinary arguments, but
        # test the DECODED userinfo, since Request._parse() unquotes the host
        # and "bob%3Apw" is a credential just as much as "bob:pw".
        dec = urllib.parse.unquote(ui)
        if ":" not in dec:
            return False
        host = re.split(r"[/?#]", after, maxsplit=1)[0]
        return _authority_like(dec.partition(":")[0], host)

    def _redact(tok):
        prefix, value = _split_flag(tok)
        return prefix + _loggable_url(value)

    return [_redact(a) if _credentialed(a) else a for a in argv]


def _regulator_decline_reasons(args, first_only=False):
    """Every precondition start_regulator() would decline for, in its order.

    A list of (code, reason); empty means it would start. first_only stops at
    the first, so with no URL the query (and its scandir) is never resolved.
    The one copy of these conditions, for start_regulator() and the
    late-change warning in _apply_regulate_keys(), which wants them all.
    --no-regulate is not checked here.
    """
    out = []
    url = getattr(args, "regulate_prometheus_url", None)
    if not url:
        out.append(("no_url", "no regulate_prometheus_url is set"))
        if first_only:
            return out
    elif _url_has_userinfo(url):
        out.append(("userinfo",
                    "regulate_prometheus_url carries embedded credentials, "
                    "which urllib does not use for authentication"))
        if first_only:
            return out
    query, why = _resolve_query(args)
    if not query:
        out.append(("query", why))
        if first_only:
            return out
    if args.regulate_slo_ms >= args.regulate_pause_ms:
        out.append(("band",
                    "regulate_slo_ms (%.0f) is not below regulate_pause_ms "
                    "(%.0f), so the soft band is empty or inverted"
                    % (args.regulate_slo_ms, args.regulate_pause_ms)))
        if first_only:
            return out
    return out


def _regulator_decline_reason(args):
    """The FIRST reason start_regulator() would decline, or None.

    Short-circuiting wrapper for callers that report one reason and stop.
    """
    found = _regulator_decline_reasons(args, first_only=True)
    return found[0] if found else None


def _apply_regulate_keys(args, cfg):
    """Apply the REGULATE_APPLY keys from a parsed config dict to args.

    A late change warns when it changes nothing: regulate_query is fixed at
    construction, and with no regulator running (never configured, declined,
    or --no-regulate) no key takes effect. On a running regulator the URL and
    the tunables are read from args every tick, so they apply; clearing the
    URL warns.
    """
    _changed = []                      # [(key, label, cleared)] actually applied
    for _k, _label in REGULATE_APPLY:
        if _k not in cfg or cfg[_k] == getattr(args, _k, None):
            continue
        _old = getattr(args, _k, None)
        if (_k == 'regulate_prometheus_url' and regulator_started
                and _url_has_userinfo(cfg[_k])):
            # Refuse a LATE credentialed URL only: a running regulator rebuilds
            # the URL from args every tick. At startup it is stored, and
            # start_regulator() refuses it, before any sample().
            logging.warning(
                "Config: %s has embedded credentials (%s), which urllib "
                "ignores; NOT applied, keeping the previous value.",
                _label, _loggable_url(cfg[_k]))
            continue
        setattr(args, _k, cfg[_k])
        if _k == 'regulate_prometheus_url':
            logging.info("Config: %s %s -> %s", _label, _loggable_url(_old),
                         _loggable_url(cfg[_k]))
        else:
            logging.info("Config: %s %s -> %s", _label, _old, cfg[_k])
        if _k == 'regulate_floor_ms' and regulator is not None:
            regulator.set_floor_base(cfg[_k])
        _changed.append((_k, _label, cfg[_k] is None or cfg[_k] == ""))

    # DECIDE ONCE, AFTER THE LOOP, FROM THE FINAL args. Deciding per key
    # mid-loop read a half-applied namespace: one reload adding both the URL
    # and the query hit regulate_prometheus_url first, while args.regulate_query
    # was still None, and advised "set regulate_query, then restart" -- then the
    # query in that same reload advised "restart the job for this to take
    # effect". Contradictory advice for the most common fix there is. This also
    # replaces N near-identical WARNINGs per reload with one naming every key.
    if not regulator_started or not _changed:
        return

    if regulator is None:
        # Clearing a key on an unregulated run has nothing waiting to take
        # effect, so the INFO lines above are the whole story for those.
        _pending = [_l for _k, _l, _cleared in _changed if not _cleared]
        if not _pending:
            return
        _labels = ", ".join(_pending)
        if getattr(args, "no_regulate", False):
            logging.warning(
                "Config: %s changed, but this run has --no-regulate; restart "
                "without it to apply", _labels)
            return
        _whys = _regulator_decline_reasons(args)
        if _whys:
            # ALL of them: naming only the first sent the operator round the
            # loop once per missing precondition.
            logging.warning(
                "Config: %s changed, but this run started unregulated and a "
                "restart would still decline: %s. Fix that first, then "
                "restart the job.", _labels,
                "; ".join(_r for _c, _r in _whys))
        else:
            logging.warning(
                "Config: %s changed, but this run started unregulated; restart "
                "the job to apply", _labels)
        return

    # A regulator IS running: what takes effect depends on the key.
    _restart = [_l for _k, _l, _c in _changed if _k == 'regulate_query']
    _clearedurl = [_l for _k, _l, _c in _changed
                   if _k == 'regulate_prometheus_url' and _c]
    _liveurl = [_l for _k, _l, _c in _changed
                if _k == 'regulate_prometheus_url' and not _c]
    if _restart:
        logging.warning(
            "Config: %s changed, but the query is fixed when the regulator "
            "starts -- restart the job for this to take effect",
            ", ".join(_restart))
    if _clearedurl:
        logging.warning(
            "Config: %s cleared under a running regulator; it holds its "
            "settings until a URL is set (or restart with --no-regulate).",
            ", ".join(_clearedurl))
    if _liveurl:
        logging.info(
            "Config: %s is re-read on every sample -- the running regulator "
            "picks it up at its next poll, no restart needed",
            ", ".join(_liveurl))


def start_regulator(args):
    """Start the regulator, or explain once why it is not running."""
    url = getattr(args, "regulate_prometheus_url", None)
    # --no-regulate is tested FIRST, and before the URL, because it has to mean
    # what its name says: regulation is off. Reading it only when no URL was
    # configured would leave the flag silently ignored by any job that has one,
    # which is the same class of quiet surprise this whole function was changed
    # to stop. A configured-URL-plus-flag conflict therefore warns; the plain
    # deliberate case stays quiet.
    if getattr(args, "no_regulate", False):
        if url:
            logging.warning(
                "Self-regulation disabled by --no-regulate; "
                "regulate_prometheus_url (%s) and regulate_query ignored. "
                "Running at file_delay=%dms threads=%d.",
                _loggable_url(url), file_delay_ms, thread_count.limit)
        else:
            logging.info("Self-regulation disabled by --no-regulate; "
                         "running at file_delay=%dms threads=%d",
                         file_delay_ms, thread_count.limit)
        return None
    # ONE evaluation of the preconditions, shared with the late-change warning
    # in _apply_regulate_keys() via _regulator_decline_reason(). Only the
    # WORDING lives here; the conditions do not, so the two callers cannot
    # drift apart (a restart does not help an inverted soft band or an
    # unresolvable {volume}, whatever URL and query are set).
    _decline = _regulator_decline_reason(args)
    if _decline:
        _code, _reason = _decline
        if _code == "no_url":
            # Warns like the other declines: an omitted --config is the
            # likeliest, and at INFO it would be invisible. RuntimeConfig.poll()
            # is silent about a missing file, so name a --config that is not
            # there.
            cfg = getattr(args, "config", None)
            if cfg and not os.path.exists(cfg):
                where = ("in --config %s, which does not exist, or with "
                         "--regulate-prometheus-url and --regulate-query" % cfg)
            elif cfg:
                where = ("in --config %s, which has no regulate_prometheus_url, "
                         "or with --regulate-prometheus-url and "
                         "--regulate-query" % cfg)
            else:
                where = ("with --regulate-prometheus-url and --regulate-query, "
                         "or in a --config file")
            logging.warning(
                "Self-regulation disabled (no regulate_prometheus_url); running "
                "at file_delay=%dms threads=%d. Set the URL and query %s, or "
                "pass --no-regulate.", file_delay_ms, thread_count.limit, where)
        elif _code == "userinfo":
            # Reached for a CLI URL and for one from --config alike: the
            # apply-time path deliberately stores a credentialed URL at
            # startup and leaves the refusal to this function, so that this
            # message -- the one that explains the remedy -- is what the
            # operator sees. Nothing has sampled it; we return before that.
            logging.warning(
                "Self-regulation disabled: regulate_prometheus_url has "
                "embedded credentials (%s), which urllib ignores; remove the "
                "user:password@. Running at file_delay=%dms threads=%d.",
                _loggable_url(url), file_delay_ms, thread_count.limit)
        elif _code == "band":
            logging.warning(
                "Self-regulation disabled: %s and the regulator would hit the "
                "pause line without ever easing off first. Fix the config.",
                _reason)
        else:
            logging.warning("Self-regulation disabled: %s", _reason)
        return None
    # Resolved a second time for the value itself. One extra scandir at job
    # startup, against a tree this job is about to walk in full.
    query, _why = _resolve_query(args)
    reg = Regulator(args, query)
    try:
        lat = reg.sample()
    except Exception as e:
        # Start anyway: a failed first sample (nan from a too-short window, a
        # Prometheus blip, a volume with no MDS traffic yet) is transient, and
        # the poll loop holds and retries on it.
        logging.warning(
            "Regulator: first sample failed (%s); starting anyway, retrying "
            "every %ds ('nan': widen the query window). Query: %s",
            _redact_text(e, url), max(5, int(args.regulate_period_s)), query)
        logging.info(
            "Regulator: enabled, holding file_delay=%dms threads=%d until a "
            "usable sample arrives.", file_delay_ms, thread_count.limit)
        reg.start()
        return reg
    # Units are the likeliest misconfiguration and the failure is silent in both
    # directions: seconds means it never triggers, microseconds means it never
    # stops. Say what came back so a factor of 1000 is obvious immediately.
    if lat < 0.01:
        logging.warning("Regulator: query returned %.6f -- is this SECONDS? "
                        "regulate_query must return MILLISECONDS. Enabled, but "
                        "it will likely never trigger.", lat)
    elif lat > 60000:
        logging.warning("Regulator: query returned %.0f -- is this MICROSECONDS? "
                        "regulate_query must return MILLISECONDS. Enabled, but "
                        "it will pause almost immediately.", lat)
    logging.info("Regulator: enabled. Sample %.2f ms; pause at %.0f ms, target "
                 "%.0f ms, poll %ds, delay floor %dms.",
                 lat, args.regulate_pause_ms, args.regulate_slo_ms,
                 args.regulate_period_s, reg.floor_ms)
    logging.info("Regulator: query = %s", query)
    reg.start()
    return reg


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


# A path that was there a moment ago and is not now. Counted as vanished, not
# failed: on a live volume files are deleted under the walk all the time.
_VANISHED_ERRNOS = (errno.ENOENT, errno.ESTALE, errno.ENOTDIR)

TMP_SUFFIX = ".vcephfs-tc-tmp"
# A staged temp file is ".<32 hex>.vcephfs-tc-tmp". Matching on the whole shape
# rather than the suffix alone keeps reclamation from touching anything a user
# happened to name similarly.
TMP_RE = re.compile(r"^\.[0-9a-f]{32}" + re.escape(TMP_SUFFIX) + r"$")

# Never reclaim a temp file younger than this: a concurrent job may be mid-copy
# into it. No single file copy runs for a day. Age is judged by ctime, not
# mtime: copystat backdates a finished copy's mtime to its source's before the
# rename, and nothing can set ctime, which every write, chown and link moves.
TMP_ORPHAN_MIN_AGE_S = 24 * 3600

_reclaimed_dirs = set()
_reclaimed_lock = threading.Lock()

# One journal per volume root records what this run did to rctime. rctime is a
# monotonic high-water mark that cannot be set, and retention reads it as an
# artifact's age, so a transcode makes untouched data look new; the journal
# keeps the pre-transcode value.
RUN_JOURNAL_NAME = ".vcephfs-transcode-runs.jsonl"
# The final write only happens if Python gets to unwind, so a run also keeps a
# per-run checkpoint beside each journal, rewritten this often or after this
# many record changes, whichever comes first. See RunJournal.checkpoint().
RUN_JOURNAL_CHECKPOINT_S = 300
RUN_JOURNAL_CHECKPOINT_CHANGES = 10000
# An artifact's first pin checkpoints this soon, so a burst of first pins shares
# one rewrite, and no sooner than this many times the last checkpoint's own
# duration after it, so a large snapshot is not rewritten constantly.
RUN_JOURNAL_PIN_DEBOUNCE_S = 5
# How long the final append waits for another run's lock on the journal.
RUN_JOURNAL_LOCK_WAIT_S = 30
RUN_JOURNAL_PIN_SPACING = 10
# Names an artifact root, matching retention_path_policy's own marker.
RETENTION_MARKER = "RETENTION"


def _is_journal_file(name):
    """The journal, or one of this tool's checkpoints of it (or their temps)."""
    return name.startswith(RUN_JOURNAL_NAME)


def read_rctime(path):
    """ceph.dir.rctime as a float, or None off CephFS."""
    try:
        raw = os.getxattr(path, "ceph.dir.rctime")
        return float(raw.decode().strip().strip('"'))
    except (OSError, ValueError, AttributeError, UnicodeDecodeError):
        return None


class RunJournal:
    """Per-artifact record of the rctime this run is about to destroy.

    note_file() runs BEFORE anything under an artifact changes, to capture
    pre_rctime; note_write() runs AFTER each change, so pinned_at is the write
    the MDS stamped into rctime.

    For a consumer: an rctime equal to pinned_at is ours and says nothing
    about the owner. pre_rctime and max_file_mtime are a floor, not an answer
    (a write after we passed is not seen), so a match means rescan the
    artifact. Rows are keyed by (run, artifact); skip blank or unparseable
    lines, and when also reading .partial checkpoints, dedupe on that key and
    prefer the row without "partial".
    """

    def __init__(self, enabled, stop_at=()):
        self.enabled = enabled
        self.run_id = uuid.uuid4().hex[:12]
        self.started = time.time()
        self._lock = threading.Lock()
        # artifact root -> {pre_rctime, max_file_mtime, pinned_at}
        self._artifacts = {}
        self._artifact_of = {}     # directory -> artifact root or None
        self._stop_at = set(stop_at)
        # Checkpointing. _io_lock orders checkpoint() against the final
        # write(), so a late checkpoint cannot land after write() removed it.
        self._io_lock = threading.Lock()
        self._stop = threading.Event()
        self._final = False
        self._changes = 0
        self._last_checkpoint = time.monotonic()
        self._ckpt_thread = None
        # Set by an artifact's first pin; see start_checkpoints().
        self._wake = threading.Event()
        self._pin_pending = False
        self._ckpt_cost = 0.0
        # Failed checkpoints in a row, and when the next pin retry may run.
        # A fast failure (ENOSPC, EDQUOT, EROFS) costs ~0 s, so the cost
        # spacing alone let it retry, and warn, every pin debounce.
        self._ckpt_failures = 0
        self._retry_at = 0.0
        self._pin_debounce_s = RUN_JOURNAL_PIN_DEBOUNCE_S
        # Volume roots whose checkpoint failure has already warned; a retry
        # of one logs at DEBUG, a first failure on another still warns.
        self._warned = set()

    def _artifact_root(self, dirpath):
        """Nearest ancestor holding a RETENTION file, or None.

        Memoized over the whole ancestor chain, so a directory costs one
        walk-up once and its descendants cost a dict lookup.
        """
        with self._lock:
            if dirpath in self._artifact_of:
                return self._artifact_of[dirpath]
        chain = []
        d = dirpath
        root = None
        while True:
            with self._lock:
                if d in self._artifact_of:
                    root = self._artifact_of[d]
                    break
            chain.append(d)
            try:
                if os.path.isfile(os.path.join(d, RETENTION_MARKER)):
                    root = d
                    break
            except OSError:
                pass
            if d in self._stop_at:
                break
            parent = os.path.dirname(d)
            if parent == d:
                break
            d = parent
        with self._lock:
            for c in chain:
                self._artifact_of[c] = root
        return root

    def note_file(self, filepath, st):
        """Fold one file into its artifact's record."""
        if not self.enabled or not stat.S_ISREG(st.st_mode):
            return
        root = self._artifact_root(os.path.dirname(filepath))
        if root is None:
            return
        with self._lock:
            rec = self._first_sighting(root)
            if st.st_mtime > rec["max_file_mtime"]:
                rec["max_file_mtime"] = st.st_mtime
                self._changes += 1

    def note_dir(self, dirpath):
        """note_file() for a change to dirpath that no stat'd file precedes.

        The walk sweeps a directory's orphans before it stats the directory's
        files, so an artifact whose root holds an orphan would otherwise read
        its pre_rctime after our own unlink.
        """
        if not self.enabled:
            return
        root = self._artifact_root(dirpath)
        if root is None:
            return
        with self._lock:
            self._first_sighting(root)

    def _first_sighting(self, root):
        """root's record, created on first sight. Caller holds _lock."""
        rec = self._artifacts.get(root)
        if rec is None:
            # First sighting: nothing under this root has been touched yet,
            # so this rctime is still the owner's, not ours.
            rec = {"pre_rctime": read_rctime(root), "max_file_mtime": 0.0,
                   "pinned_at": None}
            self._artifacts[root] = rec
            self._changes += 1
        return rec

    def note_write(self, filepath):
        """Record that we have just changed filepath's directory.

        Called after the last change (a replace, or a temp made and removed),
        so pinned_at is the write rctime shows; an artifact only read keeps
        pinned_at null. Sampled and compared under the lock and only moved
        forward, since workers on one artifact are not otherwise ordered.
        """
        if not self.enabled:
            return
        root = self._artifact_root(os.path.dirname(filepath))
        if root is None:
            return
        first = False
        with self._lock:
            rec = self._artifacts.get(root)
            if rec is not None:
                now = time.time()
                if rec["pinned_at"] is None:
                    # From here pre_rctime is the only record of what this pin
                    # destroyed, and until a checkpoint it is in memory only.
                    first = self._pin_pending = True
                if rec["pinned_at"] is None or now > rec["pinned_at"]:
                    rec["pinned_at"] = now
                    self._changes += 1
        if first:
            self._wake.set()

    def _rows_by_root(self, roots, args, **extra):
        """[(volume_root, rows)] for each root holding records of this run."""
        with self._lock:
            artifacts = {a: dict(r) for a, r in self._artifacts.items()}
        base = {
            "run": self.run_id,
            "host": os.uname().nodename,
            "pid": os.getpid(),
            "start": round(self.started, 6),
            "end": round(time.time(), 6),
            "min_size": getattr(args, "min_size", None),
        }
        base.update(extra)
        out = []
        for volume_root in roots:
            prefix = volume_root.rstrip("/") + "/"
            mine = {a: r for a, r in artifacts.items()
                    if a == volume_root or a.startswith(prefix)}
            rows = []
            for artifact, rec in sorted(mine.items()):
                row = dict(base)
                row["artifact"] = artifact
                row["pre_rctime"] = rec["pre_rctime"]
                row["max_file_mtime"] = round(rec["max_file_mtime"], 6)
                pinned_at = rec.get("pinned_at")
                row["pinned_at"] = (round(pinned_at, 6)
                                    if pinned_at is not None else None)
                rows.append(row)
            if rows:
                out.append((volume_root, rows))
        return out

    def _partial_path(self, volume_root):
        return os.path.join(volume_root, "%s.%s.partial"
                            % (RUN_JOURNAL_NAME, self.run_id))

    def checkpoint(self, roots, args):
        """Rewrite this run's snapshot beside each journal, atomically.

        Covers SIGKILL, the OOM killer and power loss, which write() never
        sees. Per-run and temp+rename; write() removes it. One left behind is
        the same rows plus "partial": true, "end" its last checkpoint time.
        """
        if not self.enabled:
            return
        with self._io_lock:
            if self._final:
                return
            with self._lock:
                self._changes = 0
                pinned, self._pin_pending = self._pin_pending, False
                self._last_checkpoint = time.monotonic()
            t0 = time.monotonic()
            failed = False
            for volume_root, rows in self._rows_by_root(
                    list(roots), args, partial=True):
                ok = self._write_partial(volume_root, rows,
                                         quiet=volume_root in self._warned)
                if ok:
                    self._warned.discard(volume_root)
                else:
                    self._warned.add(volume_root)
                    failed = True
                    with self._lock:
                        self._changes += 1     # retry next interval
                        # ... or sooner, if a first pin is what is
                        # unrecorded: its pre_rctime is in memory only.
                        self._pin_pending = self._pin_pending or pinned
            now = time.monotonic()
            self._ckpt_cost = now - t0
            if failed:
                # Back off: the pin debounce, doubling, up to the interval.
                self._ckpt_failures += 1
                self._retry_at = now + min(
                    RUN_JOURNAL_CHECKPOINT_S, self._pin_debounce_s
                    * 2 ** min(self._ckpt_failures - 1, 16))
            else:
                # A success from another trigger must not leave the next
                # first pin waiting out a backoff that no longer applies.
                self._retry_at = 0.0
                if self._ckpt_failures:
                    logging.info("Run journal checkpoint written again after "
                                 "%d failed attempt(s)", self._ckpt_failures)
                    self._ckpt_failures = 0

    def _write_partial(self, volume_root, rows, quiet=False):
        """Replace this run's checkpoint at volume_root via temp+fsync+rename.

        quiet logs a failure at DEBUG: a retry of one already warned about.
        """
        path = self._partial_path(volume_root)
        tmp = path + ".tmp"
        try:
            with open(tmp, "w") as fh:
                for row in rows:
                    fh.write(json.dumps(row, sort_keys=True) + "\n")
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, path)
            return True
        except OSError as e:
            (logging.debug if quiet else logging.warning)(
                "Could not checkpoint run journal %s: %s", path, e)
            # A temp cut short (ENOSPC after open) is never read; drop it.
            try:
                os.unlink(tmp)
            except OSError:
                pass
            return False

    def start_checkpoints(self, roots, args, every_s=RUN_JOURNAL_CHECKPOINT_S,
                          every_changes=RUN_JOURNAL_CHECKPOINT_CHANGES,
                          poll_s=1.0,
                          pin_debounce_s=RUN_JOURNAL_PIN_DEBOUNCE_S):
        """Checkpoint in the background until write() runs.

        A thread, since a pause parks the walker and the SIGTERM drain has
        none. It checkpoints on the interval, on do_exit (polled every poll_s)
        and on an artifact's first pin; triggers but the interval and do_exit
        wait until the last checkpoint is RUN_JOURNAL_PIN_SPACING times its
        own duration old, since each rewrites every row.
        """
        if not self.enabled or self._ckpt_thread is not None:
            return
        self._pin_debounce_s = pin_debounce_s     # the failure backoff's base

        def loop():
            timeout = min(poll_s, every_s)
            pin_seen = None
            while not self._stop.is_set():
                self._wake.wait(timeout)
                self._wake.clear()
                if self._stop.is_set():
                    return
                now = time.monotonic()
                with self._lock:
                    n = self._changes
                    age = now - self._last_checkpoint
                    pending = self._pin_pending
                timeout = min(poll_s, every_s)
                pin_due = False
                spaced = age >= RUN_JOURNAL_PIN_SPACING * self._ckpt_cost
                if not pending:
                    pin_seen = None
                else:
                    if pin_seen is None:
                        pin_seen = now
                    left = max(pin_debounce_s - (now - pin_seen),
                               RUN_JOURNAL_PIN_SPACING * self._ckpt_cost - age,
                               self._retry_at - now)
                    if left > 0:
                        timeout = min(timeout, left)
                    else:
                        pin_due = True
                # Once do_exit is set, systemd may SIGKILL the drain of
                # in-flight copies before write() is reached: stop waiting.
                if n and (pin_due or (n >= every_changes and spaced)
                          or age >= every_s or do_exit.is_set()):
                    pin_seen = None
                    try:
                        self.checkpoint(roots, args)
                    except Exception:
                        logging.exception("Run journal checkpoint failed")

        self._ckpt_thread = threading.Thread(
            target=loop, name="journal-checkpoint", daemon=True)
        self._ckpt_thread.start()

    def write(self, roots, args):
        """Append this run's records to a journal at each volume root.

        Under an exclusive flock (CephFS enforces it across clients), with a
        newline first if an earlier append died mid-line. A lock not granted
        within RUN_JOURNAL_LOCK_WAIT_S is logged and the append goes ahead
        unlocked: losing the record is worse.
        """
        if not self.enabled:
            return
        self._stop.set()
        self._wake.set()
        with self._io_lock:
            self._final = True
            for volume_root, rows in self._rows_by_root(roots, args):
                journal = os.path.join(volume_root, RUN_JOURNAL_NAME)
                try:
                    data = "".join(json.dumps(row, sort_keys=True) + "\n"
                                   for row in rows).encode()
                    fd = os.open(journal,
                                 os.O_RDWR | os.O_APPEND | os.O_CREAT, 0o644)
                    try:
                        # On failure it logs, and the append goes ahead.
                        _lock_journal(fd, journal)
                        if _fd_ends_mid_line(fd):
                            data = b"\n" + data
                        while data:
                            data = data[os.write(fd, data):]
                        # Durable before the checkpoint goes: the unlink is an
                        # MDS op, and these bytes may still be dirty pages.
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    logging.info("Wrote %d artifact record(s) to %s",
                                 len(rows), journal)
                except OSError as e:
                    logging.warning("Could not write run journal %s: %s",
                                    journal, e)
                    # The checkpoint is now the only record, so bring it up to
                    # date: the last periodic one misses everything since, and
                    # a run shorter than the interval never wrote one.
                    marked = [dict(r, partial=True) for r in rows]
                    if not self._write_partial(volume_root, marked):
                        _log_lost_rows(volume_root, rows)
                    continue
                try:
                    os.unlink(self._partial_path(volume_root))
                except FileNotFoundError:
                    pass
                except OSError as e:
                    logging.warning("Could not remove run journal checkpoint "
                                    "%s: %s", self._partial_path(volume_root), e)


def _fd_ends_mid_line(fd):
    """True if fd's file is non-empty and its last byte is not a newline."""
    size = os.fstat(fd).st_size
    return size > 0 and os.pread(fd, 1, size - 1) != b"\n"


def _lock_journal(fd, path):
    """flock(LOCK_EX) on fd, for up to RUN_JOURNAL_LOCK_WAIT_S. True if held.

    Released by the close that follows the append.
    """
    deadline = time.monotonic() + RUN_JOURNAL_LOCK_WAIT_S
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return True
        except BlockingIOError:
            if time.monotonic() >= deadline:
                logging.warning("Run journal %s still locked after %ds; "
                                "appending without the lock", path,
                                RUN_JOURNAL_LOCK_WAIT_S)
                return False
            time.sleep(0.1)
        except OSError as e:
            logging.warning("Could not lock run journal %s (%s); appending "
                            "without the lock", path, e)
            return False


def _log_lost_rows(volume_root, rows):
    """Last resort when the journal and its checkpoint both failed.

    pre_rctime cannot be recovered once the run is gone, and the log usually
    lives on another device than the volume that just refused two writes.
    """
    logging.error("Run journal LOST at %s: the append and the checkpoint both "
                  "failed; its %d record(s) follow, one per line", volume_root,
                  len(rows))
    keys = ("run", "artifact", "pre_rctime", "max_file_mtime", "pinned_at")
    for row in rows:
        logging.error("run-journal record: %s", json.dumps(
            {k: row.get(k) for k in keys}, sort_keys=True))


def _reclaim_named(dirpath, names):
    """Unlink aged orphans from an already-enumerated name list.

    The walk has the names in hand from os.walk, so this costs one stat per
    candidate and no extra scandir.
    """
    now = time.time()
    count = 0
    for name in names:
        # Check the name here, in the function that does the unlinking, even
        # though the only caller already filters on TMP_RE. Without it the
        # blast radius of one careless caller is "unlink every aged regular
        # file in this directory", on a filesystem holding other people's
        # data. Cheap insurance against a future refactor.
        if not TMP_RE.match(name):
            continue
        p = os.path.join(dirpath, name)
        try:
            st = os.lstat(p)
            if not stat.S_ISREG(st.st_mode):
                continue
            if now - st.st_ctime < TMP_ORPHAN_MIN_AGE_S:
                continue
            os.unlink(p)
            count += 1
        except OSError:
            pass
    if count:
        logging.info(f"Reclaimed {count} orphaned temp file(s) from {dirpath}")
    return count


def reclaim_orphans(dirpath):
    """Unlink aged `.<hex>.vcephfs-tc-tmp` orphans in dirpath, once per directory.

    Sibling staging puts the temp file beside its target, so cleanup_tmpdir --
    which only scans --tmpdir -- cannot see it. Without this a SIGKILL
    mid-copy strands hidden objects on exactly the pools this tool exists to keep
    object counts down on.
    """
    with _reclaimed_lock:
        if dirpath in _reclaimed_dirs:
            return 0
        _reclaimed_dirs.add(dirpath)
    now = time.time()
    count = 0
    try:
        for entry in os.scandir(dirpath):
            if not TMP_RE.match(entry.name):
                continue
            try:
                if not entry.is_file(follow_symlinks=False):
                    continue
                if (now - entry.stat(follow_symlinks=False).st_ctime
                        < TMP_ORPHAN_MIN_AGE_S):
                    continue
                os.unlink(entry.path)
                count += 1
            except OSError:
                pass
    except OSError:
        pass
    if count:
        logging.info(f"Reclaimed {count} orphaned temp file(s) from {dirpath}")
    return count


# --- --target-pool: stage in per-job directories, never switch a file's pool
#
# Setting a file's layout after creation records its old pool in old_pools, and
# the MDS keeps a backtrace object there for good. So a --target-pool temp is
# born in a hidden staging directory, `.vcephfs-tc-stage-<id>`, whose
# ceph.dir.layout.pool is the target (one per distinct target layout), and is
# renamed across into place. User directories' layouts are never written.
#
# The staging base is the deepest of the mount root; the file's pin boundary (a
# directory with ceph.dir.pin, or a child of one with ceph.dir.pin.distributed),
# which keeps the rename on one rank; and, after an EXDEV, its quota-realm root,
# where the copy is retried once.
#
# Nothing is removed mid-run: an rmdir of a directory the copier's kernel client
# is renaming out of waits on a cap recall that can hang and stall the rank's
# journal trimming. The end-of-run removal is bounded; a hang leaves the
# directory for --reap-staging.
STAGE_PREFIX = ".vcephfs-tc-stage-"
STAGE_RE = re.compile(r"^" + re.escape(STAGE_PREFIX) + r"[0-9a-f]{16}$")
STAGE_RMDIR_TIMEOUT_S = 60
# --reap-staging leaves a staging directory alone until nothing has been
# created, renamed or unlinked in it for this long (its ctime), so it cannot
# take a live job's directory; a temp in it is left until its own mtime and
# ctime are that old too, since writing a copy moves neither of the directory's.
# A running job refreshes its directories' ctime every quarter of that, so a
# paused one is not taken either.
REAP_STAGING_MIN_AGE_S = 3600


def _int_xattr(path, name, default):
    """An integer CephFS vxattr of path, or default when it cannot be read."""
    try:
        return int(os.getxattr(path, name, follow_symlinks=False)
                   .decode("utf-8").strip())
    except (OSError, ValueError, AttributeError):   # no os.getxattr: macOS
        return default


def _has_quota(path):
    """Does path itself carry a CephFS quota (ceph.quota.max_bytes/max_files)?"""
    for attr in ("ceph.quota.max_bytes", "ceph.quota.max_files"):
        try:
            v = os.getxattr(path, attr, follow_symlinks=False)
        except OSError:
            continue
        try:
            if int(v.decode("utf-8").strip() or 0) > 0:
                return True
        except ValueError:
            continue
    return False


class JobStaging:
    """The run's staging directories, one per (base, target layout).

    base is the deepest of the mount root, a pin boundary (_pin_base()) and a
    quota-realm root an EXDEV led to. A directory is made on first use -- the
    startup probe makes the mount root's -- and is never removed while the run
    lasts; remove_all() removes them all at the end.
    """

    def __init__(self, run_id, root, session=None,
                 rmdir_timeout_s=STAGE_RMDIR_TIMEOUT_S):
        self.run_id = run_id
        self.root = os.path.abspath(root)
        # Removals go through session (a RemovalSession), never the kernel
        # mount; without one (off CephFS, in tests) os.rmdir is used.
        self.session = session
        self.rmdir_timeout_s = rmdir_timeout_s
        self._lock = threading.Lock()
        self._make_lock = threading.Lock()
        self._made = {}            # (base, layout) -> staging path
        self._paths = []           # every staging path this run made, in order
        self._realms = []          # quota-realm roots, deepest first
        self._pin_cache = {}       # path -> (ceph.dir.pin, .pin.distributed)
        self._aborted = None       # the session a hung removal aborted
        self._keepalive_stop = threading.Event()
        self._keepalive = None

    def name_for(self, layout):
        """The staging directory's name for a target layout: STAGE_RE's shape,
        the run id's first 8 hex then 8 of the layout's hash, so each target
        layout of a run gets its own directory."""
        h = hashlib.sha256(repr(layout._fields()).encode("utf-8")).hexdigest()
        return STAGE_PREFIX + self.run_id[:8] + h[:8]

    def base_for(self, dirpath):
        """Where dirpath's files are staged: the deepest of the mount root, a
        quota-realm root an EXDEV found above it, and its pin boundary."""
        best = self.root
        with self._lock:
            for r in self._realms:
                if _under_roots(dirpath, [r]):
                    best = r
                    break
        pinned = self._pin_base(dirpath)
        if pinned is not None and len(pinned) > len(best):
            best = pinned
        return best

    def _pins(self, path):
        with self._lock:
            got = self._pin_cache.get(path)
        if got is None:
            got = (_int_xattr(path, "ceph.dir.pin", -1),
                   _int_xattr(path, "ceph.dir.pin.distributed", 0))
            with self._lock:
                self._pin_cache[path] = got
        return got

    def _pin_base(self, dirpath):
        """The deepest directory at or above dirpath, below the mount root's
        parent, whose subtree an MDS pin keeps on one rank: one with an
        explicit ceph.dir.pin, or a child of one with ceph.dir.pin.distributed.
        A staging directory there renames within that rank; one above it would
        make every rename a two-rank commit (the header's case B). None when
        no pin bounds dirpath."""
        p = os.path.abspath(dirpath)
        child = None
        while _under_roots(p, [self.root]):
            pin, distributed = self._pins(p)
            if distributed > 0 and child is not None:
                return child
            if pin >= 0:
                return p
            if p == self.root:
                break
            child, p = p, os.path.dirname(p)
        return None

    def path_for(self, dirpath, layout):
        """The staging directory for a file in dirpath going to layout, made
        if this run has none yet."""
        base = self.base_for(dirpath)
        with self._lock:
            path = self._made.get((base, layout))
        if path is not None:
            return path
        # Made once however many workers ask for it at the same time.
        with self._make_lock:
            with self._lock:
                path = self._made.get((base, layout))
            if path is None:
                path = self._create(base, layout)
                with self._lock:
                    self._made[(base, layout)] = path
        return path

    def recreate(self, dirpath, layout):
        """Make dirpath's staging directory again after it vanished (someone
        removed it under the run); the same name, so in-flight paths agree."""
        base = self.base_for(dirpath)
        with self._make_lock:
            path = self._create(base, layout)
            with self._lock:
                self._made[(base, layout)] = path
        logging.warning(f"Staging directory {path} had vanished; recreated it")
        return path

    def _create(self, base, layout):
        if not layout.valid:
            raise InvalidLayout(f"refusing to stage under {base} for {layout}")
        path = os.path.join(base, self.name_for(layout))
        if run_journal is not None:
            # Seen before the mkdir moves its rctime; claimed by the files
            # staged here (process_file) and by the removal.
            run_journal.note_dir(base)
        try:
            os.mkdir(path, 0o700)
        except FileExistsError:
            pass
        with self._lock:
            if path not in self._paths:
                # Recorded before the layout is set: a directory whose layout
                # failed is still this run's to remove, at the end and never
                # here (see the header).
                self._paths.append(path)
        os.setxattr(path, "ceph.dir.layout.pool", layout.pool.encode("utf-8"),
                    follow_symlinks=False)
        got = CephLayout.from_dir(path)
        if got != layout:
            # Only the pool was set, so anything else that differs came from
            # the base's own layout. Set every field rather than stage into
            # the wrong layout.
            for attr in ("stripe_unit", "stripe_count", "object_size", "pool"):
                os.setxattr(path, f"ceph.dir.layout.{attr}",
                            str(getattr(layout, attr)).encode("utf-8"),
                            follow_symlinks=False)
            got = CephLayout.from_dir(path)
            if got != layout:
                raise RuntimeError(
                    f"staging layout did not apply to {path}: wanted "
                    f"{layout}, read back {got}")
        return path

    def note_exdev(self, dirpath):
        """A rename out of staging into dirpath failed with EXDEV: find the
        nearest ancestor of dirpath (itself included, the mount root not) that
        carries a quota, so later files under it are staged there. Returns
        that realm root, or None when no such ancestor explains the EXDEV."""
        p = os.path.abspath(dirpath)
        while p != self.root and _under_roots(p, [self.root]):
            if _has_quota(p):
                with self._lock:
                    if p not in self._realms:
                        self._realms.append(p)
                        self._realms.sort(key=len, reverse=True)
                logging.info(f"{dirpath} is in the quota realm rooted at {p}; "
                             f"files under it are staged there")
                return p
            p = os.path.dirname(p)
        return None

    def probe(self, pool):
        """None if a file can be born in pool here, else why not.

        Makes the mount root's staging directory -- the one every file with
        no deeper base uses -- with the exact mechanism staging uses, creates
        a file in it and reads its layout back, then unlinks only the file. So
        a pool the filesystem does not have, or one the client cannot write,
        fails at startup rather than once per file. Nothing is removed: the
        directory is the run's.
        """
        try:
            base = get_layout_walking_up(self.root)
            if base is None:
                return f"cannot read the layout of {self.root}"
            path = self.path_for(self.root, base.with_pool(pool))
        except (OSError, RuntimeError) as e:
            return f"staging directory under {self.root}: {e}"
        f = os.path.join(path, f".{uuid.uuid4().hex}{TMP_SUFFIX}")
        why = None
        try:
            with open(f, "wb"):
                pass
            got = CephLayout.from_file(f)
            if got is None or got.pool != pool:
                why = f"a file created under {path} landed in {got and got.pool}"
        except OSError as e:
            why = f"{path}: {e}"
        finally:
            try:
                os.unlink(f)
            except OSError:
                pass
        if why is None and self.session is not None:
            # Mapped, not statted: open() proved this mount's mapping, and a
            # getattr through the session of a directory the kernel client
            # has just made can wait on a recall of that client's caps, as an
            # rmdir does.
            try:
                self.session.fs_path(path)
            except OSError as e:
                why = f"{path}: {e}"
        return why

    def keepalive_once(self):
        """Create and unlink a temp in each staging directory, which moves its
        ctime, so --reap-staging's age guard leaves a paused job's directory
        alone. A dentry change, like the copies' own, not a utime: setting a
        directory's times takes an exclusive cap."""
        with self._lock:
            paths = list(self._paths)
        for path in paths:
            f = os.path.join(path, f".{uuid.uuid4().hex}{TMP_SUFFIX}")
            try:
                open(f, "wb").close()
                os.unlink(f)
            except OSError as e:
                logging.warning(f"Could not refresh staging directory {path}: {e}")
                continue
            if run_journal is not None:
                run_journal.note_write(path)

    def start_keepalive(self, interval):
        """keepalive_once() every interval seconds until remove_all()."""
        def run():
            while not self._keepalive_stop.wait(interval):
                try:
                    self.keepalive_once()
                except Exception:
                    logging.exception("Staging keepalive failed")
        t = self._keepalive = threading.Thread(
            target=run, name="staging-keepalive", daemon=True)
        t.start()
        return t

    def remove_all(self):
        """End of run: remove every staging directory this run made, each
        bounded by rmdir_timeout_s. The executor has joined, so no copy is in
        flight; a temp a failure left inside is this run's and is unlinked.

        A hang aborts the removal session, killing its process, and the
        directory, like any this pass cannot remove, is left with a WARNING
        for --reap-staging. Nothing pauses: the run is over. Returns the paths
        removed."""
        self._keepalive_stop.set()
        if self._keepalive is not None:
            # A refresh in progress would add a temp after the listing below.
            self._keepalive.join(self.rmdir_timeout_s)
        with self._lock:
            paths = list(self._paths)
        removed, left = [], []
        for path in paths:
            why = self._remove(path)
            if why is None:
                removed.append(path)
                logging.info(f"Removed staging directory {path}")
                continue
            left.append(path)
            if len(left) <= 10:
                logging.warning(f"Staging directory {path} left on disk: {why}. "
                                f"Remove it with --reap-staging once "
                                f"--reap-staging-min-age old.")
        if len(left) > 10:
            logging.warning(f"{len(left) - 10} more staging directories left on "
                            f"disk, for --reap-staging likewise")
        return removed

    def _remove(self, path):
        """Unlink this run's temps in path, fsync it and its parent, rmdir it
        through the session; None once it is gone, else why not."""
        s = self.session
        if self._aborted is not None:
            return "the removal session was aborted after an earlier hang"
        phase = {"p": "listing"}

        def work():
            try:
                names = os.listdir(path)
            except FileNotFoundError:
                return
            phase["p"] = "temp unlink"
            for n in names:
                if TMP_RE.match(n):
                    try:
                        os.unlink(os.path.join(path, n))
                    except FileNotFoundError:
                        pass
            for name, d in (("staging-dir fsync", path),
                            ("parent-dir fsync", os.path.dirname(path))):
                phase["p"] = name
                fd = os.open(d, os.O_RDONLY | os.O_DIRECTORY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
            phase["p"] = "rmdir"
            try:
                (s.rmdir if s is not None else os.rmdir)(path)
            except FileNotFoundError:
                pass
            if run_journal is not None:
                run_journal.note_write(path)

        done, _, e = _run_bounded(work, self.rmdir_timeout_s, "staging-rmdir")
        if done:
            return None if e is None else f"{phase['p']} failed: {e}"
        why = (f"removal has not returned in {self.rmdir_timeout_s}s (stuck "
               f"in the {phase['p']})")
        if s is not None:
            # Closed whatever hung: a thread that gets past a slow fsync later
            # must not send its rmdir through a session this run is done with.
            global removal_session
            self._aborted = s
            if removal_session is s:
                removal_session = None
            # Logged before the abort, which kills the removal process.
            if phase["p"] == "rmdir":
                logging.warning(f"Removing staging directory {path}: {why}. "
                                f"{_stuck_session_advice(s)}")
            else:
                logging.warning(f"Removing staging directory {path}: {why} "
                                f"on the kernel mount; removal session closed.")
            why += f"; removal session client.{s.ident} was closed"
            _abort_session_bounded(s)
        return why


staging = None


# Staging directories are removed through a second CephFS session (libcephfs),
# never the kernel mount, whose own caps the MDS may be recalling. Its mount is
# in a child process (_RemovalProcess): in 18.2.8 an in-process abort_conn()
# waits for a hung call while the session keeps renewing, but killing the child
# ends the call and lets the MDS drop the session at session_autoclose, or at
# once on an eviction. No second session, no staging (see main()).
REMOVAL_SESSION_MOUNT_TIMEOUT_S = 30
REMOVAL_SESSION_CLOSE_TIMEOUT_S = 10
# An abort kills the removal process, which returns at once; still bounded, as
# the reap of a killed child.
REMOVAL_SESSION_ABORT_TIMEOUT_S = 10

removal_session = None


def _conf_fsid(path):
    """The fsid a ceph.conf names, or None."""
    try:
        with open(path) as fh:
            for line in fh:
                m = re.match(r"\s*fsid\s*=\s*([0-9a-fA-F-]{36})\s*$", line)
                if m:
                    return m.group(1).lower()
    except OSError:
        pass
    return None


_IPV4_RE = re.compile(r"(?<![\d.])(\d{1,3}(?:\.\d{1,3}){3})(?![\d.])")


def _conf_mon_ips(path):
    """IPv4 addresses on a ceph.conf's mon_host / mon host line."""
    try:
        with open(path) as fh:
            for line in fh:
                if re.match(r"\s*mon[ _]host\s*=", line):
                    return set(_IPV4_RE.findall(line))
    except OSError:
        pass
    return set()


def _mount_mon_ips(src):
    """IPv4 addresses of the monitors a kernel CephFS mount source names."""
    head = src.rsplit(":/", 1)[0] if ":/" in src else ""
    return set(_IPV4_RE.findall(head))


def _ceph_mount_for(path, mounts="/proc/self/mounts"):
    """(mountpoint, source, options dict) of the CephFS kernel mount holding
    path, or None.

    Read from the mount table, not findmnt --target: for an autofs-managed
    volume findmnt answers with the autofs parent (/shared/ceph) rather than
    the CephFS mount stacked on it (/shared/ceph/myvol).
    """
    def unescape(s):
        return re.sub(r"\\([0-7]{3})", lambda m: chr(int(m.group(1), 8)), s)
    p = os.path.realpath(path)
    best = None
    try:
        with open(mounts) as fh:
            for line in fh:
                f = line.split()
                if len(f) < 4 or f[2] != "ceph":
                    continue
                tgt = unescape(f[1])
                if p == tgt or p.startswith(tgt.rstrip("/") + "/"):
                    if best is None or len(tgt) > len(best[0]):
                        best = (tgt, unescape(f[0]), f[3])
    except OSError:
        return None
    if best is None:
        return None
    opts = {}
    for o in best[2].split(","):
        k, _, v = o.partition("=")
        if k:
            opts[k] = v
    return best[0], best[1], opts


def _source_subtree(src):
    """The fs-relative subtree a kernel CephFS mount source names: after the
    last ':/' (mons:/sub) or '=/' (name@fsid.fs=/sub)."""
    for sep in ("=", ":"):
        i = src.rfind(sep + "/")
        if i >= 0:
            return src[i + len(sep):] or "/"
    return "/"


class _CephfsErrors:
    """The cephfs module's exceptions, as the removal process reports them."""

    class Error(Exception):
        def __init__(self, errno_=None, msg=""):
            super().__init__(msg)
            self.errno = errno_

    class ObjectNotFound(Error):
        pass


# Tests point the removal process at a fake cephfs module through this.
_removal_worker_env = None


class _RemovalProcess:
    """A libcephfs mount in a child process: what RemovalSession calls as its
    LibCephFS. One JSON request and reply per line over the child's stdin and
    stdout, one call at a time.

    A call stuck in the MDS blocks only the thread waiting on its reply, and
    abort_conn() ends it by killing the child -- which an in-process
    abort_conn() cannot do, since in 18.2.8 it waits for the call. The child
    ignores the exit signals, so a SIGTERM to the job's cgroup still lets the
    job's own end-of-run removal through it, and exits when its stdin closes.
    """

    def __init__(self, conf, conffile, auth_id, fsname, timeout):
        self._p = subprocess.Popen(
            [sys.executable, os.path.abspath(__file__), "--removal-worker",
             str(os.getpid())],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, env=_removal_worker_env,
            close_fds=True, start_new_session=True)
        self.pid = self._p.pid
        self._lock = threading.Lock()
        try:
            self.ident = self._call({"op": "mount", "conf": conf,
                                     "conffile": conffile, "auth_id": auth_id,
                                     "fsname": fsname}, timeout)["ident"]
        except BaseException:
            self.abort_conn()
            raise

    def _call(self, req, timeout=None):
        with self._lock:
            try:
                self._p.stdin.write((json.dumps(req) + "\n").encode("utf-8"))
                self._p.stdin.flush()
                if timeout is not None:
                    ready, _, _ = select.select([self._p.stdout], [], [], timeout)
                    if not ready:
                        self.abort_conn()
                        raise RuntimeError(f"the removal process has not "
                                           f"answered in {timeout}s")
                line = self._p.stdout.readline()
            except (OSError, ValueError):
                line = b""
        if not line:
            raise _CephfsErrors.Error(errno.ENOTCONN, "the removal process "
                                      "has exited")
        reply = json.loads(line)
        if reply.get("err") == "notfound":
            raise _CephfsErrors.ObjectNotFound(errno.ENOENT, reply.get("msg", ""))
        if reply.get("err") == "import":
            raise RuntimeError("the CephFS Python bindings are missing ('import "
                               "cephfs'; dnf install python3-cephfs): %s"
                               % reply.get("msg", ""))
        if reply.get("err"):
            raise _CephfsErrors.Error(reply.get("errno"), reply.get("msg", ""))
        return reply

    def get_instance_id(self):
        return self.ident

    def stat(self, path):
        # Bounded like the mount: a getattr can wait on a cap recall too, and
        # it runs on the main thread at startup.
        ino = self._call({"op": "stat", "path": os.fsdecode(path)},
                         REMOVAL_SESSION_MOUNT_TIMEOUT_S)["ino"]
        return os.stat_result([0, ino] + [0] * 8)

    def rmdir(self, path):
        self._call({"op": "rmdir", "path": os.fsdecode(path)})

    def unlink(self, path):
        self._call({"op": "unlink", "path": os.fsdecode(path)})

    def abort_conn(self):
        """Kill the child: a call it is stuck in ends with it."""
        try:
            self._p.kill()
        except OSError:
            pass
        try:
            self._p.wait(REMOVAL_SESSION_ABORT_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            pass

    def unmount(self):
        self._call({"op": "quit"}, REMOVAL_SESSION_CLOSE_TIMEOUT_S)

    def shutdown(self):
        try:
            self._p.wait(REMOVAL_SESSION_CLOSE_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            self.abort_conn()


def _die_with_parent(ppid):
    """Exit when the job does, even in the middle of a libcephfs call, so a
    request stuck in the MDS goes with it: PR_SET_PDEATHSIG where there is a
    prctl (Linux), and a watcher thread either way. A killed job (kill -9, the
    OOM killer, a second SIGINT) would otherwise leave the child renewing its
    session and the request stuck, with nobody watching."""
    try:
        ctypes.CDLL(None, use_errno=True).prctl(1, signal.SIGKILL, 0, 0, 0)
    except (AttributeError, OSError):          # 1 = PR_SET_PDEATHSIG
        pass
    if os.getppid() != ppid:                   # it died before the prctl
        os._exit(1)

    def watch():
        while os.getppid() == ppid:
            time.sleep(1)
        os._exit(1)
    threading.Thread(target=watch, name="parent-watch", daemon=True).start()


def _removal_worker(ppid):
    """--removal-worker PID (internal): serve _RemovalProcess's requests on
    stdin and stdout through one libcephfs mount, until stdin closes or job
    PID exits."""
    for sig in _EXIT_SIGNALS:
        signal.signal(sig, signal.SIG_IGN)
    _die_with_parent(ppid)
    cephfs = fs = None
    out = sys.stdout.buffer
    for line in sys.stdin.buffer:
        req = json.loads(line)
        op = req.get("op")
        try:
            if op == "mount":
                try:
                    import cephfs
                except ImportError as e:
                    reply = {"err": "import", "msg": str(e)}
                else:
                    fs = cephfs.LibCephFS(conf=req["conf"],
                                          conffile=req.get("conffile") or None,
                                          auth_id=req["auth_id"])
                    fs.mount(mount_root=b"/",
                             filesystem_name=req["fsname"].encode("utf-8"))
                    reply = {"ident": fs.get_instance_id()}
            elif op == "stat":
                reply = {"ino": fs.stat(os.fsencode(req["path"])).st_ino}
            elif op in ("rmdir", "unlink"):
                getattr(fs, op)(os.fsencode(req["path"]))
                reply = {}
            elif op == "quit":
                if fs is not None:
                    fs.unmount()
                    fs.shutdown()
                out.write(b"{}\n")
                out.flush()
                return 0
            else:
                reply = {"err": "error", "msg": f"unknown op {op!r}"}
        except Exception as e:
            notfound = (cephfs is not None
                        and isinstance(e, getattr(cephfs, "ObjectNotFound", ())))
            reply = {"err": "notfound" if notfound else "error",
                     "errno": getattr(e, "errno", None), "msg": str(e)}
        out.write((json.dumps(reply) + "\n").encode("utf-8"))
        out.flush()
    # stdin closed: the job has gone without a quit.
    if fs is not None:
        try:
            fs.unmount()
            fs.shutdown()
        except Exception:
            pass
    return 0


class RemovalSession:
    """A libcephfs client session that staging directories are removed through.

    The path a staging directory has on the kernel mount is mapped to the path
    it has in the filesystem (the mount may be of a subtree), and the mapping is
    proved at open by stat()ing the mount root both ways.
    """

    def __init__(self, fs, cephfs_mod, fsname, mountpoint, subtree, ident, who):
        self._fs = fs
        self._cephfs = cephfs_mod
        self.fsname = fsname
        self.mountpoint = mountpoint
        self.subtree = subtree
        self.ident = ident
        self.who = who
        self._dead = False

    @staticmethod
    def _identity(args, root, opts, src=""):
        """(conffile, conf overrides, client name) for the session.

        An explicit --rados-name with --rados-keyring wins: that is how the
        operator chose to reach the cluster. Otherwise the kernel mount's own
        name= is used -- the copier's privileges, no more -- with the keyring
        of the cluster whose fsid the mount names (several clusters' keyrings
        for the same name live side by side in /etc/ceph).
        """
        import glob
        if args.rados_name and args.rados_keyring:
            conffile, over = _ceph_connect_args(args.rados_conffile, root,
                                                args.rados_mon_host)
            over = dict(over)
            over["keyring"] = args.rados_keyring
            return conffile, over, args.rados_name
        name = opts.get("name") or "guest"
        fsid = (opts.get("fsid") or "").lower()
        if fsid.strip("0-") == "":
            # mount.ceph without a readable conf reports an all-zero fsid; the
            # monitors in the mount source still say which cluster it is.
            fsid = ""
        mons = _mount_mon_ips(src)
        for conf in sorted(glob.glob("/etc/ceph/*.conf")):
            if fsid:
                if _conf_fsid(conf) != fsid:
                    continue
            elif not mons or not (mons & _conf_mon_ips(conf)):
                continue
            stem = os.path.basename(conf)[:-len(".conf")]
            kr = f"/etc/ceph/{stem}.client.{name}.keyring"
            if os.path.exists(kr):
                return conf, {"keyring": kr}, "client." + name
        raise ValueError(
            f"no ceph.conf in /etc/ceph matches this mount (fsid "
            f"{fsid or 'unknown'}, monitors {','.join(sorted(mons)) or 'unknown'}) "
            f"with a client.{name} keyring beside it; pass --rados-name and "
            f"--rados-keyring")

    @classmethod
    def open(cls, args, root):
        """(session, None), or (None, why) when no second session can be had."""
        try:
            os.stat(root)        # an idle autofs volume is mounted on access
        except OSError as e:
            return None, f"{root}: {e}"
        m = _ceph_mount_for(root)
        if m is None:
            # An idle autofs volume does not always mount on stat() alone.
            try:
                os.listdir(root)
            except OSError:
                pass
            m = _ceph_mount_for(root)
        if m is None:
            return None, f"{root} is not on a CephFS kernel mount"
        mp, src, opts = m
        subtree = _source_subtree(src)
        fsname = opts.get("mds_namespace") or opts.get("fs")
        if not fsname:
            # New-style source: name@fsid.fsname=/subtree
            m = re.match(r"^[^@]*@[^.]*\.([^=]+)=", src)
            fsname = m.group(1) if m else None
        if not fsname:
            return None, (f"cannot tell which filesystem {mp} is: no "
                          f"mds_namespace= or fs= in its mount options")
        try:
            conffile, conf, who = cls._identity(args, root, opts, src)
        except (ValueError, SystemExit) as e:
            return None, str(e)
        conf = dict(conf)
        conf.update({"client_mount_timeout": str(REMOVAL_SESSION_MOUNT_TIMEOUT_S),
                     "log_to_stderr": "false", "err_to_stderr": "false",
                     "admin_socket": ""})
        auth_id = who[len("client."):] if who.startswith("client.") else who
        try:
            fs = _RemovalProcess(conf, conffile or None, auth_id, fsname,
                                 REMOVAL_SESSION_MOUNT_TIMEOUT_S + 10)
        except Exception as e:
            return None, (f"libcephfs mount of filesystem {fsname} as {who} "
                          f"failed: {e}")
        s = cls(fs, _CephfsErrors, fsname, mp, subtree, fs.get_instance_id(), who)
        # The path mapping is derived, so prove it: one inode, seen both ways.
        try:
            same = s.ino(root) == os.stat(root).st_ino
        except Exception as e:
            s.abort()
            return None, f"cannot map {root} into filesystem {fsname}: {e}"
        if not same:
            s.abort()
            return None, (f"{root} maps to {s.fs_path(root)} in filesystem "
                          f"{fsname}, a different inode")
        return s, None

    def fs_path(self, local):
        """local's path inside the filesystem the session mounted.

        Matched textually first: that costs no lookups, and every staging
        directory sits under a root already proved to be on the mount. A
        path reached through a symlink (a --tmpdir of /data/tmp linked onto
        CephFS) is resolved and tried again, the way _ceph_mount_for()
        finds the mount."""
        for p in (os.path.abspath(local), None):
            if p is None:
                p = os.path.realpath(local)
            rel = os.path.relpath(p, self.mountpoint)
            if not (rel == os.pardir or rel.startswith(os.pardir + os.sep)):
                return os.path.normpath(os.path.join(self.subtree, rel))
        raise OSError(errno.EXDEV, f"not under {self.mountpoint}, the mount "
                      f"the removal session maps", local)

    def ino(self, local):
        """local's inode number as this session sees it."""
        return self._fs.stat(os.fsencode(self.fs_path(local))).st_ino

    def rmdir(self, local):
        """rmdir through this session, raising the builtin OSErrors."""
        self._call(self._fs.rmdir, local)

    def unlink(self, local):
        """unlink through this session, raising the builtin OSErrors."""
        self._call(self._fs.unlink, local)

    def _call(self, fn, local):
        if self._dead:
            raise OSError(errno.ENOTCONN,
                          f"removal session client.{self.ident} was aborted",
                          local)
        try:
            fn(os.fsencode(self.fs_path(local)))
        except self._cephfs.ObjectNotFound:
            raise FileNotFoundError(errno.ENOENT, os.strerror(errno.ENOENT),
                                    local) from None
        except self._cephfs.Error as e:
            raise OSError(getattr(e, "errno", None) or errno.EIO, str(e),
                          local) from None

    def abort(self):
        """Mark the session dead, then abort_conn(): kill the removal process,
        which ends a call stuck in it. Its session stops renewing, so the MDS
        closes it -- cancelling the stuck request -- once it goes stale, or at
        once on an eviction."""
        self._dead = True
        try:
            self._fs.abort_conn()
        except Exception:
            logging.exception(f"abort of removal session client.{self.ident} "
                              f"failed")

    def close(self):
        """Unmount at the end of a run, bounded: an aborted session is left."""
        if self._dead:
            return
        self._dead = True

        def work():
            try:
                self._fs.unmount()
                self._fs.shutdown()
            except Exception:
                logging.exception(f"closing removal session "
                                  f"client.{self.ident} failed")
        t = threading.Thread(target=work, name="removal-session-close",
                             daemon=True)
        t.start()
        t.join(REMOVAL_SESSION_CLOSE_TIMEOUT_S)


def _new_removal_session(args):
    """A new removal session for args.dirs[0]'s mount; raise RuntimeError(why)."""
    s, why = RemovalSession.open(args, args.dirs[0])
    if why:
        raise RuntimeError(why)
    return s


def _open_removal_session(args):
    """Open the module's removal session; raise RuntimeError(why)."""
    global removal_session
    removal_session = None
    removal_session = _new_removal_session(args)
    return removal_session


def _stuck_session_advice(s):
    """What clears a request stuck on removal session s, for a log line."""
    return (f"client.{s.ident}'s removal process is killed; the MDS drops "
            f"its request at session_autoclose. To clear it now: 'ceph tell "
            f"mds.{s.fsname}:<rank> client evict id={s.ident}' (find <rank> "
            f"with 'ceph tell mds.{s.fsname}:<rank> client ls id={s.ident}'; "
            f"the job's kernel mount is a different client).")


def _run_bounded(fn, timeout, name):
    """fn() in a throwaway daemon thread joined with timeout.

    (True, value, None) or (True, None, exception) when it returned in time,
    else (False, None, None): a call still running is left to finish or not
    on its own, and whatever it returns later is dropped."""
    out = {}

    def work():
        try:
            out["v"] = fn()
        except Exception as e:
            out["e"] = e
    t = threading.Thread(target=work, name=name, daemon=True)
    t.start()
    t.join(timeout)
    if t.is_alive():
        return False, None, None
    return True, out.get("v"), out.get("e")


def _abort_session_bounded(s):
    """s.abort(), bounded by REMOVAL_SESSION_ABORT_TIMEOUT_S; False if it is
    still running. RemovalSession.abort() marks the session dead before it
    calls abort_conn(), so nothing removes through it again either way."""
    done, _, e = _run_bounded(s.abort, REMOVAL_SESSION_ABORT_TIMEOUT_S,
                              "removal-session-abort")
    if not done:
        logging.error(f"Abort of removal session client.{s.ident} not done "
                      f"after {REMOVAL_SESSION_ABORT_TIMEOUT_S}s; nothing more "
                      f"is removed through it")
    elif e is not None:
        logging.error(f"Aborting removal session client.{s.ident} failed: {e}")
    return done


def _session_mismatch(s, path):
    """None if the removal session reaches path as the inode the kernel mount
    shows for it, else why not."""
    try:
        mapped = s.fs_path(path)
        same = s.ino(path) == os.stat(path).st_ino
    except Exception as e:
        return (f"{path} cannot be reached through the removal session, which "
                f"maps {s.mountpoint}: {e}")
    if not same:
        return (f"{path} maps to {mapped} in filesystem {s.fsname}, which is a "
                f"different inode")
    return None


def start_removal_session(args):
    """Open the removal session a staging run or --reap-staging needs, or exit.

    Staging, and reaping its leftovers, remove directories only through a
    second CephFS session. No session, no staging: an rmdir through the
    kernel mount the copies use can deadlock against that client's own caps.
    """
    global removal_session
    if not (args.reap_staging or (args.target_pool and not args.stage_in_tmpdir)):
        return None
    try:
        s = _open_removal_session(args)
    except RuntimeError as e:
        logging.error(
            "Refusing to start: %s needs a separate CephFS session, and none "
            "could be opened: %s"
            % ("--reap-staging" if args.reap_staging else "--target-pool staging",
               e))
        sys.exit(1)
    # The session maps one kernel mount, the one args.dirs[0] is on. A root on
    # another mount would have every staging directory under it fail to be
    # removed, one WARNING each, for the whole run.
    for d in args.dirs[1:]:
        why = _session_mismatch(s, os.path.abspath(d))
        if why:
            logging.error(f"Refusing to start: {why}. All directories must "
                          f"be on {s.mountpoint}; run one job per mount")
            s.close()
            removal_session = None
            sys.exit(1)
    logging.info(f"Removal session: libcephfs client.{s.ident} as {s.who}, "
                 f"filesystem {s.fsname}, {s.mountpoint} = {s.subtree}")
    return s


def _rmdir_staging(path, s):
    """rmdir a staging directory through removal session s -- never through
    the kernel mount, whose own caps the MDS would wait on (see the removal
    session above)."""
    if s is None:
        raise OSError(errno.ENOTCONN, "no separate CephFS session for staging "
                      "removals", path)
    s.rmdir(path)


def _bounded_rmdir_staging(path, timeout=None, unlink=()):
    """rmdir a staging directory, after unlinking the paths in unlink, all
    through the removal session, in a throwaway thread joined with a timeout,
    so a stuck request cannot block --reap-staging.

    Returns "ok", "gone" (already absent), "error" (logged), or "hung". On a
    hang the session is aborted, so nothing more is removed through it.
    """
    global removal_session
    timeout = STAGE_RMDIR_TIMEOUT_S if timeout is None else timeout
    result = {}
    # Captured before the thread starts: a hang is handled on the session the
    # rmdir went out on, not on whatever replaced it since.
    s = removal_session

    def work():
        try:
            for f in unlink:
                if s is None:
                    break
                try:
                    s.unlink(f)
                except FileNotFoundError:
                    pass
            _rmdir_staging(path, s)
            result["r"] = "ok"
        except FileNotFoundError:
            result["r"] = "gone"
        except OSError as e:
            result["r"] = "error"
            result["e"] = e
    t = threading.Thread(target=work, name="staging-rmdir", daemon=True)
    t.start()
    t.join(timeout)
    r = result.get("r")
    if r == "error":
        logging.warning(f"Could not remove staging directory {path}: "
                        f"{result['e']}")
    if r is not None:
        return r
    if s is not None and s is not removal_session:
        logging.warning(
            f"Removing staging directory {path} through already-aborted "
            f"client.{s.ident}: no return in {timeout}s; left on disk")
    elif s is not None:
        removal_session = None
        # Logged before the abort, which kills the removal process.
        logging.critical(
            f"Removing staging directory {path} through client.{s.ident}: "
            f"no return in {timeout}s; no more removals this run. "
            f"{_stuck_session_advice(s)}")
        _abort_session_bounded(s)
    return "hung"


def _group_spill_dir(args):
    """Local disk for group_by_directory's sorted runs: beside the log, which
    is where the lists live, else the system temp dir. Never the CephFS volume."""
    if getattr(args, "log_file", None):
        return os.path.dirname(os.path.abspath(args.log_file))
    return tempfile.gettempdir()


def tmpdir_conflicts(args, tmpdir_pool):
    """(dir, pool) for each root whose target pool is the tmpdir's pool.

    Both reasons for the check are about temps staged IN the tmpdir. A staged
    --target-pool run never puts one there: its temps are born in the run's
    staging directory, a no-op rewrite is already skipped as a layout match, and the
    read-back in _verify_layout() is what catches a layout that did not take.
    And its usual target, the replicated pool, IS the default pool the tmpdir
    is supposed to be on, so checking it would refuse the main use case.
    """
    if getattr(args, "target_pool", None) and not args.stage_in_tmpdir:
        return []
    out = []
    for d in args.dirs:
        target = target_layout(args, get_layout_walking_up(d))
        if target is not None and target.pool == tmpdir_pool:
            out.append((d, target.pool))
    return out


def _reap_pace():
    """Hold for the thread limit's pause and the file delay between steps of
    --reap-staging (walk included) and --paths-from-pool's path checks; False
    once the run is to stop."""
    if thread_count is not None:
        if not thread_count.acquire(cancel=do_exit.is_set, tick=_poll_config):
            return False
        thread_count.release()
    if file_delay_ms > 0:
        _file_delay()
    else:
        _poll_config()
    return not do_exit.is_set()


def _staging_candidates(args):
    """Staging directories for --reap-staging, each once.

    With --paths-from, each listed path is a directory whose staging
    subdirectories are checked -- one readdir, no walk -- or is a staging
    directory itself. Without it, the given directories are walked, which on a
    large volume is a full tree walk; a list built from a run's log is the
    cheap way.
    """
    roots = [os.path.abspath(d) for d in args.dirs]

    def inside(p):
        return any(p == r or p.startswith(r.rstrip("/") + "/") for r in roots)

    seen = set()
    if args.paths_from:
        for p in _iter_listed_paths(args.paths_from):
            if not _reap_pace():
                return
            p = os.path.abspath(p)
            if not inside(p):
                logging.warning(f"{p} is outside the directories given; skipped")
                continue
            if STAGE_RE.match(os.path.basename(p)):
                if p not in seen:
                    seen.add(p)
                    yield p
                continue
            try:
                it = os.scandir(p)
            except OSError:
                continue
            with it:
                for e in it:
                    if (STAGE_RE.match(e.name) and e.path not in seen
                            and e.is_dir(follow_symlinks=False)):
                        seen.add(e.path)
                        yield e.path
        return
    for r in roots:
        for dirpath, dirnames, _ in os.walk(r):
            if not _reap_pace():
                return
            keep = []
            for d in dirnames:
                if STAGE_RE.match(d):
                    yield os.path.join(dirpath, d)
                else:
                    keep.append(d)
            dirnames[:] = keep


def reap_staging(args):
    """--reap-staging: remove leftover staging directories that are empty,
    through the removal session; report the rest. Returns an exit status.

    Each removal moves its artifact's rctime, so it is journaled the way the
    walk journals an orphan it reclaims, and the journal is written on every
    way out."""
    roots = [os.path.abspath(d) for d in args.dirs]
    try:
        return _reap_staging(args)
    finally:
        if run_journal is not None:
            run_journal.write(roots, args)


def _reap_staging(args):
    found = removed = gone = errors = young = 0
    nonempty = []
    min_age = getattr(args, "reap_staging_min_age", REAP_STAGING_MIN_AGE_S)
    for sd in _staging_candidates(args):
        if do_exit.is_set():
            break
        found += 1
        # Paced and paused like a walk: the thread limit's pause (the
        # regulator's, or threads = 0) holds here, and the file delay spaces
        # removals. A reap is MDS load like a walk, and an unpaced one can
        # push a volume's MDS latency far past the regulator's target.
        if thread_count is not None and not thread_count.acquire(
                cancel=do_exit.is_set, tick=_poll_config):
            break
        try:
            if file_delay_ms > 0:
                _file_delay()
            if do_exit.is_set():
                break
            r = _reap_one(sd, min_age, nonempty)
        finally:
            if thread_count is not None:
                thread_count.release()
        if r == "ok":
            removed += 1
        elif r == "young":
            young += 1
        elif r == "gone":
            gone += 1
        elif r == "error":
            errors += 1
        elif r == "hung":                       # the session is aborted
            logging.error(f"Stopped after a hung removal: {removed} removed, "
                          f"{found - removed} not")
            return 1
    logging.info(f"--reap-staging: {found} staging directories found, "
                 f"{removed} removed, {len(nonempty)} not empty and left alone, "
                 f"{young} changed in the last {min_age}s and left alone, "
                 f"{gone} already gone, {errors} errors"
                 + ("; stopped by a signal" if do_exit.is_set() else ""))
    return 1 if errors else 0


def _reap_one(sd, min_age, nonempty):
    """Remove one staging directory if it is ours to take: "ok", "young",
    "nonempty", "gone", "error" or "hung".

    Ours means quiet for min_age: nothing created, renamed or unlinked in it
    (its ctime), and each temp left in it unwritten as long (its own mtime and
    ctime: a copy in progress moves those, not the directory's). Those temps
    are a dead job's, and are unlinked with the rmdir, through the session."""
    now = time.time()
    try:
        st = os.lstat(sd)
        entries = os.listdir(sd)
    except FileNotFoundError:
        return "gone"
    except OSError as e:
        logging.warning(f"Cannot read staging directory {sd}: {e}")
        return "error"
    if now - st.st_ctime < min_age:
        logging.info(f"Leaving staging directory {sd}: changed "
                     f"{now - st.st_ctime:.0f}s ago, under {min_age}s, so a "
                     f"job may still be using it")
        return "young"
    temps, other = [], []
    for e in entries:
        p = os.path.join(sd, e)
        if TMP_RE.match(e):
            try:
                t = os.lstat(p)
            except FileNotFoundError:
                continue
            except OSError:
                t = None
            if (t is not None and stat.S_ISREG(t.st_mode)
                    and now - max(t.st_mtime, t.st_ctime) >= min_age):
                temps.append(p)
                continue
        other.append(e)
    if other:
        nonempty.append(sd)
        logging.info(f"Leaving staging directory {sd}: not empty "
                     f"({len(other)} entries, e.g. {sorted(other)[0]})")
        return "nonempty"
    if run_journal is not None:
        # First sighting before the rmdir, so pre_rctime is still the
        # owner's; note_write() records nothing for an unseen artifact.
        run_journal.note_dir(os.path.dirname(sd))
    r = _bounded_rmdir_staging(sd, unlink=temps)
    if r == "ok" and run_journal is not None:
        run_journal.note_write(sd)
    return r


def _tmp_path_for(args, target):
    """Where to stage the temp copy for `target`.

    Beside the target, so the later rename is intra-directory -- see the header.
    Hidden and suffixed so an orphan left behind by a hard kill is recognizable.
    """
    if args.stage_in_tmpdir:
        return os.path.join(args.tmpdir, uuid.uuid4().hex)
    return os.path.join(os.path.dirname(target),
                        f".{uuid.uuid4().hex}{TMP_SUFFIX}")


def _apply_and_verify_layout(layout, path):
    """Apply the layout, then read it back and confirm it took.

    A temp in a shared --tmpdir on the default pool shows a failed
    apply_file() implicitly, by staying visibly in the wrong pool. A sibling
    temp file inherits the target directory's layout, so that signal is absent
    and the check has to be explicit -- which is the stronger of the two.
    """
    layout.apply_file(path)
    _verify_layout(layout, path)


def _verify_layout(layout, path):
    got = CephLayout.from_file(path)
    if got != layout:
        raise RuntimeError(
            f"layout did not apply to {path}: wanted {layout}, read back {got}")


def _undo_partial_relink(args, filepaths, st, new_id, link_tmps, err):
    """Rejoin a hard-link set after a rename failed partway through it.

    err is the rename's exception, for the log. Returns (names on the new
    inode, whose directories changed; whether the replace in fact completed,
    i.e. every name is on the new inode). Moved names are read from their
    inodes, since a rename can take effect and still raise, and each is linked
    back to the original. Unrenamed staged links are removed. A set left split
    is logged name by name.
    """
    for link_tmp, _ in link_tmps:
        try:
            os.unlink(link_tmp)
        except OSError:
            pass

    orig_id = (st.st_dev, st.st_ino)

    def _on(ident):
        out = []
        for path in filepaths:
            try:
                s = os.lstat(path)
            except OSError:
                continue
            if (s.st_dev, s.st_ino) == ident:
                out.append(path)
        return out

    moved = _on(new_id)
    if not moved:
        return moved, False
    if len(moved) == len(filepaths):
        logging.warning(
            f"A rename in the replace of {filepaths[0]} raised ({err}), but "
            f"all {len(filepaths)} name(s) are on the new inode; the replace "
            "completed")
        return moved, True
    try:
        src = next(iter(_on(orig_id)), None)
        if src is None:
            raise FileNotFoundError("no name still holds the original inode")
        for path in moved:
            stage = _tmp_path_for(args, path)
            os.link(src, stage, follow_symlinks=False)
            try:
                os.rename(stage, path)
            except Exception:
                try:
                    os.unlink(stage)
                except OSError:
                    pass
                raise
        if len(_on(orig_id)) != len(filepaths):
            raise RuntimeError("not every name is on the original inode")
    except Exception as e:
        orig, new = _on(orig_id), _on(new_id)
        other = [p for p in filepaths if p not in orig and p not in new]
        logging.error(
            f"Hard links of {filepaths[0]} are SPLIT: "
            f"{', '.join(new) or 'none'} on the new inode, "
            f"{', '.join(orig) or 'none'} on the original"
            + (f", {', '.join(other)} on neither" if other else "")
            + f", after a rename raised ({err}). Could not rejoin them "
            f"({e}); relink by hand.")
        return moved, False
    logging.warning(
        f"A rename in the replace of {filepaths[0]} raised ({err}); all "
        f"{len(filepaths)} names are back on the original inode")
    return moved, False


def process_file(args, filepaths, st, layout, file_layout, stage=False):
    # Every target whose directory this call changes. A sibling temp, a staged
    # link and a swept orphan move that directory's rctime as surely as the
    # replace does, and they happen before the dry-run, lock-skip,
    # source-changed and error exits. So each is claimed in the journal after
    # the call's last change, whether or not the replace went through.
    #
    # stage: the file's directory does not point at layout's pool (only ever
    # under --target-pool), so the temp is made in the run's staging directory.
    touched = set()
    try:
        stage_dir = None
        if stage and staging is not None and not do_exit.is_set():
            stage_dir = staging.path_for(os.path.dirname(filepaths[0]), layout)
            # The temp moves the staging directory's rctime, and so its base's.
            touched.add(stage_dir)
        try:
            _replace_file(args, filepaths, st, layout, file_layout, touched,
                          stage_dir)
        except OSError as e:
            again = _exdev_restage(e, stage_dir, filepaths[0], layout)
            if again is None:
                raise
            # The retry is the same file: counted once.
            with stats._lock:
                stats.files_submitted -= 1
            touched.add(again)
            _replace_file(args, filepaths, st, layout, file_layout, touched,
                          again)
    finally:
        if run_journal is not None:
            for path in touched:
                run_journal.note_write(path)


def _exdev_restage(err, stage_dir, target, layout):
    """The staging directory to retry target's copy in after err, or None.

    Only a staged copy whose rename crossed a quota realm (EXDEV) is retried,
    once, staged under that realm's root."""
    if stage_dir is None or getattr(err, "errno", None) != errno.EXDEV:
        return None
    if staging.note_exdev(os.path.dirname(target)) is None:
        return None
    again = staging.path_for(os.path.dirname(target), layout)
    if again == stage_dir:
        return None
    logging.info(f"Retrying {target} staged in {again}: {err}")
    return again


def _replace_file(args, filepaths, st, layout, file_layout, touched,
                  stage_dir=None):
    if do_exit.is_set():
        return

    if not args.stage_in_tmpdir:
        # Covers both the walk and --paths-from, which has no walk to hook.
        if reclaim_orphans(os.path.dirname(filepaths[0])):
            touched.add(filepaths[0])

    if stage_dir is not None:
        tmp_file = os.path.join(stage_dir, f".{uuid.uuid4().hex}{TMP_SUFFIX}")
    else:
        tmp_file = _tmp_path_for(args, filepaths[0])

    if len(filepaths) == 1:
        logging.info(
            f"Transcoding {filepaths[0]} [{st.st_size} bytes]: {file_layout.diff(layout)}"
        )
    else:
        logging.info(
            f"Transcoding {filepaths[0]} [{st.st_size} bytes] (+ {len(filepaths) - 1} hardlink(s)): {file_layout.diff(layout)} [{tmp_file}]"
        )

    try:
        try:
            ofd = open(tmp_file, "wb")
        except FileNotFoundError:
            if stage_dir is None:
                raise
            staging.recreate(os.path.dirname(filepaths[0]), layout)
            ofd = open(tmp_file, "wb")
        with ofd:
            # A staged temp is in the run's staging directory, so the file's
            # own directory changes only at the rename.
            if not args.stage_in_tmpdir and stage_dir is None:
                touched.add(filepaths[0])
            tmp_st = os.fstat(ofd.fileno())
            if stage_dir is None:
                _apply_and_verify_layout(layout, tmp_file)
            else:
                # Born in the target via the staging directory's layout, so
                # there is nothing to apply -- applying would record an old
                # pool. Reading it back is still the check that matters.
                _verify_layout(layout, tmp_file)
            with open(filepaths[0], "rb") as ifd:
                with stats._lock:
                    stats.files_submitted += 1

                # Take a shared (read) lock on the source file. This is
                # ADVISORY and weaker than it looks: it conflicts only with a
                # writer that voluntarily takes LOCK_EX, and an ordinary
                # appender takes no lock at all, so this succeeds while the
                # file is being written. What actually protects the data is
                # the mtime/ctime/size re-check before the rename below --
                # that is the guard to preserve, not this one. Still worth
                # taking: it cheaply excludes well-behaved writers before a
                # pointless copy.
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

        # chown first: it clears setuid/setgid and drops security.capability,
        # so a chown after copystat would strip what copystat had just copied.
        os.chown(tmp_file, st.st_uid, st.st_gid)
        shutil.copystat(filepaths[0], tmp_file, follow_symlinks=False)

        # No per-copy rate or copy method: copy_file_range (server-side copy)
        # is not measurably faster than a userspace copy, and per-file rates
        # swing 30x on identically sized files because they track OSD and MDS
        # contention rather than the copy path.
        #
        # The "[N bytes] in Xs" shape is fixed: log mining keys on it, across
        # years of archived runs.
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
            # Block the exit signals in this thread to reduce the chance of
            # EINTR during the rename sequence.  Note: Python's signal handler
            # runs on the main thread regardless, so this is primarily
            # belt-and-suspenders.
            signal.pthread_sigmask(signal.SIG_BLOCK, _EXIT_SIGNALS)
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

            # The file's mtime is kept (copystat): archive_* and rsync key on
            # it. The parent directory's is not restored: os.utime() moves its
            # ctime, and so rctime, anyway; nothing reads directory mtime; and
            # setting it takes an exclusive MDS cap that stalls walkers.
            #
            # Stage EVERY extra hard link, linked from tmp_file, before renaming
            # anything, so a failed link leaves the set whole. Each link's temp
            # sits in that link's own directory, so its rename stays there.
            link_tmps = []
            try:
                for path in filepaths[1:]:
                    link_tmp = _tmp_path_for(args, path)
                    logging.info(f"Linking {tmp_file} -> {path}")
                    os.link(tmp_file, link_tmp, follow_symlinks=False)
                    link_tmps.append((link_tmp, path))
                    if not args.stage_in_tmpdir:
                        touched.add(path)
            except Exception:
                # Clean up the links already staged. Leaving them to the
                # orphan reclaimer works, but only after
                # TMP_ORPHAN_MIN_AGE_S, and until then they are hidden
                # objects on exactly the pool this tool exists to keep
                # object counts down on. The outer handler only knows
                # about tmp_file.
                for staged, _ in link_tmps:
                    try:
                        os.unlink(staged)
                    except OSError:
                        pass
                raise

            logging.info(f"Renaming {tmp_file} -> {filepaths[0]}")
            try:
                os.rename(tmp_file, filepaths[0])
                for link_tmp, path in link_tmps:
                    os.rename(link_tmp, path)
            except Exception as e:
                moved, completed = _undo_partial_relink(
                    args, filepaths, st, (tmp_st.st_dev, tmp_st.st_ino),
                    link_tmps, e)
                touched.update(moved)
                if not completed:
                    raise
            # Hard links can sit under different artifacts, so every target
            # is claimed, not just the first.
            touched.update(filepaths)

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
            signal.pthread_sigmask(signal.SIG_UNBLOCK, _EXIT_SIGNALS)


def handler(future):
    try:
        future.result()
    except InvalidLayout as e:
        logging.warning(f"Skipping a file: {e}")
        with stats._lock:
            stats.files_skipped_layout_invalid += 1
    except Exception:
        logging.exception("Error processing file in worker thread")
        with stats._lock:
            stats.files_failed += 1
    finally:
        thread_count.release()


def _under_roots(path, roots):
    """Is *path* inside one of *roots*?

    Separate function so the prefix comparison is testable. A bare startswith()
    would put /vol/abc under /vol/ab, which is how a list for one volume ends up
    silently rewriting another.
    """
    return any(path == r or path.startswith(r.rstrip(os.sep) + os.sep)
               for r in roots)


def _real_dir(path, cache):
    """os.path.realpath() of directory *path*, resolving each directory once.

    realpath() lstat()s every component on every call. The paths in a list
    share nearly all their ancestors, so with *cache* each directory not yet
    seen costs one lstat().
    """
    real = cache.get(path)
    if real is None:
        parent, name = os.path.split(path)
        if not name:
            real = path
        else:
            here = os.path.join(_real_dir(parent, cache), name)
            real = os.path.realpath(here) if os.path.islink(here) else here
        cache[path] = real
    return real


def _containing_root(path, roots):
    """The innermost of *roots* that *path* is under, or None."""
    return max((r for r in roots if _under_roots(path, [r])), key=len,
               default=None)


def _symlink_below_root(dirpath, roots, cache):
    """Where *dirpath* really is when a symlink below its root leads elsewhere,
    else None. A symlink in the root's own path is the operator's choice and
    is allowed; only the part of the path under the root is held to the walk's
    rule of never following one."""
    root = _containing_root(dirpath, roots)
    if root is None:
        return None
    real = _real_dir(dirpath, cache)
    expected = os.path.normpath(os.path.join(_real_dir(root, cache),
                                             os.path.relpath(dirpath, root)))
    return None if real == expected else real


# ---------------------------------------------------------------------------
# --paths-from-pool: read the candidate list out of a RADOS pool
#
# The walk prunes machine-generated directories, so it alone never empties a
# drained pool. Listing the pool costs no MDS walk and scales with what is left
# (4,074 files/s, against 0.23 walking, on a 44 TiB pool). Only a file's first
# object, "<ino-hex>.00000000", carries the `parent` backtrace, and the MDS
# always creates it, sparse and empty files included.
#
# A backtrace path can be stale for a live file. A renamed file's backtrace is
# rewritten only when the MDS expires the log segment holding the rename ('ceph
# tell mds.<daemon> flush journal' forces it). Files under a renamed ANCESTOR
# keep the old path for good, but their parent's inode stays right. So each
# path's parent directory is checked once (InodeLocator): unchanged, the path is
# used as listed; moved, the file is found by LOOKUPINO of the parent plus its
# dname, inode-checked. A name holding another inode is skipped as replaced,
# with no further lookup; otherwise a LOOKUPINO of the file decides: ESTALE is
# gone, anything else is live but unlocatable ('dump inode <ino>' names it).
# Only backtraces inside the mounted subtree are looked up. Checks wait out a
# thread-limit pause and sleep file_delay_ms first; inode lookups are capped by
# --lookupino-rate.
# ---------------------------------------------------------------------------

BACKTRACE_XATTR = "parent"

# Cap on the distinct-inode set used by the end-of-listing cross-check.
# Each entry costs ~66 bytes, so 50M entries is ~3.3 GB; a
# 665M-object pool would need ~44 GB. Past the cap the set is dropped and
# the cross-check is reported as skipped rather than exhausting memory.
INODE_TRACK_MAX = 50_000_000


class Backtrace(tuple):
    """decode_backtrace's result, (ino, [dname ...] root-first, pool), with
    .dirinos: the ancestors' directory inodes, root-first like the dnames, so
    dirinos[-1] is the file's parent."""
    dirinos = ()


class BacktraceError(Exception):
    """A backtrace that could not be decoded. Raised, never guessed around: a
    decoder that always returns something yields plausible WRONG paths."""


def decode_backtrace(blob):
    """Decode an inode_backtrace_t. Returns a Backtrace: (ino, [dname ...]
    root-first, pool), with the ancestors' inodes in .dirinos.

        u8 struct_v  u8 compat_v  u32 payload_len
        u64 ino
        u32 n_ancestors
          each inode_backpointer_t:
            u8 struct_v  u8 compat_v  u32 payload_len
            u64 dirino   u32 dname_len  bytes dname   u64 version
        s64 pool        u32 n_old_pools   repeated s64

    Ancestors are child-first, so the result is reversed. compat_v is honored
    rather than ignored: if the encoder says we need a newer decoder than we
    are, refuse instead of misparsing.
    """
    import struct

    # Highest inode_backtrace_t encoding this decoder understands.
    SUPPORTED = 5
    # inode_backpointer_t carries its own version, which can move
    # independently of the enclosing backtrace.
    SUPPORTED_BACKPOINTER = 2
    off = 0

    def u8():
        nonlocal off
        v = blob[off]
        off += 1
        return v

    def fixed(fmt, n):
        nonlocal off
        v = struct.unpack_from(fmt, blob, off)[0]
        off += n
        return v

    try:
        struct_v, compat_v = u8(), u8()
        if compat_v > SUPPORTED:
            raise BacktraceError(
                "encoded with compat_v %d; this decoder understands %d. Refusing "
                "rather than risk a wrong path." % (compat_v, SUPPORTED))
        if not 1 <= struct_v <= 16:
            raise BacktraceError("implausible struct_v %d" % struct_v)
        fixed("<I", 4)                      # payload length, unused
        ino = fixed("<Q", 8)

        names = []
        dirinos = []
        n = fixed("<I", 4)
        if n > 4096:
            raise BacktraceError("implausible ancestor count %d" % n)
        for _ in range(n):
            bp_v, bp_compat = u8(), u8()
            if bp_compat > SUPPORTED_BACKPOINTER:
                raise BacktraceError(
                    "backpointer encoded with compat_v %d; this decoder "
                    "understands %d" % (bp_compat, SUPPORTED_BACKPOINTER))
            if not 1 <= bp_v <= 16:
                raise BacktraceError("implausible backpointer struct_v %d" % bp_v)
            fixed("<I", 4)                  # backpointer payload length
            dirinos.append(fixed("<Q", 8))
            ln = fixed("<I", 4)
            if ln > 4096:
                raise BacktraceError("implausible dname length %d" % ln)
            # A slice past the end truncates silently, so check explicitly
            # instead of leaving the next field's unpack to catch it.
            if off + ln > len(blob):
                raise BacktraceError(
                    "dname of %d bytes runs past the end of a %d byte backtrace"
                    % (ln, len(blob)))
            names.append(blob[off:off + ln])
            off += ln
            fixed("<Q", 8)                  # version

        pool = fixed("<q", 8)
        bt = Backtrace((ino, list(reversed(names)), pool))
        bt.dirinos = tuple(reversed(dirinos))
        return bt
    except BacktraceError:
        raise
    except (IndexError, struct.error) as e:
        raise BacktraceError(str(e)) from e


# Default for --lookupino-rate: inode lookups are MDS requests, and a pool with
# tens of thousands of files under renamed directories asks for one per file
# until the directory cache warms up.
LOOKUPINO_RATE = 20.0
# Directory lookups and parent checks cached per listing; past this a cache
# starts over.
PARENT_CACHE_MAX = 1_000_000
# struct ceph_nfs_fh { u64 ino; }: the kernel client's plain file handle.
_FILEID_INO32_GEN = 1


class _StopListing(Exception):
    """The run is stopping; end the pool listing early."""


class InodeLocator:
    """Find a live file's current path on one kernel CephFS mount from its inode
    and its backtrace's parent inode. Lookups go to the MDS (open_by_handle_at
    -> LOOKUPINO), are paced at rate per second, and directories are cached.
    The two lookup primitives raise OSError; ESTALE means no such inode."""

    def __init__(self, mount, rate):
        self.mount = mount
        self.rate = rate
        self.lookups = 0
        self.slept = 0.0
        self._dirs = {}              # dirino -> current path, None when gone, or the OSError
        self._parents = {}           # parent path -> its st_ino, 0 when missing
        self._next = 0.0
        self._mfd = None
        self._obha = None
        self.mount_dev = None        # st_dev of the mount; checked when known

    @classmethod
    def open(cls, mount, rate):
        """A locator for mount, or None (said once) when lookups are off or
        unavailable. It is proved on the mount root before it is used."""
        if not rate or rate <= 0:
            return None
        loc = cls(mount, rate)
        try:
            libc = ctypes.CDLL(None, use_errno=True)
            obha = libc.open_by_handle_at
        except (OSError, AttributeError) as e:
            logging.warning("--paths-from-pool: no open_by_handle_at (%s); no "
                            "inode lookups", e)
            return None
        obha.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_int]
        obha.restype = ctypes.c_int
        loc._obha = obha
        try:
            loc._mfd = os.open(mount, os.O_RDONLY | os.O_DIRECTORY)
            mst = os.stat(mount)
            loc.mount_dev = mst.st_dev
            got = loc._lookup_dir(mst.st_ino)
        except OSError as e:
            why = ("it needs CAP_DAC_READ_SEARCH; run as root"
                   if e.errno in (errno.EPERM, errno.EACCES) else str(e))
            logging.warning("--paths-from-pool: no inode lookups on %s (%s)",
                            mount, why)
            loc.close()
            return None
        if os.path.realpath(got) != os.path.realpath(mount):
            logging.warning("--paths-from-pool: %s's root inode looked up as %s; "
                            "no inode lookups", mount, got)
            loc.close()
            return None
        return loc

    def close(self):
        if self._mfd is not None:
            os.close(self._mfd)
            self._mfd = None

    def _open_fd(self, ino):
        import struct
        fh = ctypes.create_string_buffer(
            struct.pack("<Ii", 8, _FILEID_INO32_GEN) + struct.pack("<Q", ino))
        fd = self._obha(self._mfd, fh, getattr(os, "O_PATH", 0o10000000))
        if fd < 0:
            e = ctypes.get_errno()
            raise OSError(e, os.strerror(e))
        return fd

    def _lookup_dir(self, dirino):
        """The directory's current path. The kernel reconnects a directory
        handle to its dentry chain, so /proc/self/fd names it; a file handle
        can come back disconnected, which is why only directories are named
        this way. A directory removed between the open and the readlink reads
        back as "<path> (deleted)" with no links left, and is ESTALE, like one
        already gone. The suffix alone only means the dentry is unhashed, so a
        directory that still has links keeps the path it read back."""
        fd = self._open_fd(dirino)
        try:
            path = os.readlink(f"/proc/self/fd/{fd}")
            if path.endswith(" (deleted)") and os.fstat(fd).st_nlink == 0:
                raise OSError(errno.ESTALE, os.strerror(errno.ESTALE))
            return path
        finally:
            os.close(fd)

    def _lookup(self, ino):
        """Return if ino is a linked inode; OSError (ESTALE) if not."""
        os.close(self._open_fd(ino))

    def _pace(self):
        """Wait for the rate cap, in slices so an exit is not held up."""
        start = now = time.monotonic()
        while self._next > now and not do_exit.is_set():
            time.sleep(min(self._next - now, 1.0))
            now = time.monotonic()
        self.slept += now - start
        self._next = max(now, self._next) + 1.0 / self.rate
        self.lookups += 1

    def parent_exists(self, parent):
        """Was parent checked and found to exist? Cache only, no lookup."""
        got = self._parents.get(parent)
        return bool(got) and not isinstance(got, OSError)

    def parent_gone(self, dirino, parent):
        """Is dirino already known to be gone (looked up, ESTALE) and is
        nothing at its old path (parent checked missing)? A new directory at
        that path can hold a file moved in under its old name."""
        return (dirino in self._dirs and self._dirs[dirino] is None
                and self._parents.get(parent) == 0)

    def dir_path(self, dirino):
        """dirino's current path, or None when it is gone. Any other failure
        is cached too and raised again, so a directory costs one lookup."""
        if dirino not in self._dirs:
            self._pace()
            if len(self._dirs) >= PARENT_CACHE_MAX:
                self._dirs.clear()
            try:
                self._dirs[dirino] = self._lookup_dir(dirino)
            except OSError as e:
                self._dirs[dirino] = None if e.errno == errno.ESTALE else e
        got = self._dirs[dirino]
        if isinstance(got, OSError):
            raise got.with_traceback(None)   # a cached error must not grow
        return got

    def parent_unchanged(self, parent, dirino, pace=None):
        """Is parent still directory inode dirino? One lstat per directory,
        cached (up to PARENT_CACHE_MAX entries), failures included; pace()
        runs before each lstat that is not cached."""
        got = self._parents.get(parent)
        if got is None:
            if pace is not None:
                pace()
            try:
                got = os.lstat(parent).st_ino
            except OSError as e:
                got = 0 if e.errno in _VANISHED_ERRNOS else e
            if len(self._parents) >= PARENT_CACHE_MAX:
                self._parents.clear()
            self._parents[parent] = got
        if isinstance(got, OSError):
            raise got.with_traceback(None)   # a cached error must not grow
        return got == dirino

    def _is(self, st, ino):
        """Is st inode ino on this mount?"""
        return st.st_ino == ino and (self.mount_dev is None
                                     or st.st_dev == self.mount_dev)

    def alive(self, ino):
        self._pace()
        try:
            self._lookup(ino)
        except OSError as e:
            if e.errno != errno.ESTALE:
                raise
            return False
        return True

    def locate(self, path, ino, dirino, dname):
        """(path, how) for inode ino read at path from its backtrace:
        "as-is" when path is that inode, "parent" when it is dname under
        dirino's current path, (None, "replaced") when the parent is where
        the backtrace says but the dname no longer holds the inode, or the
        dname under the parent's current path holds another inode (replaced,
        deleted or renamed; no further lookup), (None, "gone") when the inode
        or its parent directory no longer exists, and (None, "unlocatable")
        when the inode exists but neither path holds it. Raises OSError for
        any other failure."""
        def holds(p):
            try:
                return self._is(os.lstat(p), ino)
            except OSError as e:
                if e.errno not in _VANISHED_ERRNOS:
                    raise
                return False
        if holds(path):
            return path, "as-is"
        if dirino:
            d = self.dir_path(dirino)
            if d is None:
                # The parent is gone (ESTALE), so is the name under it; no
                # lookup per file.
                return None, "gone"
            # A directory moved out of a subtree mount reconnects outside it;
            # its path is not on this mount, so it is not used.
            if not _under_roots(os.path.normpath(d), [self.mount]):
                d = None
            if d is not None:
                if os.path.normpath(d) == os.path.dirname(os.path.normpath(path)):
                    return None, "replaced"
                cand = os.path.join(d, dname)
                try:
                    st = os.lstat(cand)
                except OSError as e:
                    if e.errno not in _VANISHED_ERRNOS:
                        raise
                    st = None
                if st is not None:
                    if self._is(st, ino):
                        return cand, "parent"
                    return None, "replaced"
        return None, ("unlocatable" if self.alive(ino) else "gone")


def _nonnegative_float(v):
    """argparse type: a finite float >= 0."""
    import math
    try:
        f = float(v)
    except ValueError:
        raise argparse.ArgumentTypeError(f"not a number: {v!r}") from None
    if not math.isfinite(f) or f < 0:
        raise argparse.ArgumentTypeError(
            f"must be a finite number, 0 or more, not {v}")
    return f


def _inode_locator(mount, rate):
    """An InodeLocator for a kernel CephFS mount, else None. Not on an ino32
    mount: there st_ino is a hash of the inode number the handle carries, so
    no lstat would ever match."""
    if not rate or rate <= 0:
        return None                  # lookups turned off; nothing to say
    fstype = _findmnt("FSTYPE", mount)
    if fstype != "ceph":
        logging.warning("--paths-from-pool: %s is not a kernel CephFS mount "
                        "(%s); no inode lookups", mount,
                        fstype or "findmnt unavailable")
        return None
    if "ino32" in _findmnt("OPTIONS", mount).split(","):
        logging.warning("--paths-from-pool: %s is mounted ino32; no inode "
                        "lookups", mount)
        return None
    return InodeLocator.open(mount, rate)


def _findmnt(field, path):
    """One findmnt field, or "" when findmnt is unusable.

    A minimal container image may not ship util-linux, in which case
    subprocess.run raises FileNotFoundError. Callers want the curated error
    about how to reach the cluster, not a traceback.
    """
    import subprocess
    try:
        r = subprocess.run(["findmnt", "-no", field, "--target", path],
                           capture_output=True, text=True)
    except OSError as e:
        logging.debug("findmnt %s unavailable: %s", field, e)
        return ""
    if r.returncode != 0:
        logging.debug("findmnt %s on %s failed: %s", field, path, r.stderr.strip())
        return ""
    return r.stdout.strip()


def _ceph_connect_args(explicit, sample_path, mon_host=None):
    """Work out how to reach the cluster, for package AND containerized hosts.

    A missing /etc/ceph/ceph.conf is normal, not exceptional: containerized
    (cephadm) deployments keep host packages and config minimal, and a host
    attached to several clusters has only per-cluster files. So try, in order:
    the explicit flag, CEPH_CONF, the conventional path, a single *.conf, and
    finally mon addresses recovered from the mount itself -- a kernel CephFS
    mount names its monitors in the mount source.

    Returns (conffile, conf_overrides) for rados.Rados().
    """
    import glob
    # Explicit monitors win outright: an operator who supplied them must not be
    # refused by the discovery chain below, which can SystemExit.
    if mon_host:
        return "", {"mon_host": mon_host}
    if explicit:
        return explicit, {}
    env = os.environ.get("CEPH_CONF")
    if env:
        return env, {}
    if os.path.exists("/etc/ceph/ceph.conf"):
        return "/etc/ceph/ceph.conf", {}
    found = sorted(glob.glob("/etc/ceph/*.conf"))
    if len(found) == 1:
        return found[0], {}

    # No usable file. A kernel mount source looks like
    # "10.0.0.1:6789,10.0.0.2:6789:/sub" (or "name@fsid.fs=/sub", which carries
    # no addresses). Recover mon_host from the address form if we can.
    src = _findmnt("SOURCE", sample_path)
    head = src.rsplit(":/", 1)[0] if ":/" in src else ""
    mons = [a for a in head.split(",") if a and (a[0].isdigit() or a.startswith("["))]
    if mons:
        logging.info("no usable ceph.conf; using mon_host recovered from the mount: %s",
                     ",".join(mons))
        return "", {"mon_host": ",".join(mons)}

    raise SystemExit(
        "--paths-from-pool: cannot reach the cluster. No --rados-conffile, no "
        "CEPH_CONF, no /etc/ceph/ceph.conf, %d candidates in /etc/ceph (%s), and no "
        "monitor addresses in the mount source (%r -- cephadm-style sources do not "
        "carry them). Pass --rados-conffile, or --rados-mon-host." % (
            len(found), ", ".join(map(os.path.basename, found)) or "none", src)
        + " Authentication may additionally need --rados-keyring/--rados-name.")


def _mount_ancestry_prefix(path):
    """Where a backtrace's ancestry is rooted, as an absolute local path.

    A backtrace names the ancestry from the FILESYSTEM root. If the mount is at
    the fs root the local path is just mountpoint + ancestry; if it is a subtree
    mount, the leading ancestry components are already inside the mountpoint and
    must not be repeated. Both shapes exist in the wild, so this is derived from
    the mount rather than assumed, and the caller then verifies by stat.

    Returns (mountpoint, subtree) where subtree is the fs-relative path the
    mountpoint corresponds to ("/" for a root mount).
    """
    mp = _findmnt("TARGET", path)
    src = _findmnt("SOURCE", path)
    if not mp:
        raise SystemExit("--paths-from-pool: %s is not on a mounted filesystem" % path)
    # Kernel client sources look like "<mons>:/subtree" or
    # "user@fsid.fsname=/subtree"; the fs-relative subtree is after the last
    # ':' or '=' that is followed by a slash.
    subtree = "/"
    for sep in ("=", ":"):
        i = src.rfind(sep + "/")
        if i >= 0:
            subtree = src[i + len(sep):]
            break
    return mp, (subtree or "/")



def _path_source(args, roots):
    """The candidate paths, from a list file or straight out of a pool."""
    if args.paths_from_pool:
        return _iter_pool_paths(args, roots)
    return _iter_listed_paths(args.paths_from)


def _prefix_shift(moves):
    """The one wrong ancestry prefix that explains every relocated sample, as
    (extra, components): extra True when the derived paths carry those leading
    components and the files lack them, False the other way round. moves are
    (derived, found) mount-relative component lists. None when no single
    leading-component shift explains them all; a rename changes a component,
    and a directory moved under its own name shifts only its own files."""
    shift = None
    for d, f in moves:
        if len(d) == len(f):
            continue
        short, long_ = (d, f) if len(d) < len(f) else (f, d)
        if long_[len(long_) - len(short):] != short:
            continue
        cand = (len(d) > len(f), tuple(long_[:len(long_) - len(short)]))
        if shift is not None and cand != shift:
            return None
        shift = cand
    if shift is None:
        return None
    extra, comps = shift
    for d, f in moves:
        if tuple((d if extra else f)[:len(comps)]) != comps:
            return None
    return shift


def _prefix_error(checked, example):
    """Message for "every derived path is missing".

    Two causes, which the path alone cannot tell apart, so name both: a wrong
    ancestry prefix (the subtree-vs-root mount case) or a pool holding only
    objects whose files have since been deleted. Inode lookups tell them apart
    when they run (an all-gone sample is not refused); otherwise transcoding
    nothing while reporting a clean pass is the wrong outcome.
    """
    return ("--paths-from-pool: none of the %d sampled derived paths exist "
            "(e.g. %s): a wrong ancestry prefix (a subtree mount?), or only "
            "deleted files' objects are left. Refusing." % (checked, example))


def write_target_refusal(args, pool, root, cur):
    """Why --paths-from-pool must not read pool for root, or None.

    cur is root's ceph.dir.layout.pool. A pool that is still the default write
    target cannot be DRAINED -- new files keep landing in it -- so that is
    refused. A --target-pool sweep of small files is the exception: it is not
    meant to converge to empty, only to take what is there now.
    """
    if cur != pool:
        return None
    if (getattr(args, "target_pool", None) and args.target_pool != pool
            and args.max_size is not None):
        logging.info(
            "--paths-from-pool %s is still the default write target of %s; "
            "allowed for a --target-pool %s sweep of files up to %d bytes",
            pool, root, args.target_pool, args.max_size)
        return None
    return ("--paths-from-pool %s is still the default write target of %s, "
            "so a drain cannot converge. Repoint ceph.dir.layout.pool first."
            % (pool, root))


def _iter_pool_paths(args, roots):
    """Yield the CephFS path of every file currently in args.paths_from_pool.

    Refuses rather than yield a wrong or partial list: no RADOS bindings, the
    pool still a default write target, snapshots on the pool, an inode with
    no backtrace-carrying bno-0 object, or a path prefix that does not resolve
    (unless every sampled object is gone by inode lookup: a deleted tree).
    """
    try:
        import rados
    except ImportError as e:
        raise SystemExit(
            "--paths-from-pool needs the RADOS Python bindings ('import rados'), "
            "which are missing.\n"
            "  RPM  : dnf install python3-rados\n"
            "  deb  : apt install python3-rados\n"
            "  pip  : only where your distribution publishes the bindings that way "
            "-- they wrap librados, so a pip install without librados present will "
            "not work.\n"
            "They also arrive as a dependency of ceph-common, which a host using a "
            "kernel CephFS mount does not otherwise require.\n"
            "Import error: %s" % e)

    pool = args.paths_from_pool
    conffile, conf_over = _ceph_connect_args(args.rados_conffile, roots[0],
                                             args.rados_mon_host)

    # A sweep reads a pool that is still being written, where the end-of-listing
    # checks below cannot be told from damage: a file created mid-listing has
    # no backtrace yet, and one created or deleted then can show some stripe
    # objects without its bno-0. Expected there, so said at WARNING.
    live = False
    for r in roots:
        try:
            cur = os.getxattr(r, "ceph.dir.layout.pool").decode()
        except OSError as e:
            # No layout of its own: the root writes where it inherits from,
            # which is what both the drain refusal and `live` are about. Any
            # other error leaves the root out of a check that fails open, so
            # it is said, not skipped silently.
            lay = None
            if e.errno == ENODATA:
                try:
                    lay = get_layout_walking_up(r)
                except OSError as e2:
                    e = e2
            if lay is None:
                logging.warning(
                    "%s: cannot read its layout pool (%s); left out of the "
                    "write-target check", r, e)
                continue
            cur = lay.pool
        live = live or cur == pool
        why = write_target_refusal(args, pool, r, cur)
        if why:
            raise SystemExit(why)
        snapdir = os.path.join(r, ".snap")
        try:
            snaps = [e for e in os.listdir(snapdir) if not e.startswith("_")]
        except OSError:
            snaps = []
        if snaps:
            logging.warning(
                "%s has %d snapshot(s); snapshots pin objects, so %s may not reach "
                "zero objects however complete this pass is", r, len(snaps), pool)

    mp, subtree = _mount_ancestry_prefix(roots[0])
    strip = [c for c in subtree.strip("/").split("/") if c]
    logging.info("--paths-from-pool %s: conf %s, mount %s, fs subtree %s",
                 pool, conffile, mp, subtree)
    locator = _inode_locator(mp, getattr(args, "lookupino_rate", LOOKUPINO_RATE))
    # Lookups are MDS requests, so only paths the run may touch are located;
    # anything else is refused downstream whatever its path.
    bounds = [os.path.abspath(r) for r in roots]

    def check_pace():
        # The thread limit's pause and the file delay hold these checks in
        # both modes: --target-pool reads the whole listing before any copy.
        if not _reap_pace():
            raise _StopListing()

    conf = dict(conf_over)
    if args.rados_keyring:
        conf["keyring"] = args.rados_keyring
    cluster = rados.Rados(conffile=conffile, conf=conf,
                          name=args.rados_name or "client.admin")
    cluster.connect()
    try:
        ioctx = cluster.open_ioctx(pool)
    except Exception as e:
        cluster.shutdown()
        raise SystemExit("--paths-from-pool: cannot open pool %s: %s" % (pool, e))

    inodes_seen = set()
    last_unresolved = None
    first_objs = 0
    unusable = 0
    vanished = 0
    yielded = 0
    checked = 0
    sampled_gone = 0
    sampled_moves = []           # relocated samples, mount-relative components
    resolved = 0
    relocated = 0
    gone = 0
    replaced = 0
    unlocatable = 0
    lookup_errors = 0
    # The inode cross-check below is only meaningful after a COMPLETE listing.
    # Stop early -- a break in the consumer, --max-files, an exception -- and
    # stripe objects whose bno-0 object had not been reached yet look like
    # inodes with no backtrace, which is a false alarm.
    complete = False
    try:
        for obj in ioctx.list_objects():
            key = obj.key
            ino_s, _, bno_s = key.partition(".")
            if not bno_s:
                continue
            try:
                ino = int(ino_s, 16)
                bno = int(bno_s, 16)      # integer, not a string suffix match
            except ValueError:
                continue
            if inodes_seen is not None:
                inodes_seen.add(ino)
                if len(inodes_seen) > INODE_TRACK_MAX:
                    logging.warning(
                        "more than %d distinct inodes; dropping the inode set and "
                        "skipping the bno-0 cross-check to bound memory",
                        INODE_TRACK_MAX)
                    inodes_seen = None
            if bno != 0:
                continue
            first_objs += 1
            try:
                blob = ioctx.get_xattr(key, BACKTRACE_XATTR)
            except rados.NoData:
                logging.warning("%s has no %s xattr; skipping", key, BACKTRACE_XATTR)
                unusable += 1
                continue
            except rados.ObjectNotFound:
                # Listed, then gone before its backtrace was read: a file
                # deleted, or transcoded away, and purged mid-listing. A pool
                # that is still being written -- what a --target-pool sweep
                # reads by design -- does this all the time. Nothing to emit,
                # and nothing left holding the pool.
                vanished += 1
                continue
            try:
                bt = decode_backtrace(blob)
                bt_ino, names, _pool = bt
            except BacktraceError as e:
                logging.error("%s: undecodable backtrace: %s", key, e)
                unusable += 1
                continue
            # The object name already carries the inode, so comparing it against
            # the decoded one is a free corruption check: a backtrace that
            # decodes cleanly but belongs to a different inode would otherwise
            # contribute someone else's path.
            if bt_ino != ino:
                logging.error(
                    "%s: backtrace is for inode 0x%x, not 0x%x -- skipping",
                    key, bt_ino, ino)
                unusable += 1
                continue
            if not names:
                continue
            rel = [n.decode("utf-8", "surrogateescape") for n in names]
            # A backtrace outside the mount's subtree keeps its full path; no
            # lookup from this mount can reach it, so it is not looked up.
            inside = not strip or rel[:len(strip)] == strip
            if strip and inside:
                rel = rel[len(strip):]
            path = os.path.join(mp, *rel) if rel else mp
            derived = path
            how = None
            if locator is not None and inside and rel and _under_roots(path, bounds):
                dirinos = getattr(bt, "dirinos", ())
                dirino = dirinos[-1] if dirinos else 0
                try:
                    if dirino and locator.parent_unchanged(
                            os.path.dirname(path), dirino, pace=check_pace):
                        found, how = path, None
                    elif dirino and locator.parent_gone(dirino, os.path.dirname(path)):
                        found, how = None, "gone"    # settled from the cache
                    else:
                        check_pace()
                        found, how = locator.locate(path, ino, dirino, rel[-1])
                except _StopListing:
                    logging.info("--paths-from-pool %s: stopping the listing", pool)
                    return
                except OSError as e:
                    lookup_errors += 1
                    if lookup_errors <= 5:
                        logging.warning("%s (inode 0x%x): inode lookup failed "
                                        "(%s); using the backtrace path", path, ino, e)
                    found, how = path, None
                if how == "parent":
                    relocated += 1
                    if relocated <= 5:
                        logging.info("%s is now %s (a parent was renamed)",
                                     path, found)
                    path = found
                elif how == "unlocatable":
                    unlocatable += 1
                    if unlocatable <= 10:
                        logging.warning(
                            "inode 0x%x is alive but not at %s or under its "
                            "parent; re-run once its backtrace is rewritten, or "
                            "'ceph tell mds.<fs>:<rank> dump inode %d'",
                            ino, path, ino)
            # Verify the derived prefix on the first handful rather than
            # emitting a whole list of paths that do not exist.
            if checked < 64:
                checked += 1
                # A replaced name, or a derived parent the locator found on
                # disk, proves the prefix as well as an existing derived path.
                exists = (how == "replaced"
                          or (locator is not None
                              and locator.parent_exists(os.path.dirname(derived)))
                          or os.path.lexists(path))
                if exists:
                    resolved += 1
                    if how == "parent":
                        sampled_moves.append(
                            (os.path.relpath(derived, mp).split(os.sep),
                             os.path.relpath(path, mp).split(os.sep)))
                else:
                    last_unresolved = path
                    if how == "gone":
                        sampled_gone += 1
                # Inodes the MDS says are gone (ESTALE) say nothing against
                # the prefix: a fully deleted tree awaiting purge looks so.
                if checked == 64 and resolved == 0 and sampled_gone < checked:
                    raise SystemExit(_prefix_error(checked, path))
            if how == "gone":
                gone += 1
                continue
            if how == "replaced":
                replaced += 1
                continue
            if how == "unlocatable":
                continue
            yielded += 1
            yield path
        # Evaluate the prefix guard again at end of listing. Gating it solely on
        # reaching 64 samples meant a pool with fewer candidates than that never
        # reached the abort -- and that is the near-empty pool a drain converges
        # toward, so the guard was weakest exactly where it matters most.
        shift = (_prefix_shift(sampled_moves)
                 if sampled_moves and len(sampled_moves) == resolved else None)
        if checked > 0 and resolved == 0:
            if sampled_gone < checked:
                raise SystemExit(_prefix_error(checked, last_unresolved))
            logging.warning("--paths-from-pool %s: all %d sampled objects are "
                            "gone (ESTALE); prefix not checked", pool, checked)
        elif shift:
            # Every resolved sample was found only by inode, and one leading-
            # component shift explains them all: a wrong ancestry prefix.
            extra, comps = shift
            # The example shows the shift itself, not a renamed sample.
            d, f = next((d, f) for d, f in sampled_moves
                        if len(d) != len(f) and (d if extra else f)[len(comps):]
                        == (f if extra else d))
            logging.warning("--paths-from-pool %s: all %d resolved samples were "
                            "found by inode, as if the derived paths %s leading "
                            "'%s' (e.g. %s -> %s); check the ancestry prefix (a "
                            "subtree mount?)", pool, resolved,
                            "had extra" if extra else "lacked", "/".join(comps),
                            "/".join(d), "/".join(f))
        complete = True
    finally:
        missing = (len(inodes_seen) - first_objs) if inodes_seen is not None else 0
        if complete and inodes_seen is None:
            logging.warning(
                "bno-0 cross-check skipped: more than %d distinct inodes",
                INODE_TRACK_MAX)
        level = logging.WARNING if live else logging.ERROR
        note = (" Expected for files written during the listing of a pool "
                "that is still being written." if live else "")
        if complete and missing > 0:
            logging.log(
                level,
                "%d inode(s) in %s have objects but no .00000000 object: no "
                "backtrace, no path, not in this list. Pool will not reach "
                "zero.%s", missing, pool, note)
        if complete and unusable > 0:
            logging.log(
                level,
                "%d inode(s) in %s have a .00000000 object whose backtrace could "
                "not be used (absent, undecodable, or for a different inode): no "
                "path was emitted for them and they will hold the pool above "
                "zero.%s", unusable, pool, note)
        if complete and unlocatable > 0:
            logging.warning(
                "%d live inode(s) in %s not located; no path emitted, and they "
                "hold the pool until a re-run", unlocatable, pool)
        logging.info(
            "--paths-from-pool %s: %d inodes, %d bno-0 objects (%d unusable, "
            "%d gone before read), %d paths emitted, %d/%d sampled paths resolved%s",
            pool, len(inodes_seen) if inodes_seen is not None else -1,
            first_objs, unusable, vanished, yielded, resolved, checked,
            "" if complete else " (listing stopped early; counts are partial)")
        if locator is not None:
            logging.info(
                "--paths-from-pool %s: %d found under a renamed directory, %d gone "
                "(not deletion evidence; re-run to catch late backtraces), %d no "
                "longer at their name, %d alive but not locatable, %d lookup "
                "error(s); %d inode lookups, %.0fs in their rate cap",
                pool, relocated, gone, replaced, unlocatable, lookup_errors,
                locator.lookups, locator.slept)
            locator.close()
        ioctx.close()
        cluster.shutdown()

def _iter_listed_paths(src):
    """Yield paths from a --paths-from source, streaming.

    NUL- or newline-delimited, detected from the first block rather than from a
    second flag: a list built with `find -print0` is NUL-delimited, one scraped
    out of a previous run's log is newline-delimited, and a flag for it is one
    more thing to get wrong silently. Read in chunks -- a candidate list for a
    large volume runs to tens of millions of lines, which is not something to
    hold in memory just to split it.
    """
    fh = sys.stdin.buffer if src == "-" else open(src, "rb")
    try:
        first = fh.read(1 << 16)
        sep = b"\0" if b"\0" in first else b"\n"
        logging.info("--paths-from %s: %s-delimited", src,
                     "NUL" if sep == b"\0" else "newline")
        buf = first
        while True:
            parts = buf.split(sep)
            buf = parts.pop()
            for raw in parts:
                # surrogateescape: a path the filesystem accepts is not always
                # valid UTF-8, and refusing to process it would be worse than
                # round-tripping the bytes.
                t = raw.decode("utf-8", "surrogateescape").strip("\r\n")
                if t:
                    yield t
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            buf += chunk
        t = buf.decode("utf-8", "surrogateescape").strip("\r\n")
        if t:
            yield t
    finally:
        if src != "-":
            fh.close()


# Under --target-pool a directory's files share one staging directory, which
# only pays if they arrive together. A list or a pool drain comes in arbitrary
# order -- a pool lists in object-hash order, so one directory's files are
# scattered through it -- and taking that as-is would make and remove a
# staging directory per file. So the paths are grouped by directory first, with
# a bounded-memory external sort: runs of GROUP_RUN_PATHS are sorted in memory
# and spilled to local disk, then merged. The whole source is read before the
# first file is processed.
GROUP_RUN_PATHS = 500000


def _dir_key(path):
    return (os.path.dirname(path), os.path.basename(path))


def _spill_run(paths, spill_dir):
    fd, name = tempfile.mkstemp(prefix="vcephfs-tc-group-", dir=spill_dir)
    with os.fdopen(fd, "wb") as fh:
        for p in paths:
            fh.write(p.encode("utf-8", "surrogateescape") + b"\0")
    return name


def _read_run(name):
    with open(name, "rb") as fh:
        buf = b""
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            parts = (buf + chunk).split(b"\0")
            buf = parts.pop()
            for raw in parts:
                yield raw.decode("utf-8", "surrogateescape")


# Kept free on the spill filesystem beyond the run being written: it is where
# the log lives, and a full log disk is worse than an ungrouped list.
GROUP_SPILL_RESERVE_BYTES = 1 << 30


def _spill_fits(chunk, spill_dir):
    need = sum(len(p) + 1 for p in chunk)
    try:
        return shutil.disk_usage(spill_dir).free >= need + GROUP_SPILL_RESERVE_BYTES
    except OSError:
        return False


def group_by_directory(paths, spill_dir, run_paths=None, stop=None):
    """Yield paths with each directory's entries together, in bounded memory.

    stop() is polled while reading: the pre-read can take hours on a large
    pool, and an exit signal must not wait for it. If the spill filesystem
    runs short, what is read so far is yielded grouped and the rest in listing
    order, rather than filling the log disk.
    """
    run_paths = run_paths or GROUP_RUN_PATHS
    it = iter(paths)
    runs = []
    chunk = []
    read = 0
    try:
        for p in it:
            if stop is not None and stop():
                return
            chunk.append(p)
            read += 1
            if len(chunk) < run_paths:
                continue
            chunk.sort(key=_dir_key)
            if not _spill_fits(chunk, spill_dir):
                logging.warning(
                    "Not enough free space under %s to keep grouping paths by "
                    "directory; the %d read so far are grouped, the rest follow "
                    "in listing order", spill_dir, read)
                yield from heapq.merge(*(_read_run(r) for r in runs), chunk,
                                       key=_dir_key)
                yield from it
                return
            runs.append(_spill_run(chunk, spill_dir))
            chunk = []
            logging.info("Grouping by directory: %d paths read, %d run(s) "
                         "spilled under %s", read, len(runs), spill_dir)
        if stop is not None and stop():
            return
        chunk.sort(key=_dir_key)
        if not runs:
            yield from chunk
            return
        logging.info("Grouped %d paths by directory in %d sorted run(s) under %s",
                     read, len(runs) + (1 if chunk else 0), spill_dir)
        yield from heapq.merge(*(_read_run(r) for r in runs), chunk, key=_dir_key)
    finally:
        close = getattr(it, "close", None)
        if close is not None:
            close()
        for r in runs:
            try:
                os.unlink(r)
            except OSError:
                pass


# Every operator path writes thread_count, even when the value is already
# the one asked for: the write is what tells the regulator its pause or
# probe is no longer its own (see DynamicSemaphore._gen).
def sigtstp_handler(sig, frame):
    old = thread_count.limit
    thread_count.set_limit(1)
    if old != 1:
        logging.info(f"SIGTSTP received, thread limit: {old} -> 1")
    else:
        logging.info(f"SIGTSTP received, already at 1")
    _report_state()


def sigusr1_handler(sig, frame):
    old = thread_count.limit
    new = min(old + 1, _EXECUTOR_MAX_WORKERS)
    thread_count.set_limit(new)
    if new != old:
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
    thread_count.set_limit(new)
    if new != old:
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


def _apply_config_threads(n):
    """Apply a threads value from --config, logging only what changes.

    Written even when it equals the current limit, because that is not a no-op
    while the regulator holds the value: the walker reads --config during a
    regulator pause, so "threads = 0" arrives while the limit is already 0,
    and "threads = 1" during its 1-thread probe. Either is the operator taking
    over, and the write is how the regulator knows (DynamicSemaphore._gen).
    Skipped, the edit would be lost -- RuntimeConfig never passes it again --
    and the regulator would later resume over the operator's pause.
    """
    old_v = thread_count.limit
    if n == old_v:
        held = regulator is not None and regulator.holding()
        thread_count.set_limit(n)
        if held:
            logging.info("Config: threads = %d is now the operator's setting; "
                         "the regulator will not resume or pay back over it",
                         n)
        return
    thread_count.set_limit(n)
    # Match the SIGUSR2/SIGUSR1 wording so a log scan finds a pause the same
    # way regardless of which interface caused it.
    if n == 0:
        logging.info(
            "Config: thread limit %d -> 0 — processing paused "
            "(in-flight copies will complete; set threads > 0 to resume)",
            old_v)
    elif old_v == 0:
        logging.info("Config: processing resumed (thread limit: 0 -> %d)", n)
    else:
        logging.info("Config: thread limit %d -> %d", old_v, n)


def _poll_config():
    """Re-read --config if it changed. Walker and config-poller threads."""
    if runtime_config is not None:
        runtime_config.poll(apply_config)


def _start_config_poller(poll_seconds):
    """Poll --config on its own clock, not only from the walker.

    The walker polls between files, so anything that stops the walker -- a
    filesystem call that hangs -- would stop config changes too, including
    "threads = 0", the operator's pause. Plain sleeps, as in
    _file_delay(); applying from here is safe because _apply_config only
    rebinds module globals and args attributes and RuntimeConfig serializes
    polls.
    """
    def run():
        while not do_exit.is_set():
            time.sleep(max(1.0, float(poll_seconds)))
            try:
                _poll_config()
            except Exception:
                logging.exception("Config poller: poll failed")

    t = threading.Thread(target=run, name="config-poller", daemon=True)
    t.start()
    return t


# The longest single sleep inside one file delay, so an exit or a config change
# is seen this soon even at DELAY_MAX_MS. Plain sleeps, not do_exit.wait(): the
# walker is the main thread, where the signal handlers run, and a handler's
# do_exit.set() deadlocks on the Event's plain Lock if it interrupts this
# thread inside Event.wait() while it holds that lock.
FILE_DELAY_SLICE_S = 1.0


def _file_delay():
    """Sleep file_delay_ms, waking early for an exit or a shorter delay.

    One time.sleep() of the whole delay held off both for as long as 10 min
    at DELAY_MAX_MS. The delay in force is re-read after each slice, so a
    --config or regulator change applies to the sleep already under way.
    Walker thread only, like _poll_config().
    """
    start = time.monotonic()
    while not do_exit.is_set():
        left = file_delay_ms / 1000.0 - (time.monotonic() - start)
        if left <= 0:
            return
        time.sleep(min(left, FILE_DELAY_SLICE_S))
        _poll_config()


def process_paths(args, hard_links, executor, dir_layouts, roots):
    """Transcode an explicit list of paths instead of walking the tree.

    For a pass whose candidates a previous log already holds: the walker pays
    file_delay per file encountered, not per candidate. A list goes stale, so
    every path is re-stat'ed and re-checked as the walker would, and a
    vanished one is expected, not an error.
    """
    return _process_paths(args, hard_links, executor, dir_layouts, roots)


def _process_paths(args, hard_links, executor, dir_layouts, roots):
    def _limit_reached():
        return args.max_files is not None and stats.files_submitted >= args.max_files

    last_progress = time.monotonic()
    outside_logged = 0
    real_dirs = {}
    root_dev = {r: os.stat(r).st_dev for r in roots}
    restat_failed = set()

    def _skip_outside(filepath, why):
        nonlocal outside_logged
        with stats._lock:
            stats.files_outside_root += 1
        outside_logged += 1
        if outside_logged <= 10:
            logging.warning("Skipping %s: %s", filepath, why)
        elif outside_logged == 11:
            logging.warning("Further out-of-tree paths will be counted but not "
                            "logged individually")

    source = _path_source(args, roots)
    # Grouping reads the whole list first, which a --max-files run would pay
    # for in full to process a handful; those take the list as it comes.
    if staging is not None and args.max_files is None:
        source = group_by_directory(source, _group_spill_dir(args),
                                    stop=do_exit.is_set)
    for filepath in source:
        if do_exit.is_set() or _limit_reached():
            return

        if runtime_config is not None:
            runtime_config.poll(apply_config)

        if file_delay_ms > 0:
            _file_delay()

        if time.monotonic() - last_progress > 60:
            stats.log_progress()
            last_progress = time.monotonic()

        filepath = os.path.abspath(filepath)

        # Containment. A list is easy to generate against the wrong volume, and
        # the damage would be silent and large, so a path outside the roots
        # given on the command line is refused rather than followed.
        if not _under_roots(filepath, roots):
            _skip_outside(filepath, "outside the directories given on the command line")
            continue

        # The walker never follows a symlinked directory under its root, and
        # neither does a list: one can lead into another volume, or another
        # cluster's mount whose pool of the same name passes --source-pool. The
        # st_dev test below catches a directory replaced, after it was cached
        # here, by a symlink to another filesystem.
        dirpath = os.path.dirname(filepath)
        if len(real_dirs) >= PARENT_CACHE_MAX:
            real_dirs.clear()
        real = _symlink_below_root(dirpath, roots, real_dirs)
        if real is not None:
            _skip_outside(filepath, f"reached through a symlink (its directory is {real})")
            continue

        # Ours, as in the walk: the shared journal or a run's checkpoint of
        # it. A --paths-from list or a pool drain can name one, and replacing
        # the shared journal by temp+rename would race another run's append.
        if _is_journal_file(os.path.basename(filepath)):
            logging.info("Skipping %s: the run journal", filepath)
            continue

        # A pool drain can list a copy in flight in another run's staging
        # directory. It is not a candidate; it is about to replace one.
        if STAGE_RE.match(os.path.basename(dirpath)):
            logging.info("Skipping %s: inside a staging directory", filepath)
            continue

        try:
            st = os.stat(filepath, follow_symlinks=False)
            root = _containing_root(filepath, roots)
            if st.st_dev != root_dev[root]:
                # An autofs volume that expired and was mounted again has a
                # new device number, so the file's own root is looked at again
                # first. A root that cannot be stat'ed keeps its old device;
                # its error is its own, said once, not the file's.
                try:
                    root_dev[root] = os.stat(root).st_dev
                except OSError as e:
                    if root not in restat_failed:
                        restat_failed.add(root)
                        logging.warning("Could not stat %s again: %s", root, e)
            if st.st_dev != root_dev[root]:
                _skip_outside(filepath, "on another filesystem than the directories "
                              "given on the command line")
                continue
            if run_journal is not None:
                run_journal.note_file(filepath, st)
        except OSError as e:
            if e.errno in (errno.ENOENT, errno.ESTALE, errno.ENOTDIR):
                with stats._lock:
                    stats.files_vanished += 1
                logging.info("Skipping %s: no longer exists", filepath)
            else:
                logging.warning("Skipping %s: %s", filepath, e)
                with stats._lock:
                    stats.files_failed += 1
            continue

        if not stat.S_ISREG(st.st_mode):
            msg = stats.note_skipped_symlink()
            if msg:
                logging.info(msg)
            continue

        dir_layout = dir_layouts.get(dirpath)
        if dir_layout is None:
            try:
                dir_layout = get_layout_walking_up(dirpath)
            except InvalidLayout as e:
                logging.warning("Skipping %s: %s", filepath, e)
                with stats._lock:
                    stats.files_skipped_layout_invalid += 1
                continue
            except OSError as e:
                if e.errno not in _VANISHED_ERRNOS:
                    raise
                with stats._lock:
                    stats.files_vanished += 1
                logging.info("Skipping %s: no longer exists", filepath)
                continue
            if dir_layout is None:
                logging.error(f"Could not determine layout for {dirpath}, skipping")
                with stats._lock:
                    stats.files_failed += 1
                continue
            dir_layouts[dirpath] = dir_layout
        layout = target_layout(args, dir_layout)

        # Layout is checked BEFORE size here, the reverse of the walker. The
        # walker tests size first because that is the cheaper rejection: most of
        # what it encounters is tiny, and a stat alone settles it, so reading a
        # layout for every file would add an MDS round trip to the common case.
        # A path list is already filtered to plausible candidates, and its
        # staleest entries are precisely the ones an earlier pass has since
        # moved -- testing size first would log those as "below --min-size",
        # conceal that they are already done, and overstate the remaining
        # work.
        try:
            file_layout = CephLayout.from_file(filepath)
        except InvalidLayout as e:
            logging.warning("Skipping %s: %s", filepath, e)
            with stats._lock:
                stats.files_skipped_layout_invalid += 1
            continue
        except OSError as e:
            if e.errno not in _VANISHED_ERRNOS:
                raise
            with stats._lock:
                stats.files_vanished += 1
            logging.info("Skipping %s: no longer exists", filepath)
            continue
        if file_layout is None:
            logging.error(f"Could not read layout for {filepath}, skipping")
            with stats._lock:
                stats.files_failed += 1
            continue
        if file_layout == layout:
            with stats._lock:
                stats.files_skipped_layout_match += 1
            continue
        if args.source_pool is not None and file_layout.pool != args.source_pool:
            with stats._lock:
                stats.files_skipped_source_pool += 1
            continue

        if st.st_nlink == 1 and st.st_size < args.min_size:
            logging.info(
                f"Skipping {filepath}: size {st.st_size} below --min-size {args.min_size}"
            )
            with stats._lock:
                stats.files_skipped_small += 1
            continue
        if (st.st_nlink == 1 and args.max_size is not None
                and st.st_size > args.max_size):
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

        # Multiply-linked files need every link in hand before any of them can
        # be replaced, and a path list cannot promise it contains them all.
        # Transcoding a partial set would break the link relationship, so this
        # mode declines regardless of --process-hardlinks. Walk for those.
        if st.st_nlink != 1:
            logging.info(
                f"Skipping {filepath}: has {st.st_nlink} hard links -- --paths-from "
                f"cannot see the other links, use a walk for these"
            )
            with stats._lock:
                stats.files_skipped_hardlink += 1
            continue

        if not thread_count.acquire(
                cancel=lambda: do_exit.is_set() or _limit_reached(),
                tick=_poll_config):
            return
        try:
            future = executor.submit(
                process_file, args, [filepath], st, layout, file_layout,
                layout.pool != dir_layout.pool
            )
            future.add_done_callback(handler)
        except Exception:
            thread_count.release()
            raise


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

        # Our own staged temp files are not candidates: a concurrent job may be
        # writing one right now, and transcoding a partial copy would be wrong.
        # Reclaim the aged ones here, for every directory the walk enters, so a
        # subtree that holds orphans but no current candidate is still swept.
        if any(_is_journal_file(f) for f in filenames):
            # Ours, and it lives at a volume root that is inside the walk --
            # the journal, and any run's checkpoint of it. Being under
            # --min-size already keeps it from being transcoded, but that is a
            # coincidence of its size, not a rule.
            filenames[:] = [f for f in filenames if not _is_journal_file(f)]

        orphans = [f for f in filenames if TMP_RE.match(f)]
        if orphans:
            filenames[:] = [f for f in filenames if not TMP_RE.match(f)]
            if not args.stage_in_tmpdir:
                # An unlink moves rctime too: capture the artifact before it,
                # claim it after.
                if run_journal is not None:
                    run_journal.note_dir(dirpath)
                if (_reclaim_named(dirpath, orphans)
                        and run_journal is not None):
                    run_journal.note_write(os.path.join(dirpath, orphans[0]))
        with _reclaimed_lock:
            _reclaimed_dirs.add(dirpath)

        try:
            layout = dir_layouts.get(dirpath, None)
            if layout is None:
                layout = CephLayout.from_dir(dirpath)
                if layout is None:
                    layout = dir_layouts.get(os.path.split(dirpath)[0])

            if layout is None:
                layout = get_layout_walking_up(dirpath)
        except InvalidLayout as e:
            # The subtree waits for a later pass. Descending would skip the
            # pruning below, which needs this directory's layout first.
            logging.warning(f"Skipping {dirpath} and everything under it: {e}")
            with stats._lock:
                stats.files_skipped_layout_invalid += len(filenames)
            del dirnames[:]
            continue
        except OSError as e:
            # Removed between os.walk listing it and this read: nothing under
            # it to transcode, and no reason to end the run.
            if e.errno not in _VANISHED_ERRNOS:
                raise
            logging.info(f"Skipping {dirpath}: no longer exists")
            del dirnames[:]
            continue

        if layout is None:
            logging.error(f"Could not determine layout for {dirpath}, skipping")
            del dirnames[:]
            continue

        # Staging directories are ours, from this run or another, and are
        # never walked whatever --prune-dir-regex says: a file inside one is
        # a copy in flight. A killed run's leftover is --reap-staging's, never
        # removed by a walk, for the reason no run removes one mid-run (see the
        # staging header).
        dirnames[:] = [d for d in dirnames if not STAGE_RE.match(d)]

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
        # dir_layouts keeps the directory's own layout, which is what its
        # children inherit; from here on `layout` is where files go.
        dir_layout = layout
        layout = target_layout(args, dir_layout)

        def submit(filepaths, st, file_layout, stage, _layout=layout):
            if do_exit.is_set() or _limit_reached():
                return
            if not thread_count.acquire(
                    cancel=lambda: do_exit.is_set() or _limit_reached(),
                    tick=_poll_config):
                return
            try:
                future = executor.submit(
                    process_file, args, filepaths, st, _layout, file_layout,
                    stage
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

            if file_delay_ms > 0:
                _file_delay()

            if time.monotonic() - last_progress > 60:
                stats.log_progress()
                last_progress = time.monotonic()

            filepath = os.path.join(dirpath, filename)
            # Listed by os.walk, gone by the stat: deleted under the walk.
            # Expected on a live volume, as in process_paths(), not fatal.
            try:
                st = os.stat(filepath, follow_symlinks=False)
            except OSError as e:
                if e.errno not in _VANISHED_ERRNOS:
                    raise
                with stats._lock:
                    stats.files_vanished += 1
                logging.info("Skipping %s: no longer exists", filepath)
                continue
            if run_journal is not None:
                # Before any gating, and before anything here is modified: this
                # is where the pre-transcode rctime is still recoverable.
                run_journal.note_file(filepath, st)
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
            try:
                file_layout = CephLayout.from_file(filepath)
            except InvalidLayout as e:
                logging.warning("Skipping %s: %s", filepath, e)
                with stats._lock:
                    stats.files_skipped_layout_invalid += 1
                continue
            except OSError as e:
                if e.errno not in _VANISHED_ERRNOS:
                    raise
                with stats._lock:
                    stats.files_vanished += 1
                logging.info("Skipping %s: no longer exists", filepath)
                continue
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
                submit([filepath], st, file_layout,
                       layout.pool != dir_layout.pool)
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
                    hard_links[file_id] = ([filepath], [layout], [dir_layout])
                else:
                    hard_links[file_id][0].append(filepath)
                    hard_links[file_id][1].append(layout)
                    hard_links[file_id][2].append(dir_layout)

                if len(hard_links[file_id][0]) == st.st_nlink:
                    filepaths = hard_links[file_id][0]
                    layouts = hard_links[file_id][1]
                    # Staged by where the first name lives: that is the
                    # directory the new inode is created and renamed into.
                    stage = layouts[0].pool != hard_links[file_id][2][0].pool
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
                        submit(filepaths, st, file_layout, stage)
                else:
                    logging.info(
                        f"Deferring {filepath} due to hardlinks ({st.st_nlink - len(hard_links[file_id][0])} link(s) left)"
                    )


def cleanup_tmpdir(tmpdir):
    """Remove aged temp files left by previous interrupted runs.

    Aged as reclaim_orphans() ages them: another job sharing tmpdir may be
    copying into one right now, and unlinking it fails that job's rename.
    """
    if not os.path.isdir(tmpdir):
        return
    now = time.time()
    count = 0
    for entry in os.scandir(tmpdir):
        if entry.is_file(follow_symlinks=False):
            try:
                # Both staging shapes: the bare UUID hex used by --stage-in-tmpdir,
                # and the ".<hex>.vcephfs-tc-tmp" a sibling-staged run leaves if
                # --tmpdir happens to sit inside the tree being walked.
                if not TMP_RE.match(entry.name):
                    uuid.UUID(entry.name)
                if (now - entry.stat(follow_symlinks=False).st_ctime
                        < TMP_ORPHAN_MIN_AGE_S):
                    continue
                os.unlink(entry.path)
                count += 1
            except (ValueError, OSError):
                pass
    if count:
        logging.info(f"Cleaned up {count} orphaned temp file(s) from {tmpdir}")


def start_job_staging(args):
    """The run's JobStaging, at the root of the removal session's mount.

    main() refuses a staging run without a removal session; this guard is for
    a caller that skipped it, so staging never removes through the kernel
    mount."""
    if removal_session is None:
        raise SystemExit("--target-pool staging needs a separate removal "
                         "session; none is open")
    return JobStaging(uuid.uuid4().hex[:16], removal_session.mountpoint,
                      session=removal_session)


def _start_staging(args):
    """Make the run's JobStaging and probe it: None, or why not.

    On a refusal the probe's directory is left, with a WARNING naming it, for
    --reap-staging: the kernel client has just made and written it, so an
    rmdir through the session would wait on the same cap recall as a removal
    at the end of a run."""
    global staging
    staging = start_job_staging(args)
    why = staging.probe(args.target_pool)
    if why:
        for p in staging._paths:
            logging.warning(f"Staging directory {p} left by the refused "
                            f"start, for --reap-staging")
        staging = None
    return why


def process_files(args):
    args.tmpdir = os.path.abspath(args.tmpdir)

    if not os.path.exists(args.tmpdir):
        os.makedirs(args.tmpdir)

    cleanup_tmpdir(args.tmpdir)

    hard_links = {}
    dir_layouts = {}
    roots_seen = []

    global staging
    if getattr(args, "target_pool", None) and not args.stage_in_tmpdir:
        if staging is None:
            # main() makes and probes it; this is for a caller that skipped it.
            staging = start_job_staging(args)
        staging.start_keepalive(REAP_STAGING_MIN_AGE_S / 4)
        logging.info("--target-pool %s: files whose directory points elsewhere "
                     "are staged in %s%s under %s, removed when the run ends",
                     args.target_pool, STAGE_PREFIX, staging.run_id[:8] + "*",
                     staging.root)
    else:
        staging = None

    mountpoints = set()
    with open("/proc/self/mounts", "r") as f:
        for line in f:
            mountpoints.add(line.split()[1])

    global regulator, regulator_started
    regulator = start_regulator(args)
    # Must come AFTER start_regulator(): _apply_regulate_keys() stays quiet
    # while this is False, which is what keeps the startup config read from
    # warning. Set it earlier and startup warns; drop it and no late change
    # ever warns. ProcessFilesStartup pins both directions.
    regulator_started = True

    global run_journal
    run_journal = RunJournal(
        enabled=args.run_journal,
        stop_at=[os.path.abspath(d) for d in args.dirs],
    )
    run_journal.start_checkpoints(roots_seen, args)

    try:
        with ThreadPoolExecutor(max_workers=_EXECUTOR_MAX_WORKERS) as executor:
            tmpdir_dev = os.stat(args.tmpdir).st_dev

            if args.paths_from or args.paths_from_pool:
                roots = []
                for start_dir in args.dirs:
                    start_dir = os.path.abspath(start_dir)
                    if args.stage_in_tmpdir and os.stat(start_dir).st_dev != tmpdir_dev:
                        logging.error(
                            f"--stage-in-tmpdir: tmpdir {args.tmpdir} is on "
                            f"another filesystem than {start_dir} (EXDEV); "
                            f"aborting"
                        )
                        sys.exit(1)
                    roots.append(start_dir)
                    roots_seen.append(start_dir)
                for opt, val in (("--prune-dir-regex", args.prune_re),
                                 ("--prune-small-subtrees",
                                  getattr(args, "prune_small_subtrees", False))):
                    if val:
                        logging.warning(
                            "%s has no effect with --paths-from: it prunes the walk, "
                            "and the list replaces the walk", opt)
                logging.info("Processing the list in %s, under %s",
                             args.paths_from_pool or args.paths_from, ", ".join(roots))
                process_paths(args, hard_links, executor, dir_layouts, roots)
                return

            for start_dir in args.dirs:
                start_dir = os.path.abspath(start_dir)
                if args.stage_in_tmpdir and os.stat(start_dir).st_dev != tmpdir_dev:
                    logging.error(
                        f"--stage-in-tmpdir: tmpdir {args.tmpdir} is on "
                        f"another filesystem than {start_dir} (EXDEV); aborting"
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
                roots_seen.append(start_dir)
                process_dir(args, start_dir, hard_links, executor, mountpoints, dir_layouts)
                if do_exit.is_set():
                    break
    finally:
        # Every exit Python runs (a return, an exception, SIGINT, SIGTERM or
        # SIGHUP) writes the journal here; the checkpoint covers SIGKILL. The
        # executor has joined, so no copy is in flight: the run's staging
        # directories are removed here, once.
        try:
            if staging is not None:
                staging.remove_all()    # journals each removal itself
        finally:
            # Whatever remove_all() hit, the journal is still written.
            if run_journal is not None:
                run_journal.write(roots_seen, args)
            if removal_session is not None:
                removal_session.close()

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
    # Redacted: this feeds _report_state()'s log line AND setproctitle(), so
    # an unscrubbed return leaks a command-line credential to the log on
    # every signal and to `ps` for the life of the job.
    return shlex.join(_redact_argv(argv))


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


# The signals _install_exit_handlers() routes to the clean exit.
_EXIT_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


def _exit_signal_handler(sig, frame):
    # The clean exit waits for in-flight copies. A second one of the same
    # signal escalates, as a plain kill did before SIGTERM had a handler;
    # otherwise kill -9 was the only way out, and it skips the journal write.
    #
    # The journal's checkpoint thread is not woken from here: an Event takes a
    # plain Lock, and this runs on the main thread, which can be holding that
    # lock itself. It polls do_exit every second instead (start_checkpoints).
    signal.signal(sig, signal.SIG_DFL)
    name = signal.Signals(sig).name
    n = thread_count.in_use if thread_count is not None else 0
    logging.error(f"{name} received, exiting cleanly: draining {n} in-flight "
                  f"cop{'y' if n == 1 else 'ies'} (send {name} again to stop "
                  "immediately)...")
    do_exit.set()


def _install_exit_handlers():
    """Route SIGINT, SIGTERM and SIGHUP to the same clean exit.

    SIGTERM is what systemd, kill(1) and a shutdown send, and SIGHUP is a
    closed terminal; by default both kill the job outright, skipping the run
    journal. An inherited SIG_IGN is kept for those two, so a job started
    under nohup still survives its terminal closing.
    """
    signal.signal(signal.SIGINT, _exit_signal_handler)
    for sig in (signal.SIGTERM, signal.SIGHUP):
        if signal.getsignal(sig) != signal.SIG_IGN:
            signal.signal(sig, _exit_signal_handler)


def _check_tmpdir_and_target(args):
    """The tmpdir and --target-pool checks a transcoding run makes at startup;
    exits on a refusal. Not for --reap-staging, which copies nothing."""
    layout = get_layout_walking_up(args.tmpdir)

    if layout is None:
        logging.error(
            f"Could not determine layout for tmpdir {args.tmpdir}. Is this a CephFS mount?"
        )
        sys.exit(1)

    logging.info(f"Temporary directory is {args.tmpdir} with pool {layout.pool}")

    # Decided once, before the config file is read, so that a config min_size
    # below an EC target's floor is refused as it is applied (_apply_config).
    args.target_kind = args.target_floor = None
    if args.target_pool:
        args.target_kind, _how = target_pool_kind(
            args.target_pool, args.target_pool_kind,
            {n for n in (_mds_namespace_for(d) for d in args.dirs) if n})
        if args.target_kind is None:
            logging.error("Refusing to start: %s", _how)
            sys.exit(1)
        if args.target_kind == "ec":
            args.target_floor = target_pool_floor(
                args.source_pool or layout.pool, args.target_pool)
        logging.info(
            "--target-pool %s: %s (decided by %s)%s", args.target_pool,
            "replicated" if args.target_kind == "rep" else "EC", _how,
            f", floor {args.target_floor} bytes" if args.target_floor else "")

    # The tmpdir must not be in a pool we are transcoding INTO: those files
    # would be rewritten for nothing, and a failed apply_file() would go
    # unnoticed, as the temp inherits the target pool. Refused, not prompted,
    # so a detached start cannot hang on it.
    conflicts = tmpdir_conflicts(args, layout.pool)

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

    # Advisory: below the crossover a transcode consumes MORE raw space than it
    # frees, and nothing else here would notice. Source defaults to the tmpdir's
    # pool (the FS default pool, where most untranscoded data lives); --source-pool
    # overrides it so a pool-to-pool drain is judged against the right baseline.
    # Best-effort -- needs the ceph CLI, and stays silent without it.
    for _d in args.dirs:
        _t = target_layout(args, get_layout_walking_up(_d))
        if _t is None:
            continue
        # A replicated --target-pool wins only BELOW a size, and its band is
        # enforced by target_pool_refusal(); this advisory is for EC targets.
        if args.target_pool and args.target_kind == "rep":
            continue
        _msg = crossover_warning(args.source_pool or layout.pool, _t.pool, args.min_size)
        if _msg:
            logging.warning(_msg)


def main():
    if sys.argv[1:2] == ["--removal-worker"]:
        sys.exit(_removal_worker(int(sys.argv[2]) if len(sys.argv) > 2
                                 else os.getppid()))
    global thread_count
    parser = argparse.ArgumentParser(
        description="Transcode cephfs files to their directory layout",
        epilog=(
            "files held open for writing:\n"
            "  --min-age is the only guard against moving a file that a\n"
            "  process still holds open for writing. The mtime/ctime/size\n"
            "  re-check before the swap catches a write during the copy, but\n"
            "  a writer that keeps the file open, sits idle past --min-age\n"
            "  and then writes again is writing to the replaced, unlinked\n"
            "  inode: those writes never reach the path. Neither the MDS nor\n"
            "  this tool can see such a writer, because the kernel client\n"
            "  drops its wanted caps on an open file left idle for about a\n"
            "  minute. Use --min-age 2 or more, and keep the tool off trees\n"
            "  whose writers hold files open for days (long-lived logs,\n"
            "  databases).\n\n"
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
            "  SIGRTMIN+4(38) dump current state/tunables to the log\n\n"
            "exit signals:\n"
            "  SIGINT, SIGTERM and SIGHUP stop cleanly: in-flight copies\n"
            "  finish and the run journal is written, which can take as\n"
            "  long as the largest copy. A second one of the same signal\n"
            "  stops at once. So under timeout(1), give -k, e.g.\n"
            "  timeout -k 30m 24h %(prog)s ...  A SIGHUP inherited as\n"
            "  ignored (nohup) stays ignored."
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
        "--paths-from-pool", metavar="POOL",
        help="Transcode exactly the files in POOL, read from the pool instead "
             "of walking the tree. POOL must not be the default write target "
             "of the directories given (refused). Needs the RADOS Python "
             "bindings; not with --paths-from. A file under a renamed directory "
             "is found again by inode (as root; each parent checked once, held "
             "by a pause and paced by --file-delay and --lookupino-rate, only "
             "under the directories given); a replaced or gone one is skipped "
             "and a live one it cannot locate is reported. Gone is not deletion "
             "evidence: a file moved out just before its directory was deleted "
             "is skipped until a re-run sees its new backtrace. A recently "
             "renamed file itself is read at its old path and counted vanished, "
             "so re-run to converge ('ceph tell mds.<daemon> flush journal' "
             "speeds that up).")
    parser.add_argument(
        "--lookupino-rate", metavar="N", type=_nonnegative_float,
        default=LOOKUPINO_RATE,
        help="MDS inode lookups per second for --paths-from-pool (default "
             "%(default)g; 0 turns them off).")
    parser.add_argument(
        "--rados-conffile", metavar="PATH",
        help="ceph.conf for --paths-from-pool. Default: $CEPH_CONF, then "
             "/etc/ceph/ceph.conf, then a single /etc/ceph/*.conf, then monitor "
             "addresses recovered from the mount. Containerized hosts often have "
             "no ceph.conf at all, hence the fallbacks.")
    parser.add_argument(
        "--rados-mon-host", metavar="ADDRS",
        help="Comma-separated monitor addresses for --paths-from-pool, used "
             "instead of a conffile.")
    parser.add_argument(
        "--rados-keyring", metavar="PATH",
        help="Keyring for --paths-from-pool. Default: whatever the conffile says.")
    parser.add_argument(
        "--rados-name", metavar="NAME", default=None,
        help="RADOS client name for --paths-from-pool (default client.admin). "
             "Needs read access to the pool and to its objects' xattrs.")
    parser.add_argument(
        "--paths-from", metavar="FILE",
        help="Transcode exactly the files listed in FILE ('-' for stdin; NUL- "
             "or newline-delimited) instead of walking. A path outside the "
             "given directories is refused, as is one reached through a "
             "symlinked directory under them or on another filesystem (the "
             "walk never follows a symlink either). Every path is re-checked, "
             "so a stale list is safe; multiply-linked files are declined.")
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
        help="Minimum age of file before transcoding, in days (adjustable at runtime via SIGRTMIN+2/SIGRTMIN+3). "
        "The only guard against files held open for writing; see below",
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
        help="Perform transcode but do not replace files. Without "
             "--stage-in-tmpdir each copy is still staged beside its target, "
             "which moves the directory's rctime, so the run journal records "
             "that pin.",
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
        "--stage-in-tmpdir",
        action="store_true",
        default=False,
        help="Stage temp copies in --tmpdir instead of beside their target. "
             "It is slow: staging elsewhere makes "
             "every replace a cross-rank distributed rename (mean 1266 ms, tail to "
             "14.9 s) instead of an intra-directory one (mean 0.26 ms). Escape "
             "hatch only.",
    )
    parser.add_argument(
        "--no-regulate",
        action="store_true",
        help="run unregulated, deliberately. Without this, starting with no "
             "regulate_prometheus_url logs at WARNING rather than INFO, because "
             "an omitted --config is otherwise indistinguishable from an "
             "intended unregulated run.",
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
             "The DEFAULT: in a 15-thread A/B copy_file_range is no faster "
             "overall (154.0 vs 170.1 MiB/s) and 1.3-2.1x SLOWER for files "
             "under 1 MiB, which is the size range these jobs work in.",
    )
    parser.add_argument(
        "--copy-file-range",
        dest="no_copy_file_range",
        action="store_false",
        help="Opt back in to copy_file_range (server-side copy when CephFS "
             "supports it). Off by default; see --no-copy-file-range.",
    )
    parser.add_argument(
        "--regulate-prometheus-url",
        default=None,
        metavar="URL",
        help="Prometheus /api/v1/query endpoint for self-regulation. Unset, "
             "the job runs at --file-delay and --threads and says so at "
             "WARNING (--no-regulate makes that deliberate and quiet). A "
             "user:password@ URL is refused: urllib ignores URL userinfo. "
             "Never put a credential on the command line; ps shows it.",
    )
    parser.add_argument(
        "--regulate-query",
        default=None,
        metavar="PROMQL",
        help="A complete PromQL expression returning ONE value in MILLISECONDS "
             "for the latency to protect. {volume} is substituted with the "
             "CephFS name from the mount, regex-escaped; write the query without "
             "it if that cannot be derived. Single line, no '#'.",
    )
    parser.add_argument(
        "--regulate-pause-ms", type=float, default=REG_PAUSE_MS_DEFAULT, metavar="MS",
        help="Pause the job while the query exceeds this (default 150).",
    )
    parser.add_argument(
        "--regulate-slo-ms", type=float, default=REG_SLO_MS_DEFAULT, metavar="MS",
        help="Soft target, used only to express readings as a percentage in log "
             "messages (default 75). --regulate-pause-ms is what actually gates.",
    )
    parser.add_argument(
        "--regulate-period-s", type=int, default=REG_PERIOD_S_DEFAULT, metavar="SEC",
        help="Seconds between samples (default 30).",
    )
    parser.add_argument(
        "--regulate-floor-ms", type=int, default=REG_FLOOR_MS_DEFAULT, metavar="MS",
        help="Lower bound on --file-delay that regulation may ease down to. "
             "Repeated pauses raise it; sustained quiet releases it back to this "
             "baseline (default 0).",
    )
    parser.add_argument(
        "--regulate-quiet-ticks", type=int, default=REG_QUIET_TICKS_DEFAULT, metavar="N",
        help="Consecutive clean samples before easing the delay. Also the nan "
             "samples a regulator pause needs before it resumes at 1 thread; "
             "a stalled MDS reads nan too (default %d)."
             % REG_QUIET_TICKS_DEFAULT,
    )
    parser.add_argument(
        "--regulate-max-threads", type=int, default=REG_MAX_THREADS_DEFAULT,
        metavar="N",
        help="Ceiling for regulator-driven thread increases (default 0 = never "
             "change the thread count). The regulator adds at most one thread "
             "per quiet interval, and only after the file delay has already "
             "decayed to its floor.",
    )
    parser.add_argument(
        "--regulate-blind-resume-s", type=non_negative_int,
        default=REG_BLIND_RESUME_S_DEFAULT, metavar="SEC",
        help="If the regulator has paused the job and no usable sample arrives "
             "for this long (Prometheus down, empty result, nan, or a mix), "
             "resume at 1 thread "
             "rather than stay paused; the first usable sample restores the "
             "rest (default %d; 0 = stay paused until a sample arrives)."
             % REG_BLIND_RESUME_S_DEFAULT,
    )
    parser.add_argument(
        "--regulate-blind-throttle-s", type=non_negative_int,
        default=REG_BLIND_THROTTLE_S_DEFAULT, metavar="SEC",
        help="If the job is running (not paused) and no usable sample arrives "
             "for this long, throttle it to 1 thread and at least "
             "--regulate-blind-delay-ms per file until one does, rather than "
             "keep whatever settings it had earned with nothing watching the "
             "MDS (default %d; 0 = hold the last settings indefinitely)."
             % REG_BLIND_THROTTLE_S_DEFAULT,
    )
    parser.add_argument(
        "--regulate-blind-delay-ms", type=non_negative_int,
        default=REG_BLIND_DELAY_MS_DEFAULT, metavar="MS",
        help="File delay a blind throttle raises the job to (default %d)."
             % REG_BLIND_DELAY_MS_DEFAULT,
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
        "--target-pool",
        default=None,
        metavar="POOL",
        help="Send eligible files to POOL instead of their directory's layout "
             "pool, without changing any directory layout: the copy is made in "
             "a hidden staging directory with POOL's layout (under the mount "
             "root, the file's pin boundary or its quota realm) and renamed "
             "into place; staging directories are removed when the run ends. "
             "Needs a size band: a replicated POOL --max-size below %d, an EC "
             "POOL --min-size at or above its floor (at least %d). With "
             "--paths-from or --paths-from-pool the list is grouped by "
             "directory first, spilling ~100 bytes per path beside --log-file "
             "(else in the system temp directory)."
             % (YIELD_FLOOR_BYTES, YIELD_FLOOR_BYTES),
    )
    parser.add_argument(
        "--reap-staging",
        action="store_true",
        help="Instead of transcoding, remove leftover staging directories "
             "(%s<id>) through a separate CephFS session: each untouched for "
             "--reap-staging-min-age, with its aged temps; anything else is "
             "reported and left. --paths-from FILE lists directories to check "
             "without a walk. Paced like a walk (--file-delay, --config "
             "threads, the regulator); the first SIGINT, SIGTERM or SIGHUP "
             "stops it."
             % STAGE_PREFIX,
    )
    parser.add_argument(
        "--reap-staging-min-age",
        type=non_negative_int,
        default=REAP_STAGING_MIN_AGE_S,
        metavar="SEC",
        help="With --reap-staging, take a staging directory only once its "
             "ctime, and each temp's mtime and ctime, are this old (default "
             "%d). Running jobs refresh theirs every quarter of the default, "
             "so a lower value can take a live job's directory."
             % REAP_STAGING_MIN_AGE_S,
    )
    parser.add_argument(
        "--target-pool-kind",
        choices=("rep", "ec"),
        default=None,
        help="Whether --target-pool is replicated or EC, for hosts without a "
             "usable ceph CLI. Without either, the pool's name must say: "
             "*.ecK.M.* for EC, exactly cephfs.<fs>.data or cephfs.<fs>.rdata for "
             "replicated; anything else is refused. Refused if it contradicts "
             "the cluster.",
    )
    parser.add_argument(
        "--prune-small-subtrees",
        action="store_true",
        default=False,
        help="Skip subtrees whose ceph.dir.rbytes/rfiles make them not worth "
             "walking (one getfattr per directory). Off by default, since it "
             "changes what a pass covers. Inert with --min-size 0 (warned).",
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
        help="Regex (re.search) matched against directory NAMES; a match is "
             "pruned, not descended into. Anchor with ^ / $, e.g. "
             "'\\.runfiles$'. Replaces the default set (DEFAULT_PRUNE_DIRS); "
             "'' disables pruning.",
    )

    parser.add_argument(
        "--no-run-journal",
        dest="run_journal",
        action="store_false",
        default=True,
        help="Do not write .vcephfs-transcode-runs.jsonl at each volume root. "
             "It keeps each artifact's ceph.dir.rctime and newest file mtime "
             "from before the run, since a transcode pins rctime and retention "
             "would read the data as new. One getxattr per artifact root; an "
             "unclean exit leaves a checkpoint at most %d minutes old in "
             ".vcephfs-transcode-runs.jsonl.<run>.partial."
             % (RUN_JOURNAL_CHECKPOINT_S // 60),
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

    try:
        validate_path_source(args.paths_from, args.paths_from_pool)
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
    cmdline = shlex.join(_redact_argv(sys.argv))
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

    if args.max_files is not None:
        logging.info(f"Will stop after transcoding {args.max_files} file(s)")

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

    if not args.reap_staging:
        _check_tmpdir_and_target(args)

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
        if 'threads' in cfg:
            _apply_config_threads(cfg['threads'])
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
        # An EC --target-pool takes every eligible file regardless of where its
        # directory points, so its floor holds at runtime too, not only at
        # startup. The config wins over the CLI, which makes this the check.
        if ('min_size' in cfg and getattr(args, "target_kind", None) == "ec"
                and cfg['min_size'] < args.target_floor):
            logging.error(
                "Config: min-size %d refused: below the %d-byte floor for "
                "--target-pool %s; keeping %d", cfg['min_size'],
                args.target_floor, args.target_pool, args.min_size)
            cfg = {k: v for k, v in cfg.items() if k != 'min_size'}
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
        _apply_regulate_keys(args, cfg)
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
        _start_config_poller(args.config_poll_seconds)

    _install_exit_handlers()
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

    start_removal_session(args)

    if args.reap_staging:
        # Regulated like a run: the config read above, its poller, the signal
        # handlers and the regulator all apply to the reap's pacing.
        global regulator, regulator_started, run_journal
        regulator = start_regulator(args)
        regulator_started = True
        run_journal = RunJournal(
            enabled=args.run_journal,
            stop_at=[os.path.abspath(d) for d in args.dirs])
        try:
            sys.exit(reap_staging(args))
        finally:
            if removal_session is not None:
                removal_session.close()

    # After the config's first read, since min_size may come from it.
    if args.target_pool:
        why = target_pool_refusal(args.target_kind, args.target_floor,
                                  args.min_size, args.max_size, args.target_pool)
        if why is None and not args.stage_in_tmpdir:
            why = _start_staging(args)
        if why:
            logging.error("Refusing to start: %s", why)
            sys.exit(1)

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
        f"{stats.files_vanished} vanished, "
        f"{stats.subtrees_pruned} subtrees pruned ({stats.bytes_pruned} bytes), "
        f"{stats.files_skipped_recent} too recent, "
        f"{stats.files_skipped_changed} changed during processing, "
        f"{stats.files_skipped_hardlink} hardlinks skipped, "
        f"{stats.files_skipped_open} open/locked, "
        f"{stats.files_skipped_layout_invalid} invalid layout, "
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
