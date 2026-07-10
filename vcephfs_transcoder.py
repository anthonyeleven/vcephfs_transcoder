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

replace_lock = threading.Lock()
do_exit = threading.Event()
thread_count = None
file_delay_ms = 0

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
        self._cond = threading.Condition(threading.Lock())
        self._limit = value
        self._value = value  # available permits

    def acquire(self):
        with self._cond:
            while self._value <= 0:
                self._cond.wait()
            self._value -= 1

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
        self._rotate_lock = threading.Lock()
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
    files_skipped_large: int = 0
    files_failed: int = 0
    bytes_copied: int = 0
    copy_seconds: float = 0.0
    _lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)

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

        if copy_elapsed > 0:
            mbps = (st.st_size / (1024**2)) / copy_elapsed
            logging.info(
                f"Copied {filepaths[0]} [{st.st_size} bytes] in {copy_elapsed:.2f}s "
                f"({mbps:.1f} MiB/s) via {copy_method}"
            )
        else:
            logging.info(
                f"Copied {filepaths[0]} [{st.st_size} bytes] in <1ms via {copy_method}"
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

    with replace_lock:
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
        logging.debug(
            f"Scanning {dirpath} ({layout}): {len(dirnames)} dirs and {len(filenames)} files"
        )
        dir_layouts[dirpath] = layout

        def submit(filepaths, st, file_layout, _layout=layout):
            if do_exit.is_set() or _limit_reached():
                return
            thread_count.acquire()
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

            delay = file_delay_ms
            if delay > 0:
                time.sleep(delay / 1000.0)

            if time.monotonic() - last_progress > 60:
                stats.log_progress()
                last_progress = time.monotonic()

            filepath = os.path.join(dirpath, filename)
            st = os.stat(filepath, follow_symlinks=False)
            if not stat.S_ISREG(st.st_mode):
                logging.debug(f"Skipping {filepath}: not a regular file")
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
            if st.st_mtime > (time.time() - 86400 * args.min_age):
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


def main():
    global thread_count
    parser = argparse.ArgumentParser(
        description="Transcode cephfs files to their directory layout",
        epilog=(
            "runtime signals:\n"
            "  SIGUSR1  (10)  increase thread count by 1 (resumes from pause)\n"
            "  SIGUSR2  (12)  decrease thread count by 1 (0 = pause)\n"
            "  SIGTSTP  (20)  throttle to 1 thread (Ctrl+Z)\n"
            "  SIGRTMIN (34)  increase file delay by 100ms\n"
            "  SIGRTMIN+1(35) decrease file delay by 100ms"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("dirs", help="Directories to scan", nargs="+")
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
        "-m",
        default=1,
        type=int,
        help="Minimum age of file before transcoding, in days",
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
        "-t",
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
        action="store_true",
        help="Disable use of copy_file_range and always use userspace copy",
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

    args = parser.parse_args()
    try:
        validate_size_bounds(args.min_size, args.max_size)
    except ValueError as e:
        parser.error(str(e))

    try:
        validate_age_bounds(args.min_age)
    except ValueError as e:
        parser.error(str(e))

    thread_count = DynamicSemaphore(args.threads)

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

    if _cfr_label:
        logging.info(f"Using copy_file_range via {_cfr_label} (server-side copy when supported by CephFS)")
    elif args.no_copy_file_range:
        logging.info("copy_file_range disabled via --no-copy-file-range, using userspace copy")
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

    logging.warning(
        f"Temporary directory is {args.tmpdir} with pool {layout.pool}. "
        f"This should be the *default* data pool for the FS (NOT the target pool for your files). "
        f"If it is not, configure it with `setfattr -n ceph.dir.layout.pool -v default_data_pool_name {args.tmpdir}` then try again."
    )
    try:
        answer = input("Proceed? [y/N] ").strip().lower()
    except EOFError:
        answer = ""
    if answer != "y":
        logging.error("Aborted.")
        sys.exit(1)

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

    global file_delay_ms
    if args.file_delay < 0:
        parser.error("--file-delay must be non-negative")
    file_delay_ms = args.file_delay

    def sigrtmin_handler(sig, frame):
        global file_delay_ms
        old = file_delay_ms
        file_delay_ms = old + 100
        logging.info(f"SIGRTMIN received, file delay: {old}ms -> {file_delay_ms}ms")

    def sigrtmin1_handler(sig, frame):
        global file_delay_ms
        old = file_delay_ms
        file_delay_ms = max(old - 100, 0)
        logging.info(f"SIGRTMIN+1 received, file delay: {old}ms -> {file_delay_ms}ms")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTSTP, sigtstp_handler)
    signal.signal(signal.SIGUSR1, sigusr1_handler)
    signal.signal(signal.SIGUSR2, sigusr2_handler)
    signal.signal(signal.SIGRTMIN, sigrtmin_handler)
    signal.signal(signal.SIGRTMIN + 1, sigrtmin1_handler)

    logging.info(
        f"PID {os.getpid()}: "
        f"SIGUSR1/{signal.SIGUSR1} +1 thread, "
        f"SIGUSR2/{signal.SIGUSR2} -1 thread (0 = pause), "
        f"SIGTSTP (Ctrl+Z) throttle to 1, "
        f"SIGRTMIN/{signal.SIGRTMIN} / SIGRTMIN+1/{signal.SIGRTMIN + 1} adjust file delay ±100ms"
    )
    if file_delay_ms > 0:
        logging.info(f"File delay: {file_delay_ms}ms")

    process_files(args)

    if args.max_files is not None and stats.files_submitted >= args.max_files:
        logging.info(f"Stopped early: --max-files limit of {args.max_files} reached")

    logging.info(
        f"Complete: {stats.files_transcoded} transcoded, "
        f"{stats.files_failed} failed, "
        f"{stats.files_skipped_layout_match} already matched, "
        f"{stats.files_skipped_recent} too recent, "
        f"{stats.files_skipped_changed} changed during processing, "
        f"{stats.files_skipped_hardlink} hardlinks skipped, "
        f"{stats.files_skipped_open} open/locked, "
        f"{stats.files_skipped_small} below min-size, "
        f"{stats.files_skipped_large} above max-size, "
        f"{stats.bytes_copied / (1024**3):.1f} GiB copied"
        f"{stats._avg_throughput_str()}"
    )
    logging.info(f"Finished: {cmdline}")


if __name__ == "__main__":
    main()
