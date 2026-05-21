#!/usr/bin/env python3
# CephFS pool/layout migration tool ("transcoder")
#
# Loosely inspired by:
# https://git.sr.ht/~pjjw/cephfs-layout-tool/tree/master/item/cephfs_layout_tool/migrate_pools.py
# https://gist.github.com/ervwalter/5ff6632c930c27a1eb6b07c986d7439b
#
# MIT license (https://opensource.org/license/mit)

import errno
import os, re, stat, time, signal, shutil, logging, sys, fcntl, dataclasses
from concurrent.futures import ThreadPoolExecutor
import threading, uuid, argparse

replace_lock = threading.Lock()
do_exit = threading.Event()
thread_count = None

# errno for "no data available" — ENODATA on Linux.
# We check explicitly rather than hardcoding 61, which means ECONNREFUSED on
# macOS/BSD.
ENODATA = getattr(errno, "ENODATA", 61)


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
    _lock: threading.Lock = dataclasses.field(default_factory=threading.Lock)

    def log_progress(self):
        logging.info(
            f"Progress: {self.files_transcoded} transcoded, "
            f"{self.files_failed} failed, "
            f"{self.bytes_copied / (1024**3):.1f} GiB copied"
        )


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
                shutil.copyfileobj(ifd, ofd, layout.object_size)
                # Flush to disk before we compare stats
                ofd.flush()
                os.fsync(ofd.fileno())
                # Lock released when ifd is closed

        shutil.copystat(filepaths[0], tmp_file, follow_symlinks=False)
        os.chown(tmp_file, st.st_uid, st.st_gid)

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
    for dirpath, dirnames, filenames in os.walk(start_dir, topdown=True):
        if do_exit.is_set():
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
            if do_exit.is_set():
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
            if do_exit.is_set():
                return

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

    with ThreadPoolExecutor(max_workers=args.threads) as executor:
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
        description="Transcode cephfs files to their directory layout"
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

    args = parser.parse_args()
    try:
        validate_size_bounds(args.min_size, args.max_size)
    except ValueError as e:
        parser.error(str(e))

    try:
        validate_age_bounds(args.min_age)
    except ValueError as e:
        parser.error(str(e))

    thread_count = threading.BoundedSemaphore(args.threads)

    log_level = logging.DEBUG if args.debug else logging.INFO
    handlers = [logging.StreamHandler()]
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file))
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=handlers,
    )
    if os.geteuid() != 0:
        logging.error("This tool must be run as root (requires chown).")
        sys.exit(1)

    layout = get_layout_walking_up(args.tmpdir)

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
        logging.error("SIGINT received, exiting cleanly...")
        do_exit.set()

    signal.signal(signal.SIGINT, signal_handler)

    process_files(args)

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
    )


if __name__ == "__main__":
    main()
