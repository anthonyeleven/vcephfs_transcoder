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
import threading, uuid, argparse

_VERSION = "1984"

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
run_journal = None
thread_count = None
file_delay_ms = 0
min_age_days = 1

# --- staging: the temp copy goes beside its target, not in a shared tmpdir ---
#
# Renaming the finished temp file into place is where nearly all per-file wall
# time used to go. Controlled A/B/C, one client, same file size, 12 trials each
# (2026-09-05):
#
#     A. same directory (deep in the tree)     mean    0.3 ms  p50 0.3  max     0.4
#     B. tmpdir -> deep dir (the old default)  mean 1266.7 ms  p50 8.2  max 14914.7
#     C. sibling dir -> deep dir               mean  387.9 ms  p50 0.3  max  4637.0
#
# B is bimodal: its median is a healthy 8.2 ms, but the mean is set by a tail
# reaching fifteen seconds, and a stalled worker blocks everything queued behind
# it. The cause is a cross-rank distributed rename -- with `distributed=1` export
# pinning, a shared tmpdir and a deep destination hash to different MDS ranks, so
# every replace needs a two-phase commit between two MDSs.
#
# So the temp file is now created in the SAME directory as its target, making
# every replace an intra-directory rename. Re-measured 2026-09-08 on 62 real
# files, straced: 62/62 intra-directory, mean 0.26 ms, p50 0.21 ms, max 1.27 ms.
# That is case A, not C -- a true sibling, not a sibling directory.
#
# Two objections stood in the way; both were checked before this landed.
#
#   1. Detectability. The old --tmpdir had to sit on the default data pool so a
#      failed layout.apply_file() left the file somewhere visibly wrong. A
#      sibling temp file inherits the target directory's layout, which would mask
#      that. Answered by _apply_and_verify_layout(): apply the layout, then READ
#      IT BACK and compare, before any data is copied. That verifies the property
#      we care about instead of inferring it from where the file happens to live.
#
#   2. "Excess backtrace objects", per the old --tmpdir help. Measured, and
#      neutral: backtrace placement depends on the file's FINAL layout, not on
#      where the temp copy was staged. A file outside the first data pool costs
#      two objects either way. The only case that adds a third is switching a
#      file's layout after creation, which puts an entry in old_pools -- and a
#      sibling temp created directly in a target-pool directory performs no
#      switch at all.
#
# --stage-in-tmpdir restores the old behavior. Keep the A/B/C above as this
# change's regression test.

# --- what transcoding costs in objects, and where it lands -------------------
#
# Measured on a 980-OSD production cluster, 2026-09-05. All figures from live
# probes rather than from the documentation, because several of them are not
# what the documentation implies.
#
# FIRST vs DEFAULT. These are different things and conflating them is the easy
# mistake here:
#   - The filesystem's FIRST data pool is data_pools[0], fixed when the fs is
#     created. It cannot be changed and it cannot be removed.
#   - A directory's DEFAULT pool is whatever ceph.dir.layout.pool says for that
#     directory. It is freely settable, including on the volume root, which is
#     how a transcode target is chosen.
# Pointing the root layout at an EC pool changes where NEW files land. It does
# NOT change the first data pool.
#
# BACKTRACES. Every inode carries a backtrace -- the path from the inode to the
# root, used for hard-link resolution, for path lookup when the inode is not in
# the MDS cache, and by cephfs-data-scan to rebuild a lost metadata pool from
# the data pools alone. Verified placement:
#   - always in the FIRST data pool, whatever the file's layout;
#   - also in the file's CURRENT pool, when that differs from the first;
#   - also in every pool in the inode's old_pools, i.e. any pool its layout has
#     ever pointed at. Switching a file's layout after creation therefore leaves
#     a permanent backtrace behind in the old pool.
#
# So a file whose data lives outside the first pool costs TWO objects, not one:
# its data object, plus a zero-byte object in the first pool holding only the
# backtrace. Measured on real transcoded files: size 0, a 305-355 byte `parent`
# xattr, and zero omap entries. It is an xattr on a RADOS object, not omap --
# the data pools report 0 B of omap; all omap lives in the metadata pool.
#
# This is the price of using a non-first pool, NOT a price of transcoding. A
# file created directly on the EC pool carries exactly the same two objects.
# Transcoding only moves files into that state sooner. Once the volume root
# points at an EC pool, every new file pays it too.
#
# The old inode is genuinely gone. A transcode does not move a file: it creates
# a new inode, copies into it, and renames over the original, which unlinks the
# old inode completely -- verified absent from all pools after a journal flush.
# So a non-first source pool really can be emptied and deleted. The FIRST pool
# never can be: it keeps a backtrace for every inode in the filesystem.
#
# WHAT IT COSTS. Not capacity. Across 980 OSDs holding 17.4 PiB, BlueStore
# metadata (RocksDB/BlueFS) totalled 63.4 TiB, or 0.36% of used space, with the
# DB colocated on the block device on every OSD. The cost is OBJECT COUNT, which
# drives scrub duration, recovery granularity, peering cost and onode cache
# pressure. Per-GB is the wrong denominator; per-OSD and per-PG are the right
# ones. This is the mechanism behind "bytes leave, objects stay": a fully
# transcoded volume keeps one zero-byte object per file in its first pool
# forever, so the pool's object count does not fall even as its stored bytes
# approach zero.
#
# Corollary for widening an EC profile, e.g. 4+2 -> 8+2: shard count per object
# rises from 6 to 10, so object count rises with it. On a cluster where the DB
# is on a separate device rather than colocated, that is the case to size for
# before starting.

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
# Measured in production: 86.5% of files under the two largest volumes sit inside
# virtualenvs, and the largest of 585,309 of them is 503,423 bytes -- below the
# 512,000 byte threshold, so nothing eligible is lost. csa spent 22 days at
# 1.17% of its volume grinding a Bazel .runfiles tree carrying a pip
# site-packages copy of the awscli examples directory. gard4's zero-yield
# stretch was inside renv/packrat R package libraries.
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
        "# result) for this long resumes at 1 thread instead of holding 0",
        "# forever; the first usable sample restores the rest. 0 holds until",
        "# a sample arrives.",
        "regulate_blind_resume_s = %d" % REG_BLIND_RESUME_S_DEFAULT,
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
            'delay_min_ms', 'prune_subtree_max_bytes', 'prune_budget_bytes',
            'regulate_prometheus_url', 'regulate_query', 'regulate_pause_ms',
            'regulate_slo_ms', 'regulate_period_s', 'regulate_quiet_ticks',
            'regulate_floor_ms', 'regulate_max_threads',
            'regulate_blind_resume_s')

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
                elif k in ('regulate_max_threads', 'regulate_blind_resume_s'):
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

    def acquire(self, cancel=None, tick=None):
        """Acquire a permit, blocking until one is available.

        If *cancel* is a callable, it is checked each iteration; when it
        returns True the acquire is abandoned and this method returns False.
        A bounded wait (0.5 s) guarantees that signals (SIGINT, etc.) and the
        cancel callback are always serviced promptly, even when no other
        thread calls release() or set_limit().

        If *tick* is a callable, it runs between waits with the lock released.
        The walker passes its config poll here. It is the only thread that
        reads --config, and a threads=0 pause parks it right here, so without
        this the edit that would undo the pause was never read -- nor was any
        other edit, for as long as any pause lasted.
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
# Throttle this job against a latency signal the job itself cannot see. Its own
# stat latency is the wrong measure: it reports what the transcoder experiences,
# while the thing worth protecting is what the filesystem's real clients
# experience. That lives in monitoring, so we ask monitoring.
#
# This was a separate process (tc_governor.py) driving the job over SIGUSR1/
# SIGUSR2/SIGRTMIN and then grepping this log to discover whether each signal had
# landed. Roughly 150 lines of that existed only because it was a second process:
# PID discovery, signal-coalescing workarounds, a static per-host table of which
# volume lives where, and re-reading the job's own hit rate back out of its log.
# In-process those are an attribute read and an assignment. The separate process
# also died twice without anyone noticing, once leaving a job paused for four
# days; a thread that raises logs into this file, where it is visible.
#
# Everything is optional. With no regulate_prometheus_url the thread never starts
# and the job runs at whatever file_delay_ms and threads say -- the tool has to be
# fully usable by anyone who does not have these metrics, or indeed any Prometheus.
# Every regulate_* key in RuntimeConfig.KEYS must appear here, or it parses,
# validates, and is then discarded. That is exactly what happened to
# regulate_prometheus_url and regulate_query: a fully configured file still
# started with "Self-regulation disabled (no regulate_prometheus_url)", because
# only the four tuning keys below were ever copied onto args. Tested as an
# invariant rather than trusted to review.
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
    differs from the filesystem it lives on -- observed in production, where two
    similarly named directories resolved to entirely different filesystems -- and
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

    Two layers, and missing the second one is a hard parse error rather than a
    wrong match. re.escape() turns a dot into \\. , but PromQL string literals
    use Go escaping, where \\. is not a valid escape sequence:

        ceph_daemon=~"mds\\.myvol\\..*"   ->  HTTP 400
          parse error: unknown escape sequence U+002E '.'

    The backslash therefore has to survive the string layer as well, so every
    one re.escape() produces is doubled.
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
        self._last_err = 0.0
        self._paused_threads = None
        # When a regulator pause started going without a usable sample.
        self._blind_since = None
        # nan samples in a row during a regulator pause; see _nan_while_paused().
        self._nan_ticks = 0
        # What a 1-thread resume on no real evidence still owes; see _probing().
        self._probe_owed = None
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
            # under a running regulator. Concatenating it raised TypeError,
            # which run()'s hold path did log, but only as a bare
            # "unsupported operand type(s) for +: 'NoneType' and 'str'" --
            # nothing pointing at a config the operator had just emptied.
            # Raise the specific reason and let the same rate-limited hold
            # path report it.
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

        regulate_max_threads is a live tunable, and it used to be a one-way
        one: this clamped the ceiling DOWN toward it and never up, so raising
        it on a job that had already climbed to its ceiling did nothing at all
        and gave no hint why. Measured on a drain sitting at 6 threads
        with MDS reply latency at 0.3 ms against a 51 ms target -- there was
        two orders of magnitude of headroom and no supported way to use it
        short of SIGUSR1.

        An operator moving the configured maximum is an explicit instruction,
        so it wins over a ceiling this regulator earned by pausing. Pause
        evidence is still respected between such changes.

        This is the single reconciliation point for regulate_max_threads.
        _lower_ceiling() reads the raw option to decide whether adaptivity is
        on at all, but the ceiling itself is only ever moved here, so a live
        change is noticed exactly once however many callers observe it.
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
        """Add one thread, but only once the cheaper knob is exhausted.

        Ordering matters. File delay and thread count both change MDS load, so
        moving both in the same interval makes the next sample unattributable.
        The delay is eased first and this runs only when the delay has already
        reached its floor -- the point at which the regulator otherwise has no
        way left to turn spare headroom into throughput.

        Raising is deliberately slower to decide than shedding, because the two
        are not symmetric in effect: DynamicSemaphore hands out a new permit to
        a waiting worker within its 0.5 s poll, while a REDUCTION only takes
        hold as workers finish their current file and fail to re-acquire. Acting
        late on the way up costs one quiet interval; acting late on the way down
        costs however long the in-flight copies run.
        """
        ceil = self._thread_ceiling()
        if not ceil:
            return
        cur = thread_count.limit
        if cur <= 0 or cur >= ceil:
            return
        if file_delay_ms > self.floor_ms:
            return
        # "Not paused" is not the same as "has headroom": climb only while the
        # measurement is under the soft target, not merely under the pause line.
        if lat >= self.args.regulate_slo_ms:
            return
        thread_count.set_limit(cur + 1)
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
        cur = thread_count.limit
        if cur > 0:
            # Pausing a 1-thread probe must not shrink what it still owes.
            owed = self._probe_owed if self._probing() else None
            self._probe_owed = None
            self._paused_threads = max(cur, owed or 0)
            self._lower_ceiling(cur)
            thread_count.set_limit(0)
            logging.warning(
                "Regulator: PAUSE at %.1f ms (%.0f%% of the %.0f ms target) -- "
                "threads %d -> 0, in-flight copies will finish%s",
                lat, 100.0 * lat / self.args.regulate_slo_ms,
                self.args.regulate_slo_ms, cur, extra)

    def _paused_by_me(self):
        """True while threads sit at 0 because THIS regulator paused them.

        The operator can resume in the meantime (SIGUSR1, or a threads edit in
        --config). Forget the pause then, or a later operator pause reads as
        ours and gets "resumed" on the next quiet sample.
        """
        if self._paused_threads and thread_count.limit != 0:
            self._paused_threads = None
        return bool(self._paused_threads)

    def _probing(self):
        """True while threads sit at the 1 that a no-evidence resume set.

        nan and a Prometheus outage both resume a regulator pause without any
        evidence the filesystem has recovered, so they bring back 1 thread,
        not the pause's full count. The rest is owed, and the first usable
        sample under the pause line pays it through _resume(). Without the
        debt, a job with thread adaptivity off (the default) stayed at 1
        thread for good. As with _paused_by_me(), an operator change to the
        thread count forgets it.
        """
        if self._probe_owed and thread_count.limit != 1:
            self._probe_owed = None
        return bool(self._probe_owed)

    def _resume_one(self):
        """Take a regulator pause to 1 thread, owing the rest. Returns its count."""
        was = self._paused_threads
        self._paused_threads = None
        thread_count.set_limit(1)
        self._probe_owed = was if was > 1 else None
        _update_proctitle()
        return was

    def _resume(self):
        if self._paused_by_me():
            want, cur = self._paused_threads, 0
        elif self._probing():
            want, cur = self._probe_owed, 1
        else:
            return
        self._paused_threads = self._probe_owed = None
        ceil = self._thread_ceiling()
        if ceil and want > ceil:
            thread_count.set_limit(ceil)
            logging.info(
                "Regulator: resumed, threads %d -> %d (held under the %d it "
                "paused at by the learned ceiling)", cur, ceil, want)
        else:
            thread_count.set_limit(want)
            logging.info("Regulator: resumed, threads %d -> %d", cur, want)

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

        Mirror of _ease(). The cheap knob moves first -- raise the file delay
        -- and concurrency is only surrendered once the delay has climbed back
        to what the operator configured OR to REG_TIGHTEN_CAP_MS, whichever is
        lower, so a brief excursion does not cost a thread that takes
        quiet_ticks periods to earn back. With a configured delay above the cap
        the delay stops at the cap and shedding starts there; it is a bound on
        how long this will sit on the cheap knob, not a promise to restore an
        arbitrarily large delay first.

        A job configured with no file delay and no floor has want == 0, so
        there is no cheap knob to exhaust and the first soft-band tick sheds a
        thread. That is intended.

        A floor at or above REG_TIGHTEN_CAP_MS collapses the cheap-knob phase
        the same way. _note_pause() ratchets the floor up on repeated pauses,
        as far as REG_FLOOR_CAP_MS, and _ease() will not take the delay below
        it -- so a job that has been pausing enough to walk its floor past the
        cap arrives here already above the climb guard and sheds on the first
        soft-band tick. Also intended: the delay is at its floor, the cheap
        knob really is exhausted, and concurrency is the only lever left.
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
        cur = thread_count.limit
        if cur <= self._thread_base():
            return
        thread_count.set_limit(cur - 1)
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

        REG_FLOOR_DECAY_S already carries the argument for why a ratchet needs
        a release: one bad night walks the limit down step by step and it never
        comes back, so the job stays throttled long after the cause has gone.
        The delay floor got that release. The thread ceiling did not, and it is
        the more expensive of the two to lose.

        Observed 2026-09-23 on a drain, started --threads 1. It paused
        once at two threads, which set the ceiling to max(2 - 1, base) = 1, and
        then ran single-threaded for three and a half hours while its MDS
        reported 5.2 ms against a 51 ms target. Nothing short of an operator
        editing regulate_max_threads could free it, and there was no line in
        the log saying why it was slow.

        One step per quiet interval, the same shape as the floor's decay, so a
        ceiling earned by several pauses is handed back no faster than it was
        taken. Every tick that observes pressure resets the clock, so this only
        acts on quiet that has actually persisted -- see _pause() and
        _tighten().
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

        A monitoring outage is not evidence about the filesystem, and acting on
        absent data is worse than not acting -- EXCEPT when this regulator is
        the one holding threads at 0. Then "hold" is a pause with no exit: only
        a usable sample resumes it, and a Prometheus outage never sends one.
        So a regulator pause gets regulate_blind_resume_s of blindness, then
        resumes at 1 thread. The rest waits for a usable sample (_probing()),
        so that is as far as it goes blind.
        """
        now = time.time()
        if not self._paused_by_me():
            self._blind_since = None
            if now - self._last_err > REG_ERR_QUIET_S:
                logging.warning(
                    "Regulator: no usable sample (%s) -- holding current "
                    "settings, not adjusting", why)
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
            self._blind_since = None
            was = self._resume_one()
            logging.error(
                "Regulator: no usable sample for %ds while paused (%s) -- "
                "resuming BLIND, threads 0 -> 1 (paused at %d); the rest waits "
                "for a usable sample", blind, why, was)
        elif now - self._last_err > REG_ERR_QUIET_S:
            logging.warning("Regulator: still no usable sample (%s), paused "
                            "blind for %ds", why, blind)
            self._last_err = now

    def _nan_while_paused(self):
        """nan during our own pause: quiet, once it has lasted.

        A latency ratio goes 0/0 when no MDS request completed in the query
        window, which our own pause makes likely on a quiet volume, and
        holding on it was a pause that never ended. But an MDS that is
        stalled, or in replay/rejoin after a failover, completes nothing
        either, and that is what the pause is for. So it takes
        regulate_quiet_ticks nan samples in a row, the same evidence a quiet
        volume needs before easing, and then only 1 thread comes back.
        """
        self._blind_since = None
        self._nan_ticks += 1
        need = max(1, int(self.args.regulate_quiet_ticks))
        if self._nan_ticks == 1:
            logging.info(
                "Regulator: query returned nan while paused -- no MDS request "
                "completed in the query window; resuming at 1 thread if that "
                "lasts %d samples", need)
        if self._nan_ticks < need:
            return
        self._nan_ticks = 0
        was = self._resume_one()
        logging.warning(
            "Regulator: nan for %d samples while paused -- threads 0 -> 1 "
            "(paused at %d); the rest waits for a usable sample, since a "
            "stalled MDS reads nan too", need, was)

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
                    self._hold("query returned nan")
                do_exit.wait(period)
                continue
            except Exception as e:
                self._nan_ticks = 0
                self._hold(_redact_text(e, self.args.regulate_prometheus_url))
                do_exit.wait(period)
                continue
            self._blind_since = None
            self._nan_ticks = 0
            if lat >= self.args.regulate_pause_ms:
                self._quiet = 0
                self._pause(lat)
            elif lat >= self.args.regulate_slo_ms:
                # Between the soft target and the pause line the regulator had
                # nothing to say. Worse, "not paused" counted as quiet, so it
                # kept easing the delay DOWN while latency was already past
                # the target it is supposed to defend, and the only protection
                # left was the pause cliff. Back off a step instead.
                self._quiet = 0
                self._resume()
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

    Scheme-less values ARE covered: with no "://" the whole string is searched,
    so "bob:pw@host" counts as credentialed. A percent-encoded "@" counts too
    when no literal one is present.

    urlsplit() RAISES on an unbalanced "[" in the netloc (and, on recent
    CPython, on a bracketed host that is not an IP literal), while
    urllib.request does NOT validate the host at all: Request._parse() splits
    it with _splithost() and the request goes out regardless. Deriving the
    credentials from urlsplit() therefore failed OPEN -- on
    "http://bob:pw@[prom/api/v1/query" it returned nothing, so both rejection
    layers admitted the URL and _redact_text() had nothing to scrub, and the
    password reached the "first sample failed" / "no usable sample" WARNINGs.
    Parse it by hand instead: any non-empty userinfo counts as credentialed,
    so an unparseable URL fails CLOSED. The boundary matters as much as the
    parser -- see the comment below on why it is the last "@", not urllib's
    host boundary.
    """
    return _url_userinfo_split(url)[1]


def _url_userinfo_split(url):
    """(scheme_sep, userinfo, remainder) from the ORIGINAL string.

    ONE boundary, shared by _url_userinfo() and _loggable_url(). They used to
    compute it separately and a disagreement is the worst outcome available: a
    URL detected as credentialed by one and emitted intact by the other.
    Deriving both from this function makes drift impossible rather than
    merely unlikely.

    Everything is measured on the string the operator supplied, never on a
    decoded copy, so _loggable_url() can rebuild their URL minus the secret
    instead of logging a rewritten one.
    """
    s = str(url or "")
    _, sep, rest = s.partition("://")
    if not sep:
        rest = s
    # Deliberately NOT urllib's host boundary. Cutting at the first of "/?#"
    # BEFORE looking for "@" failed OPEN: in "http://bob:pa/ss@prom/api/v1/query"
    # the "@" falls past the cut, so the URL read as credential-free, neither
    # refusal layer fired, and the full password reached the startup "Config:"
    # line, the --no-regulate warning, Starting:/Finished:, every
    # _report_state() line and the process title. The secret to protect is what
    # the operator typed, not what urllib would transmit, so take the userinfo
    # from the LAST "@" anywhere after "://". A Prometheus base URL has no
    # legitimate reason to carry an "@", so over-refusing one costs nothing and
    # this fails CLOSED.
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
    that raises. It used to call .port, which raises ValueError on a
    non-numeric port while urlsplit() does not -- the raise landed outside
    the try, propagated through _apply_regulate_keys -> _apply_config ->
    RuntimeConfig.poll(), and killed the job at startup or mid-walk. Keeping
    it purely textual also preserves IPv6 brackets and the original host case.
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

    urllib.request.urlopen() does not use URL userinfo for authentication: it
    hands "user:pw@host" to http.client as the HOSTNAME. A credentialed URL
    with no explicit port therefore dies in _get_hostport() with
    InvalidURL("nonnumeric port: 'pw@host'"), and with a port it fails DNS on
    the same string -- so the exception text carries the PASSWORD, not a
    "user:pw@" pair a shape-matching regex would find. Scrub against the
    credentials the URL is known to hold instead of trusting the error's
    shape. Basic auth, if it is ever wanted, belongs in an
    HTTPBasicAuthHandler.
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

    Refusing a credentialed URL does not keep it out of the log: main() logs
    the whole command line in its Starting:/Finished: lines, and
    _report_state() logs _amended_cmdline() on every signal and every "Config
    reloaded", which also feeds setproctitle().

    Redacting TOKENS rather than the joined string, because the joined-string
    version this replaces was wrong three separate ways:
      - It matched only the full "--regulate-prometheus-url" spelling, but
        main()'s parser is built WITHOUT allow_abbrev=False, so argparse takes
        any unique prefix. "--regulate-prom http://u:pw@host" set the value
        while the scrubber saw nothing, and the password went to the log and
        the process title. Per-token redaction cannot care how a flag was
        spelled.
      - Scrubbing the credential substrings across the whole joined line hit
        every OTHER occurrence too: username "data" rewrote an unrelated
        "--tmpdir /data/tmp" as "/<redacted>/tmp".
      - shlex.join() quotes first, so a password containing an apostrophe
        came back as '"'"' and the raw-substring replace no longer matched.

    _loggable_url() handles the --flag=URL form unchanged, because the "://"
    split leaves "--flag=http" in its head.

    SCHEME-LESS values are covered too. Gating on "://" was NARROWER than
    _url_has_userinfo(), which matches a value with no scheme at all: so
    "--regulate-prometheus-url bob:pw@prom:9090/api" was refused by
    start_regulator() while the token still went unredacted into
    Starting:/Finished:, every _report_state() line and the process title.
    Detection and redaction accept the same inputs for tokens holding "://";
    for scheme-less tokens redaction stays narrower on purpose, and a
    username-only value like "tok@prom:9090/api" is refused but not redacted.

    A token containing "://" is unambiguously a URL, so it is redacted
    whenever it carries userinfo at all -- the same inputs _url_has_userinfo()
    refuses. That covers a username-only credential ("http://TOKEN@prom/...",
    a common way to pass a token) and an encoded separator
    ("http://bob%3Apw@prom/..."), both of which a ":" test misses.

    The ":" test survives only for SCHEME-LESS tokens, where it is what keeps
    ordinary arguments intact, and it is a deliberate trade-off rather than a
    shape anyone should tighten casually. Userinfo worth hiding is
    "user:password"; a lone "user@host" has no secret in it. So "/data/tmp"
    (no "@"), "/data/x@y" and an address like "bob@example.com" (an "@", but
    no ":" before it) are all returned untouched, while "bob:pw@host" is
    redacted. It tests the DECODED userinfo, so "bob%3Apw@host" is caught too.
    A path that genuinely contains both -- "/a:b@c" -- is over-redacted; that
    is the side the trade-off errs on.
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

        PromQL reaches here: --regulate-query goes through the same argv
        redaction, and a query carrying an "@" modifier with any ":" before it
        matched the ":"-in-userinfo guard. Recording-rule names
        ("job:metric:p99") and subqueries ("[1h:5m]") both hold a ":", so
        "max_over_time(x[1h:5m] @ end())" was logged as "<redacted>@ end())".
        That is not a leak, but it destroys output the operator reads, and
        start_regulator() logs the query in full elsewhere regardless.

        None of these characters is legal in an RFC 3986 userinfo or host, so
        their presence means the token is not an authority at all. Brackets
        stay legal in the HOST, because an IPv6 literal needs them: without
        that exemption "bob:pw@[::1]:9090/api" would stop being redacted,
        which is the failure direction that actually matters.

        Tested on the USERNAME and the host only, never the password: a
        password is opaque and may hold anything. Testing the whole userinfo
        let "bob:p(w@prom:9090/api" through -- refused by start_regulator(),
        then logged whole in Starting:/Finished: and ps. Most PromQL still
        fails here, on the part before its first ":" or on what follows the
        "@". Not all: "job:m:p99[5m]@1700000000" has a clean user and host,
        and is logged as "<redacted>@1700000000". That errs the safe way,
        costs only readability, and is pinned by a test.
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

    A list of (code, reason); empty means it would start. first_only=True
    returns as soon as one is found, which is what the single-reason wrapper
    below wants and, more importantly, preserves the original SHORT-CIRCUIT:
    with no URL configured the query is never resolved, so a job that never
    asked for a regulator still pays no scandir at startup.

    The late-change caller wants them ALL. Returning only the first meant that
    on an unregulated run missing both the URL and the query, a late change
    named just the URL -- the operator set it, reloaded, and only then heard
    about the query.

    ONE copy of the preconditions, shared by start_regulator() and the
    late-change warning in _apply_regulate_keys(). That warning used to
    promise a restart would help whenever a URL and a query were both
    present, which is wrong when slo >= pause or {volume} cannot resolve:
    the restart comes back unregulated with the same complaint. Callers map
    the code to their own wording, so the CONDITIONS live here only once.

    --no-regulate is deliberately not checked: it is not a misconfiguration,
    and both callers word that case differently.

    This calls _resolve_query(), which scandirs the volume root and reads
    /proc/self/mounts when the query uses {volume}. That cost is accepted on
    purpose -- it is only paid when a config reload actually changed a
    regulate_* key, which is rare, and advice a restart cannot satisfy is
    worse than one scandir.
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

    Split out of main()'s _apply_config so the late-change warning is testable.

    What a late change does depends on the key and on whether a regulator is
    actually running:

      - regulate_prometheus_url on a RUNNING regulator is live. sample()
        rebuilds the URL from args every tick and this function mutates that
        same namespace, so the next poll uses it. Advising a restart here
        would be wrong. Clearing it is the exception worth warning about.
      - regulate_query is frozen into Regulator.query at construction, so a
        change to it really does need a restart.
      - With no regulator running, no REGULATE_APPLY key takes effect,
        because the regulator is only ever started at startup -- whether it
        was never configured, was declined (e.g. slo_ms >= pause_ms), or was
        suppressed by --no-regulate, in which case the restart has to drop
        that flag too. Clearing a key there is the exception: nothing is
        waiting to take effect, so the plain INFO line is accurate.
      - The other tunables are read from args on every tick (or, for
        regulate_floor_ms, pushed through set_floor_base), so the INFO line
        is accurate for them on a running regulator.

    The bare setattr logs an INFO line that reads like success either way, so
    the cases that change nothing have to warn.
    """
    _changed = []                      # [(key, label, cleared)] actually applied
    for _k, _label in REGULATE_APPLY:
        if _k not in cfg or cfg[_k] == getattr(args, _k, None):
            continue
        _old = getattr(args, _k, None)
        if (_k == 'regulate_prometheus_url' and regulator_started
                and _url_has_userinfo(cfg[_k])):
            # Refuse a LATE credentialed URL only. A running regulator
            # rebuilds the URL from args every tick, so storing this would
            # break every subsequent sample and keep the credential in play.
            #
            # At STARTUP (regulator_started False) it is stored and
            # start_regulator() does the refusing, via
            # _regulator_decline_reason(). Refusing here too would take the
            # decision away from the one function that owns it, and -- worse
            # -- leave args.regulate_prometheus_url None, so start_regulator()
            # fell through to its "no regulate_prometheus_url" branch and
            # announced that a --config file which plainly HAS one does not.
            # The secret never reaches the network either way: that function
            # returns before sample().
            logging.warning(
                "Config: %s carries embedded credentials (%s) and was NOT "
                "applied -- urllib does not use URL userinfo for auth, so it "
                "would fail on every sample. Keeping the previous value.",
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
                "Config: %s changed, but this run is unregulated because of "
                "--no-regulate and the regulator is only started at startup "
                "-- restart the job WITHOUT --no-regulate for this to take "
                "effect", _labels)
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
                "Config: %s changed, but this run started unregulated and the "
                "regulator is only started at startup -- restart the job for "
                "this to take effect", _labels)
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
            "Config: %s was cleared while the regulator is running -- every "
            "sample will now fail and it will hold its current settings. Set "
            "a URL, or restart with --no-regulate if the run should be "
            "unregulated.", ", ".join(_clearedurl))
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
                "Self-regulation disabled by --no-regulate, which overrides the "
                "configured regulate_prometheus_url (%s) and regulate_query -- "
                "both ignored for this run. Running at file_delay=%dms "
                "threads=%d.",
                _loggable_url(url), file_delay_ms, thread_count.limit)
        else:
            logging.info("Self-regulation disabled by --no-regulate; "
                         "running at file_delay=%dms threads=%d",
                         file_delay_ms, thread_count.limit)
        return None
    # ONE evaluation of the preconditions, shared with the late-change warning
    # in _apply_regulate_keys() via _regulator_decline_reason(). Only the
    # WORDING lives here; the conditions do not, so the two callers cannot
    # drift apart the way they had (that warning promised a restart would help
    # whenever a URL and query were both set, which is false for an inverted
    # soft band or an unresolvable {volume}).
    _decline = _regulator_decline_reason(args)
    if _decline:
        _code, _reason = _decline
        if _code == "no_url":
            # Of the ways this function declines to start, an omitted --config
            # is the likeliest, and it was the only one logged below WARNING
            # while the others warn. That made the common accident the
            # quietest, and indistinguishable from a normal start in a
            # multi-gigabyte log. A run that means to go unregulated says so
            # with --no-regulate and stays quiet; an omission is greppable.
            #
            # Name the --config path when there is one: RuntimeConfig.poll() is
            # silent on FileNotFoundError for its first read (_mtime is None),
            # so a --config pointing at a file that does not exist arrives here
            # having logged nothing at all, and "set the keys in --config"
            # would otherwise be advice about a file the operator thinks they
            # already wrote. The file is on local disk, so say which it is.
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
                "pass --no-regulate to declare that this run is meant to be "
                "unregulated.", file_delay_ms, thread_count.limit, where)
        elif _code == "userinfo":
            # Reached for a CLI URL and for one from --config alike: the
            # apply-time path deliberately stores a credentialed URL at
            # startup and leaves the refusal to this function, so that this
            # message -- the one that explains the remedy -- is what the
            # operator sees. Nothing has sampled it; we return before that.
            logging.warning(
                "Self-regulation disabled: regulate_prometheus_url carries "
                "embedded credentials (%s), which urllib does not use for "
                "authentication -- it would fail on every sample. Remove the "
                "user:password@ from the URL; if the endpoint needs basic "
                "auth, it has to go through an HTTPBasicAuthHandler. Running "
                "at file_delay=%dms threads=%d.",
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
        # START ANYWAY. A failed FIRST sample is not evidence the query is
        # wrong, and this path used to disable self-regulation permanently
        # for the life of the job with no way back -- strictly less tolerant
        # than the poll loop it gates, which already holds settings and
        # retries on exactly this error.
        #
        # Three real causes, none of them permanent:
        #   * nan, because the query window is shorter than the volume's
        #     request interval: 0 requests / 0 requests. The quietest
        #     filesystems -- the safest ones to regulate -- were the ones
        #     that ended up with no regulator at all. Measured: two volumes
        #     sat at 1 thread and a 20ms delay for nearly seven hours,
        #     35-80x slower than their siblings, because of this.
        #   * Prometheus transiently down, 503, or slow at the moment this
        #     job happens to start.
        #   * a new volume with no MDS traffic yet.
        logging.warning(
            "Regulator: first sample failed (%s) -- starting anyway and will "
            "retry every %ds. If that error is 'nan', the query window is "
            "probably shorter than this volume's request interval; widen it. "
            "Query: %s",
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


TMP_SUFFIX = ".vcephfs-tc-tmp"
# A staged temp file is ".<32 hex>.vcephfs-tc-tmp". Matching on the whole shape
# rather than the suffix alone keeps reclamation from touching anything a user
# happened to name similarly.
TMP_RE = re.compile(r"^\.[0-9a-f]{32}" + re.escape(TMP_SUFFIX) + r"$")

# Never reclaim a temp file younger than this: a concurrent job may be mid-copy
# into it. No single file copy runs for a day.
TMP_ORPHAN_MIN_AGE_S = 24 * 3600

_reclaimed_dirs = set()
_reclaimed_lock = threading.Lock()

# One journal per volume root records what this run did to rctime.
#
# ceph.dir.rctime is a monotonic high-water mark, so transcoding pins every
# ancestor directory's rctime at the moment we ran, and a directory that is
# never written again stays pinned forever. Retention reads rctime as the
# answer rather than a hint (classify_retention.get_artifact_mtime, whose
# docstring forbids walking the tree instead), so a transcoded artifact reads
# as modified today and stops aging out. rctime cannot be restored: it is not
# settable, there is no ceph.dir.rmtime, and it only climbs. Preserving the
# answer before we destroy it is the only option left.
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

    note_file() must run BEFORE anything under an artifact is modified, because
    the first sighting is what captures the pre-transcode rctime. Both call
    sites stat every file ahead of any gating, so that ordering holds for the
    walk and for --paths-from alike.

    note_write() must run AFTER each replace, because what the consumer needs is
    the timestamp of the write whose value the MDS then stamped into rctime.
    Only that identifies which write an rctime is showing. The run's own
    start/end cannot: a run lasts days, so an rctime somewhere inside the window
    is as likely to be an owner write that landed mid-run as it is to be ours,
    and a consumer that treats it as ours discards real activity.

    WHAT A MATCH OBLIGES THE CONSUMER TO DO. If an artifact's current rctime
    matches its pinned_at, that rctime is ours and carries no information about
    owner activity -- but pre_rctime and max_file_mtime are NOT a substitute for
    it. Both are captured before or during the walk, so neither can see a write
    that landed after we stat'd a file and before our last replace beneath the
    artifact: a file created after the walk passed its directory is never
    note_file()'d at all, and one modified after we stat'd it keeps its old
    mtime here. That window is the whole time the run spends in the subtree,
    which is hours to days. So a match means RESCAN -- walk the artifact for a
    real max mtime. It does not mean "fall back to pre_rctime", which would
    silently date the artifact to before an owner write we never saw. Treat
    pre_rctime and max_file_mtime as a floor, not an answer.

    Consuming this requires a matching change in the retention scripts; without
    that the journal is written but nothing reads it.
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
            rec = self._artifacts.get(root)
            if rec is None:
                # First sighting: nothing under this root has been touched yet,
                # so this rctime is still the owner's, not ours.
                rec = {"pre_rctime": read_rctime(root), "max_file_mtime": 0.0,
                       "pinned_at": None}
                self._artifacts[root] = rec
                self._changes += 1
            if st.st_mtime > rec["max_file_mtime"]:
                rec["max_file_mtime"] = st.st_mtime
                self._changes += 1

    def note_write(self, filepath):
        """Record that we have just replaced a file beneath its artifact.

        The last of these is the write rctime ends up showing, which is what
        lets the consumer tell our pin from an owner write. Called after the
        rename, so a failed replace leaves no claim on the artifact -- and an
        artifact this run only READ keeps pinned_at null, because its rctime is
        still the owner's and must go on being trusted.

        Sampled and compared under the lock, and only ever moved forward. Two
        workers replacing different files under one artifact take different
        stripe locks, so nothing orders them against each other: sampling the
        clock first and assigning unconditionally lets the earlier of two
        interleaved writes land last and leave pinned_at BEHIND the write the
        MDS actually stamped. The consumer then fails to recognise its own pin.
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

        write() needs Python to unwind, so SIGKILL, the OOM killer or a power
        loss used to lose every record of the run -- and a run that died
        partway is the one whose damage most needs them. The snapshot is
        per-run, so no other run's rows are ever rewritten, and temp+rename
        means a reader sees one whole checkpoint or the one before it.

        write() supersedes and removes it. One left behind is a run that did
        not exit cleanly: the same rows plus "partial": true, with "end" the
        time of its last checkpoint.
        """
        if not self.enabled:
            return
        with self._io_lock:
            if self._final:
                return
            with self._lock:
                self._changes = 0
                self._pin_pending = False
                self._last_checkpoint = time.monotonic()
            t0 = time.monotonic()
            for volume_root, rows in self._rows_by_root(
                    list(roots), args, partial=True):
                if not self._write_partial(volume_root, rows):
                    with self._lock:
                        self._changes += 1     # retry next interval
            self._ckpt_cost = time.monotonic() - t0

    def _write_partial(self, volume_root, rows):
        """Replace this run's checkpoint at volume_root via temp+fsync+rename."""
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
            logging.warning("Could not checkpoint run journal %s: %s", path, e)
            # A temp cut short (ENOSPC after open) is never read; drop it.
            try:
                os.unlink(tmp)
            except OSError:
                pass
            return False

    def start_checkpoints(self, roots, args, every_s=RUN_JOURNAL_CHECKPOINT_S,
                          every_changes=RUN_JOURNAL_CHECKPOINT_CHANGES,
                          poll_s=10.0, pin_debounce_s=RUN_JOURNAL_PIN_DEBOUNCE_S):
        """Checkpoint in the background until write() runs.

        A thread, not a hook in the walk: a threads=0 pause parks the walker,
        and the drain after SIGTERM runs with no walker at all, while
        in-flight copies keep moving pinned_at.

        An artifact's first pin wakes it too. Its rctime is pinned from that
        moment, and a kill before the next periodic checkpoint lost the only
        pre_rctime it will ever have -- the record this whole thing exists
        to keep.
        """
        if not self.enabled or self._ckpt_thread is not None:
            return

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
                if not pending:
                    pin_seen = None
                else:
                    if pin_seen is None:
                        pin_seen = now
                    left = max(pin_debounce_s - (now - pin_seen),
                               RUN_JOURNAL_PIN_SPACING * self._ckpt_cost - age)
                    if left > 0:
                        timeout = min(timeout, left)
                    else:
                        pin_due = True
                # Once do_exit is set, systemd may SIGKILL the drain of
                # in-flight copies before write() is reached: stop waiting.
                if n and (pin_due or n >= every_changes or age >= every_s
                          or do_exit.is_set()):
                    pin_seen = None
                    try:
                        self.checkpoint(roots, args)
                    except Exception:
                        logging.exception("Run journal checkpoint failed")

        self._ckpt_thread = threading.Thread(
            target=loop, name="journal-checkpoint", daemon=True)
        self._ckpt_thread.start()

    def write(self, roots, args):
        """Append this run's records to a journal at each volume root."""
        if not self.enabled:
            return
        self._stop.set()
        self._wake.set()
        with self._io_lock:
            self._final = True
            for volume_root, rows in self._rows_by_root(roots, args):
                journal = os.path.join(volume_root, RUN_JOURNAL_NAME)
                try:
                    with open(journal, "a") as fh:
                        for row in rows:
                            fh.write(json.dumps(row, sort_keys=True) + "\n")
                        # Durable before the checkpoint goes: the unlink is an
                        # MDS op, and these bytes may still be dirty pages.
                        fh.flush()
                        os.fsync(fh.fileno())
                    logging.info("Wrote %d artifact record(s) to %s",
                                 len(rows), journal)
                except OSError as e:
                    logging.warning("Could not write run journal %s: %s",
                                    journal, e)
                    # The checkpoint is now the only record, so bring it up to
                    # date: the last periodic one misses everything since, and
                    # a run shorter than the interval never wrote one.
                    self._write_partial(
                        volume_root, [dict(r, partial=True) for r in rows])
                    continue
                try:
                    os.unlink(self._partial_path(volume_root))
                except FileNotFoundError:
                    pass
                except OSError as e:
                    logging.warning("Could not remove run journal checkpoint "
                                    "%s: %s", self._partial_path(volume_root), e)



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
            if now - st.st_mtime < TMP_ORPHAN_MIN_AGE_S:
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
    which only scans --tmpdir -- can no longer see it. Without this a SIGKILL
    mid-copy strands hidden objects on exactly the pools this tool exists to keep
    object counts down on.
    """
    with _reclaimed_lock:
        if dirpath in _reclaimed_dirs:
            return
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
                if now - entry.stat(follow_symlinks=False).st_mtime < TMP_ORPHAN_MIN_AGE_S:
                    continue
                os.unlink(entry.path)
                count += 1
            except OSError:
                pass
    except OSError:
        return
    if count:
        logging.info(f"Reclaimed {count} orphaned temp file(s) from {dirpath}")



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

    A shared --tmpdir on the default pool caught a failed apply_file() implicitly:
    the file stayed visibly in the wrong pool. A sibling temp file inherits the
    target directory's layout, so that signal is gone and the check has to be
    explicit -- which is the stronger of the two.
    """
    layout.apply_file(path)
    got = CephLayout.from_file(path)
    if got != layout:
        raise RuntimeError(
            f"layout did not apply to {path}: wanted {layout}, read back {got}")


def process_file(args, filepaths, st, layout, file_layout):
    if do_exit.is_set():
        return

    if not args.stage_in_tmpdir:
        # Covers both the walk and --paths-from, which has no walk to hook.
        reclaim_orphans(os.path.dirname(filepaths[0]))

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
        with open(tmp_file, "wb") as ofd:
            _apply_and_verify_layout(layout, tmp_file)
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

            # The FILE's mtime is preserved, by copystat above -- pipeline
            # archival (archive_*) and rsync both key on it directly.
            #
            # The parent DIRECTORY's mtime is deliberately NOT restored, and the
            # os.stat + os.utime that used to do it here are gone:
            #
            #   * It did not achieve its purpose. The point was to keep
            #     ceph.dir.rctime from jumping to the transcode date, but
            #     os.utime() updates the directory's ctime as a side effect, and
            #     rctime is a monotonic high-water mark over subtree ctimes that
            #     never rolls back. Verified live: a transcoded subtree's rctime
            #     reads as today either way, so retention reports see it as
            #     0 days old until it ages again. Deferring the utime to
            #     directory exit would not have helped either.
            #   * Nothing consumes it. Archive scripts select -type f, restic
            #     handles directory nodes independently, and rsync compares
            #     child file mtime and size.
            #   * It was the contention. Setting a directory's times requires an
            #     exclusive MDS auth cap (CEPH_CAP_AUTH_EXCL), which collides
            #     with directory walkers and with sibling workers in the same
            #     directory -- measured as utimensat stalls up to 4.94s.
            # Stage EVERY extra hardlink before renaming anything.
            #
            # The previous order renamed the first path, then linked and
            # renamed each remaining one in turn. A failure partway through
            # that loop -- ENOSPC, an MDS hiccup, a quota -- left some names
            # pointing at the new inode and the rest still at the old one:
            # files that were hardlinks to one inode silently became two
            # inodes with identical contents, with nothing detecting or
            # reporting the split.
            #
            # Linking from tmp_file (the new inode, before its rename)
            # rather than from filepaths[0] (the same inode, after) lets all
            # the staging happen first. If any link fails, nothing has been
            # renamed yet and the temp files are reclaimed as orphans. The
            # renames that follow are metadata-only.
            #
            # Hard links can live in different directories, so the staging
            # path is recomputed per target; reusing one would reintroduce
            # the cross-directory rename for every extra link.
            link_tmps = []
            try:
                for path in filepaths[1:]:
                    link_tmp = _tmp_path_for(args, path)
                    logging.info(f"Linking {tmp_file} -> {path}")
                    os.link(tmp_file, link_tmp, follow_symlinks=False)
                    link_tmps.append((link_tmp, path))
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
            os.rename(tmp_file, filepaths[0])
            for link_tmp, path in link_tmps:
                os.rename(link_tmp, path)

            if run_journal is not None:
                # After the renames: this is the write rctime now reflects.
                # Hard links can sit under different artifacts, so every target
                # is claimed, not just the first.
                for path in filepaths:
                    run_journal.note_write(path)

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


def _under_roots(path, roots):
    """Is *path* inside one of *roots*?

    Separate function so the prefix comparison is testable. A bare startswith()
    would put /vol/abc under /vol/ab, which is how a list for one volume ends up
    silently rewriting another.
    """
    return any(path == r or path.startswith(r.rstrip(os.sep) + os.sep)
               for r in roots)


# ---------------------------------------------------------------------------
# --paths-from-pool: read the candidate list out of a RADOS pool
#
# The walk prunes machine-generated directories by default, which is what makes
# it affordable -- on some volumes 86.5% of files are inside virtualenvs -- but
# it means those files are never moved, so a pool being drained never empties
# and cannot be deleted. Reading the pool directly costs no MDS walk at all and
# is proportional to what is LEFT rather than to the size of the tree. Measured
# on a 44 TiB pool: 4,074 files/s enumerating versus 0.23 files/s walking.
#
# Only the first object of a file, "<ino-hex>.00000000", carries the `parent`
# xattr, so this is one read per FILE rather than per object, and the backtrace
# holds the whole ancestry -- no per-level lookups. The MDS creates that object
# explicitly to store the backtrace (CInode.cc, op.create(false) before
# op.setxattr("parent", ...)), so sparse files with no data at offset 0 and
# zero-length files both still have one.
#
# Object naming lives above erasure coding: it is computed by the client-side
# striper purely from file_layout_t, and src/osdc/Striper.cc contains no EC
# references at all. Optimized/"fast" EC operates on the shards inside an
# already-named object, so none of this is affected by it.
#
# RENAME STALENESS -- the important caveat. Pointing the volume root at another
# pool freezes the pool's MEMBERSHIP: nothing new enters it. It does NOT freeze
# PATHS. A rename marks the inode STATE_DIRTYPARENT and the corrected backtrace
# is written only when the log segment holding that rename expires
# (LogSegment::try_to_expire walks dirty_parent_inodes calling store_backtrace).
# Until then the object still names the pre-rename path, this reads that stale
# path, the file is not there, and it is counted "vanished" -- while the file is
# alive under its new name and still in the pool. So a single enumeration is NOT
# final even on a frozen pool.
#
# It converges by RE-ENUMERATION once the MDS has trimmed its journal, and
# `ceph tell mds.<daemon> flush journal` forces that rather than waiting. For
# the last few objects in a pool, `ceph tell mds.<daemon> dump inode <ino>`
# returns the live path authoritatively; that is too costly per inode at scale
# and exactly right for closing a pool out.
# ---------------------------------------------------------------------------

BACKTRACE_XATTR = "parent"

# Cap on the distinct-inode set used by the end-of-listing cross-check.
# Measured cost is ~66 bytes per entry, so 50M entries is ~3.3 GB; a
# 665M-object pool would need ~44 GB. Past the cap the set is dropped and
# the cross-check is reported as skipped rather than exhausting memory.
INODE_TRACK_MAX = 50_000_000


class BacktraceError(Exception):
    """A backtrace that could not be decoded. Raised, never guessed around: a
    decoder that always returns something yields plausible WRONG paths."""


def decode_backtrace(blob):
    """Decode an inode_backtrace_t. Returns (ino, [dname ...] root-first, pool).

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
            fixed("<Q", 8)                  # dirino -- ancestry is by name
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
        return ino, list(reversed(names)), pool
    except BacktraceError:
        raise
    except (IndexError, struct.error) as e:
        raise BacktraceError(str(e)) from e


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


def _prefix_error(checked, example):
    """Message for "every derived path is missing".

    Two causes, and they cannot be told apart from here, so name both: a wrong
    ancestry prefix (the subtree-vs-root mount case) or a pool holding only
    objects whose files have since been deleted. Either way, transcoding
    nothing while reporting a clean pass is the wrong outcome.
    """
    return ("--paths-from-pool: none of the %d sampled derived paths exist (e.g. %s). "
            "Either the ancestry prefix is wrong for this mount -- check whether it "
            "is a subtree mount -- or every remaining object belongs to an "
            "already-deleted file. Refusing rather than reporting a clean pass "
            "that moved nothing." % (checked, example))


def _iter_pool_paths(args, roots):
    """Yield the CephFS path of every file currently in args.paths_from_pool.

    Guards, in order, because each one is a way this silently produces a wrong
    or incomplete list:

      * the bindings may be absent -- name the PACKAGE, not the module
      * the pool must not still be the volume's default write target, or the
        drain cannot converge and the list is stale the moment it is made
      * snapshots pin objects, so a snapshotted pool can never reach empty
      * every inode with objects must have a bno-0 object carrying the
        backtrace. That is what the MDS does today; asserting it at runtime
        means a future change is caught rather than silently dropping files
      * the derived path prefix must actually resolve, or the whole list is
        phantom paths that get counted as "vanished"
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

    for r in roots:
        try:
            cur = os.getxattr(r, "ceph.dir.layout.pool").decode()
        except OSError:
            continue
        if cur == pool:
            raise SystemExit(
                "--paths-from-pool %s is still the default write target of %s. New "
                "files keep landing in it, so a drain cannot converge and the list "
                "would be stale immediately. Repoint ceph.dir.layout.pool first."
                % (pool, r))
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
    yielded = 0
    checked = 0
    resolved = 0
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
            try:
                bt_ino, names, _pool = decode_backtrace(blob)
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
            if strip and rel[:len(strip)] == strip:
                rel = rel[len(strip):]
            path = os.path.join(mp, *rel) if rel else mp
            # Verify the derived prefix on the first handful rather than
            # emitting a whole list of paths that do not exist.
            if checked < 64:
                checked += 1
                exists = os.path.lexists(path)
                if exists:
                    resolved += 1
                else:
                    last_unresolved = path
                if checked == 64 and resolved == 0:
                    raise SystemExit(_prefix_error(checked, path))
            yielded += 1
            yield path
        # Evaluate the prefix guard again at end of listing. Gating it solely on
        # reaching 64 samples meant a pool with fewer candidates than that never
        # reached the abort -- and that is the near-empty pool a drain converges
        # toward, so the guard was weakest exactly where it matters most.
        if checked > 0 and resolved == 0:
            raise SystemExit(_prefix_error(checked, last_unresolved))
        complete = True
    finally:
        missing = (len(inodes_seen) - first_objs) if inodes_seen is not None else 0
        if complete and inodes_seen is None:
            logging.warning(
                "bno-0 cross-check skipped: more than %d distinct inodes",
                INODE_TRACK_MAX)
        if complete and missing > 0:
            logging.error(
                "%d inode(s) in %s have objects but no .00000000 object: no "
                "backtrace, no path, not in this list. Pool will not reach zero.",
                missing, pool)
        if complete and unusable > 0:
            logging.error(
                "%d inode(s) in %s have a .00000000 object whose backtrace could "
                "not be used (absent, undecodable, or for a different inode): no "
                "path was emitted for them and they will hold the pool above zero.",
                unusable, pool)
        logging.info(
            "--paths-from-pool %s: %d inodes, %d bno-0 objects (%d unusable), "
            "%d paths emitted, %d/%d sampled paths resolved%s",
            pool, len(inodes_seen) if inodes_seen is not None else -1,
            first_objs, unusable, yielded, resolved, checked,
            "" if complete else " (listing stopped early; counts are partial)")
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


def _poll_config():
    """Re-read --config if it changed. Walker thread only."""
    if runtime_config is not None:
        runtime_config.poll(apply_config)


def process_paths(args, hard_links, executor, dir_layouts, roots):
    """Transcode an explicit list of paths instead of walking the tree.

    A second pass at a lower --min-size already knows its candidates from the
    previous pass's log, and rediscovering them is the expensive part: the
    walker sleeps file_delay once per file ENCOUNTERED, before the stat, so a
    pass costs roughly rfiles x delay however few files actually qualify. On one
    production volume that is 195M paths visited to reach 8.7M candidates -- 45
    days of walking for 2 days of work.

    The list is a snapshot and it decays. Measured on that volume, 63% of the
    listed paths no longer existed six days later and 39% of the survivors had
    already moved to the target pool. So nothing here trusts the list: every
    path is re-stat'ed and re-checked exactly as the walker would, and a path
    that has since vanished is an expected outcome rather than an error.
    """
    def _limit_reached():
        return args.max_files is not None and stats.files_submitted >= args.max_files

    last_progress = time.monotonic()
    outside_logged = 0

    for filepath in _path_source(args, roots):
        if do_exit.is_set() or _limit_reached():
            return

        if runtime_config is not None:
            runtime_config.poll(apply_config)

        delay = file_delay_ms
        if delay > 0:
            time.sleep(delay / 1000.0)

        if time.monotonic() - last_progress > 60:
            stats.log_progress()
            last_progress = time.monotonic()

        filepath = os.path.abspath(filepath)

        # Containment. A list is easy to generate against the wrong volume, and
        # the damage would be silent and large, so a path outside the roots
        # given on the command line is refused rather than followed.
        if not _under_roots(filepath, roots):
            with stats._lock:
                stats.files_outside_root += 1
            outside_logged += 1
            if outside_logged <= 10:
                logging.warning("Skipping %s: outside the directories given on the "
                                "command line", filepath)
            elif outside_logged == 11:
                logging.warning("Further out-of-tree paths will be counted but not "
                                "logged individually")
            continue

        try:
            st = os.stat(filepath, follow_symlinks=False)
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

        dirpath = os.path.dirname(filepath)
        layout = dir_layouts.get(dirpath)
        if layout is None:
            layout = get_layout_walking_up(dirpath)
            if layout is None:
                logging.error(f"Could not determine layout for {dirpath}, skipping")
                with stats._lock:
                    stats.files_failed += 1
                continue
            dir_layouts[dirpath] = layout

        # Layout is checked BEFORE size here, the reverse of the walker. The
        # walker tests size first because that is the cheaper rejection: most of
        # what it encounters is tiny, and a stat alone settles it, so reading a
        # layout for every file would add an MDS round trip to the common case.
        # A path list is already filtered to plausible candidates, and its
        # staleest entries are precisely the ones an earlier pass has since
        # moved -- testing size first would log those as "below --min-size" and
        # conceal that they are already done, which is exactly how a previous
        # analysis came to overstate the remaining work.
        file_layout = CephLayout.from_file(filepath)
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
                process_file, args, [filepath], st, layout, file_layout
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
                _reclaim_named(dirpath, orphans)
        with _reclaimed_lock:
            _reclaimed_dirs.add(dirpath)

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
            if not thread_count.acquire(
                    cancel=lambda: do_exit.is_set() or _limit_reached(),
                    tick=_poll_config):
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
                # Both staging shapes: the bare UUID hex used by --stage-in-tmpdir,
                # and the ".<hex>.vcephfs-tc-tmp" a sibling-staged run leaves if
                # --tmpdir happens to sit inside the tree being walked.
                if not TMP_RE.match(entry.name):
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
    roots_seen = []

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
                            f"--stage-in-tmpdir was given and tmpdir {args.tmpdir} is on a "
                            f"different filesystem than {start_dir}. os.rename() will fail "
                            f"with EXDEV. Aborting."
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
                        f"--stage-in-tmpdir was given and tmpdir {args.tmpdir} is on a "
                        f"different filesystem than {start_dir}. os.rename() will fail with "
                        f"EXDEV. Aborting."
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
        # Every exit path Python gets to run: a return (including the
        # --paths-from branch's), an exception mid-walk, and SIGINT, SIGTERM
        # or SIGHUP, which set do_exit and unwind the walk to here. The
        # journal is the only record of the pre-transcode rctime, and a run
        # that died partway is exactly the one whose damage cannot be
        # reconstructed afterwards. SIGKILL, the OOM killer and power loss
        # never get here; the checkpoint covers those, one interval behind.
        if run_journal is not None:
            run_journal.write(roots_seen, args)

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


def _exit_signal_handler(sig, frame):
    # The clean exit waits for in-flight copies. A second one of the same
    # signal escalates, as a plain kill did before SIGTERM had a handler;
    # otherwise kill -9 was the only way out, and it skips the journal write.
    signal.signal(sig, signal.SIG_DFL)
    name = signal.Signals(sig).name
    logging.error(f"{name} received, exiting cleanly (send it again to stop "
                  "now)...")
    do_exit.set()


def _install_exit_handlers():
    """Route SIGINT, SIGTERM and SIGHUP to the same clean exit.

    SIGTERM is what systemd, kill(1) and a shutdown send, and SIGHUP is a
    closed terminal; both used to kill the job outright, skipping the run
    journal. An inherited SIG_IGN is kept for those two, so a job started
    under nohup still survives its terminal closing.
    """
    signal.signal(signal.SIGINT, _exit_signal_handler)
    for sig in (signal.SIGTERM, signal.SIGHUP):
        if signal.getsignal(sig) != signal.SIG_IGN:
            signal.signal(sig, _exit_signal_handler)


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
        "--paths-from-pool", metavar="POOL",
        help="Transcode exactly the files that are currently in POOL, discovered "
             "by reading the pool itself instead of walking the tree. Costs no MDS "
             "walk and is proportional to what is left in the pool rather than to "
             "the size of the filesystem. POOL must no longer be the default write "
             "target of the directories given, or the set keeps growing and the "
             "drain cannot converge; that is checked and refused. Needs the RADOS "
             "Python bindings. Mutually exclusive with --paths-from. NOTE: a file "
             "renamed after the listing keeps its old backtrace until the MDS "
             "trims the log segment holding the rename, so it is read at a stale "
             "path and counted vanished while still occupying the pool. One pass "
             "is therefore not final: re-run to converge, and "
             "'ceph tell mds.<daemon> flush journal' forces the backtrace writes "
             "instead of waiting for them.")
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
        help="Transcode exactly the files listed in FILE ('-' for stdin) instead "
             "of walking the directories. NUL- or newline-delimited, detected "
             "from the content. The directories still have to be given: they "
             "bound what the list is allowed to touch, and a listed path outside "
             "them is refused. Every path is re-stat'ed and re-checked, so a list "
             "that has gone stale is safe -- vanished files are counted and "
             "logged, not treated as errors. Multiply-linked files are declined "
             "in this mode because a list cannot promise it holds every link. "
             "Intended for a second pass whose candidates are already known from "
             "an earlier run's log, where walking the whole tree again to "
             "rediscover them is the expensive part.")
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
        "--stage-in-tmpdir",
        action="store_true",
        default=False,
        help="Stage temp copies in --tmpdir instead of beside their target. This "
             "is the pre-2026-09 behavior and it is slow: staging elsewhere makes "
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
             "DEFAULT since 2026-09-05: a 15-thread A/B measured "
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
        "--regulate-prometheus-url",
        default=None,
        metavar="URL",
        help="Prometheus /api/v1/query endpoint for self-regulation. Unset "
             "(the default) disables regulation entirely and the job simply "
             "runs at --file-delay and --threads -- and says so at WARNING, "
             "since an accidental omission looks exactly like a deliberate "
             "one. Pass --no-regulate to declare it deliberate and quiet. "
             "Credentials in the URL are NOT supported: urllib does not use "
             "URL userinfo for authentication, so a user:password@ URL is "
             "refused rather than failing on every sample -- use an "
             "HTTPBasicAuthHandler if the endpoint needs basic auth. A "
             "credential passed here is ALSO exposed in `ps`, which log "
             "scrubbing cannot undo: the title is only rewritten from the "
             "first retitle onward, so the original argv is visible until "
             "then -- and if python3-setproctitle is not installed the title "
             "is never rewritten at all and the credential stays visible for "
             "the whole run. Do not put a credential on the command line.",
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
        help="Consecutive clean samples before easing the delay (default 10).",
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
             "for this long (Prometheus down, empty result), resume at 1 thread "
             "rather than stay paused; the first usable sample restores the "
             "rest (default %d; 0 = stay paused until a sample arrives)."
             % REG_BLIND_RESUME_S_DEFAULT,
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

    parser.add_argument(
        "--no-run-journal",
        dest="run_journal",
        action="store_false",
        default=True,
        help="Do not write .vcephfs-transcode-runs.jsonl at each volume root. "
        "The journal records, per artifact, the ceph.dir.rctime and newest file "
        "mtime seen BEFORE this run modified anything, because transcoding pins "
        "rctime at the time we ran and a directory that is never written again "
        "stays pinned forever -- so retention reads transcoded data as fresh and "
        "stops expiring it. rctime cannot be restored (it is monotonic and not "
        "settable), so preserving the answer is the only option. Costs one "
        "getxattr per artifact root. A run that does not exit cleanly leaves "
        "its last checkpoint, at most %d minutes old, in "
        ".vcephfs-transcode-runs.jsonl.<run>.partial. Disable only if nothing "
        "consumes rctime." % (RUN_JOURNAL_CHECKPOINT_S // 60),
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
    # churn that burned 41 TiB of already-EC data on one volume. Second, and worse,
    # a silently failed apply_file() stops being detectable: the temp file
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

    # Advisory: below the crossover a transcode consumes MORE raw space than it
    # frees, and nothing else here would notice. Source defaults to the tmpdir's
    # pool (the FS default pool, where most untranscoded data lives); --source-pool
    # overrides it so a pool-to-pool drain is judged against the right baseline.
    # Best-effort -- needs the ceph CLI, and stays silent without it.
    for _d in args.dirs:
        _t = get_layout_walking_up(_d)
        if _t is None:
            continue
        _msg = crossover_warning(args.source_pool or layout.pool, _t.pool, args.min_size)
        if _msg:
            logging.warning(_msg)

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
