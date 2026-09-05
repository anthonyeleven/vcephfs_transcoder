# vcephfs_transcoder

This is a utility for migrating files between CephFS data pools
without changing directory paths or disrupting access.
A given CephFS volume comprises at least one data pool, but additional
pools may be added. Once attached to the MDS, new files can be directed
to one pool or the other via [file layouts](https://docs.ceph.com/en/latest/cephfs/file-layouts/).

A simple mv does not move data to a different CephFS data pool, as it is
a rename operation that touches only metadata. The
script works by writing a copy of each file to a staging directory, with
the new layout, then atomically moving the new copy on top of the original.

Use-cases include:

* Tiering by assorting files to faster, more efficient, cost-effective storage media
* Reclaiming raw capacity by converting from replication to erasure coding
* Reclaiming raw capacity by converting EC pool files to a wider EC profile

Two scripts are provided:

* `vcephfs-transcode-setup.sh` sets up a CephFS volume for migration of existing files,
  creating and attaching a new RADOS pool, along with an EC profile and
  CRUSH rule as needed. Care is taken to re-use existing EC profiles and
  CRUSH rules for clarity, and to avoid hitting the hard limit of 256
  CRUSH rules. The setup script has an option to execute migration once
  set up, but I recommend leaving this disabled so that a sanity check may
  be performed first, and because often enough the two scripts will need
  to be run on different systems.

  The new pool is set to be the _dir layout` for the specified subdirectory,
  which may or may not be the root of the CephFS mount. Migration can
  be specified for subtrees, potentially to different volumes.



* `vcephfs_transcoder.py` performs the actual migration.

Note that both require a CephX client user with the `p` [capability flag](https://docs.ceph.com/en/latest/cephfs/client-auth/).

Some feel that what this utility accomplishes does not qualify as transcoding.
Tough. Marcan42 called it that, and so it remains.

Usage:

```
Usage:
  vcephfs-transcode-setup.sh [OPTIONS]

Options:
  --mount PATH           Mount point for the volume
                         (e.g. /shared/cephlab/howie)
                         Volume name is derived from the last path
                         component (e.g. "howie").
  --ec-k INT             EC data chunks
  --ec-m INT             EC parity chunks
  --ec-plugin NAME       EC plugin (default: isa)
  --crush-device-class CLASS
                         Device class: ssd or hdd
  --replicated           Transcode to a replicated pool instead of EC.
                         Pool name: cephfs.<vol>.rdata
                         Incompatible with --ec-k and --ec-m.

  EC profile, CRUSH rule, and pool name are derived automatically:
    profile:  ec<k>.<m><class>       e.g. ec8.2hdd
    rule:     ec<k>.<m>-rule-<class> e.g. ec8.2-rule-hdd
    pool:     cephfs.<vol>.ec<k>.<m>.<class>.data
  --pg-num INT           PG count for new pool (default: 128)
  --tmpdir PATH          Temp dir for transcoder (default: mount/tmp)
  --fastec               Enable allow_ec_optimizations (FastEC)
  --compression MODE     Compression mode for new pool (default: aggressive)
                         (e.g. "aggressive", "force", "passive", "none")
  --compression-algorithm ALG
                         Compression algorithm (default: snappy)
                         (e.g. "snappy", "zstd", "lz4", "zlib")
  --min-age DAYS         Min file age in days (default: 1)
  --min-size SIZE        Min file size (e.g. "1M", "300K")
  --subdir PATH           Subdirectory under mount to transcode
                         (e.g. "ethel/merman")
  --log-file PATH        Log file for transcoder
  --skip-pool-setup      Skip pool creation (already done)
  --skip-layout          Skip layout / setfattr steps
  --transcode            Run the transcoder (off by default)
  --execute              Actually run commands (default: dry-run)
  --help                 Show this help

```


```
usage: vcephfs_transcoder.py [-h] [--version] [--tmpdir TMPDIR]
                             [--process-hardlinks] [--debug]
                             [--min-age MIN_AGE] [--min-size SIZE]
                             [--max-size SIZE] [--threads THREADS] [--dry-run]
                             [--config PATH] [--print-config-example [VOLUME]]
                             [--config-poll-seconds SEC] [--force-tmpdir-pool]
                             [--log-file LOG_FILE]
                             [--log-rotate-lines LOG_ROTATE_LINES]
                             [--log-rotate-time LOG_ROTATE_TIME]
                             [--log-rotate-size GIB] [--no-copy-file-range]
                             [--copy-file-range] [--source-pool POOL]
                             [--prune-small-subtrees]
                             [--prune-subtree-max-bytes BYTES]
                             [--prune-budget-bytes BYTES]
                             [--max-files MAX_FILES] [--file-delay MS]
                             [--prune-dir-regex REGEX]
                             [dirs ...]

Transcode cephfs files to their directory layout

positional arguments:
  dirs                  Directories to scan

optional arguments:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --tmpdir TMPDIR       Temporary directory to which to copy files. Important:
                        This directory should have its layout set to the
                        *default* data pool for the FS, to avoid excess
                        backtrace objects.
  --process-hardlinks   Process files with nlink > 1, which is potentially
                        dangerous
  --debug, -d
  --min-age MIN_AGE     Minimum age of file before transcoding, in days
                        (adjustable at runtime via SIGRTMIN+2/SIGRTMIN+3)
  --min-size SIZE       Skip files smaller than this size. Suffix B/K/M/G
                        (binary); plain number means bytes. 0 disables.
  --max-size SIZE       Skip files larger than this size (same format as
                        --min-size). Omit for no upper limit.
  --threads THREADS     Number of threads for data copying
  --dry-run, -n         Perform transcode but do not replace files
  --config PATH         key=value file of live tunables (file_delay_ms,
                        threads, min_age_days, min_size, prune_dir_regex), re-
                        read when its mtime changes. Keep it on local disk,
                        NOT in the CephFS volume: a stat there is an MDS round
                        trip.
  --print-config-example [VOLUME]
                        print a fully-populated --config file for VOLUME and
                        exit
  --config-poll-seconds SEC
                        how often to stat --config for changes (default: 10)
  --force-tmpdir-pool   proceed even if the tmpdir is in a target pool.
                        Disables the only check that makes a failed layout
                        application visible; do not use routinely.
  --log-file LOG_FILE   Also log to this file
  --log-rotate-lines LOG_ROTATE_LINES
                        Rotate the log file after this many lines (requires
                        --log-file)
  --log-rotate-time LOG_ROTATE_TIME
                        Rotate the log file after this duration, e.g. 30m, 2h,
                        1d (requires --log-file)
  --log-rotate-size GIB
                        Rotate the log file when it reaches this size in GiB
                        (requires --log-file)
  --no-copy-file-range  Disable use of copy_file_range and always use
                        userspace copy. DEFAULT since 2026-09-05: a 15-thread
                        A/B on a production volume measured copy_file_range no
                        faster overall (154.0 vs 170.1 MiB/s) and 1.3-2.1x
                        SLOWER for files under 1 MiB, which is the size range
                        these jobs now work in.
  --copy-file-range     Opt back in to copy_file_range (server-side copy when
                        CephFS supports it). Off by default; see --no-copy-
                        file-range.
  --source-pool POOL    Only transcode files whose CURRENT data pool is POOL.
                        Without it, every file not already on the target pool
                        is eligible. Use this to drain one pool into another
                        without also sweeping the default pool.
  --prune-small-subtrees
                        Skip whole subtrees whose recursive size makes them
                        not worth walking, using the MDS's own
                        ceph.dir.rbytes/rfiles (one getfattr per directory, no
                        walk). OFF by default: it changes what the pass
                        covers, which should always be deliberate. INERT
                        without --min-size, since the mean-size test compares
                        against min-size/8; a warning is logged at startup if
                        you pass this with --min-size 0.
  --prune-subtree-max-bytes BYTES
                        With --prune-small-subtrees, only prune a subtree
                        whose TOTAL recursive bytes are below this (default 1
                        GiB). This is a bound on what pruning can cost you:
                        whatever the size distribution inside, skipping the
                        subtree forgoes at most this many bytes.
  --prune-budget-bytes BYTES
                        With --prune-small-subtrees, stop pruning once this
                        many bytes have been skipped in total (default 100
                        GiB). Deliberately finite: the per-subtree bound above
                        says nothing about the aggregate, so without this a
                        pass could skip unbounded data a gigabyte at a time.
  --max-files MAX_FILES
                        Stop after submitting this many files for transcoding
  --file-delay MS       Delay in milliseconds before statting each new file
                        (adjustable at runtime via SIGRTMIN/SIGRTMIN+1)
  --prune-dir-regex REGEX
                        Regular expression (unanchored, via re.search) matched
                        against directory NAMES; any name containing a match
                        is pruned from the walk entirely (not descended into
                        or statted). Anchor with ^ / $ for exact names, e.g.
                        '\.runfiles$' to skip Bazel runfiles symlink farms.
                        Overrides the built-in default set (see
                        DEFAULT_PRUNE_DIRS); pass an empty string to disable
                        pruning entirely.

runtime signals:
  SIGUSR1  (10)  increase thread count by 1 (resumes from pause)
  SIGUSR2  (12)  decrease thread count by 1 (0 = pause)
  SIGTSTP  (20)  throttle to 1 thread (Ctrl+Z)
  SIGRTMIN (34)  increase file delay x1.25 (min +1ms, cap 600000ms)
  SIGRTMIN+1(35) decrease file delay /1.25 (min -1ms, floor 0ms)
  SIGRTMIN+2(36) increase min-age by 3 days
  SIGRTMIN+3(37) decrease min-age by 3 days (min 1)
  SIGRTMIN+4(38) dump current state/tunables to the log

example --config file (every key at its default):

    # vcephfs_transcoder runtime config -- VOLUME
    # Conventional path: /home/USER/tc_VOLUME.conf
    #
    # Re-read when this file's mtime changes (--config-poll-seconds,
    # default 10s). Only keys whose value CHANGED in the file are applied,
    # so editing one line never re-asserts the others -- a delay set by
    # signal survives an unrelated edit here.
    #
    # Keep this on LOCAL disk, never inside the CephFS volume: a stat there
    # is an MDS round trip, which is the load being bounded.
    #
    # Every key is optional. Commenting one out means this file does not
    # assert it at all, leaving it to the command line or to signals.
    
    # --- pace -------------------------------------------------------------
    # Sleep per file ENCOUNTERED in the walk, before the stat. Scan rate is
    # 1000/file_delay_ms files/s REGARDLESS of threads -- the walker is
    # single-threaded and is what gates a pass. 0 = unthrottled.
    file_delay_ms   = 0
    
    # Concurrent copies. Bounds the copier, not the walker. 0 pauses the job
    # (in-flight copies finish); set > 0 to resume.
    threads         = 1
    
    # --- how the delay signals move ---------------------------------------
    # SIGRTMIN multiplies the delay, SIGRTMIN+1 divides it. Separate rates
    # let backing off be coarser than recovering: 2.0 up with 1.1 down
    # retreats in doublings and probes back in 10% increments.
    delay_step_up   = 1.25
    delay_step_down = 1.25
    
    # Floor for the down direction. 0 permits a step to fully unthrottled;
    # raise it to guarantee the signal path can never do that on a live
    # filesystem.
    delay_min_ms    = 0
    
    # --- what counts as eligible ------------------------------------------
    # Skip files modified within this many days.
    min_age_days    = 1
    
    # Skip files smaller than this many bytes. Pre-Tentacle, EC pads small
    # objects to a whole stripe, which is why this is not lower.
    min_size        = 512000
    
    # --- what to skip entirely --------------------------------------------
    # Matched against directory NAMES during the walk; matches are never
    # descended into or statted. Cost is one regex match per directory, not
    # per file. Set empty to disable pruning entirely.
    prune_dir_regex = ^(\.runfiles|site\-packages|node_modules|__pycache__|\.venv|venv|penv|renv|packrat|\.git|\.tox|\.mypy_cache|\.pytest_cache|\.cargo|\.gradle)$

    redirect it to disk with:  vcephfs_transcoder.py --print-config-example > /home/USER/tc_VOLUME.conf
```



Example invocations:
```
/usr/local/sbin/vcephfs-transcode-setup.sh --mount /shared/ceph/ethel --ec-k 6 --ec-m 3 --crush-device-class ssd --subdir /merman

/usr/local/sbin/vcephfs_transcoder.py --min-age 1 --min-size 500k /shared/ceph/ethel/merman --tmpdir /shared/ceph/ethel/merman/tmp --log-file ~/ethel/transcode.log --dry-run

```



This was presented at Ceph Day Seattle 2026, and based on a script posted
to Reddit by `marcan42`.


## Why a file-level transcoder, and not RADOS-layer pool migration

Ceph has an in-flight alternative: transparent pool migration at the RADOS layer,
below the filesystem — see ceph/ceph
[#65746 "Pool Migration"](https://github.com/ceph/ceph/pull/65746) and
[#69112 "Pool Migration 2"](https://github.com/ceph/ceph/pull/69112) by `jamiepryde`.
It uses the redirect/manifest shape: the source pool keeps a small redirect object per
migrated object and clients are forwarded transparently.

The two approaches trade off in opposite directions, and which one you want depends
almost entirely on whether you need the source pool to go away.

| | This transcoder | RADOS-layer redirect |
|---|---|---|
| Layer | File, MDS-mediated | Object, below the filesystem |
| Residue in the source pool | A zero-byte backtrace object, and only in the filesystem's **first** data pool | A redirect object per migrated object, in **every** source pool |
| Can the source pool be deleted afterwards? | **Yes**, for any pool that is not the filesystem's first data pool | **No** — the redirects are load-bearing |
| Client transparency | Each replacement is an atomic rename, but it is per-file work | Fully transparent; no walk at all |
| Cost model | One full tree walk plus per-file MDS operations | No walk, but a permanent indirection on every read |
| Reversible mid-flight? | Yes — stop the job; whatever moved has moved | Bounded by the redirect layer's own lifecycle |

The short version: this tool pays an enormous one-time walk and in exchange leaves a
source pool that is genuinely empty and can be dropped. The redirect model skips the
walk entirely, and in exchange you carry the source pool and an extra hop forever.

### What "empty" actually means

Verified on a production cluster rather than inferred:

- A transcode does not move a file. It creates a **new inode**, copies into it, then
  renames over the original — which unlinks the old inode completely. After a journal
  flush the old inode's objects are absent from every pool, so a non-first source pool
  really does reach zero and can be deleted.
- Every inode keeps a backtrace in the filesystem's **first** data pool
  (`data_pools[0]`), regardless of where its data lives. That pool can never be
  emptied, and it is not the same thing as a directory's *default* pool
  (`ceph.dir.layout.pool`), which is freely settable and is how a transcode target is
  chosen. Pointing the volume root at an EC pool changes where new files land; it does
  not change the first data pool.
- A file whose data is outside the first pool therefore costs **two** objects: its data
  object, plus a zero-byte object in the first pool holding only the backtrace —
  measured at size 0, a 305-355 byte `parent` xattr, zero omap entries.

That last point is the price of using a non-first pool, **not** a price of transcoding:
a file created directly on the EC pool carries exactly the same two objects.

### The cost is object count, not capacity

Across 980 OSDs holding 17.4 PiB, with the BlueStore DB colocated on the block device
on every OSD:

```
    raw size         23512.7 TiB
    used total       17403.0 TiB
      of which data  17332.3 TiB
      of which META     63.4 TiB   <- RocksDB/BlueFS
      of which omap      7.4 TiB
    META as % of used: 0.36%
```

Metadata is 0.36% of used space, so capacity is not the concern. Object count is: it
drives scrub duration, recovery granularity, peering cost and onode cache pressure.
Per-GB is the wrong denominator; per-OSD and per-PG are the right ones.

This is the mechanism behind "bytes leave, objects stay" — a fully transcoded volume
keeps one zero-byte object per file in its first pool forever, so its object count does
not fall even as its stored bytes approach zero. Size pools for the object count they
will retain, not the bytes they will shed.

A corollary if you are widening an EC profile rather than migrating from replication:
4+2 to 8+2 raises shards per object from 6 to 10, so object count rises with it. On a
cluster whose DB lives on a separate device rather than colocated, that is the case to
size for before starting.
