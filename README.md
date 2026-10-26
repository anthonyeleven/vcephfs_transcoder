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

  Unless given `--skip-layout`, it also sets the new pool as the directory
  layout of the volume root, so new files land there; the normal first pass
  below skips that and keeps the root replicated. Migration can be specified
  for subtrees, potentially to different volumes.

* `vcephfs_transcoder.py` performs the actual migration.

Note that the transcoder, and the setup script's layout step, require a CephX client user with the `p` [capability flag](https://docs.ceph.com/en/latest/cephfs/client-auth/).

Some feel that what this utility accomplishes does not qualify as transcoding.
Tough. Marcan42 called it that, and so it remains.

`--help` on either script prints every option. What follows is a normal
first pass, the alternative of pointing the whole volume at the new pool, the
options most runs never need, and the full `--help` of both.

## A normal first pass: the root stays replicated

A small file on EC 4+2 costs 9 RADOS objects (6 shards plus its 3-replica
backtrace in the first data pool) against 3 on a replicated pool, so small files
belong on the replicated pool and only large files are worth moving to EC. Every
file's backtrace lives in the first data pool, and setting a file's pool after it
is created would strand its backtrace there; the transcoder instead writes a new
inode in a staging directory that carries the target layout and renames it into
place.

So:

* The volume root keeps its layout on the replicated first data pool.
* The EC pool is created and added to the filesystem, but the root is not pointed
  at it.
* The first pass moves files of 600 KiB and up to the EC pool with `--target-pool`
  and `--source-pool`.
* EC directory layouts go only on exception subtrees whose new files are mostly
  large, so those land on EC directly.
* A recurring sweep, the same invocation, moves the large files that keep landing
  on the replicated pool.

The example uses volume `myvol`, mounted at `/mnt/myvol`, with a new EC 4+2
pool on SSDs.

**1. Create and attach the pool.** Run the setup script with `--skip-layout` on a
host with the admin keyring where the volume is mounted at the `--mount` path:
under `--execute` it checks that path is a mountpoint and stops if it is not. It
is a dry run unless given `--execute`, and it prints every command it would run.

```
vcephfs-transcode-setup.sh --mount /mnt/myvol --ec-k 4 --ec-m 2 \
    --crush-device-class ssd --pg-num 256 --skip-layout
# review the commands, then the same again with --execute
```

It reuses or creates the profile `ec4.2ssd` and the rule `ec4.2-rule-ssd`, creates
`cephfs.myvol.ec4.2.ssd.data` and adds it to the filesystem. With `--skip-layout`
it touches no layout.

The volume root must already carry an explicit layout naming its default pool
(`getfattr -n ceph.dir.layout /shared/ceph/myvol`; if it shows none, set it:
`setfattr -n ceph.dir.layout.pool -v cephfs.myvol.data /shared/ceph/myvol`,
which changes where no file lands). With no explicit layout anywhere, the
transcoder cannot resolve a directory's layout and refuses to start.

**2. Write the job's config file**, on local disk, never inside CephFS:

```
vcephfs_transcoder.py --print-config-example myvol > ~/tc_myvol.conf
```

Then set at least these keys (the query assumes the Ceph exporter's MDS
metrics; any PromQL that returns one value in milliseconds works):

```
min_age_days    = 2
min_size        = 614400
threads         = 2
regulate_prometheus_url = https://prometheus.example.com/api/v1/query
regulate_query  = 1e3 * sum(increase(ceph_mds_reply_latency_sum{ceph_daemon=~"mds\\.{volume}\\..*"}[5m])) / sum(increase(ceph_mds_reply_latency_count{ceph_daemon=~"mds\\.{volume}\\..*"}[5m]))
regulate_pause_ms = 30
regulate_slo_ms   = 15
```

`min_size = 614400` keeps small files off EC. The regulator pauses the job while
the volume's MDS reply latency is above `regulate_pause_ms`. Every key in the file
is applied at startup, over the command line, and again whenever you change it.
The same settings exist as options; the file is the one you can edit while the
job runs.

**3. Try it, then run it.** The transcode host needs the volume mounted as a CephX
user with the `p` cap, and runs as root. `--target-pool` removes its staging
directories through a second CephFS session, so it also needs `python3-cephfs`
and, in `/etc/ceph`, the cluster's conf file (the one whose fsid or mon addresses
match the mount) with that user's keyring beside it as
`<cluster>.client.<name>.keyring`. `--rados-name` and `--rados-keyring` name
another user and keyring.

```
mkdir /mnt/myvol/tmp      # the --tmpdir, on the root's replicated pool

# a dry run on one subtree: copies and verifies, replaces nothing
vcephfs_transcoder.py --config ~/tc_myvol.conf --tmpdir /mnt/myvol/tmp \
    --target-pool cephfs.myvol.ec4.2.ssd.data --source-pool cephfs.myvol.data \
    --dry-run --max-files 1000 /mnt/myvol/some/dir

# the pass
systemd-run --unit tc-myvol -p TimeoutStopSec=1h \
    /usr/local/sbin/vcephfs_transcoder.py \
    --config ~/tc_myvol.conf --tmpdir /mnt/myvol/tmp \
    --target-pool cephfs.myvol.ec4.2.ssd.data --source-pool cephfs.myvol.data \
    --log-file ~/tc.myvol.log --log-rotate-size 4 /mnt/myvol
```

A dry run is not free of side effects. With `--target-pool` it creates staging
directories (`.vcephfs-tc-stage-*`, under the mount root, the file's pin boundary
or its quota realm), removes them at exit, and moves their base directory's
`ceph.dir.rctime`. Without it, each copy is staged next to its target and moves
that directory's rctime. The run journal records either. `TimeoutStopSec` should
be longer than the largest copy takes, so a stop lets in-flight copies finish
instead of being killed.

**4. Watch it.** The log has a `Progress:` line every minute and a `Complete:` line
at the end that counts what was transcoded, failed, already matched, too recent,
and so on. Change the pace by editing the config file, or with the signals listed
in `--help`. `systemctl stop tc-myvol` sends SIGTERM, which lets in-flight copies
finish; `systemctl kill -s SIGTERM tc-myvol` sends a second one, which stops at
once.

A file whose directory layout reads back invalid (all zeros, `pool=-1`) is
skipped and counted as `invalid layout`; in a walk, the directory's whole subtree
is skipped and counted in the `(+N subtrees)` beside it. If the same paths are
skipped pass after pass, look at the directory by hand
(`getfattr -n ceph.dir.layout DIR`). A path whose layout cannot be resolved at
all is counted as `no layout` instead; the fix is the root `setfattr` from
step 1, not `getfattr` archaeology.

**5. Add EC layouts on exception subtrees.** Where a subtree's new files are
mostly large, point it at the EC pool so they land there directly:

```
setfattr -n ceph.dir.layout.pool -v cephfs.myvol.ec4.2.ssd.data /mnt/myvol/sims
```

New small files under it land on EC too, so pick only subtrees whose large-file
bytes outweigh that.

**6. Sweep.** Run the same `--target-pool` invocation again as a recurring job, one
pass after another and never two at once on a volume. With `min_age_days = 2` it
moves the large files that landed on the replicated pool since the last pass. A
pass is idempotent: files already moved count as "already matched", and files
skipped as too recent, open or locked, open for write elsewhere, changed during
the copy, or with an invalid layout are picked up by the next pass.

## Alternative: point the volume root at the new pool

Run the setup script without `--skip-layout`. It also makes
`/mnt/myvol/tmp` on the old pool and points the root's layout at the new
pool, so every new file, small ones included, lands there. That needs the volume
mounted on the setup host as a CephX user with the `p` cap, besides the admin
keyring. The layout commands run through a short-lived private mount of the
volume that the script makes for them (same source and user, `noshare`), which
starts with the current osdmap however long the shared mount has been idle, so
that user's keyring must be in `/etc/ceph` too. That covers the layout step
only: every other client catches up on its osdmap the first time it writes to
the new pool, so remount long-idle mounts of the volume before changing its
root. With `--transcode`, the script refuses to start when the client behind
`--mount` is more than `--max-map-lag` maps behind (default 1000). The transcoder then moves files to their directories' layouts, with no
`--target-pool`:

```
systemd-run --unit tc-myvol -p TimeoutStopSec=1h \
    /usr/local/sbin/vcephfs_transcoder.py \
    --config ~/tc_myvol.conf --tmpdir /mnt/myvol/tmp \
    --log-file ~/tc.myvol.log --log-rotate-size 4 /mnt/myvol
```

This suits a volume whose new files are nearly all large, or a move between two
replicated pools.

## Files held open for writing

Before the copy and again right before the swap, the transcoder reads its own
client's caps on the file (`ceph.caps`). No `Fc` means another client has it open
for write; `Fw` means a process on this host does. Either way the file is skipped
and counted as "open for write elsewhere", and a later pass retries it. The
mtime/ctime/size re-check before the swap catches a write during the copy; a
writer that opens the path between that check and the rename -- a window a few
MDS round trips wide -- is missed. Neither
check sees a writer that keeps the file open and sits idle: the kernel client drops
its wanted caps on an open file left idle for about a minute. When that writer
writes again, it writes to the replaced, unlinked inode, and those writes never
reach the path. `--min-age` stays the main guard: keep it at 2 (the default) or
more, and keep the transcoder off trees whose writers hold files open for days, such
as long-lived logs and databases. If the kernel cannot report `ceph.caps`, the
transcoder says so once at startup and runs without the check.

## Advanced options

For special cases a first pass is unlikely to hit. `--help` describes each.

Setup script: `--replicated` targets a replicated pool instead of EC;
`--ec-plugin`, `--fastec`, `--compression`, `--compression-algorithm` and
`--pg-autoscale-mode` change pool properties; `--skip-pool-setup` redoes only
the layout step; `--subdir` with `--transcode` has it launch the transcoder
itself. The generated help below glosses each.

Transcoder:

* Choosing files without a walk: `--paths-from FILE` (a list, e.g. from an earlier
  run's log) and `--paths-from-pool POOL` (read the pool itself to drain it), with
  `--lookupino-rate`, `--rados-conffile`, `--rados-mon-host`, `--rados-keyring` and
  `--rados-name`.
* `--target-pool-kind`: say whether `--target-pool` is replicated or EC on a host
  without the ceph CLI, when the pool's name doesn't.
* `--reap-staging`, `--reap-staging-min-age`: remove staging directories a killed
  `--target-pool` run left behind.
* `--max-size`: an upper size bound; a replicated `--target-pool` needs one.
* Both size bands in one run: `--small-pool` and `--large-pool` send each file by
  its size, below or at and above `--split-size` (default 614400), so a single walk
  moves small files to the replicated first data pool and large ones to EC, and
  sees every name of a hard-link set. `--small-files-to-any-pool` lets files below
  614400 bytes go to a pool other than the first.
* `--process-hardlinks`: move multiply-linked files, which are skipped by default.
  Every link has to be under the directories given.
* `--stage-in-tmpdir`, `--force-tmpdir-pool`: escape hatches, slow or unsafe for
  routine use.
* Walk pruning: `--prune-dir-regex` (replace the default set of pruned directory
  names), `--prune-small-subtrees`, `--prune-subtree-max-bytes`,
  `--prune-budget-bytes`.
* `--copy-file-range`, `--no-copy-file-range`: server-side copy, off by default.
* Regulator tuning: `--regulate-period-s`, `--regulate-floor-ms`,
  `--regulate-quiet-ticks`, `--regulate-max-threads`, `--regulate-blind-resume-s`,
  `--regulate-blind-throttle-s`, `--regulate-blind-delay-ms`; `--no-regulate` runs
  unregulated on purpose.
* Logs and limits: `--log-rotate-lines`, `--log-rotate-time`,
  `--log-rotate-size`, `--max-files`,
  `--config-poll-seconds`, `--debug`, `--no-run-journal`.

## Full option reference

Generated from the scripts themselves; regenerate both blocks whenever an option
changes:

```
COLUMNS=80 NO_COLOR=1 HOME=/root vcephfs_transcoder.py --help
vcephfs-transcode-setup.sh --help
```

### `vcephfs_transcoder.py --help`

<!-- BEGIN vcephfs_transcoder.py --help -->
```
usage: vcephfs_transcoder.py [-h] [--version] [--paths-from-pool POOL]
                             [--lookupino-rate N] [--rados-conffile PATH]
                             [--rados-mon-host ADDRS] [--rados-keyring PATH]
                             [--rados-name NAME] [--paths-from FILE]
                             [--tmpdir TMPDIR] [--process-hardlinks] [--debug]
                             [--min-age MIN_AGE] [--min-size SIZE]
                             [--max-size SIZE] [--threads THREADS] [--dry-run]
                             [--config PATH] [--print-config-example [VOLUME]]
                             [--config-poll-seconds SEC] [--stage-in-tmpdir]
                             [--no-regulate] [--force-tmpdir-pool]
                             [--log-file LOG_FILE]
                             [--log-rotate-lines LOG_ROTATE_LINES]
                             [--log-rotate-time LOG_ROTATE_TIME]
                             [--log-rotate-size GIB] [--no-copy-file-range]
                             [--copy-file-range]
                             [--regulate-prometheus-url URL]
                             [--regulate-query PROMQL]
                             [--regulate-pause-ms MS] [--regulate-slo-ms MS]
                             [--regulate-period-s SEC]
                             [--regulate-floor-ms MS]
                             [--regulate-quiet-ticks N]
                             [--regulate-max-threads N]
                             [--regulate-blind-resume-s SEC]
                             [--regulate-blind-throttle-s SEC]
                             [--regulate-blind-delay-ms MS]
                             [--source-pool POOL] [--target-pool POOL]
                             [--small-pool POOL] [--large-pool POOL]
                             [--small-files-to-any-pool] [--split-size SIZE]
                             [--reap-staging] [--reap-staging-min-age SEC]
                             [--target-pool-kind {rep,ec}]
                             [--prune-small-subtrees]
                             [--prune-subtree-max-bytes BYTES]
                             [--prune-budget-bytes BYTES]
                             [--max-files MAX_FILES] [--file-delay MS]
                             [--prune-dir-regex REGEX] [--no-run-journal]
                             [dirs ...]

Transcode cephfs files to their directory layout

positional arguments:
  dirs                  Directories to scan

options:
  -h, --help            show this help message and exit
  --version             show program's version number and exit
  --paths-from-pool POOL
                        Transcode exactly the files in POOL, read from the
                        pool instead of walking the tree. POOL must not be the
                        default write target of the directories given
                        (refused). Needs the RADOS Python bindings; not with
                        --paths-from. A file under a renamed directory is
                        found again by inode (as root; each parent checked
                        once, held by a pause and paced by --file-delay and
                        --lookupino-rate, only under the directories given); a
                        replaced or gone one is skipped and a live one it
                        cannot locate is reported. Gone is not deletion
                        evidence: a file moved out just before its directory
                        was deleted is skipped until a re-run sees its new
                        backtrace. A recently renamed file itself is read at
                        its old path and counted vanished, so re-run to
                        converge ('ceph tell mds.<daemon> flush journal'
                        speeds that up).
  --lookupino-rate N    MDS inode lookups per second for --paths-from-pool
                        (default 20; 0 turns them off).
  --rados-conffile PATH
                        ceph.conf for --paths-from-pool. Default: $CEPH_CONF,
                        then /etc/ceph/ceph.conf, then a single
                        /etc/ceph/*.conf, then monitor addresses recovered
                        from the mount. Containerized hosts often have no
                        ceph.conf at all, hence the fallbacks. With --rados-
                        name and --rados-keyring, this and --rados-mon-host
                        also locate the cluster for the staging-removal
                        session of a run that stages (--target-pool, --small-
                        pool, --large-pool), and of the startup ceph CLI
                        checks (pool kinds, the first data pool). Without
                        them, those checks use the identity that session
                        would: the first root's mount name= and its cluster's
                        conf and keyring.
  --rados-mon-host ADDRS
                        Comma-separated monitor addresses for --paths-from-
                        pool, used instead of a conffile.
  --rados-keyring PATH  Keyring for --paths-from-pool. Default: whatever the
                        conffile says. Given with --rados-name, also the
                        keyring of a staging run's staging-removal session and
                        of the startup ceph CLI checks.
  --rados-name NAME     RADOS client name for --paths-from-pool (default
                        client.admin). Needs read access to the pool and to
                        its objects' xattrs. Given with --rados-keyring, also
                        the user of a staging run's staging-removal session,
                        which otherwise is the mount's name=, and of the
                        startup ceph CLI checks.
  --paths-from FILE     Transcode exactly the files listed in FILE ('-' for
                        stdin; NUL- or newline-delimited; a NUL list is read
                        byte-exact) instead of walking. A path outside the
                        given directories is refused, as is one reached
                        through a symlinked directory under them or on another
                        filesystem (the walk never follows a symlink either).
                        Every path is re-checked, so a stale list is safe;
                        multiply-linked files are declined.
  --tmpdir TMPDIR       Temporary directory to which to copy files. Important:
                        This directory should have its layout set to the
                        *default* data pool for the FS, to avoid excess
                        backtrace objects.
  --process-hardlinks   Process files with nlink > 1, which is potentially
                        dangerous
  --debug, -d
  --min-age MIN_AGE     Minimum age of file before transcoding, in days
                        (adjustable at runtime via SIGRTMIN+2/SIGRTMIN+3). The
                        main guard against files held open for writing; see
                        below
  --min-size SIZE       Skip files smaller than this size. Suffix B/K/M/G
                        (binary); plain number means bytes. 0 disables.
  --max-size SIZE       Skip files larger than this size (same format as
                        --min-size). Omit for no upper limit.
  --threads THREADS     Number of threads for data copying
  --dry-run, -n         Perform transcode but do not replace files. Without
                        --stage-in-tmpdir each copy is still staged, beside
                        its target or, with --target-pool, --small-pool or
                        --large-pool, in a staging directory, which moves that
                        directory's rctime, so the run journal records that
                        pin.
  --config PATH         key=value file of live tunables, re-read when its
                        mtime changes; the example at the end of this help
                        lists every key. Keep it on local disk, NOT in the
                        CephFS volume: a stat there is an MDS round trip.
  --print-config-example [VOLUME]
                        print a fully-populated --config file for VOLUME and
                        exit
  --config-poll-seconds SEC
                        how often to stat --config for changes (default: 10)
  --stage-in-tmpdir     Stage temp copies in --tmpdir instead of beside their
                        target. It is slow: staging elsewhere makes every
                        replace a cross-rank distributed rename (mean 1266 ms,
                        tail to 14.9 s) instead of an intra-directory one
                        (mean 0.26 ms). Escape hatch only.
  --no-regulate         run unregulated, deliberately. Without this, starting
                        with no regulate_prometheus_url logs at WARNING rather
                        than INFO, because an omitted --config is otherwise
                        indistinguishable from an intended unregulated run.
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
                        userspace copy. The DEFAULT: in a 15-thread A/B
                        copy_file_range is no faster overall (154.0 vs 170.1
                        MiB/s) and 1.3-2.1x SLOWER for files under 1 MiB,
                        which is the size range these jobs work in.
  --copy-file-range     Opt back in to copy_file_range (server-side copy when
                        CephFS supports it). Off by default; see --no-copy-
                        file-range.
  --regulate-prometheus-url URL
                        Prometheus /api/v1/query endpoint for self-regulation.
                        Unset, the job runs at --file-delay and --threads and
                        says so at WARNING (--no-regulate makes that
                        deliberate and quiet). A user:password@ URL is
                        refused: urllib ignores URL userinfo. Never put a
                        credential on the command line; ps shows it.
  --regulate-query PROMQL
                        A complete PromQL expression returning ONE value in
                        MILLISECONDS for the latency to protect. {volume} is
                        substituted with the CephFS name from the mount,
                        regex-escaped; write the query without it if that
                        cannot be derived. Single line, no '#'.
  --regulate-pause-ms MS
                        Pause the job while the query exceeds this (default
                        150).
  --regulate-slo-ms MS  Soft target, used only to express readings as a
                        percentage in log messages (default 75). --regulate-
                        pause-ms is what actually gates.
  --regulate-period-s SEC
                        Seconds between samples (default 30).
  --regulate-floor-ms MS
                        Lower bound on --file-delay that regulation may ease
                        down to. Repeated pauses raise it; sustained quiet
                        releases it back to this baseline (default 0).
  --regulate-quiet-ticks N
                        Consecutive clean samples before easing the delay.
                        Also the nan samples a regulator pause needs before it
                        resumes at 1 thread; a stalled MDS reads nan too
                        (default 10).
  --regulate-max-threads N
                        Ceiling for regulator-driven thread increases (default
                        0 = never change the thread count). The regulator adds
                        at most one thread per quiet interval, and only after
                        the file delay has already decayed to its floor.
  --regulate-blind-resume-s SEC
                        If the regulator has paused the job and no usable
                        sample arrives for this long (Prometheus down, empty
                        result, nan, or a mix), resume at 1 thread rather than
                        stay paused; the first usable sample restores the rest
                        (default 1800; 0 = stay paused until a sample
                        arrives).
  --regulate-blind-throttle-s SEC
                        If the job is running (not paused) and no usable
                        sample arrives for this long, throttle it to 1 thread
                        and at least --regulate-blind-delay-ms per file until
                        one does, rather than keep whatever settings it had
                        earned with nothing watching the MDS (default 600; 0 =
                        hold the last settings indefinitely).
  --regulate-blind-delay-ms MS
                        File delay a blind throttle raises the job to (default
                        20).
  --source-pool POOL    Only transcode files whose CURRENT data pool is POOL.
                        Without it, every file not already on the target pool
                        is eligible. Use this to drain one pool into another
                        without also sweeping the default pool.
  --target-pool POOL    Send eligible files to POOL instead of their
                        directory's layout pool, without changing any
                        directory layout: the copy is made in a hidden staging
                        directory with POOL's layout (under the mount root,
                        the file's pin boundary or its quota realm) and
                        renamed into place; staging directories are removed
                        when the run ends. Needs a size band: a replicated
                        POOL --max-size below 614400, an EC POOL --min-size at
                        or above its floor (at least 614400). With --paths-
                        from or --paths-from-pool the list is grouped by
                        directory first, spilling ~100 bytes per path beside
                        --log-file (else in the system temp directory).
  --small-pool POOL     Send files below --split-size to POOL, in the same run
                        as the larger ones: with --large-pool those go there,
                        without it to their directory's layout pool. Files
                        below 614400 bytes may go only to the volume's first
                        data pool, so POOL must be that pool unless --min-size
                        starts the band at or above 614400 or --small-files-
                        to-any-pool is given. A file whose directory points at
                        its pool is copied beside itself as usual; one whose
                        directory points elsewhere is staged as with --target-
                        pool, and --paths-from/--paths-from-pool lists are
                        grouped by directory the same way. Directory layouts
                        are never changed. Not with --target-pool or --stage-
                        in-tmpdir.
  --large-pool POOL     Send files at or above --split-size to POOL, in the
                        same run as the smaller ones: with --small-pool those
                        go there, without it to their directory's layout pool.
                        Any pool: EC, or a replicated pool on other media;
                        below 614400 bytes the first-data-pool rule of
                        --small-pool applies, and so does --small-files-to-
                        any-pool. Staged like --small-pool.
  --small-files-to-any-pool
                        Let --small-pool or --large-pool take files below
                        614400 bytes when it is not the volume's first data
                        pool. Each such file then keeps its backtrace in the
                        first pool and its data in the other, so it costs
                        objects in both; the startup log says so.
  --split-size SIZE     Where --small-pool and --large-pool divide (default
                        614400). --min-size and --max-size still bound the
                        whole run; a side they leave with no sizes is refused
                        at startup.
  --reap-staging        Instead of transcoding, remove leftover staging
                        directories (.vcephfs-tc-stage-<id>) through a
                        separate CephFS session: each untouched for --reap-
                        staging-min-age, with its aged temps; anything else is
                        reported and left. --paths-from FILE lists directories
                        to check without a walk. Paced like a walk (--file-
                        delay, --config threads, the regulator); the first
                        SIGINT, SIGTERM or SIGHUP stops it.
  --reap-staging-min-age SEC
                        With --reap-staging, take a staging directory only
                        once its ctime, and each temp's mtime and ctime, are
                        this old (default 3600). Running jobs refresh theirs
                        every quarter of the default, so a lower value can
                        take a live job's directory.
  --target-pool-kind {rep,ec}
                        Whether --target-pool is replicated or EC, for hosts
                        without a usable ceph CLI. Without either, the pool's
                        name must say: *.ecK.M.* for EC, exactly
                        cephfs.<fs>.data or cephfs.<fs>.rdata for replicated;
                        anything else is refused. Refused if it contradicts
                        the cluster.
  --prune-small-subtrees
                        Skip subtrees whose ceph.dir.rbytes/rfiles make them
                        not worth walking (one getfattr per directory). Off by
                        default, since it changes what a pass covers. Inert
                        with --min-size 0 (warned).
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
                        Regex (re.search) matched against directory NAMES; a
                        match is pruned, not descended into. Anchor with ^ /
                        $, e.g. '\.runfiles$'. Replaces the default set
                        (DEFAULT_PRUNE_DIRS); '' disables pruning.
  --no-run-journal      Do not write .vcephfs-transcode-runs.jsonl at each
                        volume root. It keeps each artifact's ceph.dir.rctime
                        and newest file mtime from before the run, since a
                        transcode pins rctime and retention would read the
                        data as new. One getxattr per artifact root; an
                        unclean exit leaves a checkpoint at most 5 minutes old
                        in .vcephfs-transcode-runs.jsonl.<run>.partial.

files held open for writing:
  Before the copy and again right before the swap, the tool reads
  its own client's caps on the file (ceph.caps). No Fc means
  another client has it open for write; Fw means a process on
  this host does. Either way the file is skipped, counted as
  "open for write elsewhere", and a later pass retries it. The
  kernel client drops its wanted caps on an open file left idle
  for about a minute, so a writer that keeps a file open, sits
  idle past --min-age and then writes again is invisible to this
  check; its writes go to the replaced, unlinked inode. --min-age
  stays the main guard: use 2 or more, and keep the tool off
  trees whose writers hold files open for days (long-lived logs,
  databases).

size split (--small-pool, --large-pool):
  Each file goes by its size: below --split-size (default
  614400) to --small-pool, at or above it to --large-pool.
  A side left unset keeps the directory's layout pool. Any two
  pools may take the bands, replicated or EC, but files below
  614400 bytes go only to the volume's first data pool, which
  the cluster must confirm (see --rados-name), unless
  --small-files-to-any-pool is given. A side that
  --min-size/--max-size leave empty is refused. The
  startup log names each side's pool, kind and band. A file
  already on its pool counts as "already matched" and is left
  alone, so a second run moves only what is still wrong. A
  hard-link set gets one decision, from its size.

runtime signals:
  SIGUSR1  (10)  increase thread count by 1 (resumes from pause)
  SIGUSR2  (12)  decrease thread count by 1 (0 = pause)
  SIGTSTP  (20)  throttle to 1 thread (Ctrl+Z)
  SIGRTMIN (34)  increase file delay x1.25 (min +1ms, cap 600000ms)
  SIGRTMIN+1(35) decrease file delay /1.25 (min -1ms, floor 0ms)
  SIGRTMIN+2(36) increase min-age by 3 days
  SIGRTMIN+3(37) decrease min-age by 3 days (min 1)
  SIGRTMIN+4(38) dump current state/tunables to the log

exit signals:
  SIGINT, SIGTERM and SIGHUP stop cleanly: in-flight copies
  finish and the run journal is written, which can take as
  long as the largest copy. A second one of the same signal
  stops at once. So under timeout(1), give -k, e.g.
  timeout -k 30m 24h vcephfs_transcoder.py ...  A SIGHUP inherited as
  ignored (nohup) stays ignored.

example --config file (every key at its default):

    # vcephfs_transcoder runtime config -- VOLUME
    # Conventional path: /root/tc_VOLUME.conf
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
    threads         = 4
    
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
    min_age_days    = 2
    
    # Skip files smaller than this many bytes. Pre-Tentacle, EC pads small
    # objects to a whole stripe, which is why this is not lower.
    min_size        = 512000
    
    # --- what to skip entirely --------------------------------------------
    # Matched against directory NAMES during the walk; matches are never
    # descended into or statted. Cost is one regex match per directory, not
    # per file. Set empty to disable pruning entirely.
    prune_dir_regex = ^(\.runfiles|site\-packages|node_modules|__pycache__|\.venv|venv|penv|renv|packrat|\.git|\.tox|\.mypy_cache|\.pytest_cache|\.cargo|\.gradle)$
    
    # --- self-regulation ----------------------------------------------------
    # Throttle against what the filesystem's OTHER clients experience, which
    # this job cannot observe from its own copy latency. Entirely optional:
    # leave the URL empty and the regulator never starts, though the job
    # then logs that at WARNING unless it was run with --no-regulate.
    #
    # The query is a complete PromQL expression and must evaluate to exactly
    # one series whose value is MILLISECONDS. Nothing here assumes Ceph, or
    # any particular exporter. {volume} is substituted with this filesystem's
    # name (regex-escaped); omit it and the query is used verbatim.
    #
    # Units are checked once at startup and called out, because seconds means
    # it never triggers and microseconds means it never stops -- both silent.
    #
    # Backslashes must be DOUBLED. PromQL string literals use Go escaping, so
    # a regex dot is \\. inside the quotes; a single backslash is a parse
    # error (HTTP 400: unknown escape sequence), not a wrong match. Example:
    #   A window SHORTER than this volume's request interval yields 0/0
    #   = nan: a quiet filesystem has no requests in the window and there
    #   is nothing to average. The regulator now starts anyway and retries,
    #   but it cannot regulate until the query returns a number. 5m copes
    #   with a volume served a few requests a minute; 1m does not.
    #   regulate_query = 1e3 * sum(increase(mds_lat_sum{n=~"mds\\.{volume}\\..*"}[5m]))
    #                        / sum(increase(mds_lat_count{n=~"mds\\.{volume}\\..*"}[5m]))
    regulate_prometheus_url =
    regulate_query  =
    
    # Pause above this. The soft target is what it eases back toward; it does
    # not gate the pause.
    regulate_pause_ms   = 150.0
    regulate_slo_ms     = 75.0
    
    # Poll period, and the delay floor the regulator decays back down to
    # after a quiet spell. Repeated pauses ratchet the floor UP; quiet time
    # releases it, but never below this baseline.
    regulate_period_s   = 30
    regulate_floor_ms   = 0
    
    # Clean samples in a row before easing the delay. Also how many nan
    # samples a regulator pause needs before it resumes at 1 thread: nan
    # means no MDS request completed, which a quiet volume and a stalled
    # MDS look the same.
    regulate_quiet_ticks = 10
    
    # Thread adaptivity, opt-in. 0 leaves the thread count exactly where
    # --threads put it. Set it to a ceiling and the regulator adds one
    # thread per quiet interval, but only once the delay has already
    # decayed to its floor -- delay is the cheaper knob, so it is spent
    # first. A pause lowers an internal ceiling below the level that
    # caused it, so the climb does not simply repeat.
    regulate_max_threads = 0
    
    # A regulator pause with no usable sample (Prometheus down, empty
    # result, nan, or a mix) for this long resumes at 1 thread instead
    # of holding 0 forever; the first usable sample restores the rest.
    # 0 holds until a sample arrives.
    regulate_blind_resume_s = 1800
    
    # A running job with no usable sample for this long throttles
    # itself to 1 thread and at least regulate_blind_delay_ms per file,
    # instead of holding whatever it had earned while nothing watches the
    # MDS. A usable sample hands control back. 0 holds forever.
    regulate_blind_throttle_s = 600
    regulate_blind_delay_ms = 20

    redirect it to disk with:  vcephfs_transcoder.py --print-config-example > /root/tc_VOLUME.conf
```
<!-- END vcephfs_transcoder.py --help -->

### `vcephfs-transcode-setup.sh --help`

<!-- BEGIN vcephfs-transcode-setup.sh --help -->
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
  --pg-autoscale-mode MODE
                         PG autoscale mode for new pool (default: off)
                         (e.g. "off", "on", "warn")
  --tmpdir PATH          Temp dir for transcoder (default: mount/tmp)
  --fastec               Enable allow_ec_optimizations (FastEC)
  --compression MODE     Compression mode for new pool (default: aggressive)
                         (e.g. "aggressive", "force", "passive", "none")
  --compression-algorithm ALG
                         Compression algorithm (default: snappy)
                         (e.g. "snappy", "zstd", "lz4", "zlib")
  --min-age DAYS         Min file age in days (default: 2)
  --min-size SIZE        Min file size (e.g. "1M", "300K")
  --subdir PATH           Subdirectory under mount to transcode
                         (e.g. "ethel/merman")
  --log-file PATH        Log file for transcoder
  --skip-pool-setup      Skip pool creation (already done)
  --skip-layout          Skip layout / setfattr steps
  --transcode            Run the transcoder (off by default)
  --max-map-lag N        Refuse --transcode when the client behind --mount
                         is more than N osdmaps behind the cluster
                         (default: 1000). "any" skips the check.
  --execute              Actually run commands (default: dry-run)
  --help                 Show this help
```
<!-- END vcephfs-transcode-setup.sh --help -->
