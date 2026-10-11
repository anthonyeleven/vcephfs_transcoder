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

  The new pool is set as the directory layout of the volume root, so new
  files land there. Migration can be specified for subtrees, potentially
  to different volumes.

* `vcephfs_transcoder.py` performs the actual migration.

Note that both require a CephX client user with the `p` [capability flag](https://docs.ceph.com/en/latest/cephfs/client-auth/).

Some feel that what this utility accomplishes does not qualify as transcoding.
Tough. Marcan42 called it that, and so it remains.

`--help` on either script prints every option. What follows is a normal
first pass, then the options most runs never need.

## A normal first pass

The example moves the large files of volume `myvol`, mounted at `/mnt/myvol`,
to a new EC 4+2 pool on SSDs.

**1. Create and attach the pool**, on a mon or any host with the admin keyring.
The setup script is a dry run unless given `--execute`; it prints every
command it would run.

```
vcephfs-transcode-setup.sh --mount /mnt/myvol --ec-k 4 --ec-m 2 \
    --crush-device-class ssd --pg-num 256
# review the commands, then the same again with --execute
```

It reuses or creates the profile `ec4.2ssd` and the rule `ec4.2-rule-ssd`,
creates `cephfs.myvol.ec4.2.ssd.data` and adds it to the filesystem. It then
makes `/mnt/myvol/tmp` on the old pool (the transcoder's `--tmpdir`) and
points the volume root's layout at the new pool, so new files land there.

To leave the root layout alone and move only the existing large files, add
`--skip-layout`, make the tmpdir yourself (`mkdir /mnt/myvol/tmp`), and give
the transcoder `--target-pool cephfs.myvol.ec4.2.ssd.data` in step 3.

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

`min_size = 614400` leaves small files where they are. Below about 600 KiB a
file costs more on EC 4+2 than it saves: 9 RADOS objects, counting its
backtrace, against 3 on a replicated pool. The regulator pauses the job while
the volume's MDS reply latency is above `regulate_pause_ms`. Every key in the
file is applied at startup, over the command line, and again whenever you
change it. The same settings exist as options (`--min-age`, `--min-size`,
`--threads`, `--file-delay`, `--regulate-prometheus-url`, `--regulate-query`,
`--regulate-pause-ms`, `--regulate-slo-ms`); the file is the one you can edit
while the job runs.

**3. Try it, then run it**, as root on a client with the volume mounted:

```
# a dry run on one subtree: copies and verifies, replaces nothing
vcephfs_transcoder.py --config ~/tc_myvol.conf --tmpdir /mnt/myvol/tmp \
    --dry-run --max-files 1000 /mnt/myvol/some/dir

# the pass
systemd-run --unit tc-myvol /usr/local/sbin/vcephfs_transcoder.py \
    --config ~/tc_myvol.conf --tmpdir /mnt/myvol/tmp \
    --log-file ~/tc.myvol.log --log-rotate-size 4 /mnt/myvol
```

**4. Watch it.** The log has a `Progress:` line every minute and a `Complete:`
line at the end that counts what was transcoded, failed, already matched, too
recent, vanished, and so on. Change the pace by editing the config file, or
with the signals listed in `--help`. `systemctl stop tc-myvol` (or SIGINT)
lets in-flight copies finish; a second signal stops at once.

**5. Run it again to converge.** A pass is idempotent: files already in place
count as "already matched". Files skipped as too recent, open or locked,
changed during the copy, vanished, or with an invalid layout are picked up by
the next pass.

## Files held open for writing

`--min-age` is the only guard against moving a file that a process still holds
open for writing. The mtime/ctime/size re-check before the swap catches a write
during the copy. But a writer that keeps the file open, sits idle past
`--min-age` and then writes again is writing to the replaced, unlinked inode,
and those writes never reach the path. Neither the MDS nor the transcoder can
see such a writer: the kernel client drops its wanted caps on an open file left
idle for about a minute. Use `--min-age 2` or more, and keep the transcoder off
trees whose writers hold files open for days, such as long-lived logs and
databases.

## Advanced options

For special cases a first pass is unlikely to hit. `--help` describes each.

Setup script:

* `--replicated`: target a replicated pool (`cephfs.<vol>.rdata`) instead of EC.
* `--ec-plugin`, `--fastec`, `--compression`, `--compression-algorithm`,
  `--pg-autoscale-mode`: pool properties other than the defaults (isa, no
  FastEC, aggressive snappy, autoscale off).
* `--skip-pool-setup`, `--skip-layout`: redo only part of the setup.
* `--subdir`, `--transcode`, `--min-age`, `--min-size`, `--log-file`,
  `--tmpdir`: have the setup script launch the transcoder itself instead of
  launching it by hand as above.

Transcoder:

* Choosing files without a walk: `--paths-from FILE` (a list, e.g. from an
  earlier run's log) and `--paths-from-pool POOL` (read the pool itself to
  drain it), with `--lookupino-rate`, `--rados-conffile`, `--rados-mon-host`,
  `--rados-keyring` and `--rados-name`.
* Choosing pools: `--source-pool` (only files now in one pool, e.g. a
  pool-to-pool drain); `--target-pool` and `--target-pool-kind` (move files to
  a pool without changing any directory layout); `--reap-staging` and
  `--reap-staging-min-age` (remove staging directories a killed
  `--target-pool` run left behind).
* `--max-size`: an upper size bound; a replicated `--target-pool` needs one.
* `--process-hardlinks`: move multiply-linked files, which are skipped by
  default. Every link has to be under the directories given.
* `--stage-in-tmpdir`, `--force-tmpdir-pool`: escape hatches, slow or unsafe
  for routine use.
* Walk pruning: `--prune-dir-regex` (replace the default set of pruned
  directory names), `--prune-small-subtrees`, `--prune-subtree-max-bytes`,
  `--prune-budget-bytes`.
* `--copy-file-range`, `--no-copy-file-range`: server-side copy, off by
  default.
* Regulator tuning: `--regulate-period-s`, `--regulate-floor-ms`,
  `--regulate-quiet-ticks`, `--regulate-max-threads`,
  `--regulate-blind-resume-s`, `--regulate-blind-throttle-s`,
  `--regulate-blind-delay-ms`; `--no-regulate` runs unregulated on purpose.
* Logs and limits: `--log-rotate-lines`, `--log-rotate-time`, `--max-files`,
  `--config-poll-seconds`, `--debug`, `--no-run-journal`.
