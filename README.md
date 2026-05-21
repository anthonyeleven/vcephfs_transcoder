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

Usage:

```
# /usr/local/sbin/vcephfs-transcode-setup.sh
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

# /usr/local/sbin/vcephfs-transcode-setup.sh
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

  EC profile, CRUSH rule, and pool name are derived automatically:
    profile:  ec<k>.<m><class>       e.g. ec8.2hdd
    rule:     ec<k>.<m>-rule-<class> e.g. ec8.2-rule-hdd
    pool:     cephfs.<vol>.ec<k>.<m>.<class>.data
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

Example invocations:
```
/usr/local/sbin/vcephfs-transcode-setup.sh --mount /shared/ceph/ethel --ec-k 6 --ec-m 3 --crush-device-class ssd --subdir /merman

/usr/local/sbin/vcephfs_transcoder.py --min-age 1 --min-size 500k /shared/ceph/ethel/merman --tmpdir /shared/ceph/ethel/merman/tmp --log-file ~/ethel/transcode.log --dry-run

```



This was presented at Ceph Day Seattle 2026, and based on a script posted
to Reddit by `marcan42`.

