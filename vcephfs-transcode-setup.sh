#!/usr/bin/env bash
#
# vcephfs-transcode-setup.sh
#
# Automates a CephFS replication-to-erasure-coding transcoding plan:
# create the EC pool, set directory layouts, then transcode existing files
# into the new layout.
#
# This script performs the pool-creation and layout-setting steps on a mon,
# then invokes the transcoder on a client.  It is idempotent where Ceph
# commands allow, and dry-runs by default (--execute to apply).
#
# Terminal: VT52-safe.  Pure 7-bit ASCII, no escape sequences, no color.
#
# Prerequisites:
#   - Run from a mon (or a node with admin keyring for pool/crush steps).
#   - A CephX user "client.cephfs" with the 'p' cap must exist:
#       [client.cephfs]
#           key = <redacted>
#           caps mds = "allow rwp"
#           caps mon = "allow r"
#           caps osd = "allow rw tag cephfs data=all"
#   - Run inside a tmux session.  Beware of DoSing other traffic.
#   - The layout steps mount the volume privately (needs mount.ceph and
#     the user's keyring here) instead of acting through --mount.
#   - --transcode writes through --mount, so it first checks that the
#     kernel client behind it is not far behind on osdmaps (needs debugfs).
#     Other hosts' idle mounts of the volume are not checked: once the root
#     points at the new pool, each one catches up on its first write there,
#     so remount any that have sat idle for weeks before changing the root.
#
# Usage:
#   vcephfs-transcode-setup.sh [OPTIONS]
#
# Options:
#   --mount PATH           Mount point for the volume
#                          (e.g. /shared/cephlab/howie)
#                          Volume name is derived from the last path
#                          component (e.g. "howie").
#   --ec-k INT             EC data chunks
#   --ec-m INT             EC parity chunks
#   --ec-plugin NAME       EC plugin (default: isa)
#   --crush-device-class CLASS
#                          Device class: ssd or hdd
#   --replicated           Transcode to a replicated pool instead of EC.
#                          Pool name: cephfs.<vol>.rdata
#                          Incompatible with --ec-k and --ec-m.
#
#   EC profile, CRUSH rule, and pool name are derived automatically:
#     profile:  ec<k>.<m><class>       e.g. ec8.2hdd
#     rule:     ec<k>.<m>-rule-<class> e.g. ec8.2-rule-hdd
#     pool:     cephfs.<vol>.ec<k>.<m>.<class>.data
#   --pg-num INT           PG count for new pool (default: 128)
#   --pg-autoscale-mode MODE
#                          PG autoscale mode for new pool (default: off)
#                          (e.g. "off", "on", "warn")
#   --tmpdir PATH          Temp dir for transcoder (default: mount/tmp)
#   --fastec               Enable allow_ec_optimizations (FastEC)
#   --compression MODE     Compression mode for new pool (default: aggressive)
#                          (e.g. "aggressive", "force", "passive", "none")
#   --compression-algorithm ALG
#                          Compression algorithm (default: snappy)
#                          (e.g. "snappy", "zstd", "lz4", "zlib")
#   --min-age DAYS         Min file age in days (default: 2)
#   --min-size SIZE        Min file size (e.g. "1M", "300K")
#   --subdir PATH           Subdirectory under mount to transcode
#                          (e.g. "ethel/merman")
#   --log-file PATH        Log file for transcoder
#   --skip-pool-setup      Skip pool creation (already done)
#   --skip-layout          Skip layout / setfattr steps
#   --transcode            Run the transcoder (off by default)
#   --max-map-lag N        Refuse --transcode when the client behind --mount
#                          is more than N osdmaps behind the cluster
#                          (default: 1000). "any" skips the check.
#   --execute              Actually run commands (default: dry-run)
#   --help                 Show this help
#
set -euo pipefail

# --- Defaults ---------------------------------------------------------------
EC_PLUGIN="isa"
COMPRESSION="aggressive"
COMPRESSION_ALG="snappy"
MIN_AGE=2
MIN_SIZE=""
FASTEC=false
DRY_RUN=true
SKIP_POOL_SETUP=false
SKIP_LAYOUT=false
SKIP_TRANSCODE=true
MAX_MAP_LAG=1000
TRANSCODE_TMPDIR_OVERRIDE=""
SUBDIR=""
LOG_FILE=""
VOLUME=""
MOUNT=""
EC_K=""
EC_M=""
CRUSH_DEVICE_CLASS=""
REPLICATED=false
PG_NUM=128
PG_AUTOSCALE_MODE="off"

# --- Helpers ----------------------------------------------------------------
usage() {
    sed -n '/^# Usage:/,/^[^#]/{ /^#/s/^# \?//p }' "$0"
}

log_info() { echo "[INFO]  $*"; }
log_warn() { echo "[WARN]  $*"; }
log_err()  { echo "[ERROR] $*" >&2; }
log_cmd()  { echo "[CMD]   $*"; }

# Run or print a command depending on DRY_RUN
run() {
    log_cmd "$(printf '%q ' "$@")"
    if [ "$DRY_RUN" = true ]; then
        echo "         (dry-run, not executed)"
    else
        "$@"
    fi
}

# Existence checks -- return 0 (true) if the object exists.
# In dry-run mode, assume it does not exist so commands are shown.
pool_exists() {
    if [ "$DRY_RUN" = true ]; then return 1; fi
    ceph osd pool ls 2>/dev/null | grep -qx "$1"
}

ec_profile_exists() {
    if [ "$DRY_RUN" = true ]; then return 1; fi
    ceph osd erasure-code-profile ls 2>/dev/null | grep -qx "$1"
}

crush_rule_exists() {
    if [ "$DRY_RUN" = true ]; then return 1; fi
    ceph osd crush rule ls 2>/dev/null | grep -qx "$1"
}

# Survey bluestore_min_alloc_size across the OSDs that will back a pool.
#
# Why this matters at pool-creation time: min_alloc sets the per-shard rounding,
# so it decides the size below which erasure coding costs MORE raw space than
# the replication it replaces. Getting it wrong is not a performance problem you
# can tune away later -- stripe_unit and min_alloc are fixed for the life of the
# pool, and the only remedy is creating a new pool and moving the data again.
#
# A MIXED population is not simply "use the largest". Shards land on whichever
# OSDs CRUSH picks, so the expected allocation is the weighted mean across the
# population; the largest value is the worst case, not the expectation. Both are
# reported.
#
# Best-effort throughout: this needs `ceph` and a working keyring. It runs on a
# mon in our deployment, but that is a local convention, not a property of the
# tool -- anywhere it cannot query, it warns once and returns without blocking.
survey_min_alloc() {
    local rule="$1"
    local -a osds=()
    local out

    if ! command -v ceph >/dev/null 2>&1; then
        log_warn "ceph CLI not found; skipping min_alloc_size survey."
        log_warn "Run this on a host with cluster admin access to check it."
        return 0
    fi

    out=$(ceph osd crush rule dump "$rule" -f json 2>/dev/null) || {
        log_warn "Could not read CRUSH rule '$rule'; skipping min_alloc_size survey."
        return 0
    }

    # Device class the rule targets, e.g. "default~ssd" -> ssd
    local cls
    cls=$(printf '%s' "$out" | tr ',' '\n' | grep -o '"item_name"[^,]*' | head -1 |
          sed -E 's/.*"default~?([a-z0-9-]*)".*/\1/')

    if [ -n "$cls" ]; then
        mapfile -t osds < <(ceph osd crush class ls-osd "$cls" 2>/dev/null)
    fi
    if [ "${#osds[@]}" -eq 0 ]; then
        mapfile -t osds < <(ceph osd ls 2>/dev/null)
    fi
    if [ "${#osds[@]}" -eq 0 ]; then
        log_warn "Could not enumerate OSDs; skipping min_alloc_size survey."
        return 0
    fi

    # Sample rather than query every OSD: metadata is one round trip each and a
    # fleet can be thousands. A mixed population shows up in a sample of 40.
    local sample=40 n=0 val
    local -A counts=()
    local i
    for i in "${osds[@]}"; do
        [ "$n" -ge "$sample" ] && break
        val=$(ceph osd metadata "$i" -f json 2>/dev/null |
              grep -o '"bluestore_min_alloc_size"[^,]*' |
              sed -E 's/.*: *"?([0-9]+)"?.*/\1/')
        [ -n "$val" ] || continue
        counts["$val"]=$(( ${counts[$val]:-0} + 1 ))
        n=$(( n + 1 ))
    done

    if [ "$n" -eq 0 ]; then
        log_warn "No OSD metadata readable; skipping min_alloc_size survey."
        return 0
    fi

    if [ "${#counts[@]}" -eq 1 ]; then
        local only
        for only in "${!counts[@]}"; do :; done
        log_info "min_alloc_size uniform across $n sampled OSDs (class ${cls:-any}): $only bytes"
        return 0
    fi

    log_warn "MIXED bluestore_min_alloc_size across $n sampled OSDs (class ${cls:-any}):"
    local k maxv=0
    for k in "${!counts[@]}"; do
        log_warn "    $k bytes on ${counts[$k]}/$n sampled ($(( 100 * counts[$k] / n ))%)"
        [ "$k" -gt "$maxv" ] && maxv="$k"
    done
    log_warn "  Space amplification will be a weighted MEAN across this population,"
    log_warn "  not a single figure. Worst case per shard is $maxv bytes, which is"
    log_warn "  what the EC-vs-replication crossover should be computed against."
    log_warn "  Consider confining the pool to one class so the math is predictable."
}

require_arg() {
    local varname="$1"
    local val="${!varname:-}"
    if [ -z "$val" ]; then
        log_err "Missing required argument: --$(echo "$varname" | tr 'A-Z_' 'a-z-')"
        exit 1
    fi
}

# --- Parse arguments --------------------------------------------------------
if [ $# -eq 0 ]; then
    usage >&2
    exit 1
fi

while [ $# -gt 0 ]; do
    case "$1" in
        --mount)             MOUNT="$2";               shift 2 ;;
        --ec-k)              EC_K="$2";                shift 2 ;;
        --ec-m)              EC_M="$2";                shift 2 ;;
        --ec-plugin)         EC_PLUGIN="$2";           shift 2 ;;
        --crush-device-class) CRUSH_DEVICE_CLASS="$2"; shift 2 ;;
        --replicated)        REPLICATED=true;          shift   ;;
        --pg-num)            PG_NUM="$2";              shift 2 ;;
        --pg-autoscale-mode) PG_AUTOSCALE_MODE="$2";  shift 2 ;;
        --tmpdir)            TRANSCODE_TMPDIR_OVERRIDE="$2";     shift 2 ;;
        --fastec)          FASTEC=true;            shift   ;;
        --compression)       COMPRESSION="$2";         shift 2 ;;
        --compression-algorithm) COMPRESSION_ALG="$2"; shift 2 ;;
        --min-age)           MIN_AGE="$2";             shift 2 ;;
        --min-size)          MIN_SIZE="$2";            shift 2 ;;
        --subdir)            SUBDIR="$2";               shift 2 ;;
        --log-file)          LOG_FILE="$2";            shift 2 ;;
        --skip-pool-setup)   SKIP_POOL_SETUP=true;     shift   ;;
        --skip-layout)       SKIP_LAYOUT=true;         shift   ;;
        --transcode)         SKIP_TRANSCODE=false;     shift   ;;
        --max-map-lag)       MAX_MAP_LAG="$2";         shift 2 ;;
        --execute)           DRY_RUN=false;            shift   ;;
        --help|-h)           usage; exit 0 ;;
        *) log_err "Unknown option: $1"; exit 1 ;;
    esac
done

# --- Validate ---------------------------------------------------------------
require_arg MOUNT
[[ "$MIN_AGE" =~ ^[1-9][0-9]*$ ]] || { log_err "--min-age must be a positive integer"; exit 1; }
[[ "$PG_NUM" =~ ^[1-9][0-9]*$ ]] || { log_err "--pg-num must be a positive integer"; exit 1; }
(( (PG_NUM & (PG_NUM - 1)) == 0 )) || { log_err "--pg-num must be a power of 2 (got ${PG_NUM})"; exit 1; }
[[ "$PG_AUTOSCALE_MODE" =~ ^(on|off|warn)$ ]] || { log_err "--pg-autoscale-mode must be 'on', 'off', or 'warn'"; exit 1; }
[[ "$MAX_MAP_LAG" =~ ^(0|[1-9][0-9]*|any)$ ]] || { log_err "--max-map-lag must be a number or 'any'"; exit 1; }

# Derive volume name from the last component of the mount path
# e.g. /shared/cephlab/howie -> howie
VOLUME=$(basename "${MOUNT}")
# From here on, compare paths as the kernel shows them: no trailing slash,
# no symlinks. The volume name above comes from the path as typed.
MOUNT=$(realpath -m "${MOUNT}")

# --- Mode: replicated vs erasure-coded --------------------------------------
if [ "$REPLICATED" = true ]; then
    if [ -n "$EC_K" ] || [ -n "$EC_M" ]; then
        log_err "--replicated is incompatible with --ec-k and --ec-m"
        exit 1
    fi
    if [ -n "$CRUSH_DEVICE_CLASS" ]; then
        log_warn "--crush-device-class is ignored with --replicated"
    fi
    POOL_TYPE="replicated"
    POOL_NAME="cephfs.${VOLUME}.rdata"
    EC_PROFILE=""
    CRUSH_RULE=""
else
    require_arg EC_K
    require_arg EC_M
    require_arg CRUSH_DEVICE_CLASS
    [[ "$EC_K" =~ ^[1-9][0-9]*$ ]] || { log_err "--ec-k must be a positive integer"; exit 1; }
    [[ "$EC_M" =~ ^[1-9][0-9]*$ ]] || { log_err "--ec-m must be a positive integer"; exit 1; }
    POOL_TYPE="erasure"
    # Derive EC profile, CRUSH rule, and pool name from k, m, device class
    # e.g. k=8 m=2 class=hdd -> ec8.2hdd, ec8.2-rule-hdd, cephfs.howie.ec8.2.hdd.data
    EC_PROFILE="ec${EC_K}.${EC_M}${CRUSH_DEVICE_CLASS}"
    CRUSH_RULE="ec${EC_K}.${EC_M}-rule-${CRUSH_DEVICE_CLASS}"
    POOL_NAME="cephfs.${VOLUME}.ec${EC_K}.${EC_M}.${CRUSH_DEVICE_CLASS}.data"
fi

TRANSCODE_TMPDIR=$(realpath -m "${TRANSCODE_TMPDIR_OVERRIDE:-${MOUNT}/tmp}")

# Auto-select minimum file size based on EC width if not specified
if [ -z "$MIN_SIZE" ]; then
    if [ "$REPLICATED" = true ]; then
        # No space-amp concern for replicated pools
        MIN_SIZE="1M"
        log_info "Auto-selected --min-size=${MIN_SIZE} (replicated)"
    else
        # The transcoder's own EC floor (YIELD_FLOOR_BYTES, 614400): below it
        # the object-count cost of k+m shards plus the backtrace outweighs
        # the bytes saved, whatever the profile. Override with --min-size.
        MIN_SIZE="600K"
        log_info "Auto-selected --min-size=${MIN_SIZE} (the EC yield floor)"
    fi
fi

# --- Summary ----------------------------------------------------------------
if [ "$DRY_RUN" = true ]; then
    MODE_STR="DRY-RUN"
else
    MODE_STR="EXECUTE"
fi
# Construct full transcode path from mount + subdir
if [ -n "$SUBDIR" ]; then
    TRANSCODE_PATH="${MOUNT}/${SUBDIR}"
else
    TRANSCODE_PATH=""
fi

if [ -z "$TRANSCODE_PATH" ]; then
    TPATH_STR="<not set -- skipping transcode>"
else
    TPATH_STR="$TRANSCODE_PATH"
fi
if [ -z "$LOG_FILE" ]; then
    LFILE_STR="<none>"
else
    LFILE_STR="$LOG_FILE"
fi

echo ""
echo "==================================================================="
echo "  CephFS Transcoding Setup"
echo "==================================================================="
echo "  Volume:           ${VOLUME}"
echo "  Mount:            ${MOUNT}"
echo "  Pool type:        ${POOL_TYPE}"
if [ "$REPLICATED" = false ]; then
echo "  EC profile:       ${EC_PROFILE}  (k=${EC_K} m=${EC_M} plugin=${EC_PLUGIN})"
echo "  Device class:     ${CRUSH_DEVICE_CLASS}"
echo "  CRUSH rule:       ${CRUSH_RULE}"
fi
echo "  Pool name:        ${POOL_NAME}"
echo "  PG num:           ${PG_NUM}"
echo "  PG autoscale:     ${PG_AUTOSCALE_MODE}"
echo "  Compression:      ${COMPRESSION} (${COMPRESSION_ALG})"
echo "  Temp dir:         ${TRANSCODE_TMPDIR}"
if [ "$REPLICATED" = false ]; then
echo "  FastEC mode:      ${FASTEC}"
fi
echo "  Min age (days):   ${MIN_AGE}"
echo "  Min size:         ${MIN_SIZE}"
echo "  Transcode path:   ${TPATH_STR}"
echo "  Log file:         ${LFILE_STR}"
echo "  Mode:             ${MODE_STR}"
echo "==================================================================="
echo ""

if [ "$DRY_RUN" = true ]; then
    log_warn "DRY-RUN mode. Pass --execute to apply. Commands shown below."
    echo ""
fi

# --- Verify mount path exists ------------------------------------------------
log_info "Checking mount path '${MOUNT}'"
run ls -l "${MOUNT}" || { log_err "Mount path '${MOUNT}' does not exist or is not accessible"; exit 1; }
# ls triggers an idle autofs mount, so mountpoint(1) then sees it. An existing
# but unmounted directory would otherwise take the pool setup and a local
# mkdir of ${MOUNT}/tmp that masks the real volume.
if [ "$DRY_RUN" = false ] && ! mountpoint -q "${MOUNT}"; then
    log_err "'${MOUNT}' exists but is not a mountpoint; refusing to set up a volume on the bare directory"
    exit 1
fi

# Prints how many osdmap epochs the kernel client behind mount $1 is behind
# the cluster, or "unreadable: <why>" when that can't be read. The ceph CLI
# runs as the mount's own user (its name= option), whose keyring mount.ceph
# already needs here, and falls back to the CLI's default user.
map_lag() {
    local client fsid have cur user id
    client=$(getfattr -n ceph.client_id --only-values "$1" 2>/dev/null) ||
        { echo "unreadable: no ceph.client_id xattr on $1"; return 0; }
    fsid=$(getfattr -n ceph.cluster_fsid --only-values "$1" 2>/dev/null) ||
        { echo "unreadable: no ceph.cluster_fsid xattr on $1"; return 0; }
    have=$(awk 'NR == 1 && $1 == "epoch" { print $2 }' \
        "/sys/kernel/debug/ceph/${fsid}.${client}/osdmap" 2>/dev/null)
    [[ "$have" =~ ^[0-9]+$ ]] ||
        { echo "unreadable: can't read /sys/kernel/debug/ceph/${fsid}.${client}/osdmap (debugfs mounted?)"; return 0; }
    user=$(awk -v m="$1" '{ p = $2; gsub(/\\040/, " ", p) } p == m && $3 == "ceph" {
        n = split($4, o, ","); for (i = 1; i <= n; i++) if (o[i] ~ /^name=/) print substr(o[i], 6) }' /proc/self/mounts)
    for id in ${user:+"-n client.${user}"} ""; do
        # shellcheck disable=SC2086
        [ "$(ceph $id fsid 2>/dev/null)" = "$fsid" ] || continue
        # shellcheck disable=SC2086
        cur=$(ceph $id osd dump 2>/dev/null | awk '$1 == "epoch" { print $2; exit }')
        [[ "$cur" =~ ^[0-9]+$ ]] && break
    done
    if ! [[ "${cur:-}" =~ ^[0-9]+$ ]]; then
        echo "unreadable: 'ceph${user:+ -n client.${user}} osd dump' failed or names another cluster than ${fsid}"
    elif (( cur < have )); then
        echo "unreadable: the client reports epoch ${have}, newer than the cluster's ${cur}"
    else
        echo $(( cur - have ))
    fi
}

# Exits unless the client behind ${MOUNT} is within --max-map-lag maps of the
# cluster. Its first write to the new pool would otherwise make one mon send
# it every map since its own (see the layout step below).
check_map_lag() {
    local lag
    [ "$MAX_MAP_LAG" = any ] && { log_warn "Skipping the osdmap check of '${MOUNT}' (--max-map-lag any)"; return 0; }
    lag=$(map_lag "${MOUNT}")
    if ! [[ "$lag" =~ ^[0-9]+$ ]]; then
        log_err "Can't tell how far behind the client behind '${MOUNT}' is: ${lag#unreadable: }."
        log_err "Fix that, or check the client's epoch by hand and re-run with --max-map-lag any."
        exit 1
    elif (( lag > MAX_MAP_LAG )); then
        log_err "The client behind '${MOUNT}' is ${lag} osdmaps behind the cluster (limit ${MAX_MAP_LAG})."
        log_err "Its first write to '${POOL_NAME}' would make one mon send it all of them."
        log_err "Unmount '${MOUNT}' and any other mount on this host that shares its client,"
        log_err "let autofs mount it again (a new client gets only the current map), and re-run."
        exit 1
    fi
    log_info "The client behind '${MOUNT}' is ${lag} osdmaps behind the cluster (limit ${MAX_MAP_LAG})"
}

# --transcode writes through ${MOUNT}, so check its client before anything
# changes: a refused run then leaves the pool and layouts as they were.
if [ "$SKIP_TRANSCODE" = false ] && [ -n "$TRANSCODE_PATH" ]; then
    if [ "$DRY_RUN" = false ]; then
        check_map_lag
    elif [ "$MAX_MAP_LAG" != any ]; then
        NOW_LAG=$(map_lag "${MOUNT}")
        log_info "Would check that the client behind '${MOUNT}' is within ${MAX_MAP_LAG} osdmaps of the cluster (now: ${NOW_LAG})"
    fi
fi

# --- Pool setup --------------------------------------------------------------
if [ "$SKIP_POOL_SETUP" = false ]; then

    if [ "$REPLICATED" = true ]; then
        # --- Replicated pool: just create and attach -------------------------
        if pool_exists "${POOL_NAME}"; then
            log_info "Pool '${POOL_NAME}' already exists, skipping creation"
        else
            log_info "Pool 1: Create replicated data pool '${POOL_NAME}' (pg_num=${PG_NUM})"
            run ceph osd pool create "${POOL_NAME}" "${PG_NUM}" "${PG_NUM}" replicated
            run ceph osd pool set "${POOL_NAME}" pg_autoscale_mode "${PG_AUTOSCALE_MODE}"
        fi

        log_info "Pool 2: Add data pool to filesystem '${VOLUME}'"
        run ceph fs add_data_pool "${VOLUME}" "${POOL_NAME}"

        log_info "Pool 3: Set compression on '${POOL_NAME}'"
        run ceph osd pool set "${POOL_NAME}" compression_mode "${COMPRESSION}"
        run ceph osd pool set "${POOL_NAME}" compression_algorithm "${COMPRESSION_ALG}"
    else
        # --- EC pool: profile, rule, pool, overwrites, attach ----------------
        if ec_profile_exists "${EC_PROFILE}"; then
            log_info "EC profile '${EC_PROFILE}' already exists, skipping creation"
        else
            log_info "Pool 1: Create EC profile '${EC_PROFILE}'"
            run ceph osd erasure-code-profile set "${EC_PROFILE}" \
                "k=${EC_K}" "m=${EC_M}" "plugin=${EC_PLUGIN}" \
                "crush-device-class=${CRUSH_DEVICE_CLASS}"
        fi

        if crush_rule_exists "${CRUSH_RULE}"; then
            log_info "CRUSH rule '${CRUSH_RULE}' already exists, skipping creation"
        else
            log_info "Pool 2: Create CRUSH rule '${CRUSH_RULE}'"
            run ceph osd crush rule create-erasure "${CRUSH_RULE}" "${EC_PROFILE}"
        fi

        # Check the media before committing: stripe_unit and min_alloc are fixed
        # for the life of the pool, and they set the size below which EC costs
        # more raw space than the replication it replaces. Also on an existing
        # rule; in a dry run the rule may not exist yet, and the survey says so.
        survey_min_alloc "${CRUSH_RULE}" || true

        if pool_exists "${POOL_NAME}"; then
            log_info "Pool '${POOL_NAME}' already exists, skipping creation"
        else
            log_info "Pool 3: Create EC data pool '${POOL_NAME}' (pg_num=${PG_NUM}, rule ${CRUSH_RULE})"
            # Naming the rule at creation leaves no auto-created rule to swap
            # out and remove afterwards.
            run ceph osd pool create "${POOL_NAME}" "${PG_NUM}" "${PG_NUM}" erasure "${EC_PROFILE}" "${CRUSH_RULE}"
            run ceph osd pool set "${POOL_NAME}" pg_autoscale_mode "${PG_AUTOSCALE_MODE}"
        fi

        # These are safe to re-apply on an existing pool
        log_info "Pool 4: Enable EC overwrites on '${POOL_NAME}'"
        run ceph osd pool set "${POOL_NAME}" allow_ec_overwrites true

        if [ "$FASTEC" = true ]; then
            log_info "Pool 4b: Enable EC optimizations (FastEC) on '${POOL_NAME}'"
            run ceph osd pool set "${POOL_NAME}" allow_ec_optimizations true
        fi

        log_info "Pool 5: Add data pool to filesystem '${VOLUME}'"
        run ceph fs add_data_pool "${VOLUME}" "${POOL_NAME}"

        log_info "Pool 6: Set compression on '${POOL_NAME}'"
        run ceph osd pool set "${POOL_NAME}" compression_mode "${COMPRESSION}"
        run ceph osd pool set "${POOL_NAME}" compression_algorithm "${COMPRESSION_ALG}"
    fi

else
    log_info "Skipping pool setup (--skip-pool-setup)"
fi

# --- Set layouts -------------------------------------------------------------
# The layout step runs through its own short-lived kernel mount of the volume,
# never through ${MOUNT}. A kernel client accepts a layout that names a pool only
# once its osdmap has that pool. An idle autofs mount can hold a map months old,
# and catching it up makes one mon send every map since: tens of GB, during
# which that mon stops answering its other sessions. A new client is sent only
# the current map. noshare keeps the kernel from reusing the existing client.
# The source, user, file system name, messenger mode and mon addresses are
# copied from ${MOUNT}'s entry in /proc/self/mounts; mount.ceph finds the
# secret in the local keyring.
# This protects the layout step only. Any client writes its first file to the
# new pool only after catching up to a map that has the pool, so the
# --transcode step checks ${MOUNT}'s client first (see check_map_lag), and
# other hosts' long-idle mounts of the volume should be remounted beforehand.
LAYOUT_MNT=""
cleanup_layout_mount() {
    [ -n "$LAYOUT_MNT" ] || return 0
    if mountpoint -q "$LAYOUT_MNT"; then
        umount "$LAYOUT_MNT" 2>/dev/null || umount -l "$LAYOUT_MNT" 2>/dev/null ||
            log_warn "Could not unmount ${LAYOUT_MNT}; unmount it by hand"
    fi
    rmdir "$LAYOUT_MNT" 2>/dev/null || true
    LAYOUT_MNT=""
}
trap cleanup_layout_mount EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# Prints "<source> <options>" for a private mount of the volume at $1, from its
# /proc/self/mounts entry: the user, file system name, messenger mode and mon
# addresses, without the secret. /proc/self/mounts writes a space as \040.
layout_mount_args() {
    awk -v m="$1" '{ p = $2; gsub(/\\040/, " ", p) }
    p == m && $3 == "ceph" {
        n = split($4, o, ","); keep = "noshare"
        for (i = 1; i <= n; i++)
            if (o[i] ~ /^(name|mds_namespace|fs|ms_mode|mon_addr)=/) keep = keep "," o[i]
        src = $1
    } END { if (src != "") print src, keep }' /proc/self/mounts
}

if [ "$SKIP_LAYOUT" = false ]; then

    # Where the layout commands act: the private mount when executing, ${MOUNT}
    # in a dry run, which mounts nothing.
    LROOT="${MOUNT}"
    if [ "$DRY_RUN" = false ]; then
        read -r LSRC LOPTS <<< "$(layout_mount_args "${MOUNT}")"
        if [ -z "${LSRC:-}" ]; then
            log_err "No ceph mount at '${MOUNT}' in /proc/self/mounts; can't make the layout mount"
            exit 1
        fi
        LAYOUT_MNT=$(mktemp -d /run/vcephfs-setup-layout.XXXXXX)
        log_info "Layout 0: Mount '${VOLUME}' privately at ${LAYOUT_MNT} for the layout steps"
        run mount -t ceph "${LSRC}" "${LAYOUT_MNT}" -o "${LOPTS}" ||
            { log_err "Private mount failed; no layout was changed"; exit 1; }
        LROOT="${LAYOUT_MNT}"
    else
        log_info "Layout 0: Mount '${VOLUME}' privately for the layout steps"
        log_cmd "mount -t ceph <source of ${MOUNT}> /run/vcephfs-setup-layout.XXXXXX -o noshare,<name, fs, ms_mode and mon_addr of ${MOUNT}>"
        echo "         (dry-run, not executed; below, ${MOUNT} stands for that mount)"
    fi
    # The temp dir, as seen through the layout mount. One outside ${MOUNT} is
    # not on this volume's layout mount, so it is pinned by its own path.
    case "${TRANSCODE_TMPDIR}" in
        "${MOUNT}"|"${MOUNT}"/*) LTMPDIR="${LROOT}${TRANSCODE_TMPDIR#"${MOUNT}"}" ;;
        *) LTMPDIR="${TRANSCODE_TMPDIR}"
           log_warn "Temp dir '${TRANSCODE_TMPDIR}' is outside '${MOUNT}'; pinning it by its own path" ;;
    esac

    # Detect the current/old data pool BEFORE changing the volume root layout.
    # Reading the volume root gives the pool that existing files live on.
    # This matters for EC 4+2 -> EC 8+2 transcoding where the old pool
    # is not the conventional cephfs.<vol>.data.
    OLD_POOL=""
    if command -v getfattr >/dev/null 2>&1; then
        OLD_POOL=$(getfattr -n ceph.dir.layout.pool --only-values "${LROOT}" 2>/dev/null || true)
    fi
    if [ -z "$OLD_POOL" ]; then
        OLD_POOL="cephfs.${VOLUME}.data"
        log_warn "Could not detect old pool; assuming '${OLD_POOL}'"
        log_warn "If this is wrong, set it on the temp dir by hand, through a fresh"
        log_warn "mount (mount -o noshare), not through '${MOUNT}':"
        log_warn "  setfattr -n ceph.dir.layout.pool -v <OLD_POOL> <temp dir>"
    fi
    if [ "$OLD_POOL" = "$POOL_NAME" ]; then
        # A re-run: run 1 already pointed the root at the new pool, so the
        # root no longer says where the old data lives. Pinning the tmpdir
        # to what the root says now would pin it to the TARGET pool, which
        # the transcoder refuses (a temp born in the target pool hides a
        # failed layout application).
        OLD_POOL="cephfs.${VOLUME}.data"
        log_warn "Volume root already points at '${POOL_NAME}' (a re-run);"
        log_warn "pinning the temp dir to '${OLD_POOL}' instead. If that is"
        log_warn "not the default data pool, re-pin it by hand."
    fi

    log_info "Layout 1: Create temp directory and pin it to old pool '${OLD_POOL}'"
    run mkdir -p "${LTMPDIR}"
    run setfattr -n ceph.dir.layout.pool -v "${OLD_POOL}" "${LTMPDIR}"

    log_info "Layout 2: Set default layout on volume root to new pool"
    run setfattr -n ceph.dir.layout.pool -v "${POOL_NAME}" "${LROOT}"

    # --- Verify --------------------------------------------------------------
    log_info "Layout 3: Verify layouts"
    if [ "$DRY_RUN" = false ]; then
        echo "  Volume root:"
        getfattr -n ceph.dir.layout.pool "${LROOT}" 2>&1 | sed 's/^/    /'
        echo "  Temp dir:"
        getfattr -n ceph.dir.layout.pool "${LTMPDIR}" 2>&1 | sed 's/^/    /'
    else
        log_cmd "getfattr -n ceph.dir.layout.pool ${LROOT}"
        log_cmd "getfattr -n ceph.dir.layout.pool ${LTMPDIR}"
        echo "         (dry-run, not executed)"
    fi
    cleanup_layout_mount
else
    log_info "Skipping layout steps (--skip-layout)"
fi

# --- Step 10: Invoke the transcoder -----------------------------------------
if [ "$SKIP_TRANSCODE" = false ]; then
    if [ -z "$TRANSCODE_PATH" ]; then
        log_warn "No --subdir specified; skipping transcoder invocation."
        log_warn "To transcode, re-run with:"
        log_warn "  $0 --skip-pool-setup --skip-layout --subdir <path> --transcode --execute"
    else
        log_info "Transcode: Invoke transcoder on '${TRANSCODE_PATH}'"

        TRANSCODER_ARGS=(/usr/local/sbin/vcephfs_transcoder.py
            --min-age "${MIN_AGE}"
            --min-size "${MIN_SIZE}"
            "${TRANSCODE_PATH}"
            --tmpdir "${TRANSCODE_TMPDIR}")
        if [ -n "$LOG_FILE" ]; then
            TRANSCODER_ARGS+=(--log-file "${LOG_FILE}")
        fi

        run "${TRANSCODER_ARGS[@]}"

        echo ""
        log_info "Note: multiply-linked files are skipped by default."
        log_info "Successive passes are idempotent. Re-run to pick up new files."
    fi
else
    log_info "Skipping transcoder invocation (pass --transcode to enable)"
    log_info "Would run: /usr/local/sbin/vcephfs_transcoder.py --min-age ${MIN_AGE} --min-size ${MIN_SIZE} ${TRANSCODE_PATH:-<path>} --tmpdir ${TRANSCODE_TMPDIR}${LOG_FILE:+ --log-file ${LOG_FILE}}"
    log_info "Note: the mount path on the client system may differ."
fi

echo ""
log_info "Done."
if [ "$DRY_RUN" = true ]; then
    echo ""
    log_warn "This was a DRY-RUN. Review the commands above, then re-run with --execute."
fi
