#!/usr/bin/env bash
#
# vcephfs-transcode-setup.sh
#
# Automates the 2026Q2 CephFS transcoding plan from:
#   https://voleon.atlassian.net/wiki/spaces/STORAGE/pages/160436861
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
#   --tmpdir PATH          Temp dir for transcoder (default: mount/tmp)
#   --fastec               Enable allow_ec_optimizations (FastEC)
#   --compression MODE     Compression mode for new pool (default: aggressive)
#                          (e.g. "aggressive", "force", "passive", "none")
#   --compression-algorithm ALG
#                          Compression algorithm (default: snappy)
#                          (e.g. "snappy", "zstd", "lz4", "zlib")
#   --min-age DAYS         Min file age in days (default: 1)
#   --min-size SIZE        Min file size (e.g. "1M", "300K")
#   --subdir PATH           Subdirectory under mount to transcode
#                          (e.g. "ethel/merman")
#   --log-file PATH        Log file for transcoder
#   --skip-pool-setup      Skip pool creation (already done)
#   --skip-layout          Skip layout / setfattr steps
#   --transcode            Run the transcoder (off by default)
#   --execute              Actually run commands (default: dry-run)
#   --help                 Show this help
#
set -euo pipefail

# --- Defaults ---------------------------------------------------------------
EC_PLUGIN="isa"
COMPRESSION="aggressive"
COMPRESSION_ALG="snappy"
MIN_AGE=1
MIN_SIZE=""
FASTEC=false
DRY_RUN=true
SKIP_POOL_SETUP=false
SKIP_LAYOUT=false
SKIP_TRANSCODE=true
TRANSCODE_TMPDIR_OVERRIDE=""
SUBDIR=""
LOG_FILE=""
VOLUME=""
MOUNT=""
EC_K=""
EC_M=""
CRUSH_DEVICE_CLASS=""
REPLICATED=false

# --- Helpers ----------------------------------------------------------------
usage() {
    sed -n '/^# Usage:/,/^[^#]/{ /^#/s/^# \?//p }' "$0"
    exit 0
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
    usage
fi

while [ $# -gt 0 ]; do
    case "$1" in
        --mount)             MOUNT="$2";               shift 2 ;;
        --ec-k)              EC_K="$2";                shift 2 ;;
        --ec-m)              EC_M="$2";                shift 2 ;;
        --ec-plugin)         EC_PLUGIN="$2";           shift 2 ;;
        --crush-device-class) CRUSH_DEVICE_CLASS="$2"; shift 2 ;;
        --replicated)        REPLICATED=true;          shift   ;;
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
        --execute)           DRY_RUN=false;            shift   ;;
        --help|-h)           usage ;;
        *) log_err "Unknown option: $1"; exit 1 ;;
    esac
done

# --- Validate ---------------------------------------------------------------
require_arg MOUNT
[[ "$MIN_AGE" =~ ^[1-9][0-9]*$ ]] || { log_err "--min-age must be a positive integer"; exit 1; }

# Derive volume name from the last component of the mount path
# e.g. /shared/cephlab/howie -> howie
VOLUME=$(basename "${MOUNT}")

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

TRANSCODE_TMPDIR="${TRANSCODE_TMPDIR_OVERRIDE:-${MOUNT}/tmp}"

# Auto-select minimum file size based on EC width if not specified
if [ -z "$MIN_SIZE" ]; then
    if [ "$REPLICATED" = true ]; then
        # No space-amp concern for replicated pools
        MIN_SIZE="1M"
        log_info "Auto-selected --min-size=${MIN_SIZE} (replicated)"
    else
        # Pre-FastEC recommendations from the plan:
        #   EC 4+2 -> 300K (alloc unit 24 KiB)
        #   EC 8+2 -> 600K (alloc unit 40 KiB)
        # Post-FastEC:
        #   EC 4+2 -> 256K (alloc unit 16 KiB)
        #   EC 8+2 -> 180K (alloc unit 24 KiB, but doc suggests 256K)
        STRIPE_WIDTH=$(( EC_K + EC_M ))
        if [ "$FASTEC" = true ]; then
            if [ "$STRIPE_WIDTH" -le 6 ]; then
                MIN_SIZE="160K"
            else
                MIN_SIZE="256K"
            fi
        else
            if [ "$STRIPE_WIDTH" -le 6 ]; then
                MIN_SIZE="300K"
            else
                MIN_SIZE="600K"
            fi
        fi
        log_info "Auto-selected --min-size=${MIN_SIZE} (k=${EC_K}, m=${EC_M}, fastec=${FASTEC})"
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
if [ "$DRY_RUN" = true ]; then
    log_cmd "ls -l ${MOUNT}"
    echo "         (dry-run, not executed)"
else
    if ! ls -l "${MOUNT}"; then
        log_err "Mount path '${MOUNT}' does not exist or is not accessible"
        exit 1
    fi
fi

# --- Pool setup --------------------------------------------------------------
if [ "$SKIP_POOL_SETUP" = false ]; then

    if [ "$REPLICATED" = true ]; then
        # --- Replicated pool: just create and attach -------------------------
        if pool_exists "${POOL_NAME}"; then
            log_info "Pool '${POOL_NAME}' already exists, skipping creation"
        else
            log_info "Pool 1: Create replicated data pool '${POOL_NAME}'"
            run ceph osd pool create "${POOL_NAME}" replicated
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

        if pool_exists "${POOL_NAME}"; then
            log_info "Pool '${POOL_NAME}' already exists, skipping creation"
        else
            log_info "Pool 3: Create EC data pool '${POOL_NAME}'"
            run ceph osd pool create "${POOL_NAME}" erasure "${EC_PROFILE}"

            log_info "Pool 4: Set pool CRUSH rule to '${CRUSH_RULE}' and remove auto-created rule"
            run ceph osd pool set "${POOL_NAME}" crush_rule "${CRUSH_RULE}"
            # The auto-created rule has the same name as the pool
            run ceph osd crush rule rm "${POOL_NAME}" || true
        fi

        # These are safe to re-apply on an existing pool
        log_info "Pool 5: Enable EC overwrites on '${POOL_NAME}'"
        run ceph osd pool set "${POOL_NAME}" allow_ec_overwrites true

        if [ "$FASTEC" = true ]; then
            log_info "Pool 5b: Enable EC optimizations (FastEC) on '${POOL_NAME}'"
            run ceph osd pool set "${POOL_NAME}" allow_ec_optimizations true
        fi

        log_info "Pool 6: Add data pool to filesystem '${VOLUME}'"
        run ceph fs add_data_pool "${VOLUME}" "${POOL_NAME}"

        log_info "Pool 7: Set compression on '${POOL_NAME}'"
        run ceph osd pool set "${POOL_NAME}" compression_mode "${COMPRESSION}"
        run ceph osd pool set "${POOL_NAME}" compression_algorithm "${COMPRESSION_ALG}"
    fi

else
    log_info "Skipping pool setup (--skip-pool-setup)"
fi

# --- Set layouts -------------------------------------------------------------
if [ "$SKIP_LAYOUT" = false ]; then

    # Detect the current/old data pool BEFORE changing the volume root layout.
    # Reading from ${MOUNT} gives the pool that existing files live on.
    # This matters for EC 4+2 -> EC 8+2 transcoding where the old pool
    # is not the conventional cephfs.<vol>.data.
    OLD_POOL=""
    if command -v getfattr >/dev/null 2>&1 && [ "$DRY_RUN" = false ]; then
        OLD_POOL=$(getfattr -n ceph.dir.layout.pool --only-values "${MOUNT}" 2>/dev/null || true)
    fi
    if [ -z "$OLD_POOL" ]; then
        OLD_POOL="cephfs.${VOLUME}.data"
        log_warn "Could not detect old pool; assuming '${OLD_POOL}'"
        log_warn "If this is wrong, manually run:"
        log_warn "  setfattr -n ceph.dir.layout.pool -v <OLD_POOL> ${TRANSCODE_TMPDIR}"
    fi

    log_info "Layout 1: Create temp directory and pin it to old pool '${OLD_POOL}'"
    run mkdir -p "${TRANSCODE_TMPDIR}"
    run setfattr -n ceph.dir.layout.pool -v "${OLD_POOL}" "${TRANSCODE_TMPDIR}"

    log_info "Layout 2: Set default layout on volume root to new pool"
    run setfattr -n ceph.dir.layout.pool -v "${POOL_NAME}" "${MOUNT}"

    # --- Verify --------------------------------------------------------------
    log_info "Layout 3: Verify layouts"
    if [ "$DRY_RUN" = false ]; then
        echo "  Volume root:"
        getfattr -n ceph.dir.layout.pool "${MOUNT}" 2>&1 | sed 's/^/    /'
        echo "  Temp dir:"
        getfattr -n ceph.dir.layout.pool "${TRANSCODE_TMPDIR}" 2>&1 | sed 's/^/    /'
    else
        log_cmd "getfattr -n ceph.dir.layout.pool ${MOUNT}"
        log_cmd "getfattr -n ceph.dir.layout.pool ${TRANSCODE_TMPDIR}"
        echo "         (dry-run, not executed)"
    fi
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
    TRANSCODER_ARGS=(/usr/local/sbin/vcephfs_transcoder.py
        --min-age "${MIN_AGE}"
        --min-size "${MIN_SIZE}")
    if [ -n "$TRANSCODE_PATH" ]; then
        TRANSCODER_ARGS+=("${TRANSCODE_PATH}")
    else
        TRANSCODER_ARGS+=("<path>")
    fi
    TRANSCODER_ARGS+=(--tmpdir "${TRANSCODE_TMPDIR}")
    if [ -n "$LOG_FILE" ]; then
        TRANSCODER_ARGS+=(--log-file "${LOG_FILE}")
    fi
    log_info "Would run: ${TRANSCODER_ARGS[*]}"
    log_info "Note: the mount path on the client system may differ."
fi

echo ""
log_info "Done."
if [ "$DRY_RUN" = true ]; then
    echo ""
    log_warn "This was a DRY-RUN. Review the commands above, then re-run with --execute."
fi
