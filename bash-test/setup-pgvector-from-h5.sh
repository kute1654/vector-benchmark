#!/usr/bin/env bash
# =============================================================================
# Setup pgvector Table from HDF5 Dataset
# =============================================================================
# Creates a pgvector table and loads data from an HDF5 ANN benchmark dataset.
#
# Usage:
#   PSQL=/usr/local/pgsql/bin/psql ./setup-pgvector-from-h5.sh \
#       ../datasets/sift-128-euclidean.hdf5 \
#       benchmark_sift_128_1k \
#       1000 10 l2 hnsw
#
# Arguments:
#   $1 = HDF5 file path
#   $2 = Table name
#   $3 = Number of base vectors (train_count)
#   $4 = K (neighbors)
#   $5 = Distance metric (l2, ip, cosine)
#   $6 = Index type (hnsw, ivfflat, none)
# =============================================================================

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
: "${PSQL:=psql}"
: "${HOST:=127.0.0.1}"
: "${PORT:=5432}"
: "${USER:=postgres}"
: "${PASSWORD:=123456}"
: "${DATABASE:=postgres}"
: "${PYTHON:=python3}"

# ── Logging ──────────────────────────────────────────────────────────────────
log_info()  { echo -e "\033[34m[INFO]\033[0m  $*"; }
log_ok()    { echo -e "\033[32m[OK]\033[0m    $*"; }
log_warn()  { echo -e "\033[33m[WARN]\033[0m  $*"; }
log_error() { echo -e "\033[31m[ERROR]\033[0m $*"; }

# ── Usage ────────────────────────────────────────────────────────────────────
usage() {
    cat <<EOF
Usage: $(basename "$0") <h5_file> <table> <train_count> [K] [distance] [index_type]

Arguments:
  h5_file      Path to HDF5 dataset file
  table        Target table name
  train_count  Number of base vectors to load
  K            Number of neighbors (default: 10)
  distance     Distance metric: l2, ip, cosine (default: l2)
  index_type   Index type: hnsw, ivfflat, none (default: hnsw)

Environment:
  PSQL, HOST, PORT, USER, PASSWORD, DATABASE
EOF
    exit 1
}

# ── Parse arguments ──────────────────────────────────────────────────────────
H5_FILE="${1:-}"
TABLE="${2:-}"
TRAIN_COUNT="${3:-}"
K="${4:-10}"
DISTANCE="${5:-l2}"
INDEX_TYPE="${6:-hnsw}"

if [ -z "$H5_FILE" ] || [ -z "$TABLE" ] || [ -z "$TRAIN_COUNT" ]; then
    log_error "Missing required arguments"
    usage
fi

if [ ! -f "$H5_FILE" ]; then
    log_error "HDF5 file not found: $H5_FILE"
    exit 1
fi

# ── Check prerequisites ──────────────────────────────────────────────────────
if ! command -v "$PSQL" &>/dev/null; then
    log_error "psql not found: $PSQL"
    exit 1
fi

# Detect psql's lib directory
if [ "$PSQL" != "psql" ]; then
    _psql_dir="$(dirname "$PSQL")"
    _psql_lib="${_psql_dir}/../lib"
    if [ -d "$_psql_lib" ]; then
        export LD_LIBRARY_PATH="${_psql_lib}:${LD_LIBRARY_PATH:-}"
    fi
fi

_psql() {
    PGPASSWORD="$PASSWORD" "$PSQL" -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" "$@"
}

# ── Check connection ─────────────────────────────────────────────────────────
log_info "Checking PostgreSQL connection..."
if ! CONN_CHECK=$(timeout 5 _psql -t -A -c "SELECT 1" 2>&1); then
    log_error "Cannot connect to ${HOST}:${PORT} as ${USER}"
    log_error "Error: ${CONN_CHECK}"
    exit 1
fi
log_ok "Connected"

# ── Check pgvector extension ─────────────────────────────────────────────────
EXT_CHECK=$(_psql -t -A -c "SELECT extversion FROM pg_extension WHERE extname='pgvector'" 2>&1 || true)
if [ -z "$EXT_CHECK" ]; then
    log_warn "pgvector extension not found. Creating..."
    _psql -c "CREATE EXTENSION IF NOT EXISTS vector" 2>/dev/null || {
        log_error "Cannot create pgvector extension"
        exit 1
    }
fi
log_info "pgvector version: ${EXT_CHECK:-unknown}"

# ── Use Python to load data ──────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

log_info "Loading data from HDF5 using Python..."
log_info "  H5:         ${H5_FILE}"
log_info "  Table:      ${TABLE}"
log_info "  Train:      ${TRAIN_COUNT}"
log_info "  K:          ${K}"
log_info "  Distance:   ${DISTANCE}"
log_info "  Index:      ${INDEX_TYPE}"

cd "$PROJECT_DIR"

"$PYTHON" "$PROJECT_DIR/benchmark/bash-test/setup_from_h5.py" \
    --engine pgvector \
    --h5 "$H5_FILE" \
    --table "$TABLE" \
    --train-count "$TRAIN_COUNT" \
    --query-count "$TRAIN_COUNT" \
    --top-k "$K" \
    --distance "$DISTANCE" \
    --index-type "$INDEX_TYPE" \
    --host "$HOST" \
    --port "$PORT" \
    --user "$USER" \
    --password "$PASSWORD" \
    --database "$DATABASE" \
    --drop

# ── Verify ───────────────────────────────────────────────────────────────────
log_info "Verifying..."
ROW_COUNT=$(_psql -t -A -c "SELECT count(*) FROM ${TABLE}" 2>/dev/null || echo "0")
log_ok "Table ${TABLE} created with ${ROW_COUNT} rows"

# Check index
IDX_COUNT=$(_psql -t -A -c "
    SELECT count(*) FROM pg_indexes
    WHERE tablename = '${TABLE}' AND indexname LIKE '%_idx'
" 2>/dev/null || echo "0")
log_ok "Indexes: ${IDX_COUNT}"

log_info ""
log_info "=========================================="
log_info " Setup Complete!"
log_info "  Table:   ${TABLE} (${ROW_COUNT} rows)"
log_info "  Queries: ${TABLE}_queries"
log_info "  GT:      ${TABLE}_ground_truth"
log_info "  Meta:    ${TABLE}_meta"
log_info "=========================================="