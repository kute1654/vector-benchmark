#!/usr/bin/env bash
# =============================================================================
# PolarDB-pase Query Forms Benchmark
# =============================================================================
# Uses pgbench to measure QPS for different vector query forms against PolarDB-pase.
#
# Query forms:
#   text_pase_op_id           - vector <?> '{1,2,3}' (id only)
#   text_pase_op_extra        - vector <?> '{1,2,3}' (distance as extra)
#   text_pase_op_extra_ds     - vector <?> '{1,2,3}' (distance as ds)
#   pase_fn_text_extra        - pase_distance(vector, '{1,2,3}'::float4[])
#   pase_fn_text_extra_ds     - pase_distance(vector, '{1,2,3}'::float4[])
#
# Usage:
#   PSQL=/usr/local/pgsql/bin/psql PGBENCH=/usr/local/pgsql/bin/pgbench \
#       REPEAT=5 TIMELIMIT=30 ./polardb-pase-query-forms-benchmark.sh my_table
#
#   PORT=5433 ./polardb-pase-query-forms-benchmark.sh my_table
# =============================================================================

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
: "${PSQL:=psql}"
: "${PGBENCH:=pgbench}"
: "${HOST:=127.0.0.1}"
: "${PORT:=5433}"
: "${USER:=postgres}"
: "${PASSWORD:=123456}"
: "${DATABASE:=postgres}"
: "${REPEAT:=3}"
: "${TIMELIMIT:=30}"
: "${SQL_DIR:=sql-bench/polardb-pg}"
: "${RESULTS_DIR:=results}"
: "${TOP_K:=10}"

# ── Logging ──────────────────────────────────────────────────────────────────
log_info()  { echo -e "\033[34m[INFO]\033[0m  $*"; }
log_ok()    { echo -e "\033[32m[OK]\033[0m    $*"; }
log_warn()  { echo -e "\033[33m[WARN]\033[0m  $*"; }
log_error() { echo -e "\033[31m[ERROR]\033[0m $*"; }

# ── Help ─────────────────────────────────────────────────────────────────────
usage() {
    cat <<EOF
Usage: $(basename "$0") [OPTIONS] <table_name>

Options:
  -h, --help        Show this help message
  TABLE             Base table name (required)

Environment variables:
  PSQL, PGBENCH     Paths to psql and pgbench
  HOST, PORT        Database connection (default: 127.0.0.1:5433)
  USER, PASSWORD    Database credentials
  DATABASE          Database name
  REPEAT            Number of repetitions (default: 3)
  TIMELIMIT         Duration per run in seconds (default: 30)
  SQL_DIR           Directory for SQL files (default: sql-bench/polardb-pg)
  RESULTS_DIR       Directory for results (default: results)
  TOP_K             Top-K limit (default: 10)

Query Forms:
  text_pase_op_id           vector <?> '{1,2,3}' (id only)
  text_pase_op_extra        vector <?> '{1,2,3}' (distance)
  text_pase_op_extra_ds     vector <?> '{1,2,3}' (distance as ds)
  pase_fn_text_extra        pase_distance(vector, '{1,2,3}'::float4[])
  pase_fn_text_extra_ds     pase_distance(vector, '{1,2,3}'::float4[])
EOF
    exit 1
}

# ── Parse arguments ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage ;;
        *)         break ;;
    esac
    shift
done

TABLE="${1:-}"
if [ -z "$TABLE" ]; then
    log_error "Table name is required"
    usage
fi

# ── Check prerequisites ──────────────────────────────────────────────────────
for cmd in "$PSQL" "$PGBENCH"; do
    if ! command -v "$cmd" &>/dev/null; then
        log_error "Command not found: $cmd"
        exit 1
    fi
done

# Detect psql's lib directory for LD_LIBRARY_PATH
if [ "$PSQL" != "psql" ]; then
    _psql_dir="$(dirname "$PSQL")"
    _psql_lib="${_psql_dir}/../lib"
    if [ -d "$_psql_lib" ]; then
        export LD_LIBRARY_PATH="${_psql_lib}:${LD_LIBRARY_PATH:-}"
    fi
fi

# psql shortcut
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
log_ok "Connected (${CONN_CHECK})"

# ── Check table ──────────────────────────────────────────────────────────────
if ! _psql -t -A -c "SELECT 1 FROM ${TABLE} LIMIT 1" &>/dev/null; then
    log_error "Table '${TABLE}' not found"
    exit 1
fi
ROW_COUNT=$(_psql -t -A -c "SELECT count(*) FROM ${TABLE}" 2>/dev/null || echo "0")
log_info "Table: ${TABLE} (${ROW_COUNT} rows)"

# ── Detect query forms ───────────────────────────────────────────────────────
QUERY_FORMS=(
    "text_pase_op_id"
    "text_pase_op_extra"
    "text_pase_op_extra_ds"
    "pase_fn_text_extra"
    "pase_fn_text_extra_ds"
)

declare -A FORM_SQL_FILES
for form in "${QUERY_FORMS[@]}"; do
    _file=$(find "$SQL_DIR" -maxdepth 1 -name "*${TABLE}*${form}*.sql" 2>/dev/null | head -1)
    if [ -z "$_file" ]; then
        _file=$(find "$SQL_DIR" -maxdepth 1 -name "*${form}*.sql" 2>/dev/null | head -1)
    fi
    if [ -n "$_file" ]; then
        FORM_SQL_FILES[$form]="$_file"
        _count=$(wc -l < "$_file")
        log_info "Found: ${form} -> ${_file} (${_count} lines)"
    else
        log_warn "No SQL file for form: ${form}"
    fi
done

if [ ${#FORM_SQL_FILES[@]} -eq 0 ]; then
    log_error "No SQL files found in ${SQL_DIR}"
    log_info "Run: python -m benchmark.generate_sql_files --engine polardb --table ${TABLE}"
    exit 1
fi

# ── Results output ───────────────────────────────────────────────────────────
mkdir -p "$RESULTS_DIR"
RESULT_FILE="${RESULTS_DIR}/polardb-pase-query-forms-${TABLE}-results.csv"
{
    echo "table,type,rows,concurrency,repeat,qps,avg_latency_ms"
} > "$RESULT_FILE"

# ── Parse total QPS from pgbench -r output ───────────────────────────────────
parse_total_qps() {
    local output="$1"
    local num_queries="${2:-1}"

    # Parse per-statement latencies from -r output (accurate per-query QPS)
    local section
    section=$(printf '%s\n' "$output" | sed -n '/^statement latencies in milliseconds/,$p' | tail -n +2)
    if [ -n "$section" ]; then
        local avg_latency
        avg_latency=$(echo "$section" | awk '/^[[:space:]]+[0-9.]+[[:space:]]+[0-9]+[[:space:]]+[A-Za-z]/ {sum += $1; n++} END {if (n > 0) printf "%.3f", sum/n; else print "0"}')
        if [ "$avg_latency" != "0" ] && [ -n "$avg_latency" ]; then
            # QPS = 1000 / avg_latency_ms
            echo "1000 $avg_latency" | awk '{printf "%.3f", $1/$2}'
            return
        fi
    fi

    # Fallback: TPS is transaction rate, each transaction = num_queries queries.
    local tps
    tps=$(printf '%s\n' "$output" | grep -oP 'tps = \K[0-9.]+' | head -1)
    if [ -n "$tps" ] && [ "$num_queries" -gt 1 ] 2>/dev/null; then
        echo "$tps $num_queries" | awk '{printf "%.3f", $1 * $2}'
    else
        echo "${tps:-0}"
    fi
}

# ── Run pgbench ──────────────────────────────────────────────────────────────
run_benchmark_file() {
    local sql_file="$1"
    local concurrency="$2"
    local log_file="$3"

    PGPASSWORD="$PASSWORD" "$PGBENCH" \
        -h "$HOST" \
        -p "$PORT" \
        -U "$USER" \
        -d "$DATABASE" \
        -f "$sql_file" \
        -c "$concurrency" \
        -j 1 \
        -T "$TIMELIMIT" \
        -n \
        -r \
        > "$log_file" 2>&1
}

# ── Main benchmark loop ──────────────────────────────────────────────────────
log_info "Starting benchmark..."
log_info "Repeat: ${REPEAT}, Duration: ${TIMELIMIT}s, Concurrency: 1"

for form in "${QUERY_FORMS[@]}"; do
    SQL_FILE="${FORM_SQL_FILES[$form]:-}"
    if [ -z "$SQL_FILE" ]; then
        continue
    fi

    _row_count=$(wc -l < "$SQL_FILE")
    log_info ""
    log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log_info "  Form: ${form}"
    log_info "  File: ${SQL_FILE}"
    log_info "  Rows: ${_row_count}"
    log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    for ((r=1; r<=REPEAT; r++)); do
        log_file="/tmp/pgbench_${form}_${r}.log"
        log_info "  (${r}/${REPEAT}) Running..."

        run_benchmark_file "$SQL_FILE" 1 "$log_file"

        qps=$(parse_total_qps "$(cat "$log_file")" "$_row_count")
        log_info "  (${r}/${REPEAT}) QPS=${qps}"

        echo "${TABLE},${form},${_row_count},1,${r},${qps}," >> "$RESULT_FILE"
    done
done

# ── Summary ───────────────────────────────────────────────────────────────────
log_info ""
log_info "=========================================="
log_info " Benchmark Complete"
log_info "=========================================="
log_info "Results: ${RESULT_FILE}"

log_info ""
log_info "Summary:"
printf "  %-24s %-10s %-10s %-10s %-10s\n" "Form" "Min" "Max" "Avg" "Median"
for form in "${QUERY_FORMS[@]}"; do
    SQL_FILE="${FORM_SQL_FILES[$form]:-}"
    if [ -z "$SQL_FILE" ]; then
        continue
    fi
    _vals=$(grep "^${TABLE},${form}," "$RESULT_FILE" | cut -d',' -f5 | sort -n)
    if [ -n "$_vals" ]; then
        _min=$(echo "$_vals" | head -1)
        _max=$(echo "$_vals" | tail -1)
        _avg=$(echo "$_vals" | awk '{sum+=$1; n++} END {printf "%.3f", sum/n}')
        _med=$(echo "$_vals" | awk '{
            a[NR]=$1
        } END {
            if (NR%2) print a[(NR+1)/2];
            else print (a[NR/2]+a[NR/2+1])/2
        }')
        printf "  %-24s %-10s %-10s %-10s %-10s\n" "${form}" "${_min}" "${_max}" "${_avg}" "${_med}"
    fi
done

log_info ""
log_info "Done."