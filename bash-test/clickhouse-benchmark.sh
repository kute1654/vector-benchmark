#!/usr/bin/env bash
# =============================================================================
# ClickHouse Vector Query Benchmark
# =============================================================================
# Uses clickhouse-benchmark (or clickhouse-client) to measure QPS for vector
# queries against ClickHouse.
#
# Query forms:
#   normal        - L2Distance(vector, [1,2,3])
#   cast          - L2Distance(vector, CAST([1,2,3] AS Array(Float32)))
#   cast_array    - L2Distance(vector, CAST([1,2,3] AS Array(Float32)))
#
# Usage:
#   CLICKHOUSE=/usr/bin/clickhouse ./clickhouse-benchmark.sh my_table
#
#   PORT=9000 ./clickhouse-benchmark.sh my_table
# =============================================================================

set -euo pipefail

# ── Configuration ────────────────────────────────────────────────────────────
: "${CLICKHOUSE:=clickhouse}"
: "${CLICKHOUSE_BENCHMARK:=clickhouse-benchmark}"
: "${HOST:=127.0.0.1}"
: "${PORT:=9000}"
: "${USER:=default}"
: "${PASSWORD:=}"
: "${DATABASE:=default}"
: "${REPEAT:=3}"
: "${TIMELIMIT:=30}"
: "${SQL_DIR:=sql-bench/clickhouse}"
: "${RESULTS_DIR:=results}"
: "${TOP_K:=10}"
: "${DISTANCE:=l2}"

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
  CLICKHOUSE        Path to clickhouse-client (default: clickhouse)
  CLICKHOUSE_BENCHMARK  Path to clickhouse-benchmark (default: clickhouse-benchmark)
  HOST, PORT        Database connection (default: 127.0.0.1:9000)
  USER, PASSWORD    Database credentials
  DATABASE          Database name
  REPEAT            Number of repetitions (default: 3)
  TIMELIMIT         Duration per run in seconds (default: 30)
  SQL_DIR           Directory for SQL files (default: sql-bench/clickhouse)
  RESULTS_DIR       Directory for results (default: results)
  TOP_K             Top-K limit (default: 10)
  DISTANCE          Distance function (default: l2)
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
if ! command -v "$CLICKHOUSE" &>/dev/null; then
    log_error "clickhouse-client not found: $CLICKHOUSE"
    exit 1
fi

# clickhouse-client shortcut
_ch() {
    if [ -n "$PASSWORD" ]; then
        "$CLICKHOUSE" client \
            --host "$HOST" \
            --port "$PORT" \
            --user "$USER" \
            --password "$PASSWORD" \
            --database "$DATABASE" \
            "$@"
    else
        "$CLICKHOUSE" client \
            --host "$HOST" \
            --port "$PORT" \
            --user "$USER" \
            --database "$DATABASE" \
            "$@"
    fi
}

# ── Check connection ─────────────────────────────────────────────────────────
log_info "Checking ClickHouse connection..."
if ! CONN_CHECK=$(timeout 5 _ch --query "SELECT 1" 2>&1); then
    log_error "Cannot connect to ${HOST}:${PORT} as ${USER}"
    log_error "Error: ${CONN_CHECK}"
    exit 1
fi
log_ok "Connected"

# ── Check table ──────────────────────────────────────────────────────────────
if ! _ch --query "SELECT 1 FROM ${TABLE} LIMIT 1" &>/dev/null; then
    log_error "Table '${TABLE}' not found"
    exit 1
fi
ROW_COUNT=$(_ch --query "SELECT count() FROM ${TABLE}" 2>/dev/null || echo "0")
log_info "Table: ${TABLE} (${ROW_COUNT} rows)"

# ── Detect vector column ─────────────────────────────────────────────────────
VEC_COL=$(_ch --query "
    SELECT name FROM system.columns
    WHERE database = '${DATABASE}' AND table = '${TABLE}'
    AND type LIKE 'Array(Float%%)' LIMIT 1
" 2>/dev/null || echo "")

if [ -z "$VEC_COL" ]; then
    log_error "No Array(Float) column found in ${TABLE}"
    exit 1
fi
log_info "Vector column: ${VEC_COL}"

# ── Distance function ────────────────────────────────────────────────────────
case "$DISTANCE" in
    l2)     DIST_FUNC="L2Distance" ;;
    ip)     DIST_FUNC="cosineDistance" ;;
    cosine) DIST_FUNC="cosineDistance" ;;
    *)      DIST_FUNC="L2Distance" ;;
esac
SORT_DIR="ASC"
if [ "$DISTANCE" = "ip" ]; then
    SORT_DIR="DESC"
fi

# ── Results output ───────────────────────────────────────────────────────────
mkdir -p "$RESULTS_DIR"
RESULT_FILE="${RESULTS_DIR}/clickhouse-query-forms-${TABLE}-results.csv"
{
    echo "table,type,rows,concurrency,repeat,qps,avg_latency_ms"
} > "$RESULT_FILE"

# ── Generate SQL files on the fly if not present ─────────────────────────────
log_info "Generating SQL files..."

SQL_DIR_FULL="${SQL_DIR}/${TABLE}"
mkdir -p "$SQL_DIR_FULL"

QUERY_SIZES=(10 100 1000)
for count in "${QUERY_SIZES[@]}"; do
    tmp_vectors=$(mktemp)
    _ch --query "
        SELECT id, arrayStringConcat(${VEC_COL}, ','),
               hex(reinterpretAsString(${VEC_COL}))
        FROM ${TABLE} ORDER BY rand() LIMIT ${count}
        SETTINGS use_query_cache=0
    " > "$tmp_vectors"

    normal_file="${SQL_DIR_FULL}/${TABLE}_normal_${count}.sql"
    > "$normal_file"

    while IFS=$'\t' read -r _id vec_str _vec_hex; do
        [ -z "$vec_str" ] && continue
        echo "SELECT id, ${DIST_FUNC}(${VEC_COL}, [${vec_str}]) as dis FROM ${TABLE} ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};" >> "$normal_file"
    done < "$tmp_vectors"

    rm -f "$tmp_vectors"
    log_info "  Generated: ${normal_file} ($(wc -l < "$normal_file") lines)"
done

# ── Run benchmark using clickhouse-benchmark ─────────────────────────────────
log_info "Starting benchmark..."
log_info "Repeat: ${REPEAT}, Duration: ${TIMELIMIT}s, Concurrency: 1"

for count in "${QUERY_SIZES[@]}"; do
    sql_file="${SQL_DIR_FULL}/${TABLE}_normal_${count}.sql"
    if [ ! -f "$sql_file" ]; then
        continue
    fi

    _row_count=$(wc -l < "$sql_file")
    log_info ""
    log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    log_info "  Size: ${count} queries"
    log_info "  File: ${sql_file}"
    log_info "  Rows: ${_row_count}"
    log_info "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    for ((r=1; r<=REPEAT; r++)); do
        log_info "  (${r}/${REPEAT}) Running..."

        # Use clickhouse-benchmark if available, otherwise use clickhouse-client with timing
        if command -v "$CLICKHOUSE_BENCHMARK" &>/dev/null; then
            output=$(timeout $((TIMELIMIT + 5)) "$CLICKHOUSE_BENCHMARK" \
                --host "$HOST" \
                --port "$PORT" \
                --user "$USER" \
                ${PASSWORD:+--password "$PASSWORD"} \
                --database "$DATABASE" \
                --concurrency 1 \
                --iterations 0 \
                --delay 0 \
                --timelimit "$TIMELIMIT" \
                < "$sql_file" 2>&1 || true)
        else
            # Fallback: run clickhouse-client with time measurement
            t_start=$(date +%s.%N)
            timeout "$TIMELIMIT" _ch --multiquery --query "$(cat "$sql_file")" > /dev/null 2>&1 || true
            t_end=$(date +%s.%N)
            elapsed=$(echo "$t_end - $t_start" | bc)
            if [ "$(echo "$elapsed > 0" | bc)" = "1" ]; then
                _qps=$(echo "scale=3; $_row_count / $elapsed" | bc)
                output="tps = ${_qps}"
            else
                output="tps = 0"
            fi
        fi

        qps=$(printf '%s\n' "$output" | grep -oP '(tps|Queries per second):\s*\K[0-9.]+' | head -1 || echo "0")
        if [ -z "$qps" ] || [ "$qps" = "0" ]; then
            qps=$(printf '%s\n' "$output" | grep -oP 'QPS:\s*\K[0-9.]+' | head -1 || echo "0")
        fi
        log_info "  (${r}/${REPEAT}) QPS=${qps}"

        echo "${TABLE},normal,${_row_count},1,${r},${qps}," >> "$RESULT_FILE"
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
for count in "${QUERY_SIZES[@]}"; do
    _vals=$(grep "^${TABLE},normal,${count}," "$RESULT_FILE" 2>/dev/null | cut -d',' -f5 | sort -n)
    if [ -n "$_vals" ]; then
        _min=$(echo "$_vals" | head -1)
        _max=$(echo "$_vals" | tail -1)
        _avg=$(echo "$_vals" | awk '{sum+=$1; n++} END {printf "%.3f", sum/n}')
        printf "  %-20s rows=%-6s  min=%-10s max=%-10s avg=%-10s\n" \
            "${count} queries" "${count}" "${_min}" "${_max}" "${_avg}"
    fi
done

log_info ""
log_info "Done."