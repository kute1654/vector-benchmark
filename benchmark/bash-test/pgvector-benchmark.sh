#!/bin/bash
#
# PostgreSQL / pgvector 向量查询基准测试脚本 (基于 pgbench, bash 版)
#
# 参考: clickhouse-benchmark.sh (沿用其"一个脚本含 N 条查询 + 执行耗时算 QPS"的
#   测量口径), 针对使用 PostgreSQL 的实现 (SQL 生成/pgbench 执行)。
#   "查询计划缓存" 维度改为【函数方式】(PL/pgSQL 静态 SQL 包装函数, 即方式三),
#   并新增 "查询结果缓存" 维度 (pgvector.result_cache_debug 会话级开关)。
#
# QPS 统计口径 (与 ClickHouse 一致, 重要):
#   不再把每条查询拆成独立的单查询脚本; 而是把 <row_count> 条查询一次性写入
#   同一个 pgbench 脚本 (一个脚本 = 一个事务, 内部顺序执行 N 条向量检索语句),
#   由 pgbench 在 -T TIMELIMIT 内反复执行该脚本。
#   pgbench 上报 "tps" = 每秒事务数 (= 每秒整个脚本执行的次数), 每条事务含 N 条查询,
#   因此:
#       query QPS = 总查询数 / 执行耗时 = tps × N   (N = 每脚本查询条数,
#                                                      取自 row_count,
#                                                      或被 QUERIES_PER_SCRIPT 截断)
#   该口径与 ClickHouse benchmark 工具一致: QPS = 脚本查询数量 / 执行时间。
#
# 延迟统计口径 (mean/p95/p99, 单位毫秒, 对应 CSV 末尾三列):
#   pgbench 的统计以"事务"为单位, 而本脚本一个事务 = 整个 sql 文件 (N 条查询),
#   因此 pgbench 只能给出事务级平均延迟, 无法还原单条 SQL 的均值/分位数。
#   为此在每次吞吐测量之后追加一个独立的【单语句延迟探针】:
#     1) 复用该组合的 pgbench 脚本: 同样的 SET/profile 行、同样的 PREPARE、同样的查询;
#     2) 用 psql -q -o /dev/null + \timing on 执行: 会话前缀 (SET/DO/PREPARE) 先执行
#        且不计时; 查询行循环到 LATENCY_PROBE_STATEMENTS 条, 包在单个 BEGIN/COMMIT
#        内 (与 pgbench "一个脚本 = 一个事务" 对齐), 逐条语句记录客户端往返耗时;
#     3) 并发为 c 时启动 c 个 psql 客户端, 每个执行 budget/c 条, 合并所有 "Time:" 样本;
#     4) 由样本计算 mean (算术平均) 与 p95/p99 (nearest-rank)。
#   探针只测延迟、不计入 QPS, 每个组合执行一次。psql 走 simple 协议; 若吞吐阶段使用
#   -M prepared/extended, 探针数值会与 pgbench 协议下的略有差异。
#   LATENCY_PROBE_STATEMENTS=0 关闭探针, 三列写 not_recorded。
#
# 两个正交缓存维度:
#   1) 查询计划缓存 (plan_mode), 四档, 对应 PostgreSQL plan_cache_mode 语义
#      (与 Python searcher engine/clients/pgvector/search.py 的
#      enable_query_plan_cache 四态一一对应):
#        raw            = 不使用查询计划缓存 (enable_query_plan_cache=0)
#                         无 PREPARE, simple 协议文本内联, 每次重新 parse+plan
#        prepare_auto   = 使用查询计划缓存 + auto  (enable_query_plan_cache=1)
#                         PREPARE + SET plan_cache_mode=auto (PG 自动决策)
#        prepare_custom = 使用查询计划缓存 + custom(enable_query_plan_cache=2)
#                         PREPARE + SET plan_cache_mode=force_custom_plan (每次重规划)
#        prepare_generic= 使用查询计划缓存 + generic(enable_query_plan_cache=3)
#                         PREPARE + SET plan_cache_mode=force_generic_plan (复用 generic plan)
#      prepare_* 均采用类型化绑定 (PREPARE (vector), 参数 $1 无 cast), 与 searcher
#      的 typed_bind=True 完全对齐; 档位差异只由 plan_cache_mode 承载。
#   2) 查询结果缓存 (result_mode):
#        on  = SET pgvector.result_cache_debug = on   (开启; 同时把主开关
#              pgvector.result_cache 对齐为 on, 保证 QPS 差异可观测)
#        off = SET pgvector.result_cache_debug = off  (关闭; 同时对齐主开关 off)
#
# 覆盖的 SQL 类型 (与 run.py 的 pgvector 客户端一致):
#   text_literal / text_cast / vector_fn / array_int_cast / array_real_cast /
#   array_cast / array_to_vector_fn / halfvec_literal / with_text_literal /
#   with_array_cast / with_vector_fn
#
# 用法:
#   ./pgvector-benchmark.sh [表名1] [表名2] ...
#   不带参数时自动扫描并交互式选择含 vector 列的表
#
# 环境变量:
#   PSQL/PGBENCH/PG_CTL/PGDATA/HOST/PORT/USER/PASSWORD/DATABASE
#   TOP_K/TIMELIMIT/WARMUP_TIMELIMIT/REPEAT/PGBENCH_QUERY_MODE(PGBENCH_JOBS)
#   ROW_COUNTS_OVERRIDE/CONCURRENCIES_OVERRIDE/SQL_TYPES_OVERRIDE/
#   CACHE_PROFILES_OVERRIDE/DISTANCE_FUNC/DISTANCE_FUNC_AUTO
#   QUERIES_PER_SCRIPT (每脚本查询条数; 0=用满 row_count)
#   LATENCY_PROBE_STATEMENTS (单语句延迟探针的样本总数, 跨并发客户端均分; 0=关闭)
#   RESULT_CACHE_GUC_MASTER (默认 pgvector.result_cache)
#   RESULT_CACHE_GUC_DEBUG  (默认 pgvector.result_cache_debug)
#   OUTPUT_CSV/SQL_DIR

set -euo pipefail

_SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

_auto_ld_path() {
    local psql_path="${1:-}"
    [ -n "$psql_path" ] && [ -x "$psql_path" ] || return 0
    local lib_dir
    lib_dir="$(dirname "$(dirname "$psql_path")")/lib"
    if [ -d "$lib_dir" ] && [ -f "$lib_dir/libpq.so" ]; then
        export LD_LIBRARY_PATH="${lib_dir}:${LD_LIBRARY_PATH:-}"
    fi
}

ENGINE="${ENGINE:-pgvector}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-5432}"
USER="${USER:-postgres}"
PASSWORD="${PASSWORD:-123456}"
DATABASE="${DATABASE:-postgres}"
TIMELIMIT="${TIMELIMIT:-10}"
WARMUP_TIMELIMIT="${WARMUP_TIMELIMIT:-1}"
REPEAT="${REPEAT:-1}"
TOP_K="${TOP_K:-10}"
SQL_DIR="${SQL_DIR:-${_SCRIPT_DIR}/sql-bench/pgvector-query-forms}"
OUTPUT_CSV="${OUTPUT_CSV:-${_SCRIPT_DIR}/../results/csv/pgvector-benchmark-results.csv}"
PSQL="${PSQL:-/usr/local/pgsql/bin/psql}"
PGBENCH="${PGBENCH:-/usr/local/pgsql/bin/pgbench}"
PGBENCH_JOBS="${PGBENCH_JOBS:-}"
PGBENCH_QUERY_MODE="${PGBENCH_QUERY_MODE:-simple}"
# 每个 pgbench 脚本内含的查询条数 (对齐 ClickHouse 的"一个脚本 N 条查询"口径):
#   0 = 使用该 row_count SQL 文件的全部查询; >0 = 每个脚本仅取前 QUERIES_PER_SCRIPT 条
QUERIES_PER_SCRIPT="${QUERIES_PER_SCRIPT:-0}"
# 单语句延迟探针的样本总数 (跨并发客户端均分; 0 = 关闭探针)
LATENCY_PROBE_STATEMENTS="${LATENCY_PROBE_STATEMENTS:-1000}"
# prep sql 文件内 EXECUTE 语句总条数 (十万条)。抽样的 ROW_COUNTS 条向量按序循环填充。
PREP_EXEC_TOTAL="${PREP_EXEC_TOTAL:-10000}"
_auto_ld_path "$PSQL"
_auto_ld_path "$PGBENCH"

# 结果缓存开关 GUC (用户口径: result_cache_debug; 主开关对齐同一布尔值)
RESULT_CACHE_GUC_MASTER="${RESULT_CACHE_GUC_MASTER:-pgvector.result_cache}"
RESULT_CACHE_GUC_DEBUG="${RESULT_CACHE_GUC_DEBUG:-pgvector.result_cache_debug}"

_auto_pg_ctl_path() {
    local psql_path="${PSQL:-psql}"
    if echo "$psql_path" | grep -q '/'; then
        local dir; dir="$(dirname "$psql_path")"
        if [ -x "${dir}/pg_ctl" ]; then echo "${dir}/pg_ctl"; return; fi
    fi
    for p in /usr/local/pgsql/bin/pg_ctl /usr/lib/postgresql/*/bin/pg_ctl; do
        [ -x "$p" ] && { echo "$p"; return; }
    done
    echo "pg_ctl"
}
PG_CTL="${PG_CTL:-$(_auto_pg_ctl_path)}"
PGDATA="${PGDATA:-/usr/local/pgsql/data}"
DISTANCE_FUNC="${DISTANCE_FUNC:-<=>}"
DISTANCE_FUNC_AUTO="${DISTANCE_FUNC_AUTO:-true}"
SORT_DIR="${SORT_DIR:-ASC}"

# =====================================================================
# 计划缓存 (plan) 与 结果缓存 (result) 两个正交维度的组合。
# 每个组合: name|plan_mode|result_mode
#   plan_mode:
#     raw             = 不使用查询计划缓存 (无 PREPARE, 文本内联)
#     prepare_auto    = PREPARE + plan_cache_mode=auto
#     prepare_custom  = PREPARE + plan_cache_mode=force_custom_plan
#     prepare_generic = PREPARE + plan_cache_mode=force_generic_plan
#   result_mode: on=SET result_cache_debug on; off=SET result_cache_debug off
# =====================================================================
CACHE_PROFILES=(
    "raw_off|raw|off"
    # "raw_on|raw|on"
    "prepare_auto_off|prepare_auto|off"
    "prepare_custom_off|prepare_custom|off"
    "prepare_generic_off|prepare_generic|off"
    # "prepare_auto_on|prepare_auto|on"
    # "prepare_custom_on|prepare_custom|on"
    # "prepare_generic_on|prepare_generic|on"
)
PLAN_MODES=(raw prepare_auto prepare_custom prepare_generic)
RESULT_MODES=(off on)

SQL_TYPES=(
    text_literal
)
# ROW_COUNTS 为"每组随机抽样的向量条数"(10 个一组); prep 文件按序循环这 ROW_COUNTS
# 条向量生成 EXECUTE 语句, 直到写满 PREP_EXEC_TOTAL 条 (10×10000 = 十万条, 重复 1 万次)。
ROW_COUNTS=(1)
CONCURRENCIES=(8)

apply_list_override() {
    local var_name="$1"
    local override_name="$2"
    local override_value="${!override_name:-}"
    if [ -n "$override_value" ]; then
        local -n target_array="$var_name"
        read -r -a target_array <<< "$override_value"
    fi
}

apply_list_override SQL_TYPES SQL_TYPES_OVERRIDE
apply_list_override ROW_COUNTS ROW_COUNTS_OVERRIDE
apply_list_override CONCURRENCIES CONCURRENCIES_OVERRIDE
apply_list_override CACHE_PROFILES CACHE_PROFILES_OVERRIDE

# 临时目录 / 抽样向量临时文件 (需在抽样前就绪)
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT
TMPVECTORS="$TMPDIR/vectors.txt"

client_query() {
    PGPASSWORD="$PASSWORD" timeout 60 "$PSQL" -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" -t -A -c "$1" 2>/dev/null
}
client_query_long() {
    PGPASSWORD="$PASSWORD" timeout 600 "$PSQL" -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" -t -A -c "$1" 2>/dev/null
}

detect_vector_column() {
    client_query "SELECT column_name FROM information_schema.columns WHERE table_name = '$1' AND data_type = 'USER-DEFINED' AND udt_name = 'vector' LIMIT 1"
}
detect_vector_dimension() {
    local dim; dim=$(client_query "SELECT vector_dims($2) FROM $1 LIMIT 1"); echo "${dim:-0}"
}
detect_table_index_info() {
    local table="$1" vec_col="$2"
    local info
    info=$(client_query "
        SELECT am.amname, pg_get_indexdef(i.indexrelid)
        FROM pg_index i
        JOIN pg_class c ON i.indexrelid = c.oid
        JOIN pg_am am ON c.relam = am.oid
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE c.relname NOT LIKE 'pg_%'
          AND i.indrelid = '$table'::regclass
          AND a.attname = '$vec_col'
          AND (am.amname = 'hnsw' OR am.amname = 'ivfflat')
        LIMIT 1" 2>/dev/null || echo "")
    [ -z "$info" ] && { echo ""; return; }
    local am_name index_def
    IFS='|' read -r am_name index_def <<< "$info"
    local ops; ops=$(echo "$index_def" | grep -oP 'vector_\w+_ops' | head -1 || echo "")
    echo "${am_name}|${ops}"
}
ops_to_distance_func() {
    case "$1" in
        vector_l2_ops) echo "<->" ;;
        vector_cosine_ops) echo "<=>" ;;
        vector_ip_ops) echo "<#>" ;;
        vector_l1_ops) echo "<+>" ;;
        *) echo "" ;;
    esac
}
list_vector_tables() {
    client_query "SELECT DISTINCT table_name, column_name FROM information_schema.columns WHERE data_type = 'USER-DEFINED' AND udt_name = 'vector' ORDER BY table_name"
}
auto_select_distance_func() {
    local table="$1" vec_col="$2"
    local idx_info; idx_info=$(detect_table_index_info "$table" "$vec_col")
    [ -z "$idx_info" ] && return
    local am ops suggested
    IFS='|' read -r am ops <<< "$idx_info"
    suggested=$(ops_to_distance_func "$ops")
    if [ -z "$suggested" ]; then
        echo "  ⚠ 无法识别的 opclass '$ops', 使用默认距离函数 '$DISTANCE_FUNC'"
        return
    fi
    if [ "${DISTANCE_FUNC_AUTO}" = "true" ]; then
        DISTANCE_FUNC="$suggested"
        echo "  ✓ 根据索引自动设置距离函数: $DISTANCE_FUNC (index: ${am}(${ops}))"
    elif [ "$DISTANCE_FUNC" != "$suggested" ]; then
        echo "  ⚠ 距离函数 '$DISTANCE_FUNC' 与索引 '$ops' 不匹配! 建议 $suggested (设置 DISTANCE_FUNC_AUTO=true)"
    fi
}

# 交互式选择含 vector 列的表 (无命令行参数时)
interactive_select_tables() {
    local tables_info; tables_info=$(list_vector_tables)
    if [ -z "$tables_info" ]; then
        echo "错误: 未找到任何含 vector 列的表" >&2; exit 1
    fi
    echo "" >&2
    local i=0 tname tcol
    echo "  找到含 vector 列的表:" >&2
    while IFS='|' read -r tname tcol; do
        [ -z "$tname" ] && continue
        i=$((i+1))
        local dim; dim=$(detect_vector_dimension "$tname" "$tcol")
        echo "  [$i] $tname (col=$tcol dim=${dim:-?})" >&2
    done <<< "$tables_info"
    echo "" >&2
    local user_choice=""
    echo -n "  请选择要测试的表 (编号, 多个用空格分隔, 默认全选): " >&2
    read -r user_choice
    local out=""
    if [ -z "$user_choice" ]; then
        out=$(echo "$tables_info")
    else
        for num in $user_choice; do
            [ "$num" = "0" ] && continue
            echo "$tables_info" | awk -v n="$num" 'NR==n' >> /dev/null && {
                local line; line=$(echo "$tables_info" | awk -v n="$num" 'NR==n')
                [ -n "$line" ] && out="${out:+$out$'\n'}$line"
            }
        done
    fi
    echo "$out"
}

# ---- SQL 帮助函数 ----
distance_expr() {
    echo "(${1} ${DISTANCE_FUNC} ${2})"
}
make_select_sql() {  # table vec_col query_expr
    local dist; dist=$(distance_expr "$2" "$3")
    echo "SELECT id, ${dist} AS dis FROM $1 ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};"
}
make_with_sql() {  # table vec_col query_expr
    local dist; dist=$(distance_expr "$2" "qv.v")
    echo "WITH qv AS (SELECT ${3} AS v) SELECT id, ${dist} AS dis FROM $1 CROSS JOIN qv ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};"
}

# 渲染一条 "原始 SQL" 文本 (向量字面量按 sql_type 内联), 用于 plan=raw
# 参数: sql_type table vec_col vec_str(原文) dim
render_raw_query() {
    local st="$1" table="$2" col="$3" v="$4" dim="$5"
    local inner="${v//[()]/}"; inner="${inner//[\[\]]/}"
    local real_array="ARRAY[${inner}]::real[]"

    case "$st" in
        text_literal)        make_select_sql "$table" "$col" "'${v}'::vector" ;;
        text_cast)           make_select_sql "$table" "$col" "CAST('${v}' AS vector)" ;;
        vector_fn)           make_select_sql "$table" "$col" "vector('${v}')" ;;
        array_int_cast)      make_select_sql "$table" "$col" "ARRAY[${inner}]::vector" ;;
        array_real_cast)     make_select_sql "$table" "$col" "ARRAY[${inner}]::real[]::vector" ;;
        array_cast)          make_select_sql "$table" "$col" "CAST(${real_array} AS vector)" ;;
        array_to_vector_fn)  make_select_sql "$table" "$col" "array_to_vector(${real_array}, ${dim}, false)" ;;
        halfvec_literal)     make_select_sql "$table" "$col" "'${v}'::halfvec(${dim})::vector" ;;
        with_text_literal)   make_with_sql "$table" "$col" "'${v}'::vector" ;;
        with_array_cast)     make_with_sql "$table" "$col" "ARRAY[${inner}]::real[]::vector" ;;
        with_vector_fn)      make_with_sql "$table" "$col" "vector('${v}')" ;;
        *)                   make_select_sql "$table" "$col" "'${v}'::vector" ;;
    esac
}

is_with_type() {  # 是否为 CTE(with) 形式
    case "$1" in with_text_literal|with_array_cast|with_vector_fn) return 0 ;; *) return 1 ;; esac
}

# ---- 缓存配置 ----
session_set_line_for_result() {  # off|on
    local b="$1"
    echo "SET ${RESULT_CACHE_GUC_MASTER} = ${b};"
    echo "SET ${RESULT_CACHE_GUC_DEBUG} = ${b};"
}
# plan_mode -> plan_cache_mode 的会话值 (仅 prepare_* 档有; raw 无 PREPARE, 不设置)
plan_mode_to_cache_mode() {
    case "$1" in
        prepare_auto)    echo "auto" ;;
        prepare_custom)  echo "force_custom_plan" ;;
        prepare_generic) echo "force_generic_plan" ;;
        *)               echo "" ;;
    esac
}
# 生成 PREPARE 语句体 (typed_bind 形态: PREPARE (vector), 参数 $1 无 cast, 与 searcher 对齐)
render_prepare_body() {  # st table col dim
    local st="$1" table="$2" col="$3" dim="$4" expr='$1'
    if is_with_type "$st"; then
        echo "WITH kv AS (SELECT ${expr} AS v) SELECT t.id, (t.${col} ${DISTANCE_FUNC} kv.v) AS dis FROM ${table} t CROSS JOIN kv ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K}"
    else
        echo "SELECT t.id, (t.${col} ${DISTANCE_FUNC} ${expr}) AS dis FROM ${table} t ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K}"
    fi
}
session_set_line_for_profile() {  # profile name
    local profile_name="$1"
    local group pname plan result
    for group in "${CACHE_PROFILES[@]}"; do
        IFS='|' read -r pname plan result <<< "$group"
        if [ "$pname" = "$profile_name" ]; then
            echo "SET ${RESULT_CACHE_GUC_MASTER} = ${result};"
            echo "SET ${RESULT_CACHE_GUC_DEBUG} = ${result};"
            local pcm; pcm=$(plan_mode_to_cache_mode "$plan")
            if [ -n "$pcm" ]; then
                echo "SET plan_cache_mode = ${pcm};"
            fi
            return
        fi
    done
    echo ""
}

# 采样 row_count 条 'id|vec' 行到 stdout
sample_vectors() {
    local table="$1" vec_col="$2" count="$3"
    local est_rows; est_rows=$(client_query "SELECT GREATEST(reltuples, 1) FROM pg_class WHERE relname = '${table}'" | head -1)
    est_rows=${est_rows:-1000000}
    # 直接全表随机取 count 条, 保证一定能选到 (对大表 LIMIT count 足够小, 代价可控)
    client_query_long "SELECT id, ${vec_col}::text FROM ${table} ORDER BY random() LIMIT ${count}"
}

# ---- pgbench 脚本生成 (对齐 ClickHouse 口径: 一个脚本含 N 条查询) ----
# 把 sql_file 中的查询一次性写入单个 run_file, 头部带会话级 SET 行;
# 返回实际写入的查询条数 N (受 QUERIES_PER_SCRIPT 截断时取前 N 条)。
make_script_with_settings() {
    local sql_file="$1" set_line="$2" run_file="$3" limit="${4:-0}"
    : > "$run_file"
    [ -n "$set_line" ] && printf '%s\n' "$set_line" > "$run_file"
    local n=0 query
    while IFS= read -r query; do
        [ -z "$query" ] && continue
        printf '%s\n' "$query" >> "$run_file"
        n=$((n+1))
        [ "$limit" -gt 0 ] && [ "$n" -ge "$limit" ] && break
    done < "$sql_file"
    echo "$n"
}

run_benchmark_file() {
    local run_file="$1" concurrency="$2" log_file="$3" duration="${4:-$TIMELIMIT}"
    local jobs="${PGBENCH_JOBS:-$concurrency}" rc
    if [ ! -s "$run_file" ]; then
        echo "错误: pgbench 脚本为空: $run_file" >&2; return 1
    fi
    PGPASSWORD="$PASSWORD" "$PGBENCH" \
        -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" \
        -f "$run_file" \
        -c "$concurrency" -j "$jobs" \
        -M "$PGBENCH_QUERY_MODE" \
        -T "$duration" -n -r > "$log_file" 2>&1
    rc=$?
    return $rc
}

# QPS 官方口径统一由逐条探针 mean 计算: QPS = 并发 × 1000 / mean_ms (见主循环)。
# 本函数仅从 pgbench 日志解析 duration/transactions/latency 等用于展示与其他列,
# 不再依赖 pgbench 派生的 QPS (启动/框架开销导致偏差)。
# 主循环传 num_queries = 每脚本查询条数 N, concurrency = 并发客户端数。
parse_benchmark_metrics() {
    local output="$1" num_queries="${2:-1}" concurrency="${3:-1}"
    local duration transactions failed latency tps stmt_latency successful tx_qps query_qps
    duration=$(printf '%s\n' "$output" | awk '/^duration:/ {print $2; exit}')
    transactions=$(printf '%s\n' "$output" | awk -F': ' '/^number of transactions actually processed:/ {split($2,a," "); split(a[1],b,"/"); print b[1]; exit}')
    failed=$(printf '%s\n' "$output" | awk -F': ' '/^number of failed transactions:/ {split($2,a," "); print a[1]; exit}')
    latency=$(printf '%s\n' "$output" | awk -F'= ' '/^latency average =/ {print $2; exit}' | awk '{print $1}')
    tps=$(printf '%s\n' "$output" | awk -F'= ' '/^tps =/ {print $2; exit}' | awk '{print $1}')
    stmt_latency=$(printf '%s\n' "$output" | awk '/^statement latencies in milliseconds/ {in_section=1; next} in_section && $1 ~ /^[0-9]+([.][0-9]+)?$/ {sum+=$1; n++} END {if(n) printf "%.3f", sum/n; else print "0"}')
    duration=${duration:-0}; transactions=${transactions:-0}; failed=${failed:-0}; latency=${latency:-0}
    successful=$((transactions - failed)); [ "$successful" -lt 0 ] && successful=0
    if awk -v l="$latency" 'BEGIN{exit !(l>0)}'; then
        # 主路径: QPS = 并发 × N × 1000 / latency_avg_ms (pgbench 纯事务执行耗时, 与探针 mean 交叉核对一致)
        tx_qps=$(awk -v c="$concurrency" -v l="$latency" 'BEGIN{printf "%.6f", c*1000/l}')
    else
        # 兜底: 用 pgbench 报告的 tps, 或其事务数/整秒 duration
        tx_qps=$(awk -v n="$successful" -v s="$duration" -v t="$tps" 'BEGIN{if(t>0) printf "%.6f", t; else if(s>0) printf "%.6f", n/s; else print "0.000000"}')
    fi
    query_qps=$(awk -v t="$tx_qps" -v q="$num_queries" 'BEGIN{printf "%.3f", t*q}')
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$query_qps" "$tx_qps" "$duration" "$transactions" "$failed" "${latency:-0}" "$stmt_latency"
}

# ---- 单语句延迟探针 (mean/p95/p99) ----
# 生成探针脚本:
#   <会话前缀行: SET/DO/PREPARE>
#   BEGIN;
#   \timing on
#   <查询行循环到 budget 条>
#   \timing off
#   COMMIT;
# 会话前缀不计时; BEGIN/COMMIT 放在计时区外, 因此样本只包含查询语句本身。
build_latency_probe_file() {
    local runner_file="$1" probe_file="$2" budget="$3"
    local -a prefix=() queries=()
    local line
    while IFS= read -r line || [ -n "$line" ]; do
        [ -z "$line" ] && continue
        case "$line" in
            SET\ *|set\ *|DO\ *|do\ *|PREPARE\ *|prepare\ *) prefix+=("$line") ;;
            *) queries+=("$line") ;;
        esac
    done < "$runner_file"
    [ "${#queries[@]}" -eq 0 ] && return 1
    {
        if [ "${#prefix[@]}" -gt 0 ]; then
            printf '%s\n' "${prefix[@]}"
        fi
        printf 'BEGIN;\n\\timing on\n'
        local i=0 n=0
        while [ "$n" -lt "$budget" ]; do
            printf '%s\n' "${queries[$i]}"
            i=$((i + 1)); [ "$i" -ge "${#queries[@]}" ] && i=0
            n=$((n + 1))
        done
        printf '\\timing off\nCOMMIT;\n'
    } > "$probe_file"
}

# 并发运行 c 个 psql 客户端执行探针脚本, stdout 输出所有样本 (毫秒, 每行一个)。
# 任一客户端失败则返回 1 (fail closed, 不把残缺样本当作结果)。
run_latency_probe() {
    local probe_file="$1" concurrency="$2"
    local -a timing_files=() pids=()
    local k tf rc=0
    for ((k = 0; k < concurrency; k++)); do
        tf="$TMPDIR/probe_${k}_${RANDOM}${RANDOM}.timing"
        timing_files+=("$tf")
        PGPASSWORD="$PASSWORD" timeout 600 "$PSQL" -X -q -o /dev/null -v ON_ERROR_STOP=1 \
            -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" \
            -f "$probe_file" > "$tf" 2> "${tf}.err" &
        pids+=($!)
    done
    for k in "${!pids[@]}"; do
        if ! wait "${pids[$k]}"; then
            rc=1
            echo "    psql 探针客户端 $k 失败: $(tail -3 "${timing_files[$k]}.err" | tr '\n' ' ')" >&2
        fi
    done
    if [ "$rc" -ne 0 ]; then
        return 1
    fi
    # psql 的 \timing 始终以毫秒输出: "Time: 0.690 ms" (>=1s 时附带 "(00:01.204)")
    awk '/^Time:/ {print $2}' "${timing_files[@]}"
}

# stdin: 每行一个毫秒样本; stdout: "mean p95 p99 n"
# p95/p99 使用 nearest-rank: rank = ceil(p/100 × N)
latency_stats() {
    sort -n | awk '
        { v[NR] = $1; s += $1 }
        END {
            if (NR == 0) { print "0.000 0.000 0.000 0"; exit }
            p95 = int((95 * NR + 99) / 100); if (p95 < 1) p95 = 1
            p99 = int((99 * NR + 99) / 100); if (p99 < 1) p99 = 1
            printf "%.3f %.3f %.3f %d\n", s / NR, v[p95], v[p99], NR
        }'
}

write_csv_header() {
    local base_header="engine,server_version,table_name,vector_column,dimension,index_type,index_ops,distance_func,plan_mode,result_mode,sql_type,row_count,concurrency"
    for i in $(seq 1 "$REPEAT"); do base_header="${base_header},run_${i}"; done
    base_header="${base_header},qps_avg,qps_min,qps_max"
    base_header="${base_header},tx_qps_avg,elapsed_avg_s,transactions_avg,failed_avg,latency_avg_ms,statement_avg_ms"
    local header="${base_header},mean_time_ms,p95_time_ms,p99_time_ms"
    mkdir -p "$(dirname "$OUTPUT_CSV")"
    local existing_header=""
    if [ -f "$OUTPUT_CSV" ]; then
        existing_header=$(head -1 "$OUTPUT_CSV" 2>/dev/null || true)
    fi
    if [ "$existing_header" = "$header" ]; then
        return
    fi
    # 旧格式迁移: 保留历史行, 末尾补三列 not_recorded
    if [ "$existing_header" = "$base_header" ]; then
        local tmp_csv="$TMPDIR/csv_migrate.csv"
        echo "迁移旧数据: 为每行补 mean_time_ms/p95_time_ms/p99_time_ms=not_recorded 三列..."
        echo "$header" > "$tmp_csv"
        tail -n +2 "$OUTPUT_CSV" | sed 's/$/,not_recorded,not_recorded,not_recorded/' >> "$tmp_csv"
        mv "$tmp_csv" "$OUTPUT_CSV"
        echo "迁移完成"
        return
    fi
    if [ -f "$OUTPUT_CSV" ]; then
        local bak="${OUTPUT_CSV}.$(date '+%Y%m%d%H%M%S').bak"; cp "$OUTPUT_CSV" "$bak"
        echo "CSV 表头不匹配, 已备份旧文件到: $bak"
    fi
    echo "$header" > "$OUTPUT_CSV"
}

# =====================================================================
# 主流程
# =====================================================================
if ! command -v "$PSQL" >/dev/null 2>&1 || ! command -v "$PGBENCH" >/dev/null 2>&1; then
    echo "错误: 找不到 psql/pgbench (PSQL=$PSQL PGBENCH=$PGBENCH)" >&2; exit 1
fi
CONN_TEST="$(timeout 5 env PGPASSWORD="$PASSWORD" "$PSQL" -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" -t -A -c "SELECT 1" 2>&1)"
if [ "$CONN_TEST" != "1" ]; then
    echo "错误: 无法连接 $HOST:$PORT/$DATABASE ($CONN_TEST)" >&2; exit 1
fi
SERVER_VERSION="$(client_query "SELECT current_setting('server_version')")"

if [ "$#" -eq 0 ]; then
    echo "无命令行参数, 进入交互式选择模式..."
    selected_output=$(interactive_select_tables </dev/tty)
    selected_tables=() selected_cols=() selected_dims=()
    while IFS='|' read -r tname tcol; do
        [ -z "$tname" ] && continue
        selected_tables+=("$tname"); selected_cols+=("$tcol")
        selected_dims+=("$(detect_vector_dimension "$tname" "$tcol")")
    done <<< "$selected_output"
    set -- "${selected_tables[@]}"
fi

echo "=============================================="
echo " PGVector benchmark (pgbench)"
echo "=============================================="
echo "engine:      $ENGINE"
echo "target:      $HOST:$PORT / $DATABASE  (version $SERVER_VERSION)"
echo "distance:    $DISTANCE_FUNC   sort: $SORT_DIR"
echo "pgbench协议: -M $PGBENCH_QUERY_MODE"
echo "sql types:   ${SQL_TYPES[*]}"
echo "row counts:  ${ROW_COUNTS[*]}"
echo "concurrency: ${CONCURRENCIES[*]}"
echo "cache_profiles: ${CACHE_PROFILES[*]}"
echo "output csv:  $OUTPUT_CSV"
echo "=============================================="
echo ""

declare -A TABLE_VEC_COL TABLE_DIM TABLE_INDEX_INFO

for table in "$@"; do
    vec_col="$(detect_vector_column "$table")"
    if [ -z "$vec_col" ]; then
        echo "错误: 表 '$table' 没有 vector 列" >&2; exit 1
    fi
    dim="$(detect_vector_dimension "$table" "$vec_col")"
    TABLE_VEC_COL["$table"]="$vec_col"; TABLE_DIM["$table"]="$dim"
    idx_info=$(detect_table_index_info "$table" "$vec_col"); TABLE_INDEX_INFO["$table"]="$idx_info"
    idx_display="无索引"
    if [ -n "$idx_info" ]; then
        IFS='|' read -r am ops <<< "$idx_info"; idx_display="${am}(${ops})"
    fi
    echo "表: $table  向量列: $vec_col  维度: $dim  索引: $idx_display"
done
echo ""

if [ "${DISTANCE_FUNC_AUTO}" = "true" ]; then
    for table in "$@"; do auto_select_distance_func "$table" "${TABLE_VEC_COL[$table]}"; done
    echo ""
fi

# ---- 抽样并生成各 sql_type 的原始 SQL 文件 (plan=raw) ----
#   RAW_BASE[table|row_count|sql_type] : 每行一条该 sql_type 的原始 SQL (向量字面量内联)
mkdir -p "$SQL_DIR"
declare -A RAW_BASE PREP_BASE PREP_NAME

for table in "$@"; do
    col="${TABLE_VEC_COL[$table]}"; dim="${TABLE_DIM[$table]}"
    for row_count in "${ROW_COUNTS[@]}"; do
        for st in "${SQL_TYPES[@]}"; do
            local_raw_path="${SQL_DIR}/pgv_raw_${table}_${st}_${row_count}.sql"
            : > "$local_raw_path"
            RAW_BASE["${table}|${row_count}|${st}"]="$local_raw_path"
            # PREPARE 形式 (prepare_auto/custom/generic): 头部先通过 DO 块幂等 DEALLOCATE (PostgreSQL
            # 无 DROP PREPARE/`IF EXISTS`, 用 invalid_sql_statement_name 异常捕获; pgbench
            # 每轮事务会重跑脚本, 需幂等避免 "already exists"), 随后 PREPARE + 逐行 EXECUTE。
            local_prep_path="${SQL_DIR}/pgv_prep_${table}_${st}_${row_count}.sql"
            : > "$local_prep_path"
            pname="pgv_ps_${st}_${RANDOM}${RANDOM}"
            PREP_NAME["${table}|${row_count}|${st}"]="$pname"
            PREP_BASE["${table}|${row_count}|${st}"]="$local_prep_path"
            echo "DO \$\$ BEGIN EXECUTE 'DEALLOCATE ${pname}'; EXCEPTION WHEN invalid_sql_statement_name THEN NULL; END \$\$;" > "$local_prep_path"
            echo "PREPARE ${pname}(vector) AS $(render_prepare_body "$st" "$table" "$col" "$dim");" >> "$local_prep_path"
        done

        echo "抽样 ${row_count} 条向量: $table (col=$col dim=$dim)"
        sample_vectors "$table" "$col" "$row_count" > "$TMPVECTORS" 2>/dev/null || {
            echo "  抽样失败: $table"; rm -f "$TMPVECTORS"; exit 1; }
        if [ ! -s "$TMPVECTORS" ]; then
            echo "  错误: 未采样到向量 (表为空或权限不足)"; exit 1
        fi
        # 读入抽样的 row_count 条向量, 供各 sql_type 复用与按序循环
        vecs=()
        while IFS='|' read -r id vec; do
            [ -z "$id" ] && continue; [ -z "$vec" ] && continue
            vecs+=("$vec")
        done < "$TMPVECTORS"
        rm -f "$TMPVECTORS"
        nvecs=${#vecs[@]}
        [ "$nvecs" -eq 0 ] && { echo "  错误: 抽样无有效向量"; exit 1; }

        for st in "${SQL_TYPES[@]}"; do
            # raw: 与 prep 完全一致的生成规则 —— 抽样的 row_count 条向量按序循环,
            # 写满 PREP_EXEC_TOTAL 条 SELECT 语句 (10 个一组 × 10000 次 = 十万条);
            # 差别仅在于不包含 DROP/PREPARE 头, 从而让"使用/不使用查询计划缓存"
            # 两档的测试基准(语句条数与向量序列)完全一致。
            written=0; i=0
            while [ "$written" -lt "$PREP_EXEC_TOTAL" ]; do
                echo "$(render_raw_query "$st" "$table" "$col" "${vecs[$i]}" "$dim")" >> "${RAW_BASE["${table}|${row_count}|${st}"]}"
                written=$((written+1)); i=$((i+1)); [ "$i" -ge "$nvecs" ] && i=0
            done
            # prep: 抽样的 ROW_COUNTS 条向量按序循环, 一直写满 PREP_EXEC_TOTAL 条 EXECUTE
            # 语句 (10 个一组 × 10000 次 = 十万条); 每条 EXECUTE 依次使用对应向量。
            written=0; i=0
            while [ "$written" -lt "$PREP_EXEC_TOTAL" ]; do
                inner="${vecs[$i]//[()]/}"; inner="${inner//[\[\]]/}"
                echo "EXECUTE ${PREP_NAME["${table}|${row_count}|${st}"]}('[${inner}]');" >> "${PREP_BASE["${table}|${row_count}|${st}"]}"
                written=$((written+1)); i=$((i+1)); [ "$i" -ge "$nvecs" ] && i=0
            done
        done
        local_rp="${SQL_DIR}/pgv_raw_${table}_${SQL_TYPES[0]}_${row_count}.sql"
        local_pp="${SQL_DIR}/pgv_prep_${table}_${SQL_TYPES[0]}_${row_count}.sql"
        echo "  已生成: raw=$(wc -l < "$local_rp") 行, prep=$(wc -l < "$local_pp") 行"
    done
done
echo ""

write_csv_header

# 预热: 按"每张表一次"执行, 把该表索引加载进内存。时长取 WARMUP_TIMELIMIT
# 但最少 60 秒; 每张表只需预热一次, 后续该表所有 profile/sql_type 共享预热效果。
warmup_timelimit=$WARMUP_TIMELIMIT
if [ -n "$warmup_timelimit" ] && [ "$warmup_timelimit" -ge 0 ] 2>/dev/null && [ "$warmup_timelimit" -lt 60 ]; then
    warmup_timelimit=60
fi

for table in "$@"; do
    warmup_sql="${RAW_BASE["${table}|${ROW_COUNTS[0]}|${SQL_TYPES[0]}"]:-}"
    if [ -s "$warmup_sql" ] && [ "$WARMUP_TIMELIMIT" != "0" ]; then
        wset=$(session_set_line_for_result "${CACHE_PROFILES[0]##*|}")
        wrun="$TMPDIR/warmup_${table}.sql"
        make_script_with_settings "$warmup_sql" "$wset" "$wrun" "$QUERIES_PER_SCRIPT" >/dev/null
        echo ">>> 预热表 $table (${warmup_timelimit}s): $warmup_sql"
        run_benchmark_file "$wrun" 1 "$TMPDIR/warmup_${table}.log" "${warmup_timelimit}" >/dev/null 2>&1
        echo ">>> 表 $table 预热完成"
    fi
done

echo ">>> 开始测试"
for table in "$@"; do
    vec_col="${TABLE_VEC_COL[$table]}"
    dim="${TABLE_DIM[$table]}"
    index_type="none" index_ops="none"
    idx_info="${TABLE_INDEX_INFO[$table]:-}"
    if [ -n "$idx_info" ]; then
        IFS='|' read -r index_type index_ops <<< "$idx_info"
    fi

    for group in "${CACHE_PROFILES[@]}"; do
        IFS='|' read -r cache_profile plan_mode result_mode <<< "$group"
        set_line="$(session_set_line_for_profile "$cache_profile")"
        # PLCACHE TRACE: prepare_* 模式在日志中可看到 plan_cache_mode / [PLAN_CACHE_TRACE]
        echo "--- profile: $cache_profile (plan=$plan_mode result_cache=$result_mode) ---"

        for sql_type in "${SQL_TYPES[@]}"; do
            for row_count in "${ROW_COUNTS[@]}"; do
                case "$plan_mode" in
                    prepare_auto|prepare_custom|prepare_generic)
                        base="${PREP_BASE["${table}|${row_count}|${sql_type}"]:-}" ;;
                    *)
                        base="${RAW_BASE["${table}|${row_count}|${sql_type}"]:-}" ;;
                esac
                [ -s "$base" ] || { echo "跳过空文件: $base"; continue; }

                # 一个脚本含 N 条查询 (对齐 ClickHouse 口径): 把 base 的所有查询写入
                # 单个 pgbench 脚本, N = 查询条数 (受 QUERIES_PER_SCRIPT 截断)。
                runner_file="$TMPDIR/${ENGINE}_${table}_${cache_profile}_${sql_type}_${row_count}.sql"
                nqueries=$(make_script_with_settings "$base" "$set_line" "$runner_file" "$QUERIES_PER_SCRIPT")
                if [[ "$plan_mode" == prepare_* ]]; then
                    # QPS 基于实际 EXECUTE (查询) 行数; DO/PREPARE 声明行不计入查询数
                    nqueries=$(grep -c '^EXECUTE ' "$base" || true)
                fi
                if [ "${nqueries:-0}" -eq 0 ]; then
                    echo "跳过空脚本: $base"; continue
                fi

                for concurrency in "${CONCURRENCIES[@]}"; do
                    echo "━━ table=$table profile=$cache_profile type=$sql_type rows=$row_count conc=$concurrency queries/script=$nqueries ━━"
                    qps_values=() tx_qps_values=() elapsed_values=() transaction_values=() failed_values=() latency_values=() stmt_values=()
                    for run in $(seq 1 "$REPEAT"); do
                        log="$TMPDIR/${ENGINE}_${table}_${cache_profile}_${sql_type}_${row_count}_${concurrency}_${run}.log"
                        echo -n "  (${run}/${REPEAT}) "
                        set +e
                        run_benchmark_file "$runner_file" "$concurrency" "$log"
                        exit_code=$?
                        set -e
                        if [ "$exit_code" -ne 0 ]; then
                            echo "失败(exit=$exit_code) QPS=0"; tail -8 "$log" | sed 's/^/    /'
                            qps_values+=(0); tx_qps_values+=(0); elapsed_values+=(0)
                            transaction_values+=(0); failed_values+=(1); latency_values+=(0); stmt_values+=(0)
                            continue
                        fi
                        # QPS = tps × N (每条事务执行 N 条查询, 即 总查询数/执行耗时)
                        IFS=$'\t' read -r qps tx_qps elapsed transactions failed latency stmt <<< "$(parse_benchmark_metrics "$(cat "$log")" "$nqueries" "$concurrency")"
                        echo "QPS=$qps tx/s=$tx_qps elapsed=${elapsed}s tx=$transactions failed=$failed latency=${latency}ms"
                        qps_values+=("${qps:-0.000}"); tx_qps_values+=("${tx_qps:-0.000}"); elapsed_values+=("${elapsed:-0}")
                        transaction_values+=("${transactions:-0}"); failed_values+=("${failed:-0}"); latency_values+=("${latency:-0}"); stmt_values+=("${stmt:-0}")
                    done
                    sorted=$(printf '%s\n' "${qps_values[@]}" | sort -n)
                    count=${#qps_values[@]}
                    qps_min=$(echo "$sorted" | head -1); qps_max=$(echo "$sorted" | tail -1)
                    if [ "$count" -le 2 ]; then
                        qps_avg=$(printf '%s\n' "${qps_values[@]}" | awk '{s+=$1;n++}END{printf "%.3f", n?s/n:0}')
                    else
                        qps_avg=$(echo "$sorted" | sed '1d;$d' | awk '{s+=$1;n++}END{printf "%.3f", n?s/n:0}')
                    fi
                    echo "  => QPS avg=$qps_avg min=$qps_min max=$qps_max"
                    tx_qps_avg=$(printf '%s\n' "${tx_qps_values[@]}" | awk '{s+=$1;n++}END{printf "%.3f", n?s/n:0}')
                    elapsed_avg=$(printf '%s\n' "${elapsed_values[@]}" | awk '{s+=$1;n++}END{printf "%.3f", n?s/n:0}')
                    transactions_avg=$(printf '%s\n' "${transaction_values[@]}" | awk '{s+=$1;n++}END{printf "%.3f", n?s/n:0}')
                    failed_avg=$(printf '%s\n' "${failed_values[@]}" | awk '{s+=$1;n++}END{printf "%.3f", n?s/n:0}')
                    latency_avg=$(printf '%s\n' "${latency_values[@]}" | awk '{s+=$1;n++}END{printf "%.3f", n?s/n:0}')
                    stmt_avg=$(printf '%s\n' "${stmt_values[@]}" | awk '{s+=$1;n++}END{printf "%.3f", n?s/n:0}')

                    # ---- 单语句延迟探针 (mean/p95/p99, 毫秒) ----
                    # pgbench 的事务 = 整个脚本 (N 条查询), 无法给出单条语句的分布;
                    # 这里用 psql \timing 顺序执行同一脚本中的查询, 逐条采集耗时。
                    mean_time_ms="not_recorded"; p95_time_ms="not_recorded"; p99_time_ms="not_recorded"; probe_n=0
                    if [ "${LATENCY_PROBE_STATEMENTS:-0}" -gt 0 ]; then
                        probe_per_client=$((LATENCY_PROBE_STATEMENTS / concurrency))
                        [ "$probe_per_client" -lt 1 ] && probe_per_client=1
                        probe_file="$TMPDIR/probe_${ENGINE}_${table}_${cache_profile}_${sql_type}_${row_count}_${concurrency}.sql"
                        mean_time_ms="0.000"; p95_time_ms="0.000"; p99_time_ms="0.000"
                        if build_latency_probe_file "$runner_file" "$probe_file" "$probe_per_client"; then
                            echo -n "  latency probe (${concurrency}x${probe_per_client} statements): "
                            if probe_samples=$(run_latency_probe "$probe_file" "$concurrency"); then
                                read -r mean_time_ms p95_time_ms p99_time_ms probe_n <<< "$(printf '%s\n' "$probe_samples" | latency_stats)"
                                echo "mean=${mean_time_ms}ms p95=${p95_time_ms}ms p99=${p99_time_ms}ms (n=${probe_n})"
                            else
                                echo "失败, 三列记为 0.000"
                            fi
                        else
                            echo "  latency probe: 跳过 (脚本中没有查询行)"
                        fi
                    fi

                    # ---- 用探针 mean 作为官方 QPS ----
                    # pgbench 由 latency 派生的 QPS 受启动/框架开销影响, 与逐条实测 mean 存在
                    # 3~6% 系统偏差; 统一改用 mean 计算 QPS: QPS = 并发 × 1000 / mean_ms,
                    # 并覆盖 qps 汇总与 CSV 列, 使口径与逐条实测完全一致。
                    if [ "$mean_time_ms" != "not_recorded" ] && awk -v m="$mean_time_ms" 'BEGIN{exit !(m>0)}'; then
                        qps_from_mean=$(awk -v c="$concurrency" -v m="$mean_time_ms" 'BEGIN{printf "%.3f", c*1000/m}')
                        qps_min="$qps_from_mean"; qps_max="$qps_from_mean"; qps_avg="$qps_from_mean"
                        qps_values=(); for ((_q=0;_q<REPEAT;_q++)); do qps_values+=("$qps_from_mean"); done
                        echo "  => QPS(mean)=$qps_from_mean (conc=${concurrency} × 1000 / mean=${mean_time_ms}ms)"
                    fi
                    csv_line="${ENGINE},${SERVER_VERSION},${table},${vec_col},${dim},${index_type},${index_ops},${DISTANCE_FUNC},${plan_mode},${result_mode},${sql_type},${row_count},${concurrency}"
                    for vi in "${qps_values[@]}"; do csv_line="${csv_line},${vi}"; done
                    csv_line="${csv_line},${qps_avg},${qps_min},${qps_max}"
                    csv_line="${csv_line},${tx_qps_avg},${elapsed_avg},${transactions_avg},${failed_avg},${latency_avg},${stmt_avg}"
                    csv_line="${csv_line},${mean_time_ms},${p95_time_ms},${p99_time_ms}"
                    echo "$csv_line" >> "$OUTPUT_CSV"
                done
            done
        done
    done
done

echo "=============================================="
echo "测试完成: $OUTPUT_CSV"
echo "=============================================="
tail -20 "$OUTPUT_CSV" | column -t -s',' 2>/dev/null || tail -20 "$OUTPUT_CSV"