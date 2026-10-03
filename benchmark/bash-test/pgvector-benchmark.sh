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
# 两个正交缓存维度 (2 × 2 组合):
#   1) 查询计划缓存 (plan_mode):
#        raw  = 原始 SQL 文本 (向量字面量内联, simple 协议 → 每次重新 parse+plan)
#        func = 调用 PL/pgSQL 静态 SQL 包装函数 (函数内静态 SQL 隐式计划缓存,
#               同一后端会话内复用 generic plan)
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
TIMELIMIT="${TIMELIMIT:-1}"
WARMUP_TIMELIMIT="${WARMUP_TIMELIMIT:-10}"
REPEAT="${REPEAT:-1}"
TOP_K="${TOP_K:-10}"
SQL_DIR="${SQL_DIR:-${_SCRIPT_DIR}/sql-bench/pgvector-benchmark}"
OUTPUT_CSV="${OUTPUT_CSV:-${_SCRIPT_DIR}/../results/pgvector-benchmark-results.csv}"
PSQL="${PSQL:-/usr/local/pgsql/bin/psql}"
PGBENCH="${PGBENCH:-/usr/local/pgsql/bin/pgbench}"
PGBENCH_JOBS="${PGBENCH_JOBS:-}"
PGBENCH_QUERY_MODE="${PGBENCH_QUERY_MODE:-simple}"
# 每个 pgbench 脚本内含的查询条数 (对齐 ClickHouse 的"一个脚本 N 条查询"口径):
#   0 = 使用该 row_count SQL 文件的全部查询; >0 = 每个脚本仅取前 QUERIES_PER_SCRIPT 条
QUERIES_PER_SCRIPT="${QUERIES_PER_SCRIPT:-0}"
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
#   plan_mode:   raw=原始SQL文本; func=PL/pgSQL函数(隐式计划缓存)
#   result_mode: on=SET result_cache_debug on; off=SET result_cache_debug off
# =====================================================================
CACHE_PROFILES=(
    "raw_off|raw|off"
    "raw_on|raw|on"
    # "func_off|func|off"
    # "func_on|func|on"
)
PLAN_MODES=(raw func)
RESULT_MODES=(off on)

SQL_TYPES=(
    text_literal
    text_cast
    vector_fn
    array_int_cast
    array_real_cast
    array_cast
    array_to_vector_fn
    halfvec_literal
    with_text_literal
    with_array_cast
    with_vector_fn
)
ROW_COUNTS=(1)
CONCURRENCIES=(1)

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

# ---- func 模式: 按 sql_type 生成 PL/pgSQL 包装函数 (计划缓存模式) ----
# 说明: "查询计划缓存收益"的本质是省掉每查询的 analyze+plan+常量折叠; 是否有效取决于
# 向量能否在"计划阶段"就完成构造、并在重复调用时被缓存复用, 与 sql_type 形态有关:
#
#   [func 有效(收益大)]  函数参数传"已解析的 vector"、函数体直接用 qv:
#                         text_literal / with_text_literal
#                         → vector_in 已在 EXECUTE 绑定期完成一次, generic plan 中不再
#                         反复 vector_in, 计划缓存收益最干净、最大。
#
#   [func 有效(收益中等)]函数体仍需在 exec 期把 数组/文本 转成 vector (无法常量折叠):
#                         array_int_cast / array_real_cast / array_cast /
#                         array_to_vector_fn / text_cast / vector_fn /
#                         halfvec_literal / with_array_cast / with_vector_fn
#                         → 计划缓存仍省掉 analyze+plan, 但每次 EXECUTE 都会重复做一次
#                         数组/文本→vector 转换, 所以收益被 exec 期转换成本部分抵消;
#                         转换成本越高(如 array_to_vector_fn) 收益相对越小。
#
#   [func 无效果的类型] 无 —— 所有类型都能被计划缓存覆盖, 差异在"收益大小"。
#   因此 func 对这些类型都"能用", 只是收益不同; 报告 QPS 时应按上述分组解读。
#
# 查询结果缓存 (pgvector.result_cache): 缓存"查询向量 → top-k 结果", 与 sql_type 形态
# 无关, 对所有类型一致有效; 其收益只取决于查询向量重复度(命中率), 与形态选择无关。
param_type_of() {  # sql_type -> pg 参数类型
    case "$1" in
        text_literal|with_text_literal) echo "vector" ;;
        array_int_cast) echo "int[]" ;;
        array_real_cast|array_cast|array_to_vector_fn|all) echo "real[]" ;;
        *) echo "text" ;;
    esac
}
conv_expr_of() {  # sql_type dim -> 函数体内向量表达式 (以参数 qv 为源)
    local st="$1" dim="$2"
    case "$st" in
        text_literal|with_text_literal) echo "qv" ;;
        array_int_cast|array_real_cast|with_array_cast) echo "qv::vector" ;;
        text_cast|array_cast) echo "CAST(qv AS vector)" ;;
        vector_fn|with_vector_fn|halfvec_fn) echo "vector(qv)" ;;
        halfvec_literal) echo "qv::halfvec(${dim})::vector" ;;
        array_to_vector_fn|array_to_vector_cast) echo "array_to_vector(qv, ${dim}, false)" ;;
        *) echo "qv" ;;
    esac
}
is_with_type() {  # 是否为 CTE(with) 形式
    case "$1" in with_text_literal|with_array_cast|with_vector_fn) return 0 ;; *) return 1 ;; esac
}
func_def_sql() {  # table col dim fn st -> 打印 CREATE FUNCTION (TOP_K 作为常量内联, 保证 HNSW 近似索引可被规划命中)
    local table="$1" col="$2" dim="$3" fn="$4" st="$5" pt conv body
    pt=$(param_type_of "$st"); conv=$(conv_expr_of "$st" "$dim")
    if is_with_type "$st"; then
        body="WITH kv AS (SELECT ${conv} AS v) SELECT t.id, (t.${col} ${DISTANCE_FUNC} kv.v) AS dis FROM ${table} t CROSS JOIN kv ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K}"
    else
        body="SELECT t.id, (t.${col} ${DISTANCE_FUNC} ${conv}) AS dis FROM ${table} t ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K}"
    fi
    echo "CREATE FUNCTION ${fn}${st}(qv ${pt}) RETURNS TABLE(id int, dis double precision) LANGUAGE plpgsql STABLE AS \$\$ BEGIN RETURN QUERY ${body}; END; \$\$;"
}
func_call_sql() {  # fn st vec dim -> 打印该 sql_type 的函数调用行 (参数类型按 param_type_of 决定)
    local fn="$1" st="$2" vec="$3" dim="$4" pt inner real_array
    pt=$(param_type_of "$st")
    inner="${vec//[()]/}"; inner="${inner//[\[\]]/}"
    real_array="ARRAY[${inner}]"
    case "$pt" in
        vector) echo "SELECT * FROM ${fn}${st}('[${inner}]'::vector);" ;;
        int[])  echo "SELECT * FROM ${fn}${st}(${real_array}::int[]);" ;;
        real[]) echo "SELECT * FROM ${fn}${st}(${real_array}::real[]);" ;;
        *)      echo "SELECT * FROM ${fn}${st}('[${inner}]'::text);" ;;
    esac
}

# ---- 缓存配置 ----
session_set_line_for_result() {  # off|on
    local b="$1"
    echo "SET ${RESULT_CACHE_GUC_MASTER} = ${b};"
    echo "SET ${RESULT_CACHE_GUC_DEBUG} = ${b};"
}
session_set_line_for_profile() {  # profile name
    local profile_name="$1"
    local group pname plan result
    for group in "${CACHE_PROFILES[@]}"; do
        IFS='|' read -r pname plan result <<< "$group"
        if [ "$pname" = "$profile_name" ]; then
            echo "SET ${RESULT_CACHE_GUC_MASTER} = ${result};"
            echo "SET ${RESULT_CACHE_GUC_DEBUG} = ${result};"
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
    local jobs="${PGBENCH_JOBS:-$concurrency}"
    if [ ! -s "$run_file" ]; then
        echo "错误: pgbench 脚本为空: $run_file" >&2; return 1
    fi
    PGPASSWORD="$PASSWORD" "$PGBENCH" \
        -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" \
        -f "$run_file" \
        -c "$concurrency" -j "$jobs" \
        -M "$PGBENCH_QUERY_MODE" \
        -T "$duration" -n -r > "$log_file" 2>&1
}

# QPS = tps × num_queries; 每条事务含 num_queries 条查询 (对齐 ClickHouse 的
# QPS = 脚本查询数量 / 执行耗时)。主循环传 num_queries = 每脚本查询条数 N。
parse_benchmark_metrics() {
    local output="$1" num_queries="${2:-1}"
    local duration transactions failed latency tps stmt_latency successful tx_qps query_qps
    duration=$(printf '%s\n' "$output" | awk '/^duration:/ {print $2; exit}')
    transactions=$(printf '%s\n' "$output" | awk -F': ' '/^number of transactions actually processed:/ {split($2,a," "); split(a[1],b,"/"); print b[1]; exit}')
    failed=$(printf '%s\n' "$output" | awk -F': ' '/^number of failed transactions:/ {split($2,a," "); print a[1]; exit}')
    latency=$(printf '%s\n' "$output" | awk -F'= ' '/^latency average =/ {print $2; exit}' | awk '{print $1}')
    tps=$(printf '%s\n' "$output" | awk -F'= ' '/^tps =/ {print $2; exit}' | awk '{print $1}')
    stmt_latency=$(printf '%s\n' "$output" | awk '/^statement latencies in milliseconds/ {in_section=1; next} in_section && $1 ~ /^[0-9]+([.][0-9]+)?$/ {sum+=$1; n++} END {if(n) printf "%.3f", sum/n; else print "0"}')
    duration=${duration:-0}; transactions=${transactions:-0}; failed=${failed:-0}
    successful=$((transactions - failed)); [ "$successful" -lt 0 ] && successful=0
    tx_qps=$(awk -v n="$successful" -v s="$duration" -v t="$tps" 'BEGIN{if(t>0) printf "%.3f", t; else if(s>0) printf "%.3f", n/s; else print "0.000"}')
    query_qps=$(awk -v t="$tx_qps" -v q="$num_queries" 'BEGIN{printf "%.3f", t*q}')
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$query_qps" "$tx_qps" "$duration" "$transactions" "$failed" "${latency:-0}" "$stmt_latency"
}

write_csv_header() {
    local header="engine,server_version,table_name,vector_column,dimension,index_type,index_ops,distance_func,plan_mode,result_mode,sql_type,row_count,concurrency"
    for i in $(seq 1 "$REPEAT"); do header="${header},run_${i}"; done
    header="${header},qps_avg,qps_min,qps_max"
    header="${header},tx_qps_avg,elapsed_avg_s,transactions_avg,failed_avg,latency_avg_ms,statement_avg_ms"
    mkdir -p "$(dirname "$OUTPUT_CSV")"
    if [ ! -f "$OUTPUT_CSV" ] || [ "$(head -1 "$OUTPUT_CSV" 2>/dev/null || true)" != "$header" ]; then
        if [ -f "$OUTPUT_CSV" ]; then
            local bak="${OUTPUT_CSV}.$(date '+%Y%m%d%H%M%S').bak"; cp "$OUTPUT_CSV" "$bak"
            echo "CSV 表头不匹配, 已备份旧文件到: $bak"
        fi
        echo "$header" > "$OUTPUT_CSV"
    fi
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

declare -A TABLE_VEC_COL TABLE_DIM TABLE_INDEX_INFO TABLE_FN

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
    # 方式三的 PL/pgSQL 包装函数名 (计划缓存=func 时使用)
    TABLE_FN["$table"]="pgv_search_$(echo "$table" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_' '_')"
    echo "表: $table  向量列: $vec_col  维度: $dim  索引: $idx_display"
done
echo ""

if [ "${DISTANCE_FUNC_AUTO}" = "true" ]; then
    for table in "$@"; do auto_select_distance_func "$table" "${TABLE_VEC_COL[$table]}"; done
    echo ""
fi

# ---- 为 func 模式创建 PL/pgSQL 静态 SQL 包装函数 ----
echo ">>> 创建 PL/pgSQL 包装函数 (func / 计划缓存模式, 按 sql_type 分别创建)"
{
    for table in "$@"; do
        col="${TABLE_VEC_COL[$table]}"; dim="${TABLE_DIM[$table]}"; fn="${TABLE_FN[$table]}"
        for st in "${SQL_TYPES[@]}"; do
            # 统一向量入参为 vector; 同时清理历史遗留的 text/int[]/real[] 签名 (幂等重建)
            for legacy in "text" "int[]" "real[]" "vector"; do
                echo "DROP FUNCTION IF EXISTS ${fn}${st}(${legacy});"
            done
            func_def_sql "$table" "$col" "$dim" "$fn" "$st"
        done
    done
} | PGPASSWORD="$PASSWORD" "$PSQL" -X -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" -v ON_ERROR_STOP=1 -q
echo "  已创建 ${#@} 个表 × ${#SQL_TYPES[@]} 个 sql_type 的函数"
echo ""

# ---- 抽样并生成各 sql_type 的原始 SQL 文件 (plan=raw) 与 函数调用文件 (plan=func) ----
#   RAW_BASE[table|row_count|sql_type] : 每行一条该 sql_type 的原始 SQL (向量字面量内联)
#   FUNC_BASE[table|row_count|sql_type]: 每行一条该 sql_type 的专用函数调用
mkdir -p "$SQL_DIR"
declare -A RAW_BASE FUNC_BASE

for table in "$@"; do
    col="${TABLE_VEC_COL[$table]}"; dim="${TABLE_DIM[$table]}"
    fn="${TABLE_FN[$table]}"
    for row_count in "${ROW_COUNTS[@]}"; do
        for st in "${SQL_TYPES[@]}"; do
            local_func_path="${SQL_DIR}/pgv_func_${table}_${st}_${row_count}.sql"
            : > "$local_func_path"
            FUNC_BASE["${table}|${row_count}|${st}"]="$local_func_path"
            local_raw_path="${SQL_DIR}/pgv_raw_${table}_${st}_${row_count}.sql"
            : > "$local_raw_path"
            RAW_BASE["${table}|${row_count}|${st}"]="$local_raw_path"
        done

        echo "抽样 ${row_count} 条向量: $table (col=$col dim=$dim)"
        sample_vectors "$table" "$col" "$row_count" > "$TMPVECTORS" 2>/dev/null || {
            echo "  抽样失败: $table"; rm -f "$TMPVECTORS"; exit 1; }
        if [ ! -s "$TMPVECTORS" ]; then
            echo "  错误: 未采样到向量 (表为空或权限不足)"; exit 1
        fi
        while IFS='|' read -r id vec; do
            [ -z "$id" ] && continue; [ -z "$vec" ] && continue
            for st in "${SQL_TYPES[@]}"; do
                echo "$(render_raw_query "$st" "$table" "$col" "$vec" "$dim")" >> "${RAW_BASE["${table}|${row_count}|${st}"]}"
                echo "$(func_call_sql "$fn" "$st" "$vec" "$dim")" >> "${FUNC_BASE["${table}|${row_count}|${st}"]}"
            done
        done < "$TMPVECTORS"
        rm -f "$TMPVECTORS"
        local_fp="${SQL_DIR}/pgv_func_${table}_${SQL_TYPES[0]}_${row_count}.sql"
        local_rp="${SQL_DIR}/pgv_raw_${table}_${SQL_TYPES[0]}_${row_count}.sql"
        echo "  已生成: func=$(wc -l < "$local_fp") 行, 各 sql_type raw=$(wc -l < "$local_rp") 行"
    done
done
echo ""

write_csv_header

# 预热
warmup_sql=""
for table in "$@"; do
    warmup_sql="${RAW_BASE["${table}|${ROW_COUNTS[0]}|${SQL_TYPES[0]}"]:-}"
    [ -s "$warmup_sql" ] && break
done
if [ -s "$warmup_sql" ] && [ "$WARMUP_TIMELIMIT" != "0" ]; then
    wset=$(session_set_line_for_result "${CACHE_PROFILES[0]##*|}")
    wrun="$TMPDIR/warmup.sql"
    make_script_with_settings "$warmup_sql" "$wset" "$wrun" "$QUERIES_PER_SCRIPT" >/dev/null
    run_benchmark_file "$wrun" 1 "$TMPDIR/warmup.log" "${WARMUP_TIMELIMIT}" >/dev/null 2>&1
    echo ">>> 预热完成"
fi

echo ">>> 开始测试"
for table in "$@"; do
    vec_col="${TABLE_VEC_COL[$table]}"
    dim="${TABLE_DIM[$table]}"
    fn="${TABLE_FN[$table]}"
    index_type="none" index_ops="none"
    idx_info="${TABLE_INDEX_INFO[$table]:-}"
    if [ -n "$idx_info" ]; then
        IFS='|' read -r index_type index_ops <<< "$idx_info"
    fi

    for group in "${CACHE_PROFILES[@]}"; do
        IFS='|' read -r cache_profile plan_mode result_mode <<< "$group"
        set_line="$(session_set_line_for_result "$result_mode")"
        # PLCACHE TRACE 开关: func 模式可在日志中看到 [PLAN_CACHE_TRACE]
        echo "--- profile: $cache_profile (plan=$plan_mode result_cache=$result_mode) ---"

        for sql_type in "${SQL_TYPES[@]}"; do
            for row_count in "${ROW_COUNTS[@]}"; do
                if [ "$plan_mode" = "func" ]; then
                    base="${FUNC_BASE["${table}|${row_count}|${sql_type}"]:-}"
                else
                    base="${RAW_BASE["${table}|${row_count}|${sql_type}"]:-}"
                fi
                [ -s "$base" ] || { echo "跳过空文件: $base"; continue; }

                # 一个脚本含 N 条查询 (对齐 ClickHouse 口径): 把 base 的所有查询写入
                # 单个 pgbench 脚本, N = 查询条数 (受 QUERIES_PER_SCRIPT 截断)。
                runner_file="$TMPDIR/${ENGINE}_${table}_${cache_profile}_${sql_type}_${row_count}.sql"
                nqueries=$(make_script_with_settings "$base" "$set_line" "$runner_file" "$QUERIES_PER_SCRIPT")
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
                        IFS=$'\t' read -r qps tx_qps elapsed transactions failed latency stmt <<< "$(parse_benchmark_metrics "$(cat "$log")" "$nqueries")"
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

                    csv_line="${ENGINE},${SERVER_VERSION},${table},${vec_col},${dim},${index_type},${index_ops},${DISTANCE_FUNC},${plan_mode},${result_mode},${sql_type},${row_count},${concurrency}"
                    for vi in "${qps_values[@]}"; do csv_line="${csv_line},${vi}"; done
                    csv_line="${csv_line},${qps_avg},${qps_min},${qps_max}"
                    csv_line="${csv_line},${tx_qps_avg},${elapsed_avg},${transactions_avg},${failed_avg},${latency_avg},${stmt_avg}"
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