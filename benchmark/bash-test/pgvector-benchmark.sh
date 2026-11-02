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
# CSV 时间列 (第 1 列 timestamp):
#   格式 `YYYY-MM-DD HH:MM:SS` (本地时区), 与 Python 客户端
#   (engine/base_client/client.py) 写的 timestamp 列同名同格式, 两类结果可混排比较。
#   记录的是【该行所有测量结束、写行的时刻】——即 REPEAT 次吞吐 + 延迟探针都跑完之后,
#   不是该行开始测量的时刻; 起始时刻可由 timestamp 减去 (REPEAT × elapsed_avg_s) 倒推。
#   与服务端日志对齐时按 [timestamp - 该行耗时, timestamp] 取窗口。
#   历史行没有这一列, 迁移时补 not_recorded (见 write_csv_header)。
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
#   TOP_K/EF_S/TIMELIMIT/WARMUP_TIMELIMIT/REPEAT/PGBENCH_QUERY_MODE(PGBENCH_JOBS)
#   固定行为: 每张表在当前调用里的第一个组合, 在 REPEAT 次正式记录之前先跑一次
#             同参数 (同 profile/SQL 文件/并发/时长) 的 pgbench 并丢弃, 用于消除
#             首跑冷启动 (索引页/连接与 prepared 状态); 该次不写 CSV、不计入均值,
#             成本约每表 +1 次 TIMELIMIT。
#   ROW_COUNTS_OVERRIDE/CONCURRENCIES_OVERRIDE/SQL_TYPES_OVERRIDE/
#   CACHE_PROFILES_OVERRIDE/DISTANCE_FUNC/DISTANCE_FUNC_AUTO
#   QUERIES_PER_SCRIPT (每脚本查询条数; 0=用满 row_count)
#   LATENCY_PROBE_STATEMENTS (单语句延迟探针的样本总数, 跨并发客户端均分; 0=关闭)
#   EF_S               HNSW 检索候选集大小 hnsw.ef_search (默认 100, 即 PG 默认值)
#                      召回率旋钮: 越大召回越高、QPS 越低。pgvector 的 ef_search
#                      是会话级 GUC, 不能像 MyScale 的 distance('ef_s=…') 那样写进
#                      SQL 文本, 因此脚本会在每个 pgbench 脚本头部追加
#                      `SET hnsw.ef_search = <EF_S>;`(延迟探针脚本继承同一前缀)。
#   RESULT_CACHE_GUC_MASTER (默认 pgvector.result_cache)
#   RESULT_CACHE_GUC_DEBUG  (默认 pgvector.result_cache_debug)
#   OUTPUT_CSV/SQL_DIR
#   SQL_REGEN          SQL 文件重建策略 (默认 1):
#                      1 = 每次运行都重新抽样生成 (默认; SQL 生成已足够快, 不复用);
#                      0 = 复用已有文件 (缺文件时仍会重新生成), 仅用于临时跳过抽样;
#                      auto = n=1 每次重新生成 / n>1 复用。
#                      复用前校验: 各 sql_type 的 raw/prep 文件存在非空, 且 raw
#                      文件行数与 PREP_EXEC_TOTAL 一致 (防止复用到半截文件)。
#   SQL_SET            选择第几组单条 SQL (仅 n=1 生效; 也可用 --sql-set 传参):
#                      文件名 pgv_{raw,prep}_<表>_<type>_1_s<组号>.sql。
#                      配合 run-benchmark-loop.sh: 第 i 轮传 SQL_SET=$i, 共 RUNS 组
#                      不同向量; 默认 SQL_REGEN=1 下每轮仍会重新抽样生成。

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
WARMUP_TIMELIMIT="${WARMUP_TIMELIMIT:-80}"
REPEAT="${REPEAT:-10}"
TOP_K="${TOP_K:-10}"
# HNSW 查询候选集大小 (对应 config.json 里 search_params.params.ef_s)。
# 默认 100 与 pgvector 的 hnsw.ef_search 默认值一致, 因此不设该变量时行为不变。
EF_S="${EF_S:-100}"
if ! [[ "$EF_S" =~ ^[1-9][0-9]*$ ]]; then
    echo "错误: EF_S 必须是正整数 (当前: $EF_S)" >&2; exit 1
fi
SQL_DIR="${SQL_DIR:-${_SCRIPT_DIR}/sql-bench/pgvector-query-forms}"
# SQL 文件重建策略: 默认 1 = 每次都重新生成 (生成已足够快, 不复用); 0 = 全部复用
SQL_REGEN="${SQL_REGEN:-1}"
# 选择第几组单条 SQL (仅 n=1 生效, 也可用 --sql-set 传参): 文件名
# pgv_{raw,prep}_<表>_<type>_1_s<组号>.sql。
# run-benchmark-loop.sh 第 i 轮传 SQL_SET=$i, 共 RUNS 组不同向量;
# 默认 SQL_REGEN=1 下每轮重新抽样生成, 不再复用旧文件。
SQL_SET="${SQL_SET:-}"
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
# prep sql 文件内 EXECUTE 语句总条数 (默认 1000 条)。抽样的 ROW_COUNTS 条向量按序循环填充。
# 取 1000 是为了与延迟探针样本数 (LATENCY_PROBE_STATEMENTS) 同量级: 每个组合的主跑
# 由 -T TIMELIMIT 内反复执行该脚本构成, 单个事务很短, 整轮基准的耗时显著下降。
PREP_EXEC_TOTAL="${PREP_EXEC_TOTAL:-1000}"
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
    array_int_cast
    array_real_cast
    with_array_cast
)
# ROW_COUNTS 为"每组随机抽样的向量条数"(10 个一组); prep 文件按序循环这 ROW_COUNTS
# 条向量生成 EXECUTE 语句, 直到写满 PREP_EXEC_TOTAL 条 (默认 1000 条)。
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
        # 距离函数必须按表各自记录 (多表 opclass 可能不同), 不能只覆盖全局, 否则
        # 后面生成的脚本会对所有表使用"最后一张表"的算子, 与自身索引不匹配而退化为
        # 全表顺序扫描, 导致单表正常、多表时极慢。
        TABLE_DISTANCE_FUNC["$table"]="$suggested"
        echo "  ✓ 根据索引自动设置距离函数: $suggested (index: ${am}(${ops}))"
    elif [ "$DISTANCE_FUNC" != "$suggested" ]; then
        TABLE_DISTANCE_FUNC["$table"]="$suggested"
        echo "  ⚠ 距离函数 '$DISTANCE_FUNC' 与索引 '$ops' 不匹配! 建议 $suggested (设置 DISTANCE_FUNC_AUTO=true)"
    fi
}

# ---- 命令行参数解析 (数据表序号 / 随机抽样条数的数值输入) ----
usage() {
    local self; self=$(basename "$0")
    cat >&2 <<EOF
用法: $self [选项] [表名 ...]

选项:
  -t, --tables <序号序列>     数据表序号, 空格/逗号分隔 (如 "1 3 5" 或 "1,3,5")
  -n, --row-counts <数值序列> 随机抽样向量条数, 空格/逗号分隔 (如 "10 100")
      --sql-set <组号>        选择第几组单条 SQL (仅 n=1 生效): 组文件存在即复用,
                              缺失则重新生成; run-benchmark-loop.sh 每轮传轮次号
  -l, --list                 列出库中含 vector 列的表及序号后退出
  -h, --help                 显示本帮助
  位置参数                    表名 (与 -t 二选一, 可混用)

示例:
  $self -l
  $self -t "1 2" -n "10 100"
  $self -n "10 100" benchmark_1536_1m benchmark_768_1m
EOF
    exit "${1:-0}"
}

# 列出含 vector 列的表/字段/维度并带行号 (供 -l 与 -t 序号使用)
list_tables_numbered() {
    local tables_info i=0 tname tcol dim
    tables_info=$(list_vector_tables)
    if [ -z "$tables_info" ]; then
        echo "错误: 未找到任何含 vector 列的表" >&2; return 1
    fi
    while IFS='|' read -r tname tcol; do
        [ -z "$tname" ] && continue
        i=$((i+1))
        dim=$(detect_vector_dimension "$tname" "$tcol")
        printf '%3d. %s (col=%s dim=%s)\n' "$i" "$tname" "$tcol" "${dim:-?}"
    done <<< "$tables_info"
}

# 按序号序列解析出 "table|col" 行; 序号不合法/越界时提示并忽略, 无有效表时输出为空
resolve_table_numbers() {
    local tables_info="$1" nums="$2" out="" n line total
    total=$(printf '%s\n' "$tables_info" | grep -c . || true)
    [ "$total" -eq 0 ] && { echo "错误: 未找到任何含 vector 列的表" >&2; return 1; }
    for n in $(echo "$nums" | tr ',' ' '); do
        case "$n" in
            ''|*[!0-9]*) echo "  ⚠ 忽略非法序号: '$n'" >&2 ;;
            *)
                if [ "$n" -ge 1 ] && [ "$n" -le "$total" ] 2>/dev/null; then
                    line=$(printf '%s\n' "$tables_info" | awk -v n="$n" 'NR==n')
                    [ -n "$line" ] && out="${out:+$out$'\n'}$line"
                else
                    echo "  ⚠ 序号 $n 超出范围 (1-$total), 已忽略" >&2
                fi ;;
        esac
    done
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

# ---- 会话级 SET 行 ----
# HNSW 检索候选数。所有生成 pgbench/探针脚本的地方都要带上这一行, 否则
# 该组合会悄悄跑在服务端默认 ef_search 上, QPS 与召回都无法与指定档位对齐。
session_set_line_for_ef() {
    echo "SET hnsw.ef_search = ${EF_S};"
}

session_set_line_for_result() {  # off|on
    local b="$1"
    session_set_line_for_ef
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
            session_set_line_for_ef
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
    # TABLESAMPLE SYSTEM 按"页"随机采样(页间随机, 页内全取), 只读约 pct% 的页,
    # 避免全表扫描 + 逐行 TOAST 解引用 (1M×1536 维表上 ORDER BY random() 全扫要 30-90s)。
    # 采样比例 = count/est_rows ×3 冗余, 保证 LIMIT count 大概率取满; 取不满也安全:
    # 生成侧对向量序列做回卷循环 (i>=nvecs 时从头复用)。比例下限 0.05% (页采样
    # 随机性下更小的比例可能一页都采不到, 实测 0.01% 在 1M 行表返回 0 行)。
    local pct; pct=$(awk "BEGIN{p=${count}*300.0/${est_rows}; if(p<0.05)p=0.05; if(p>100)p=100; printf \"%.4f\", p}")
    client_query_long "SELECT id, ${vec_col}::text FROM ${table} TABLESAMPLE SYSTEM (${pct}) ORDER BY random() LIMIT ${count}"
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

# QPS 由 pgbench 日志推导: QPS = 事务数/秒(tx/s) × 每脚本查询条数 N, 其中 tx/s 优先
# 取 latency average (纯事务执行耗时) 反推: tx/s = 并发 × 1000 / latency_ms,
# 兜底用 pgbench 上报的 tps 或 事务数/整秒 duration。探针(mean/p95/p99)只测延迟、
# 不计入 QPS。主循环传 num_queries = 每脚本查询条数 N, concurrency = 并发客户端数。
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
        # 主路径: tx/s = 并发 × 1000 / latency_avg_ms (pgbench 纯事务执行耗时)
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

# CSV 的 timestamp 列取值: 与 Python 客户端 (engine/base_client/client.py 的
# '%Y-%m-%d %H:%M:%S') 一致, 且字典序 == 时间序, 分析脚本可直接 max() 取最新一行。
csv_timestamp() { date '+%Y-%m-%d %H:%M:%S'; }

# 按【列名】把旧 CSV 的数据行搬到新表头下: 新表头有、旧表头没有的列写 not_recorded。
# 之所以按列名而不是按位置: 表头已经变过三代 (timestamp / top_k+ef_s / mean+p95+p99),
# 位置式插入要为每种组合写一条 awk, 且插入位置一变就全错。
# 字段值不含逗号, 所以直接按 ',' 切分即可。
#   $1 = 新表头  $2 = 旧文件  $3 = 输出文件
# 返回非 0 表示无法安全映射(旧表头含新表头没有的列, 或有空/重复列名), 调用方改为备份重建,
# 避免静默丢列。
remap_csv_by_header() {
    local new_header="$1" src="$2" dst="$3"
    printf '%s\n' "$new_header" > "$dst" || return 1
    awk -v newh="$new_header" -v miss="not_recorded" '
        BEGIN { FS = OFS = ","; n = split(newh, col, ","); for (i = 1; i <= n; i++) isnew[col[i]] = 1 }
        NR == 1 {
            for (i = 1; i <= NF; i++) {
                if (!isnew[$i] || $i == "" || seen[$i]++) { exit 3 }
                idx[$i] = i
            }
            next
        }
        {
            line = ""
            for (i = 1; i <= n; i++) {
                f = idx[col[i]]
                # 该列旧表头没有, 或旧行本身残缺(字段数少于表头)→ 一律记 not_recorded,
                # 不留空单元格: 本 CSV 没有"合法的空值", 空 = 当时没记录。
                v = (f && $f != "") ? $f : miss
                line = (i == 1 ? v : line OFS v)
            }
            print line
        }' "$src" >> "$dst"
}

write_csv_header() {
    # 表头按"段"拼装: 早期版本没有 time_cols (timestamp)、query_cols (top_k/ef_s)
    # 与 probe_cols (mean/p95/p99)。历史文件按列名原地迁移(见 remap_csv_by_header),
    # 所以这里只需描述"当前"表头, 不必再枚举旧表头。
    local time_cols="timestamp"                     # 本行测量结束(写行)的时刻
    local desc_cols="engine,server_version,table_name,vector_column,dimension,index_type,index_ops,distance_func"
    local query_cols="top_k,ef_s"                   # 查询形态参数 (召回旋钮)
    local profile_cols="plan_mode,result_mode,sql_type,row_count,concurrency"
    local probe_cols="mean_time_ms,p95_time_ms,p99_time_ms"
    # run_i 列数随 REPEAT 变化; 指标列各代相同。
    local run_cols="" i
    for i in $(seq 1 "$REPEAT"); do run_cols="${run_cols},run_${i}"; done
    local stat_cols="qps_avg,qps_min,qps_max,tx_qps_avg,elapsed_avg_s,transactions_avg,failed_avg,latency_avg_ms,statement_avg_ms"
    local header="${time_cols},${desc_cols},${query_cols},${profile_cols}${run_cols},${stat_cols},${probe_cols}"
    mkdir -p "$(dirname "$OUTPUT_CSV")"
    local existing_header=""
    if [ -f "$OUTPUT_CSV" ]; then
        existing_header=$(head -1 "$OUTPUT_CSV" 2>/dev/null || true)
    fi
    if [ "$existing_header" = "$header" ]; then
        return
    fi
    local tmp_csv="$TMPDIR/csv_migrate.csv"
    if [ -n "$existing_header" ] && remap_csv_by_header "$header" "$OUTPUT_CSV" "$tmp_csv"; then
        # 报告补了哪些列, 免得历史行里的 not_recorded 被当成漏数
        local backfill="" c
        for c in ${header//,/ }; do
            case ",${existing_header}," in
                *",${c},"*) ;;
                *) backfill="${backfill:+${backfill} }${c}" ;;
            esac
        done
        echo "迁移旧数据: 旧表头缺 [${backfill:-无}] 等列, 历史行补 not_recorded..."
        mv "$tmp_csv" "$OUTPUT_CSV"
        echo "迁移完成"
        return
    fi
    rm -f "$tmp_csv"
    if [ -s "$OUTPUT_CSV" ]; then
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

# ---- 命令行参数解析 (数据表序号 / 随机抽样条数的数值输入) ----
TABLE_NUMS="" ROW_COUNTS_CLI="" DO_LIST=0
POS_TABLES=()
while [ "$#" -gt 0 ]; do
    case "$1" in
        -t|--tables)
            if [ -z "${2:-}" ]; then echo "错误: -t 缺少序号序列" >&2; usage 1; fi
            TABLE_NUMS="$2"; shift 2 ;;
        -n|--row-counts)
            if [ -z "${2:-}" ]; then echo "错误: -n 缺少数值序列" >&2; usage 1; fi
            ROW_COUNTS_CLI="$2"; shift 2 ;;
        --sql-set)
            if [ -z "${2:-}" ]; then echo "错误: --sql-set 缺少组号" >&2; usage 1; fi
            SQL_SET="$2"; shift 2 ;;
        -l|--list)   DO_LIST=1; shift ;;
        -h|--help)   usage 0 ;;
        --)          shift; POS_TABLES+=("$@"); break ;;
        -*)          echo "未知选项: $1" >&2; usage 1 ;;
        *)           POS_TABLES+=("$1"); shift ;;
    esac
done

if [ "$DO_LIST" -eq 1 ]; then
    echo "库中含 vector 列的表:" >&2
    list_tables_numbered || exit 1
    exit 0
fi

# 随机抽样条数数值输入 (-n): 未指定则沿用 ROW_COUNTS_OVERRIDE/env 值
if [ -n "$ROW_COUNTS_CLI" ]; then
    ROW_COUNTS=()
    for n in $(echo "$ROW_COUNTS_CLI" | tr ',' ' '); do
        case "$n" in
            ''|*[!0-9]*) echo "  ⚠ 忽略非法抽样数量: '$n'" >&2 ;;
            *) if [ "$n" -gt 0 ] 2>/dev/null; then ROW_COUNTS+=("$n"); else
                   echo "  ⚠ 忽略非正整数抽样数量: '$n'" >&2
               fi ;;
        esac
    done
    [ "${#ROW_COUNTS[@]}" -eq 0 ] && { echo "错误: -n 未提供有效抽样条数" >&2; usage 1; }
fi

# 数据表序号数值输入 (-t): 解析为表名, 与位置参数表名可混用
if [ -n "$TABLE_NUMS" ]; then
    tables_info=$(list_vector_tables)
    if [ -z "$tables_info" ]; then echo "错误: 未找到任何含 vector 列的表" >&2; exit 1; fi
    selected_output=$(resolve_table_numbers "$tables_info" "$TABLE_NUMS")
    if [ -z "$selected_output" ]; then echo "错误: -t 未解析出任何有效表" >&2; exit 1; fi
    POS_TABLES=()
    while IFS='|' read -r tname tcol; do
        [ -z "$tname" ] && continue
        POS_TABLES+=("$tname")
    done <<< "$selected_output"
fi

if [ "${#POS_TABLES[@]}" -eq 0 ]; then
    echo "错误: 未指定任何表 (用 -t <序号> 或 <表名>, -l 查看序号, -h 帮助)" >&2
    usage 1
fi

set -- "${POS_TABLES[@]}"

echo "=============================================="
echo " PGVector benchmark (pgbench)"
echo "=============================================="
echo "engine:      $ENGINE"
echo "target:      $HOST:$PORT / $DATABASE  (version $SERVER_VERSION)"
echo "distance:    $DISTANCE_FUNC   sort: $SORT_DIR"
echo "top_k:       $TOP_K   hnsw.ef_search: $EF_S"
echo "pgbench协议: -M $PGBENCH_QUERY_MODE"
echo "sql types:   ${SQL_TYPES[*]}"
echo "row counts:  ${ROW_COUNTS[*]}"
echo "concurrency: ${CONCURRENCIES[*]}"
echo "cache_profiles: ${CACHE_PROFILES[*]}"
echo "output csv:  $OUTPUT_CSV"
echo "=============================================="
echo ""

declare -A TABLE_VEC_COL TABLE_DIM TABLE_INDEX_INFO TABLE_DISTANCE_FUNC

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
    # 生成前按表切换到该表自己的距离函数, 否则 render_* 会对所有表使用全局(最后一张表)的算子
    DISTANCE_FUNC="${TABLE_DISTANCE_FUNC["$table"]:-$DISTANCE_FUNC}"
    for row_count in "${ROW_COUNTS[@]}"; do
        # 组号后缀: n=1 且指定 SQL_SET (选择第几组单条 SQL) 时, 文件名带 _s<组号>;
        # run-benchmark-loop.sh 第 i 轮传 SQL_SET=$i, 第 i 轮固定用第 i 组不同向量。
        suffix=""
        if [ "$row_count" = "1" ] && [ -n "$SQL_SET" ]; then
            suffix="_s${SQL_SET}"
        fi
        # 复用判断 (默认 SQL_REGEN=1, 每次运行都重新抽样生成, 不复用):
        #   指定 SQL_SET (n=1): 仅 SQL_REGEN!=1 时复用所选组文件, 否则重新生成;
        #   未指定时按 SQL_REGEN: 1 = 全部重新生成 / 0 = 全部复用 / auto = n=1 重建、
        #   n>1 复用。
        # 复用前校验: 各 sql_type 的 raw/prep 文件都存在且非空, 且 raw 文件行数
        # 与 PREP_EXEC_TOTAL 一致 (写满才算完整, 防止复用到上次中断的半截文件)。
        reuse=0
        if [ -n "$suffix" ] && [ "$SQL_REGEN" != "1" ]; then
            reuse=1
        elif [ "$SQL_REGEN" = "0" ] || { [ "$SQL_REGEN" != "1" ] && [ "$row_count" -gt 1 ]; }; then
            reuse=1
        fi
        if [ "$reuse" = "1" ]; then
            for st in "${SQL_TYPES[@]}"; do
                { [ -s "${SQL_DIR}/pgv_raw_${table}_${st}_${row_count}${suffix}.sql" ] && \
                  [ -s "${SQL_DIR}/pgv_prep_${table}_${st}_${row_count}${suffix}.sql" ]; } || { reuse=0; break; }
            done
            if [ "$reuse" = "1" ]; then
                rep="${SQL_DIR}/pgv_raw_${table}_${SQL_TYPES[0]}_${row_count}${suffix}.sql"
                [ "$(wc -l < "$rep")" -eq "$PREP_EXEC_TOTAL" ] || reuse=0
            fi
        fi
        if [ "$reuse" = "1" ]; then
            for st in "${SQL_TYPES[@]}"; do
                # prep 文件头部的 DEALLOCATE 是幂等的 (DO 块捕获异常), 每个事务重跑
                # 也不会报 "already exists", 因此旧 prep 文件可直接复用, 无需重建。
                RAW_BASE["${table}|${row_count}|${st}"]="${SQL_DIR}/pgv_raw_${table}_${st}_${row_count}${suffix}.sql"
                PREP_BASE["${table}|${row_count}|${st}"]="${SQL_DIR}/pgv_prep_${table}_${st}_${row_count}${suffix}.sql"
            done
            echo "复用已有 SQL 文件: $table n=$row_count${suffix} (SQL_REGEN=$SQL_REGEN, 跳过抽样)"
            continue
        fi
        for st in "${SQL_TYPES[@]}"; do
            local_raw_path="${SQL_DIR}/pgv_raw_${table}_${st}_${row_count}${suffix}.sql"
            : > "$local_raw_path"
            RAW_BASE["${table}|${row_count}|${st}"]="$local_raw_path"
            # PREPARE 形式 (prepare_auto/custom/generic): 头部先通过 DO 块幂等 DEALLOCATE (PostgreSQL
            # 无 DROP PREPARE/`IF EXISTS`, 用 invalid_sql_statement_name 异常捕获; pgbench
            # 每轮事务会重跑脚本, 需幂等避免 "already exists"), 随后 PREPARE + 逐行 EXECUTE。
            local_prep_path="${SQL_DIR}/pgv_prep_${table}_${st}_${row_count}${suffix}.sql"
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
        # 校验抽样结果 (awk 生成阶段直接读 TMPVECTORS, 不再展开进 bash 数组)
        nvecs=$(grep -c . "$TMPVECTORS")
        [ "$nvecs" -eq 0 ] && { echo "  错误: 抽样无有效向量"; rm -f "$TMPVECTORS"; exit 1; }

        for st in "${SQL_TYPES[@]}"; do
            # 各 sql_type 的向量表达式模板: __V__=PG 原样向量 (含括号), __I__=去括号 inner。
            # 与旧 bash 逐行 render_raw_query/render_prepare_body 生成的文本完全一致,
            # 只是改为 awk 批量渲染 (旧版 22 万行逐行 echo+替换要 2-3 分钟, awk <2s)。
            expr="" is_with=0
            case "$st" in
                text_literal)       expr="'__V__'::vector" ;;
                text_cast)          expr="CAST('__V__' AS vector)" ;;
                vector_fn)          expr="vector('__V__')" ;;
                array_int_cast)     expr="ARRAY[__I__]::vector" ;;
                array_real_cast)    expr="ARRAY[__I__]::real[]::vector" ;;
                array_cast)         expr="CAST(ARRAY[__I__]::real[] AS vector)" ;;
                array_to_vector_fn) expr="array_to_vector(ARRAY[__I__]::real[], ${dim}, false)" ;;
                halfvec_literal)    expr="'__V__'::halfvec(${dim})::vector" ;;
                with_text_literal)  expr="'__V__'::vector"; is_with=1 ;;
                with_array_cast)    expr="ARRAY[__I__]::real[]::vector"; is_with=1 ;;
                with_vector_fn)     expr="vector('__V__')"; is_with=1 ;;
                *)                  expr="'__V__'::vector" ;;
            esac
            if [ "$is_with" = 1 ]; then
                raw_tpl="WITH qv AS (SELECT ${expr} AS v) SELECT id, (${col} ${DISTANCE_FUNC} qv.v) AS dis FROM ${table} CROSS JOIN qv ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};"
            else
                raw_tpl="SELECT id, (${col} ${DISTANCE_FUNC} ${expr}) AS dis FROM ${table} ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};"
            fi
            prep_tpl="EXECUTE ${PREP_NAME["${table}|${row_count}|${st}"]}('[__I__]');"
            # raw: 抽样的 row_count 条向量按序回卷循环写满 PREP_EXEC_TOTAL 条;
            # prep: 同序 EXECUTE 行 (头部 DEALLOCATE/PREPARE 已在上面写入)。
            awk -v total="$PREP_EXEC_TOTAL" \
                -v raw_tpl="$raw_tpl" -v prep_tpl="$prep_tpl" \
                -v raw_out="${RAW_BASE["${table}|${row_count}|${st}"]}" \
                -v prep_out="${PREP_BASE["${table}|${row_count}|${st}"]}" '
                { vec[NR] = $0 }
                END {
                    m = 0
                    for (j = 1; j <= NR; j++) {
                        v0 = vec[j]
                        if (v0 !~ /\|/) continue
                        sub(/^[^|]*\|/, "", v0)
                        if (v0 == "") continue
                        vv[++m] = v0
                    }
                    if (m == 0) exit 1
                    for (k = 0; k < total; k++) {
                        v = vv[(k % m) + 1]
                        inner = v; gsub(/[()\[\]]/, "", inner)
                        r = raw_tpl; gsub(/__V__/, v, r); gsub(/__I__/, inner, r)
                        print r >> raw_out
                        p = prep_tpl; gsub(/__I__/, inner, p)
                        print p >> prep_out
                    }
                }' "$TMPVECTORS" || { echo "  错误: awk 生成 SQL 失败 ($st)"; rm -f "$TMPVECTORS"; exit 1; }
        done
        rm -f "$TMPVECTORS"
        local_rp="${SQL_DIR}/pgv_raw_${table}_${SQL_TYPES[0]}_${row_count}${suffix}.sql"
        local_pp="${SQL_DIR}/pgv_prep_${table}_${SQL_TYPES[0]}_${row_count}${suffix}.sql"
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

# 首个组合标记: 每张表在当前调用里的第一个被测组合正式记录前先跑一次同参数测量
# 并丢弃 (不写 CSV、不计入均值), 用于消除首跑冷启动。
discarded_tables="|"

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
    echo ">>> 开始测试"
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
                runner_file="$TMPDIR/${ENGINE}_${table}_${cache_profile}_${sql_type}_${row_count}${suffix}.sql"
                nqueries=$(make_script_with_settings "$base" "$set_line" "$runner_file" "$QUERIES_PER_SCRIPT")
                if [[ "$plan_mode" == prepare_* ]]; then
                    # QPS 基于实际 EXECUTE (查询) 行数; DO/PREPARE 声明行不计入查询数
                    nqueries=$(grep -c '^EXECUTE ' "$base" || true)
                fi
                if [ "${nqueries:-0}" -eq 0 ]; then
                    echo "跳过空脚本: $base"; continue
                fi

                # 该表在当前调用里的第一个组合: 每个并发在正式记录前各做一次丢弃预热
                discard_warmup=0
                case "$discarded_tables" in
                    *"|${table}|"*) ;;
                    *) discard_warmup=1; discarded_tables="${discarded_tables}${table}|" ;;
                esac

                for concurrency in "${CONCURRENCIES[@]}"; do
                    echo "━━ table=$table profile=$cache_profile type=$sql_type rows=$row_count conc=$concurrency queries/script=$nqueries ━━"

                    # 首个组合的丢弃预热: 脚本/并发/时长与正式记录完全一致, 结果不解析。
                    # 失败不影响后续记录 (set -e 临时关闭)。
                    if [ "$discard_warmup" -eq 1 ]; then
                        echo -n "  预热首跑(同参数, 不计入统计): "
                        discard_log="$TMPDIR/${ENGINE}_${table}_${cache_profile}_${sql_type}_${row_count}_${concurrency}_discard.log"
                        set +e
                        run_benchmark_file "$runner_file" "$concurrency" "$discard_log" >/dev/null 2>&1
                        discard_exit=$?
                        set -e
                        if [ "$discard_exit" -ne 0 ]; then
                            echo "失败(exit=$discard_exit), 继续正式记录"
                        else
                            echo "完成"
                        fi
                    fi
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

                    # 列序必须与 write_csv_header 完全一致 (timestamp 在最前,
                    # top_k/ef_s 紧随 distance_func); timestamp = 写行时刻 = 本行测量结束时刻
                    csv_line="$(csv_timestamp),${ENGINE},${SERVER_VERSION},${table},${vec_col},${dim},${index_type},${index_ops},${DISTANCE_FUNC},${TOP_K},${EF_S},${plan_mode},${result_mode},${sql_type},${row_count},${concurrency}"
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