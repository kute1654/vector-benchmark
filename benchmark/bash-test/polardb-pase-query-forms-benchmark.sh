#!/bin/bash
#
# Benchmark all supported vector query literal forms for PolarDB-for-PostgreSQL
# pase extension against the database currently listening on HOST:PORT.
#
# Examples:
#   ENGINE=polardb ./polardb-pase-query-forms-benchmark.sh Benchmark_768_1m
#
# Useful overrides:
#   PSQL=/path/to/psql PGBENCH=/path/to/pgbench HOST=127.0.0.1 PORT=5432 \
#   USER=postgres PASSWORD=123456 DATABASE=postgres \
#   ROW_COUNTS_OVERRIDE="1000" CONCURRENCIES_OVERRIDE="1 4" \
#   REPEAT=5 TIMELIMIT=10 ./polardb-pase-query-forms-benchmark.sh Benchmark_768_1m
#
# IMPORTANT: The target PolarDB instance must have the pase extension already
# installed and the target table must exist with a float4[] vector column.
# Note: PolarDB's pase differs from pgvector:
#   * Vector column type is float4[] (not `vector` type)
#   * Distance operators: <?> (question_op) or <#> (hash_op) between float4[] and pase
#   * `text <!> pase` uses text-encoded vectors
#
# SQL_TYPES enumerates the different vector literal forms supported by pase:
#
#   text_pase_op_id        : vec_col <?> '0.1,0.2,...'::pase
#                            - pase type cast from plain comma float text (no brackets)
#   text_pase_op_extra     : vec_col <?> '0.1,0.2,...:5'::pase
#                            - with HNSW extra (ef_search) encoded in pase literal
#   text_pase_op_extra_ds  : vec_col <?> '0.1,0.2,...:5:1'::pase
#                            - with extra + ds=1 (IP distance)
#   pase_fn_text_default   : vec_col <?> '0.1,0.2,...'::pase
#                            - 文本 cast (pase_in), 默认 extra/ef=索引 ef_search, ds=0 (L2)
#   pase_fn_text_extra     : (已移除) pase(text, extra) 是 base64 陷阱
#   pase_fn_text_ip        : (已移除) pase(text, extra, ds) 是 base64 陷阱
#   !! pase 文本构造函数 pase('0.1,0.2,...') (含单参数) 一律绑定 pase_text_i_i,
#      把字符串当 base64 解码 -> 解出垃圾维度 (pase 的 base64 只有
#      '{"dim":128,"extra":5,"ds":0,"vector":"..."}' 这种结构才能解对),
#      报 "query dimemsion(N) not equal to data dimemsion(D)"。文本进 pase 的
#      唯一安全路径是 'txt'::pase 输入函数 (pase_in, 逗号解析)。
#      pase_fn_text_default / with_pase_fn_text 已改为 cast 形式 (语义不变: 走 pase_in)。
#   pase_fn_array_default  : vec_col <?> pase(ARRAY[0.1,0.2,...]::float4[])
#                            - pase(float4[]) constructor, defaults extra=0 ds=0 (L2)
#   pase_fn_array_extra    : vec_col <?> pase(ARRAY[0.1,0.2,...]::float4[], 5)
#                            - with extra for HNSW search
#   pase_fn_array_ip       : vec_col <?> pase(ARRAY[0.1,0.2,...]::float4[], 5, 1)
#                            - with extra + ds=1 (IP)
#   hash_op_default        : vec_col <?> pase(ARRAY[0.1,0.2,...]::float4[])
#                            - 使用 <?> 替代 <#>, 因为 <#> 只兼容 IVFFlat 索引
#   hash_op_extra          : vec_col <?> pase(ARRAY[0.1,0.2,...]::float4[], 5)
#                            - 使用 <?> 替代 <#>, 同上
#   with_pase_fn_array     : WITH q AS (SELECT pase(ARRAY[0.1,0.2,...]::float4[]) AS p) SELECT ...
#   with_pase_fn_text      : WITH q AS (SELECT '0.1,0.2,...'::pase AS p) SELECT ...
#   subquery_id            : vec_col <?> (SELECT pase(vec_col) FROM t WHERE id = N)
#   with_subquery_id       : WITH q AS (SELECT pase(vec_col) FROM t WHERE id = N) SELECT ...
#
# 计划缓存/结果缓存测量口径 (重要, 2026-09-21 修正, 与 pgvector 脚本一致):
#   PostgreSQL/PolarDB 没有原生"查询结果缓存", result_cache 仅用 shared_buffers 中已加载的
#   数据页/索引页模拟"结果缓存命中"; 冷启动时重启 PG 只清空 shared_buffers,
#   root 下还会额外清空 OS 页缓存 (/proc/sys/vm/drop_caches)。
#   计划缓存 (plan_cache_mode) 只在 plancache.c 的 GetCachedPlan 路径生效:
#     * pgbench 默认 simple 协议每次执行都重新 parse+analyze+plan, 不经过 plancache,
#       plan_cache_mode 完全是 no-op. 因此脚本默认使用 -M prepared (PGBENCH_QUERY_MODE).
#     * 字面量查询(向量直接写在 SQL 里)即使走 prepared 协议也没有绑定参数,
#       choose_custom_plan() 在 boundParams==NULL 时忽略 plan_cache_mode 直接走 generic,
#       force_custom_plan 依然无效. 因此对"向量作为常数"的 SQL 形态, 脚本会把向量
#       提升为服务端绑定参数 (:v -> $1 经 \gset 注入, $1::pase), 使
#       force_custom_plan = 每次重规划, force_generic_plan = 命中缓存计划, 可测量.
#       等价性已验证: EXECUTE ... ('vec[:extra[:ds]]') 与字面量 'vec[:extra[:ds]]'::pase
#       返回完全相同的 id/dis Top-K.
#     * subquery_id / with_subquery_id (向量来自子查询) 无法参数化, 该形态下两种
#       plan_cache_mode 不可区分, 属 PostgreSQL 语义限制.
#   pase 特有陷阱: pase(text) / pase(text, extra) / pase(text, extra, ds) 构造函数
#   一律把文本当 base64 解码 (pase_text_i_i, pase--0.0.1.sql CREATE FUNCTION pase(text,...)),
#   逗号文本会被解出垃圾维度, SQL_TYPES 已移除该形态; 文本进 pase 只有 'text'::pase
#   输入函数一条路 (pase_in, 逗号解析)。数组构造函数 pase(float4[]) /
#   pase(float4[], extra) / pase(float4[], extra, ds) 走 pase_f4_i_i, 全部正常。
#   参数化统一用 $1::pase + 文本后缀 :extra[:ds].
#
# IMPORTANT: The target PolarDB instance must have the pase extension already
# installed and the target table must exist with a float4[] vector column.

set -euo pipefail

# 自动检测 psql 对应的 lib 目录，避免 "undefined symbol" 错误
_auto_ld_path() {
    local psql_path="${1:-}"
    [ -n "$psql_path" ] && [ -x "$psql_path" ] || return 0
    local lib_dir
    lib_dir="$(dirname "$(dirname "$psql_path")")/lib"
    if [ -d "$lib_dir" ] && [ -f "$lib_dir/libpq.so" ]; then
        export LD_LIBRARY_PATH="${lib_dir}:${LD_LIBRARY_PATH:-}"
    fi
}

ENGINE="${ENGINE:-polardb}"
HOST="${HOST:-127.0.0.1}"
PORT="${PORT:-5432}"
USER="${USER:-postgres}"
PASSWORD="${PASSWORD:-123456}"
DATABASE="${DATABASE:-postgres}"
TIMELIMIT="${TIMELIMIT:-30}"
WARMUP_TIMELIMIT="${WARMUP_TIMELIMIT:-10}"
REPEAT="${REPEAT:-5}"
TOP_K="${TOP_K:-10}"
SQL_DIR="${SQL_DIR:-sql-bench/polardb-pase-query-forms}"
OUTPUT_CSV="${OUTPUT_CSV:-../results/vector-query-forms-${ENGINE}-results.csv}"
PSQL="${PSQL:-psql}"
PGBENCH="${PGBENCH:-pgbench}"
PGBENCH_JOBS="${PGBENCH_JOBS:-}"
MAX_PGBENCH_SCRIPTS="${MAX_PGBENCH_SCRIPTS:-128}"
# pgbench 发送查询的协议: simple|extended|prepared (默认 prepared).
# simple 协议每次执行都重新 parse+analyze+plan, 根本不经 plancache, plan_cache_mode 不生效.
PGBENCH_QUERY_MODE="${PGBENCH_QUERY_MODE:-prepared}"
# 热启动: 计时前用本 profile 查询暖场 shared_buffers+OS页缓存 的时长 (秒)。
# 本基准不测冷启动 (与 CK 口径一致): 两种 profile 都 drop_buffer=false, 计时前暖场到稳态,
# 保证每种 SQL 形态(含第 1 种)都从热态开始, 结果稳定可复现。
BUFFER_WARMUP_SEC="${BUFFER_WARMUP_SEC:-10}"
# 暖场时是否顺带预热堆表 (用 <#> hash_op 顺序扫描形态把 1M 行堆表页载入), 使 <#> 形态也处热态。
WARM_TABLE_FOR_SEQUENTIAL="${WARM_TABLE_FOR_SEQUENTIAL:-true}"
_auto_ld_path "$PSQL"
_auto_ld_path "$PGBENCH"

# 自动推导 pg_ctl 路径: 与 PSQL 同目录
_auto_pg_ctl_path() {
    local psql_path="${PSQL:-psql}"
    if echo "$psql_path" | grep -q '/'; then
        local dir; dir="$(dirname "$psql_path")"
        if [ -x "${dir}/pg_ctl" ]; then
            echo "${dir}/pg_ctl"
            return
        fi
    fi
    for p in /usr/local/pgsql/bin/pg_ctl /usr/lib/postgresql/*/bin/pg_ctl; do
        [ -x "$p" ] && { echo "$p"; return; }
    done
    echo "pg_ctl"
}
PG_CTL="${PG_CTL:-$(_auto_pg_ctl_path)}"
PGDATA="${PGDATA:-/usr/local/pgsql/data}"
DISTANCE_OP="${DISTANCE_OP:-<?>}"
# <#> (hash) 只在 pase_ivfflat_float_ops 里, hnsw 索引表上会退化为全表扫描;
# 默认改用 <?> 保住索引路径; 若表是 ivfflat 索引可覆盖为 <#>
HASH_DISTANCE_OP="${HASH_DISTANCE_OP:-<?>}"
SORT_DIR="${SORT_DIR:-ASC}"
PASE_EXTRA="${PASE_EXTRA:-5}"    # HNSW ef_search parameter

# cache_profiles 定义不同查询计划缓存/结果缓存组合。
# 在 PostgreSQL 中:
#   - plan_cache_mode = force_custom_plan  每次都重新规划 (关闭缓存)
#   - plan_cache_mode = force_generic_plan 使用缓存的通用计划 (开启缓存)
#   - enable_seqscan = on/off              控制是否允许顺序扫描
#   - drop_buffer_cache = true/false       是否在测试前清理 shared_buffers
#     true  = 冷启动: 清理所有缓存, 模拟无结果缓存场景
#     false = 热启动: 保留 shared_buffers 中的数据页/索引页, 模拟结果缓存命中
# PostgreSQL 没有 MySQL/ClickHouse 的原生结果缓存, 但 shared_buffers 缓冲池
# 缓存了数据页和向量索引页, 重复查询命中内存即等效于"结果缓存"。
# 每个配置: name|plan_cache_mode|enable_seqscan|drop_buffer_cache
# 只对比"查询计划缓存"两类, 且都不测冷启动 (与 CK 口径一致: 都是热态、结果稳定可复现):
#   两者 drop_buffer_cache=false (不重启/不清页缓存), 计时前先暖场到稳态;
#   唯一变量是 plan_cache_mode:
#     off       = plan 缓存关闭 (force_custom_plan, 每次 exec 重新规划)
#     plan_cache= plan 缓存开启 (force_generic_plan, 复用缓存计划)
# 注: PostgreSQL/PolarDB 无原生"查询结果缓存", 不再单列 result_cache 档。
#
# 重要 confound (2026-09-27 live 验证): force_generic_plan 的 generic 计划用
# genericcostestimate 描述 `vector <?> pase`, 不是 pase 感知的 HNSW top-k 成本;
# enable_seqscan=on (默认) 时 generic 计划会退化为 Seq Scan + Sort (对 10M 表
# prepared cache=1 实测 ~5.7 rps, 全表逐行算距离), 而 force_custom (已知查询向量)
# 走 Index Scan (HNSW)。于是默认 `enable_seqscan=on` 时 "plan_cache vs off" 测得
# 的是 "索引 vs 全表扫描" 差异, 不是纯计划缓存收益!
# 因此两档统一 enable_seqscan=off, 使 custom/generic 都保住 HNSW 索引, 差异只剩
# "每次重规划 vs 复用缓存计划" (pase_verify 实测: custom 0.056ms/次 vs generic
# 0.004ms/次)。想对照真实默认行为, 用 CACHE_PROFILES_OVERRIDE 把第 3 段改回 on。
CACHE_PROFILES=(
    "off|force_custom_plan|off|false"
    "plan_cache|force_generic_plan|off|false"
)

SQL_TYPES=(
    text_pase_op_id
    text_pase_op_extra
    text_pase_op_extra_ds
    pase_fn_text_default
    pase_fn_array_default
    pase_fn_array_extra
    pase_fn_array_ip
    hash_op_default
    hash_op_extra
    with_pase_fn_array
    with_pase_fn_text
    subquery_id
    with_subquery_id
)
ROW_COUNTS=(1000)
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

client_query() {
    PGPASSWORD="$PASSWORD" timeout 60 "$PSQL" -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" -t -A -c "$1" 2>/dev/null
}

detect_vector_column() {
    local table="$1"
    local table_exists
    table_exists=$(client_query "SELECT 1 FROM information_schema.tables WHERE table_name = '$table' AND table_schema = 'public' LIMIT 1")
    if [ -z "$table_exists" ]; then
        echo "错误: 表 '$table' 不存在 (请检查 HOST=$HOST PORT=$PORT DATABASE=$DATABASE 是否正确)" >&2
        return 1
    fi
    local col
    col=$(client_query "SELECT column_name FROM information_schema.columns WHERE table_name = '$table' AND data_type = 'ARRAY' AND udt_name = '_float4' LIMIT 1")
    if [ -z "$col" ]; then
        echo "错误: 表 '$table' 中未找到 float4[] 类型向量列" >&2
        return 1
    fi
    echo "$col"
}

detect_vector_dimension() {
    local table="$1"
    local vec_col="$2"
    local dim
    dim=$(client_query "SELECT array_length($vec_col, 1) FROM $table LIMIT 1")
    echo "${dim:-0}"
}

distance_expr() {
    local vec_col="$1"
    local query_expr="$2"
    echo "(${vec_col} ${DISTANCE_OP} ${query_expr})"
}

make_select_sql() {
    local table="$1"
    local vec_col="$2"
    local query_expr="$3"
    local dist
    dist=$(distance_expr "$vec_col" "$query_expr")
    echo "SELECT id, ${dist} AS dis FROM ${table} ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};"
}

make_with_sql() {
    local table="$1"
    local vec_col="$2"
    local query_expr="$3"
    local dist
    dist=$(distance_expr "$vec_col" "query_pase.v")
    echo "WITH query_pase AS (SELECT ${query_expr} AS v) SELECT id, ${dist} AS dis FROM ${table} CROSS JOIN query_pase ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};"
}

sql_file_for() {
    local table="$1"
    local sql_type="$2"
    local count="$3"
    local cache_profile="${4:-}"
    if [ -n "$cache_profile" ]; then
        echo "${SQL_DIR}/${ENGINE}_${table}_${cache_profile}_${sql_type}_${count}.sql"
    else
        echo "${SQL_DIR}/${ENGINE}_${table}_${sql_type}_${count}.sql"
    fi
}

generate_sql_for_table() {
    local table="$1"
    local vec_col="$2"
    local dim="$3"
    local cache_profile="${4:-}"
    local profile_suffix=""
    [ -n "$cache_profile" ] && profile_suffix=" (profile=$cache_profile)"

    mkdir -p "$SQL_DIR"

    for count in "${ROW_COUNTS[@]}"; do
        local tmp_vectors tmp_error
        tmp_vectors=$(mktemp)
        tmp_error=$(mktemp)

        echo "  抽样 ${count} 条向量: table=$table column=$vec_col dim=$dim${profile_suffix}"
        # float4[]::text outputs: {0.1,0.2,...}
        if ! client_query "SELECT id, ${vec_col}::text FROM ${table} ORDER BY random() LIMIT ${count}" \
            > "$tmp_vectors" 2>"$tmp_error"; then
            echo "错误: 抽样失败，无法生成 SQL"
            tail -20 "$tmp_error" | sed 's/^/  /'
            rm -f "$tmp_vectors" "$tmp_error"
            return 1
        fi

        declare -A files
        local sql_type
        for sql_type in "${SQL_TYPES[@]}"; do
            files["$sql_type"]="$(sql_file_for "$table" "$sql_type" "$count" "$cache_profile")"
            : > "${files[$sql_type]}"
        done

        local id vec_str
        local first_id=""
        while IFS='|' read -r id vec_str; do
            [ -z "${id:-}" ] && continue
            [ -z "${vec_str:-}" ] && continue

            # vec_str is of the form: {0.1,0.2,...}
            local inner="${vec_str//[\{\}]/}"
            local f4_array="ARRAY[$inner]::float4[]"

            if [ -z "$first_id" ]; then
                first_id="$id"
            fi

            # --- pase text literal cast: '0.1,0.2,...'::pase ---
            local pase_text="'${inner}'::pase"
            if [ -n "${files[text_pase_op_id]:-}" ]; then
                make_select_sql "$table" "$vec_col" "$pase_text" >> "${files[text_pase_op_id]}"
            fi

            # text with extra: '0.1,0.2,...:5'::pase
            if [ -n "${files[text_pase_op_extra]:-}" ]; then
                make_select_sql "$table" "$vec_col" "'${inner}:${PASE_EXTRA}'::pase" >> "${files[text_pase_op_extra]}"
            fi

            # text with extra + ds=1 (IP): '0.1,0.2,...:5:1'::pase
            if [ -n "${files[text_pase_op_extra_ds]:-}" ]; then
                make_select_sql "$table" "$vec_col" "'${inner}:${PASE_EXTRA}:1'::pase" >> "${files[text_pase_op_extra_ds]}"
            fi

            # 文本 cast 形态 (pase_in, 逗号解析): 文本构造函数的 base64 陷阱见文件头,
            # 这里用 'text'::pase 表示"文本输入"这一档, 语义与 pase(text) 意图相同。
            if [ -n "${files[pase_fn_text_default]:-}" ]; then
                make_select_sql "$table" "$vec_col" "'${inner}'::pase" >> "${files[pase_fn_text_default]}"
            fi

            # pase(text, extra) / pase(text, extra, ds) 已移除 (base64 陷阱, 见文件头)

            # pase(float4[] array) constructor defaults
            if [ -n "${files[pase_fn_array_default]:-}" ]; then
                make_select_sql "$table" "$vec_col" "pase(${f4_array})" >> "${files[pase_fn_array_default]}"
            fi

            # pase(float4[] array, extra)
            if [ -n "${files[pase_fn_array_extra]:-}" ]; then
                make_select_sql "$table" "$vec_col" "pase(${f4_array}, ${PASE_EXTRA})" >> "${files[pase_fn_array_extra]}"
            fi

            # pase(float4[] array, extra, ds=1) for IP
            if [ -n "${files[pase_fn_array_ip]:-}" ]; then
                make_select_sql "$table" "$vec_col" "pase(${f4_array}, ${PASE_EXTRA}, 1)" >> "${files[pase_fn_array_ip]}"
            fi

            # hash_op: 使用 <?> 替代 <#>, 因为 <#> 只兼容 IVFFlat 索引
            if [ -n "${files[hash_op_default]:-}" ]; then
                echo "SELECT id, ${vec_col} ${HASH_DISTANCE_OP} pase(${f4_array}) AS dis FROM ${table} ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};" >> "${files[hash_op_default]}"
            fi

            if [ -n "${files[hash_op_extra]:-}" ]; then
                echo "SELECT id, ${vec_col} ${HASH_DISTANCE_OP} pase(${f4_array}, ${PASE_EXTRA}) AS dis FROM ${table} ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};" >> "${files[hash_op_extra]}"
            fi

            # WITH clause with array constructor
            if [ -n "${files[with_pase_fn_array]:-}" ]; then
                make_with_sql "$table" "$vec_col" "pase(${f4_array})" >> "${files[with_pase_fn_array]}"
            fi

            # WITH clause with text-cast form (pase_in, 避 base64 陷阱)
            if [ -n "${files[with_pase_fn_text]:-}" ]; then
                make_with_sql "$table" "$vec_col" "'${inner}'::pase" >> "${files[with_pase_fn_text]}"
            fi

            # subquery to reuse a stored vector from same table
            if [ -n "${files[subquery_id]:-}" ]; then
                make_select_sql "$table" "$vec_col" "(SELECT pase(${vec_col}) FROM ${table} WHERE id = ${id})" >> "${files[subquery_id]}"
            fi

            # WITH subquery to reuse a stored vector
            if [ -n "${files[with_subquery_id]:-}" ]; then
                make_with_sql "$table" "$vec_col" "(SELECT pase(${vec_col}) FROM ${table} WHERE id = ${id})" >> "${files[with_subquery_id]}"
            fi
        done < "$tmp_vectors"

        for sql_type in "${SQL_TYPES[@]}"; do
            echo "    已生成: ${files[$sql_type]} ($(wc -l < "${files[$sql_type]}") 条)"
        done

        rm -f "$tmp_vectors" "$tmp_error"
    done
}

# ---- 缓存配置相关函数 ----

# 生成当前 cache_profile 的会话级 SET 行。
# 计划缓存等开关必须在会话级设置; 每条查询前附带 SET 子句会增加解析开销,
# 因此 benchmark 运行时将 SET 行写入单独的脚本文件头部, 作为同一会话的前置命令。
session_set_line_for_profile() {
    local profile_name="$1"
    local group pname plan_cache_mode enable_seqscan drop_buffer_cache
    for group in "${CACHE_PROFILES[@]}"; do
        IFS='|' read -r pname plan_cache_mode enable_seqscan drop_buffer_cache <<< "$group"
        if [ "$pname" = "$profile_name" ]; then
            echo "SET plan_cache_mode = ${plan_cache_mode}; SET enable_seqscan = ${enable_seqscan};"
            return
        fi
    done
    echo ""
}

# 重启 PostgreSQL 以清空 shared_buffers (确保冷启动时干净)
restart_postgresql() {
    echo -n "    重启 PostgreSQL (清空 shared_buffers)... "
    local pg_ctl_path="$PG_CTL"
    local pgdata_dir="$PGDATA"

    # 1) 尝试直接重启 (当前用户即有权限), 加 timeout 防止挂起
    if timeout 15 "$pg_ctl_path" -D "$pgdata_dir" restart -w -m fast > /dev/null 2>&1; then
        :
    # 2) 尝试通过 su 切换 postgres 用户
    elif command -v su >/dev/null 2>&1; then
        if timeout 15 su - postgres -c "$pg_ctl_path -D $pgdata_dir restart -w -m fast" > /dev/null 2>&1; then
            :
        else
            echo "失败!"
            echo "    ⚠ 无法重启 PG (超时或无权限), 冷启动数据将不准确!"
            echo "    ⚠ 建议手动重启: su - postgres -c \"$pg_ctl_path -D $pgdata_dir restart -m fast\""
            return 1
        fi
    else
        echo "失败!"
        echo "    ⚠ 无 sudo/su 权限, 无法重启 PostgreSQL"
        return 1
    fi

    local i
    for i in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15; do
        client_query "SELECT 1 AS ping" > /dev/null 2>&1 && {
            echo "成功 (${i}s)"
            return 0
        }
        sleep 2
    done
    echo "超时!"
    echo "    错误: 重启后无法连接数据库!"
    return 1
}

# 诊断并记录当前缓存配置
verify_cache_settings() {
    local profile_name="$1"
    local label="$2"

    local current_mode
    current_mode=$(client_query "SHOW plan_cache_mode" 2>/dev/null || echo "UNKNOWN")
    echo "   [诊断] ($label) 当前 plan_cache_mode = $current_mode (期望: $profile_name)"
}

# 通过 EXPLAIN (ANALYZE, BUFFERS) 诊断缓冲池状态和计划缓存是否生效
# 参数: table_name, plan_cache_mode, sql_type, row_count, cache_profile, label
diagnose_cache_with_explain() {
    local table="$1"
    local pcm="$2"
    local sql_type="$3"
    local row_count="$4"
    local cache_profile="${5:-}"
    local label="$6"

    local sample_sql_file
    sample_sql_file=$(sql_file_for "$table" "$sql_type" "$row_count" "$cache_profile" 2>/dev/null || echo "")
    [ -z "$sample_sql_file" ] || [ ! -s "$sample_sql_file" ] && return
    local sample_query
    sample_query=$(head -1 "$sample_sql_file" 2>/dev/null || echo "")
    [ -z "$sample_query" ] && return

    echo "   [诊断] ($label) 通过 EXPLAIN 诊断缓存状态..."

    client_query "SET plan_cache_mode = ${pcm};" > /dev/null 2>&1 || true

    local explain1
    explain1=$(timeout 5 client_query "EXPLAIN (SUMMARY, BUFFERS, FORMAT TEXT) ${sample_query}" 2>/dev/null || echo "EXPLAIN失败")
    local planning_time1 buffers_info1
    planning_time1=$(echo "$explain1" | grep -oP 'Planning Time: \K[0-9.]+' | head -1 || true)
    buffers_info1=$(echo "$explain1" | grep -oP 'Buffers:.*' | head -1 || true)
    echo "   [诊断] ($label) 第1次 EXPLAIN: 规划=${planning_time1:-N/A}ms 缓冲=${buffers_info1:-N/A}"

    local explain2
    explain2=$(timeout 5 client_query "EXPLAIN (SUMMARY, BUFFERS, FORMAT TEXT) ${sample_query}" 2>/dev/null || echo "EXPLAIN失败")
    local planning_time2 buffers_info2
    planning_time2=$(echo "$explain2" | grep -oP 'Planning Time: \K[0-9.]+' | head -1 || true)
    buffers_info2=$(echo "$explain2" | grep -oP 'Buffers:.*' | head -1 || true)
    echo "   [诊断] ($label) 第2次 EXPLAIN: 规划=${planning_time2:-N/A}ms 缓冲=${buffers_info2:-N/A}"

    # 说明: psql 的 EXPLAIN 走 simple 协议, 每次都重新规划, 因此这里第2次规划
    #    时间不可能接近0, 不能用它判断计划缓存。缓存是否命中以上面的
    #    PREPARE+EXECUTE 探测 (diagnose_plan_cache) 为准。
    if [ -n "$planning_time1" ] && [ -n "$planning_time2" ]; then
        local diff
        diff=$(awk "BEGIN {printf \"%.4f\", $planning_time1 - $planning_time2}")
        echo "   [诊断] ($label) psql(EXPLAIN/simple 协议) 第1/2次规划 = ${planning_time1}ms / ${planning_time2}ms (simple 协议每次都重规划, 属预期)"
    fi

    if [ -n "$buffers_info2" ]; then
        local shared_hit shared_read
        shared_hit=$(echo "$buffers_info2" | grep -oP 'shared hit=\K[0-9]+' | head -1 || true)
        shared_read=$(echo "$buffers_info2" | grep -oP 'shared read=\K[0-9]+' | head -1 || true)
        if [ -n "$shared_hit" ] && [ -n "$shared_read" ]; then
            local total_blocks=$(( shared_hit + shared_read ))
            local hit_ratio
            hit_ratio=$(awk "BEGIN {printf \"%.1f\", ($total_blocks > 0 ? 100 * $shared_hit / $total_blocks : 100)}")
            echo "   [诊断] ($label) 缓冲池命中: hit=${shared_hit} read=${shared_read} 命中率=${hit_ratio}%"
            if [ "$total_blocks" -gt 0 ] && [ "$shared_read" -gt 0 ]; then
                echo "   [诊断] ⚠ ($label) 存在 shared_read (${shared_read}块), 部分页面不在缓冲池中"
            else
                echo "   [诊断] ✓ ($label) 所有页面均在 shared_buffers 中 (100% 命中)"
            fi
        fi
    fi
    echo ""
}

# 每个 profile 开始前清服务端缓存, 使首行 QPS 测量口径一致
# 参数: plan_cache_mode (保留兼容) 和 drop_buffer_cache (true=清理所有缓存, false=保留缓冲池)
drop_caches_for_profile() {
    local plan_cache_mode="$1"
    local drop_buffer_cache="${2:-true}"

    # 1) 始终清理查询计划缓存
    client_query "DISCARD PLANS" > /dev/null 2>&1 || true
    echo "    已清理查询计划缓存 (DISCARD PLANS)"

    if [ "$drop_buffer_cache" = "true" ]; then
        # 冷启动: 重启 PG 以清空 shared_buffers (最可靠的方式)
        echo "    冷启动模式: 需要清空 shared_buffers..."
        restart_postgresql
        # 重启后再清理一次计划缓存
        client_query "DISCARD PLANS" > /dev/null 2>&1 || true
        # 验证 plan_cache_mode 默认值 (重启后恢复为 auto)
        verify_cache_settings "$plan_cache_mode" "重启后"
        # 重启 PG 只清空 shared_buffers; Linux 页缓存里的数据页/索引页仍存在,
        # 冷启动必须顺带清空 OS 页缓存 (需 root) 才真正"冷"。
        if [ "$(id -u)" = "0" ]; then
            if [ -w /proc/sys/vm/drop_caches ]; then
                sync
                if echo 3 > /proc/sys/vm/drop_caches 2>/dev/null; then
                    echo "    已清空 OS 页缓存 (/proc/sys/vm/drop_caches) — 真正的冷启动"
                else
                    echo "    ⚠ 清空 OS 页缓存失败 (无法写入 /proc/sys/vm/drop_caches)"
                fi
            else
                echo "    ⚠ /proc/sys/vm/drop_caches 不可写 (容器/权限限制), 冷启动仅清空 shared_buffers"
            fi
        else
            echo "    ⚠ 非 root, 无法清空 OS 页缓存; 冷启动仅清空 shared_buffers"
        fi
    fi
}

# 生成带会话级 SET 语句的单查询 pgbench 脚本目录
make_single_query_scripts_with_settings() {
    local sql_file="$1"
    local set_line="$2"
    local script_dir="$3"
    rm -rf "$script_dir"
    mkdir -p "$script_dir"
    local i=0
    while IFS= read -r query; do
        [ -z "$query" ] && continue
        # 每条查询前先执行 SET 语句, 确保该会话处于正确的缓存配置状态
        printf '%s\n%s\n' "$set_line" "$query" > "$script_dir/query_$(printf '%04d' "$i")"
        i=$((i + 1))
    done < "$sql_file"
    # 返回生成的脚本数
    echo "$i"
}

# 该 SQL 形态是否属于"向量作为常数" (可被提升为绑定参数, 从而让 plan_cache_mode 生效)
is_parameterizable_sql_type() {
    case "$1" in
        text_pase_op_id|text_pase_op_extra|text_pase_op_extra_ds|pase_fn_text_default|pase_fn_array_default|pase_fn_array_extra|pase_fn_array_ip|hash_op_default|hash_op_extra|with_pase_fn_array|with_pase_fn_text)
            return 0 ;;
        *) return 1 ;;
    esac
}

# 从一条 pase 查询行中提取"向量文本参数" (逗号分隔浮点串, 可带 :extra[:ds] 后缀), 供 \gset 注入。
# 文本形态 (text_pase_op_*/pase_fn_text_default/with_pase_fn_text): pase 文本在单引号内,
#   extra/ds 后缀已编码在文本里 ('0.1,...,:5' 或 ':5:1'), 原样取回即可。
# 数组形态 (pase_fn_array_*/hash_op_*/with_pase_fn_array): 提取 ARRAY[...] 内的逗号串,
#   并按形态合成 :extra[:ds] 后缀 (与 pase(ARRAY[...], 5[, 1]) 语义等价, 已验证)。
extract_pase_vector_param() {
    local sql_type="$1" query="$2" raw="" inner=""
    case "$sql_type" in
        text_pase_op_id|text_pase_op_extra|text_pase_op_extra_ds|pase_fn_text_default|with_pase_fn_text)
            inner="$(printf '%s\n' "$query" | sed -n "s/.*'\([^']*\)'.*/\1/p" | head -1 || true)" ;;
        *)
            inner="$(printf '%s\n' "$query" | grep -oE '\[[0-9.,eE+-]+\]' | head -1 || true)"
            inner="${inner//[\[\]]/}" ;;
    esac
    [ -z "$inner" ] && { printf '%s' ""; return 0; }
    case "$sql_type" in
        pase_fn_array_ip)        printf '%s:%s:1' "$inner" "$PASE_EXTRA" ;;
        pase_fn_array_extra|hash_op_extra) printf '%s:%s' "$inner" "$PASE_EXTRA" ;;
        *)                       printf '%s' "$inner" ;;
    esac
}

# 参数化脚本: 把向量从 SQL 中抽离, 通过 \gset 设为 pgbench 变量,
# 在 -M prepared 下作为绑定参数 ($1) 发送, 使 plan_cache_mode 真正生效。
# 脚本结构:  \gset 注入向量 -> SET 行 -> 含 :v::pase 的查询。
#   1) prepared 协议让 plancache 参与 (simple 协议根本不缓存计划);
#   2) 绑定参数使 boundParams != NULL, choose_custom_plan() 才会区分
#      force_custom_plan (每次重规划) / force_generic_plan (复用缓存计划)。
make_single_query_scripts_parameterized() {
    local sql_file="$1" set_line="$2" script_dir="$3" table="$4" vec_col="$5" sql_type="$6"
    rm -rf "$script_dir"
    mkdir -p "$script_dir"
    local i=0 query vparam pquery
    while IFS= read -r query; do
        [ -z "$query" ] && continue
        vparam="$(extract_pase_vector_param "$sql_type" "$query")"
        if [ -z "$vparam" ]; then
            echo "警告: 无法从查询提取 pase 向量文本, 回退为字面量脚本: $(printf '%s' "$query" | cut -c1-70)..." >&2
            printf '%s\n%s\n' "$set_line" "$query" > "$script_dir/query_$(printf '%04d' "$i")"
            i=$((i + 1))
            continue
        fi
        case "$sql_type" in
            with_*)
                pquery="WITH query_pase AS (SELECT :v::pase AS v) SELECT id, (${vec_col} ${DISTANCE_OP} query_pase.v) AS dis FROM ${table} CROSS JOIN query_pase ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};"
                ;;
            hash_op_*)
                pquery="SELECT id, (${vec_col} ${HASH_DISTANCE_OP} :v::pase) AS dis FROM ${table} ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};"
                ;;
            *)
                pquery="SELECT id, (${vec_col} ${DISTANCE_OP} :v::pase) AS dis FROM ${table} ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};"
                ;;
        esac
        printf "SELECT '%s'::text AS v \\gset\n%s\n%s\n" "$vparam" "$set_line" "$pquery" > "$script_dir/query_$(printf '%04d' "$i")"
        i=$((i + 1))
    done < "$sql_file"
    echo "$i"
}

# 通过 PREPARE + 两次 EXECUTE 探测计划缓存是否真正生效。
# 注意: psql 的 EXPLAIN 走 simple 协议, 每次都重新规划, 永远看不出计划缓存;
# 必须用 PREPARE/EXECUTE 走 plancache.c 的 GetCachedPlan 才能测量。
diagnose_plan_cache() {
    local table="$1" vec_col="$2" sql_file="$3" sql_type="$4"
    [ -s "$sql_file" ] || return 0
    local sample_vec
    sample_vec="$(extract_pase_vector_param "$sql_type" "$(head -1 "$sql_file")")"
    [ -z "$sample_vec" ] && { echo "   [诊断] 无法解析 pase 向量, 跳过计划缓存探测"; return 0; }

    local diag_sql diag_out
    diag_sql=$(mktemp)
    # enable_seqscan 与 CACHE_PROFILES 对齐 (off): 否则 generic 计划会退化为
    # Seq Scan (见文件头 confound 说明), 测得的是索引/全扫差异而非计划缓存收益。
    cat > "$diag_sql" <<EOF
SET enable_seqscan = off;
SET plan_cache_mode = force_custom_plan;
PREPARE diag_pc AS SELECT id, (${vec_col} ${DISTANCE_OP} \$1::pase) AS dis FROM ${table} ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};
EXPLAIN (COSTS OFF) EXECUTE diag_pc('${sample_vec}');
EXPLAIN (SUMMARY) EXECUTE diag_pc('${sample_vec}');
EXPLAIN (SUMMARY) EXECUTE diag_pc('${sample_vec}');
SET plan_cache_mode = force_generic_plan;
EXPLAIN (COSTS OFF) EXECUTE diag_pc('${sample_vec}');
EXPLAIN (SUMMARY) EXECUTE diag_pc('${sample_vec}');
EXPLAIN (SUMMARY) EXECUTE diag_pc('${sample_vec}');
DEALLOCATE diag_pc;
EOF
    echo "   [诊断] 计划缓存探测 (PREPARE+EXECUTE, enable_seqscan=off, 各执行2次)..."
    diag_out=$(PGPASSWORD="$PASSWORD" timeout 60 "$PSQL" -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" -t -A -f "$diag_sql" 2>&1)
    rm -f "$diag_sql"

    local plan_shapes
    plan_shapes=$(printf '%s\n' "$diag_out" | grep -oE 'Index Scan using [^ ]+|Index Only Scan using [^ ]+|Seq Scan' || true)
    echo "   [诊断] custom/generic 计划形态: $(printf '%s' "$plan_shapes" | tr '\n' ' ' | sed 's/  */ /g')"
    local pts
    pts=($(printf '%s\n' "$diag_out" | grep -oE 'Planning Time: [0-9.]+ ms' | awk '{print $(NF-1)}' || true))
    echo "   [诊断] 规划耗时: force_custom_plan(1/2次) = ${pts[0]:-N/A}ms / ${pts[1]:-N/A}ms ; force_generic_plan(1/2次) = ${pts[2]:-N/A}ms / ${pts[3]:-N/A}ms"
    if [ -n "${pts[3]:-}" ]; then
        if awk "BEGIN { exit (${pts[3]} < 0.02) ? 0 : 1 }"; then
            echo "   [诊断] ✓ 计划缓存生效: force_generic_plan 第2次 EXECUTE 不再重新规划 (${pts[3]}ms)"
        else
            echo "   [诊断] ⚠ 计划缓存未生效: force_generic_plan 第2次 EXECUTE 仍重新规划 (${pts[3]}ms)"
        fi
    fi
    echo ""
}

parse_benchmark_metrics() {
    local output="$1"
    local num_queries="${2:-1}"

    local duration transactions failed latency stmt_latency tps successful query_qps tx_qps
    duration=$(printf '%s\n' "$output" | awk '/^duration:/ {print $2; exit}')
    transactions=$(printf '%s\n' "$output" | awk -F': ' '/^number of transactions actually processed:/ {split($2, a, " "); split(a[1], b, "/"); print b[1]; exit}')
    failed=$(printf '%s\n' "$output" | awk -F': ' '/^number of failed transactions:/ {split($2, a, " "); print a[1]; exit}')
    latency=$(printf '%s\n' "$output" | awk -F'= ' '/^latency average =/ {print $2; exit}' | awk '{print $1}')
    tps=$(printf '%s\n' "$output" | awk -F'= ' '/^tps =/ {print $2; exit}' | awk '{print $1}')
    stmt_latency=$(printf '%s\n' "$output" | awk '/^statement latencies in milliseconds/ {in_section=1; next} in_section && $1 ~ /^[0-9]+([.][0-9]+)?$/ {sum += $1; n++} END {if (n) printf "%.3f", sum/n; else print "0"}')
    duration=${duration:-0}; transactions=${transactions:-0}; failed=${failed:-0}
    successful=$((transactions - failed)); [ "$successful" -lt 0 ] && successful=0
    tx_qps=$(awk -v n="$successful" -v s="$duration" -v t="$tps" 'BEGIN {if (t > 0) printf "%.3f", t; else if (s > 0) printf "%.3f", n/s; else print "0.000"}')
    query_qps=$(awk -v t="$tx_qps" -v q="$num_queries" 'BEGIN {printf "%.3f", t*q}')
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$query_qps" "$tx_qps" "$duration" "$transactions" "$failed" "${latency:-0}" "$stmt_latency"
}

# 汇总 off (force_custom_plan, 每次重规划) vs plan_cache (force_generic_plan, 缓存计划)
# 两种 profile 在冷启动 (drop_buffer=true) 下的 QPS 对比, 按 sql_type 分组。
# 想排除其他 profile 的干扰: 只取当前 engine + 指定表的行。
summarize_plan_cache_comparison() {
    local table="$1"
    [ -f "$OUTPUT_CSV" ] || return 0
    local tmp_sum
    tmp_sum=$(mktemp)
    awk -F, -v want="$table" -v eng="$ENGINE" -v conc="${CONCURRENCIES[0]}" '
        NR==1 {
            for (i=1;i<=NF;i++) h[$i]=i
            c_eng=h["engine"]; c_tbl=h["table_name"]; c_prof=h["cache_profile"]
            c_drop=h["drop_buffer_cache"]; c_type=h["sql_type"]; c_conc=h["concurrency"]; c_q=h["qps_avg"]
        }
        NR>1 && $(c_tbl)==want && $(c_conc)==conc && $(c_drop)=="true" && $(c_eng)==eng {
            if ($(c_prof)=="off") off[$(c_type)]=$(c_q)+0
            else if ($(c_prof)=="plan_cache") pc[$(c_type)]=$(c_q)+0
        }
        END {
            for (t in off) {
                g = (off[t]>0) ? (pc[t]-off[t])/off[t]*100 : 0
                printf "%s\t%.1f\t%.1f\t%.1f\n", t, off[t], pc[t], g
            }
        }' "$OUTPUT_CSV" | sort > "$tmp_sum"
    if [ ! -s "$tmp_sum" ]; then
        rm -f "$tmp_sum"
        echo "  (CSV 中暂无 off/plan_cache 对照数据, 跳过汇总)"
        return 0
    fi
    echo ""
    echo "===== 查询计划缓存 QPS 对比 ($table): off(每次重规划) vs plan_cache(缓存计划) ====="
    printf "%-24s %12s %14s %10s  %s\n" "sql_type" "off_qps" "plan_cache_qps" "增益" "说明"
    local t offq pcq g note
    while IFS=$'\t' read -r t offq pcq g; do
        case "$t" in
            subquery_id|with_subquery_id) note="[向量来自子查询, 不可参数化, 两模式等价]" ; g="n/a" ;;
            *) note="" ;;
        esac
        printf "%-24s %12s %14s %10s  %s\n" "$t" "$offq" "$pcq" "$g" "$note"
    done < "$tmp_sum"
    rm -f "$tmp_sum"
    echo ""
}

write_csv_header() {
    local header="engine,server_version,table_name,vector_column,dimension,distance_op,pase_extra,cache_profile,plan_cache_mode,enable_seqscan,drop_buffer_cache,sql_type,row_count,concurrency"
    for i in $(seq 1 "$REPEAT"); do
        header="${header},run_${i}"
    done
    header="${header},qps_avg,qps_min,qps_max"
    header="${header},tx_qps_avg,elapsed_avg_s,transactions_avg,failed_avg,latency_avg_ms,statement_avg_ms"

    mkdir -p "$(dirname "$OUTPUT_CSV")"
    if [ ! -f "$OUTPUT_CSV" ] || [ "$(head -1 "$OUTPUT_CSV" 2>/dev/null || true)" != "$header" ]; then
        if [ -f "$OUTPUT_CSV" ]; then
            local backup_file="${OUTPUT_CSV}.$(date '+%Y%m%d%H%M%S').bak"
            cp "$OUTPUT_CSV" "$backup_file"
            echo "CSV 表头不匹配，已备份旧文件到: $backup_file"
        fi
        echo "$header" > "$OUTPUT_CSV"
    fi
}

run_benchmark_file() {
    local script_dir="$1"
    local concurrency="$2"
    local stderr_file="$3"
    local log_file="$4"
    local duration="${5:-$TIMELIMIT}"
    local bench_files=() query_file
    while IFS= read -r query_file; do
        bench_files+=( -f "$query_file" )
    done < <(find "$script_dir" -type f -name 'query_*' -print | sort | head -n "$MAX_PGBENCH_SCRIPTS")
    if [ "${#bench_files[@]}" -eq 0 ]; then
        echo "错误: 未找到单查询 pgbench 脚本: $script_dir" >&2
        return 1
    fi

    PGPASSWORD="$PASSWORD" "$PGBENCH" \
        -h "$HOST" \
        -p "$PORT" \
        -U "$USER" \
        -d "$DATABASE" \
        "${bench_files[@]}" \
        -c "$concurrency" \
        -j "${PGBENCH_JOBS:-$concurrency}" \
        -M "$PGBENCH_QUERY_MODE" \
        -T "$duration" \
        -n \
        -r \
        > "$log_file" 2>&1
}

make_single_query_scripts() {
    local sql_file="$1"
    local script_dir="$2"
    rm -rf "$script_dir"
    mkdir -p "$script_dir"
    split -d -a 4 -l 1 "$sql_file" "$script_dir/query_"
}

if ! command -v "$PSQL" >/dev/null 2>&1; then
    echo "错误: 找不到 psql 可执行文件: $PSQL" >&2
    exit 1
fi

if ! command -v "$PGBENCH" >/dev/null 2>&1; then
    echo "错误: 找不到 pgbench 可执行文件: $PGBENCH" >&2
    exit 1
fi

if [ "$#" -eq 0 ]; then
    echo "用法: $0 <table1> [table2 ...]" >&2
    exit 1
fi

CONN_TEST="$(timeout 5 env PGPASSWORD="$PASSWORD" "$PSQL" -h "$HOST" -p "$PORT" -U "$USER" -d "$DATABASE" -t -A -c "SELECT 1" 2>&1)"
conn_exit=$?
if [ "$conn_exit" -eq 124 ]; then
    echo "错误: 连接 $HOST:$PORT 超时 (5秒), psql 可能版本不兼容" >&2
    echo "  psql 路径: $(command -v "$PSQL" 2>/dev/null || echo "$PSQL (未找到)")" >&2
    echo "  psql 版本: $("$PSQL" --version 2>&1)" >&2
    exit 1
fi
if [ "$CONN_TEST" != "1" ]; then
    echo "错误: 无法连接到 $HOST:$PORT 数据库 $DATABASE" >&2
    echo "  psql 路径: $(command -v "$PSQL" 2>/dev/null || echo "$PSQL (未找到)")" >&2
    echo "  错误详情: $CONN_TEST" >&2
    echo "  提示: 如果 PolarDB 自带的 psql 与系统 psql 不同, 请设置 PSQL 和 PGBENCH 环境变量, 例如:" >&2
    echo "        PSQL=\$HOME/tmp_polardb_pg_17_base/bin/psql PGBENCH=\$HOME/tmp_polardb_pg_17_base/bin/pgbench $0 $*" >&2
    exit 1
fi

SERVER_VERSION="$(client_query "SELECT current_setting('server_version')")"

echo "=============================================="
echo " PolarDB-pase query forms benchmark"
echo "=============================================="
echo "engine:       $ENGINE"
echo "target:       $HOST:$PORT"
echo "user:         $USER"
echo "database:     $DATABASE"
echo "version:      $SERVER_VERSION"
echo "distance:     $DISTANCE_OP"
echo "pase_extra:   $PASE_EXTRA"
echo "sort:         $SORT_DIR"
echo "sql types:    ${SQL_TYPES[*]}"
echo "row counts:   ${ROW_COUNTS[*]}"
echo "concurrency:  ${CONCURRENCIES[*]}"
echo "cache_profiles: ${CACHE_PROFILES[*]}"
echo "repeat:       $REPEAT"
echo "timelimit:    ${TIMELIMIT}s"
echo "sql dir:      $SQL_DIR"
echo "output csv:   $OUTPUT_CSV"
echo "=============================================="
echo ""

echo ">>> 验证 cache_profiles:"
for group in "${CACHE_PROFILES[@]}"; do
    IFS='|' read -r pname plan_cache_mode enable_seqscan drop_buffer_cache <<< "$group"
    echo "  $pname: plan_cache_mode=$plan_cache_mode, enable_seqscan=$enable_seqscan, drop_buffer_cache=$drop_buffer_cache"
done
echo ""

declare -A TABLE_VEC_COL
declare -A TABLE_DIM

for table in "$@"; do
    vec_col="$(detect_vector_column "$table")"
    if [ $? -ne 0 ] || [ -z "$vec_col" ]; then
        exit 1
    fi
    dim="$(detect_vector_dimension "$table" "$vec_col")"
    TABLE_VEC_COL["$table"]="$vec_col"
    TABLE_DIM["$table"]="$dim"
    echo "表: $table  向量列: $vec_col  维度: $dim"
done
echo ""

echo ">>> 生成 SQL"
for table in "$@"; do
    for group in "${CACHE_PROFILES[@]}"; do
        IFS='|' read -r pname _ _ _ <<< "$group"
        generate_sql_for_table "$table" "${TABLE_VEC_COL[$table]}" "${TABLE_DIM[$table]}" "$pname"
    done
done
echo ""

# 检查是否有 SQL 文件生成
has_sql_files=false
for table in "$@"; do
    for group in "${CACHE_PROFILES[@]}"; do
        IFS='|' read -r pname _ _ _ <<< "$group"
        for sql_type in "${SQL_TYPES[@]}"; do
            for row_count in "${ROW_COUNTS[@]}"; do
                f="$(sql_file_for "$table" "$sql_type" "$row_count" "$pname")"
                [ -s "$f" ] && has_sql_files=true && break 4
            done
        done
    done
done

if [ "$has_sql_files" = false ]; then
    echo "错误: 未生成任何 SQL 文件，请检查表名和连接配置" >&2
    exit 1
fi

write_csv_header

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

warmup_sql=""
for table in "$@"; do
    for group in "${CACHE_PROFILES[@]}"; do
        IFS='|' read -r pname _ _ _ <<< "$group"
        warmup_sql="$(sql_file_for "$table" "${SQL_TYPES[0]}" "${ROW_COUNTS[0]}" "$pname")"
        [ -s "$warmup_sql" ] && break 2
    done
done

if [ -n "$warmup_sql" ] && [ -f "$warmup_sql" ] && [ "$WARMUP_TIMELIMIT" != "0" ]; then
    echo ">>> 预热 ${WARMUP_TIMELIMIT}s: $warmup_sql"
    # 预热使用第一个 profile 的 SET 行
    IFS='|' read -r warmup_pname _ _ _ <<< "${CACHE_PROFILES[0]}"
    warmup_set_line="$(session_set_line_for_profile "$warmup_pname")"
    set +e
    warmup_dir="$TMPDIR/warmup_queries"
    make_single_query_scripts_with_settings "$warmup_sql" "$warmup_set_line" "$warmup_dir"
    set +e
    run_benchmark_file "$warmup_dir" 1 "$TMPDIR/warmup.stderr" "$TMPDIR/warmup.log" "$WARMUP_TIMELIMIT" >/dev/null 2>&1
    set -e
    set -e
fi
echo ""

echo ">>> 开始测试"
for table in "$@"; do
    vec_col="${TABLE_VEC_COL[$table]}"
    dim="${TABLE_DIM[$table]}"

    for group in "${CACHE_PROFILES[@]}"; do
        IFS='|' read -r cache_profile plan_cache_mode enable_seqscan drop_buffer_cache <<< "$group"

        # 每个 profile 开始前清服务端缓存, 保证测量口径一致
        echo ""
        echo "--- cache profile: $cache_profile (plan_cache=$plan_cache_mode drop_buffer=$drop_buffer_cache) ---"
        drop_caches_for_profile "$plan_cache_mode" "$drop_buffer_cache"
        echo ""

        set_line="$(session_set_line_for_profile "$cache_profile")"

        # 诊断: 验证 SET 语句可正常执行并记录当前配置
        echo "   [诊断] 验证会话缓存配置..."
        verify_a=$(client_query "SET plan_cache_mode = force_custom_plan; SHOW plan_cache_mode;" 2>/dev/null || echo "失败")
        verify_b=$(client_query "SET plan_cache_mode = force_generic_plan; SHOW plan_cache_mode;" 2>/dev/null || echo "失败")
        echo "   [诊断] SET force_custom_plan → $verify_a"
        echo "   [诊断] SET force_generic_plan → $verify_b"
        client_query "SET plan_cache_mode = ${plan_cache_mode};" > /dev/null 2>&1 || true
        echo "   [诊断] 已恢复至: plan_cache_mode = ${plan_cache_mode}"

        # EXPLAIN 诊断: 检查计划缓存和缓冲池状态
        first_sql_type="${SQL_TYPES[0]}"
        first_row_count="${ROW_COUNTS[0]}"
        diagnose_cache_with_explain "$table" "$plan_cache_mode" "$first_sql_type" "$first_row_count" "$cache_profile" "profile=${cache_profile}"

        # 额外诊断: 计划缓存是否真正生效。
        # 注意: psql 的 EXPLAIN 走 simple 协议每次都重新规划, 永远无法反映计划缓存;
        # 必须用 PREPARE+EXECUTE (走 plancache.c GetCachedPlan) 才能得到真实结果。
        sample_sql_file=""
        sample_sql_file=$(sql_file_for "$table" "$first_sql_type" "$first_row_count" "$cache_profile" 2>/dev/null || echo "")
        if [ -s "$sample_sql_file" ]; then
            diagnose_plan_cache "$table" "$vec_col" "$sample_sql_file" "$first_sql_type"
            # 恢复本次 profile 的 plan_cache_mode
            client_query "SET plan_cache_mode = ${plan_cache_mode};" > /dev/null 2>&1 || true
        fi
        echo ""

        # 热启动暖场 (两种 profile 均 drop_buffer=false, 故都会执行本段):
        #   计时前把 (a) HNSW 索引工作集 和 (b) 堆表 都载入 shared_buffers+OS页缓存,
        #   保证每种 SQL 形态(含第 1 种, 以及紧跟在全表扫描形态之后的 with_*/subquery)
        #   都从热态开始。否则前一个全表扫描形态会经 LRU 把索引页刷出缓冲池,
        #   让后面的 <?> 形态在冷索引上测出 ~330 QPS 的伪影 (实测热态下它们与 const 同速)。
        # 注意: 每个 profile 的 SQL 文件各自独立随机抽样, 暖场必须用本 profile 的查询。
        if [ "$drop_buffer_cache" = "false" ]; then
            set +e
            # (a) HNSW 索引工作集: 用 <?> 索引形态(text_pase_op_id)的 1000 向量循环预热。
            warmup_sql_p="$(sql_file_for "$table" "${SQL_TYPES[0]}" "${ROW_COUNTS[0]}" "$cache_profile" 2>/dev/null || echo "")"
            if [ -s "$warmup_sql_p" ]; then
                echo "  暖场 HNSW 索引 (${BUFFER_WARMUP_SEC}s, profile=${cache_profile})..."
                warmup_dir="$TMPDIR/warmup_buffer"
                make_single_query_scripts_with_settings "$warmup_sql_p" "$set_line" "$warmup_dir" >/dev/null
                [ "$BUFFER_WARMUP_SEC" -gt 0 ] && run_benchmark_file "$warmup_dir" 1 "$TMPDIR/bufwarm.stderr" "$TMPDIR/bufwarm.log" "$BUFFER_WARMUP_SEC" >/dev/null 2>&1 || true
            fi
            # (b) 堆表: 用 <#> hash_op 顺序扫描形态把 1M 行堆表页预载入, 使 <#> 形态也处热态。
            if [ "$WARM_TABLE_FOR_SEQUENTIAL" = "true" ]; then
                warmup_tbl_sql="$(sql_file_for "$table" "hash_op_default" "${ROW_COUNTS[0]}" "$cache_profile" 2>/dev/null || echo "")"
                if [ -s "$warmup_tbl_sql" ]; then
                    echo "  暖场堆表 (${BUFFER_WARMUP_SEC}s, profile=${cache_profile})..."
                    warmup_tbl_dir="$TMPDIR/warmup_table"
                    make_single_query_scripts_with_settings "$warmup_tbl_sql" "$set_line" "$warmup_tbl_dir" >/dev/null
                    [ "$BUFFER_WARMUP_SEC" -gt 0 ] && run_benchmark_file "$warmup_tbl_dir" 1 "$TMPDIR/tblwarm.stderr" "$TMPDIR/tblwarm.log" "$BUFFER_WARMUP_SEC" >/dev/null 2>&1 || true
                fi
            fi
            set -e
            echo "  暖场完成 (HNSW 索引 + 堆表均已驻留内存)"
        fi

        for sql_type in "${SQL_TYPES[@]}"; do
            for row_count in "${ROW_COUNTS[@]}"; do
                sql_file="$(sql_file_for "$table" "$sql_type" "$row_count" "$cache_profile")"
            if [ ! -s "$sql_file" ]; then
                echo "跳过空 SQL 文件: $sql_file"
                continue
            fi

            for concurrency in "${CONCURRENCIES[@]}"; do
                    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                    echo "table=$table profile=$cache_profile type=$sql_type rows=$row_count concurrency=$concurrency"
                    echo "  plan_cache_mode=$plan_cache_mode  enable_seqscan=$enable_seqscan  drop_buffer_cache=$drop_buffer_cache"
                    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

                    qps_values=()
                    tx_qps_values=()
                    elapsed_values=()
                    transaction_values=()
                    failed_values=()
                    latency_values=()
                    statement_latency_values=()
                    for run in $(seq 1 "$REPEAT"); do
                        stderr_file="$TMPDIR/${ENGINE}_${table}_${cache_profile}_${sql_type}_${row_count}_${concurrency}_${run}.stderr"
                        log_file="$TMPDIR/${ENGINE}_${table}_${cache_profile}_${sql_type}_${row_count}_${concurrency}_${run}.log"
                        echo -n "(${run}/${REPEAT}) "

                        set +e
                        query_script_dir="$TMPDIR/${ENGINE}_${table}_${cache_profile}_${sql_type}_${row_count}_queries"
                        # 构建 pgbench 脚本。默认 (prepared 协议 + 可参数化形态) 把向量提升为
                        # 绑定参数 (:v -> $1), 使 plan_cache_mode (force_custom/generic) 真正生效;
                        # 否则 (simple 协议或 subquery 形态) 退回字面量脚本。
                        if is_parameterizable_sql_type "$sql_type" && [ "$PGBENCH_QUERY_MODE" = "prepared" ]; then
                            make_single_query_scripts_parameterized "$sql_file" "$set_line" "$query_script_dir" "$table" "$vec_col" "$sql_type" >/dev/null 2>&1
                        else
                            make_single_query_scripts_with_settings "$sql_file" "$set_line" "$query_script_dir" >/dev/null
                        fi
                        run_benchmark_file "$query_script_dir" "$concurrency" "$stderr_file" "$log_file"
                        exit_code=$?
                        set -e

                        if [ "$exit_code" -ne 0 ]; then
                            echo "失败(exit=$exit_code)，QPS=0"
                            tail -8 "$log_file" | sed 's/^/    /'
                            qps_values+=(0)
                            tx_qps_values+=(0)
                            elapsed_values+=(0)
                            transaction_values+=(0)
                            failed_values+=(1)
                            latency_values+=(0)
                            statement_latency_values+=(0)
                            continue
                        fi

                        IFS=$'\t' read -r qps tx_qps elapsed transactions failed latency statement_latency <<< "$(parse_benchmark_metrics "$(cat "$log_file")" 1)"
                        echo "QPS=$qps tx/s=$tx_qps elapsed=${elapsed}s tx=$transactions failed=$failed latency=${latency}ms stmt_avg=${statement_latency}ms"
                        qps_values+=("${qps:-0.000}"); tx_qps_values+=("${tx_qps:-0.000}")
                        elapsed_values+=("${elapsed:-0}"); transaction_values+=("${transactions:-0}")
                        failed_values+=("${failed:-0}"); latency_values+=("${latency:-0}"); statement_latency_values+=("${statement_latency:-0}")
                    done

                    sorted_qps=$(printf '%s\n' "${qps_values[@]}" | sort -n)
                    count=${#qps_values[@]}
                    qps_min=$(echo "$sorted_qps" | head -1)
                    qps_max=$(echo "$sorted_qps" | tail -1)
                    if [ "$count" -le 2 ]; then
                        qps_avg=$(printf '%s\n' "${qps_values[@]}" | awk '{sum += $1; n++} END {printf "%.3f", n ? sum / n : 0}')
                    else
                        qps_avg=$(echo "$sorted_qps" | sed '1d;$d' | awk '{sum += $1; n++} END {printf "%.3f", n ? sum / n : 0}')
                    fi

                    echo "  => avg=$qps_avg min=$qps_min max=$qps_max"
                    tx_qps_avg=$(printf '%s\n' "${tx_qps_values[@]}" | awk '{sum += $1; n++} END {printf "%.3f", n ? sum / n : 0}')
                    elapsed_avg=$(printf '%s\n' "${elapsed_values[@]}" | awk '{sum += $1; n++} END {printf "%.3f", n ? sum / n : 0}')
                    transactions_avg=$(printf '%s\n' "${transaction_values[@]}" | awk '{sum += $1; n++} END {printf "%.3f", n ? sum / n : 0}')
                    failed_avg=$(printf '%s\n' "${failed_values[@]}" | awk '{sum += $1; n++} END {printf "%.3f", n ? sum / n : 0}')
                    latency_avg=$(printf '%s\n' "${latency_values[@]}" | awk '{sum += $1; n++} END {printf "%.3f", n ? sum / n : 0}')
                    statement_latency_avg=$(printf '%s\n' "${statement_latency_values[@]}" | awk '{sum += $1; n++} END {printf "%.3f", n ? sum / n : 0}')

                    csv_line="${ENGINE},${SERVER_VERSION},${table},${vec_col},${dim},${DISTANCE_OP},${PASE_EXTRA},${cache_profile},${plan_cache_mode},${enable_seqscan},${drop_buffer_cache},${sql_type},${row_count},${concurrency}"
                    for value in "${qps_values[@]}"; do
                        csv_line="${csv_line},${value}"
                    done
                    csv_line="${csv_line},${qps_avg},${qps_min},${qps_max}"
                    csv_line="${csv_line},${tx_qps_avg},${elapsed_avg},${transactions_avg},${failed_avg},${latency_avg},${statement_latency_avg}"
                    echo "$csv_line" >> "$OUTPUT_CSV"
                done
            done
        done
    done
done

for table in "$@"; do
    summarize_plan_cache_comparison "$table"
done

echo "=============================================="
echo "测试完成: $OUTPUT_CSV"
echo "=============================================="
tail -20 "$OUTPUT_CSV" | column -t -s',' 2>/dev/null || tail -20 "$OUTPUT_CSV"