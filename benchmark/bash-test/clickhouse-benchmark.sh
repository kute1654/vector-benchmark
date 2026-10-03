#!/bin/bash
#
# ClickHouse / MyScale 向量查询性能基准测试脚本 (bash 版)
# 支持录入表名，根据表名自动生成SQL，测试结果写入CSV
#
# 用法:
#   ./clickhouse-benchmark.sh [表名1] [表名2] ...
#   不带参数时交互式输入表名
#
# 引擎选择 (环境变量 ENGINE, 默认 clickhouse):
#   ENGINE=myscale    ./clickhouse-benchmark.sh 表名    # 连 MyScale, 检索函数固定为 distance() 一族 (走 ANN 索引)
#   ENGINE=clickhouse ./clickhouse-benchmark.sh 表名    # 默认, L2Distance/cosineDistance/dotProduct
#
# MyScale 专属配置 (环境变量):
#   MYS_SCALE_EF       distance() 检索参数 ef_s (默认 100)
#   MYS_SEARCH_PARAMS  额外检索参数, 逗号分隔, 如 MYS_SEARCH_PARAMS="k=20,alpha=16"
#
# 说明: 计划缓存/结果缓存开关必须用会话级 `SET` 装配 (与 run.py 客户端
#   apply_query_plan_cache_settings 一致)。逐条查询内联的 `SETTINGS ...` 子句在
#   enable_vector_performance_test 打开时每次执行都要被重新解析, 会拖慢 ~1ms 的
#   ANN 查询、抵消缓存收益。因此脚本在运行副本头部写入本 profile 的会话级 SET 行,
#   并把 SQL 行尾的内联 SETTINGS 子句剥掉 (磁盘上的 .sql 生成文件保持不变);
#   关闭 --randomize 保证 SET 行先于查询在同一条会话上执行。
#   服务器支持 enable_vector_performance_test 时另写一行该会话开关 (不支持自动省略)。
#
# 测试维度: engine × table × settings_profile × precise_float_parsing × sql_type(normal/cast/cast_array/type_hint/raw_bytes/raw_bytes_x/with/with_cast/with_cast_array/with_type_hint/with_raw_bytes/with_raw_bytes_x/subquery_id/with_subquery_id) × row_count × concurrency
#


set -euo pipefail

# ============ 配置 ============
ENGINE="${ENGINE:-clickhouse}"
if [ "$ENGINE" = "myscale" ]; then
    CLICKHOUSE="${CLICKHOUSE:-/home/clickhouse/build/programs/clickhouse}"
else
    CLICKHOUSE="${CLICKHOUSE:-/home/ClickHouse/build/programs/clickhouse}"
fi
HOST="127.0.0.1"
PORT="9000"
MYS_SCALE_EF="${MYS_SCALE_EF:-100}"
MYS_SEARCH_PARAMS="${MYS_SEARCH_PARAMS:-}"
TIMELIMIT="${TIMELIMIT:-1}"
WARMUP_TIMELIMIT="${WARMUP_TIMELIMIT:-1}"
REPEAT="${REPEAT:-1}"
if [ "$ENGINE" = "myscale" ]; then
    OUTPUT_CSV="${OUTPUT_CSV:-../results/myscale-benchmark-results.csv}"
else
    OUTPUT_CSV="${OUTPUT_CSV:-../results/clickhouse-benchmark-results.csv}"
fi
SQL_DIR="${SQL_DIR:-sql-bench}"
TOP_K="${TOP_K:-10}"

SQL_TYPES=(
    normal
    cast
    cast_array
    raw_bytes
    raw_bytes_x
)
ROW_COUNTS=(1000)
CONCURRENCIES=(8)
PRECISE_FLOAT_PARSING_VALUES=(1)

if [ "$ENGINE" = "myscale" ]; then
    # MyScale profile 字段(f1..f5):
    #   f1=use_query_cache, f2=enable_query_plan_cache, f3=enable_cast_vector,
    #   f4=query_plan_cache_only_vector, f5=only_cache_query_plan
    SETTINGS_GROUPS=(
        "off|0|0|0|0|0"
        "plan_cache|0|1|0|0|0"
        "plan_cast|0|1|1|0|0"
        "plan_only_vector|0|1|0|1|0"
        "only_plan_cache|0|1|0|0|1"
        "only_plan_cast|0|1|1|0|1"
        "only_plan_only_vector|0|1|0|1|1"
        "query_off|1|0|0|0|0"
        "query_plan_cache|1|1|0|0|0"
        "query_plan_cast|1|1|1|0|0"
        "query_plan_only_vector|1|1|0|1|0"
        "query_only_plan_cache|1|1|0|0|1"
        "query_only_plan_cast|1|1|1|0|1"
        "query_only_plan_only_vector|1|1|0|1|1"
    )
else
    # clickhouse profile 字段:
    #   f1=use_query_cache, f2=vector_query_plan_cache, f3=vector_only_cache_query_plan,
    #   f4=vector_query_plan_cache_only_vector, f5=vector_use_cast
    # use_query_cache 与 vector_query_plan_cache 可同时开启;
    # f3/f4=1 仅在 f2=1 时有效。
    SETTINGS_GROUPS=(
        "ck_off|0|0|0|0|0"
        "ck_plan_cache|0|1|0|0|0"
        "ck_only_plan_cache|0|1|1|0|0"
        "ck_plan_cast|0|1|0|0|1"
        "ck_only_plan_cast|0|1|1|0|1"
        "result_off|1|0|0|0|0"
        "result_plan_cache|1|1|0|0|0"
        "result_only_plan_cache|1|1|1|0|0"
        "result_plan_cast|1|1|0|0|1"
        "result_only_plan_cast|1|1|1|0|1"
    )
fi

# MyScale 不支持子查询形式的向量参数
MYS_SCALE_UNSUPPORTED_TYPES=(subquery_id with_subquery_id)

# ============ 工具函数 ============

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
apply_list_override PRECISE_FLOAT_PARSING_VALUES PRECISE_FLOAT_PARSING_OVERRIDE
apply_list_override SETTINGS_GROUPS SETTINGS_GROUPS_OVERRIDE

case "$ENGINE" in
    clickhouse|myscale) ;;
    *) echo "错误: ENGINE 只支持 clickhouse 或 myscale (当前: $ENGINE)" >&2; exit 1 ;;
esac

if [ "$ENGINE" = "myscale" ]; then
    filtered=()
    removed=()
    for t in "${SQL_TYPES[@]}"; do
        if [[ " ${MYS_SCALE_UNSUPPORTED_TYPES[*]} " == *" $t "* ]]; then
            removed+=("$t")
        else
            filtered+=("$t")
        fi
    done
    if [ ${#removed[@]} -gt 0 ]; then
        echo "注意: ENGINE=myscale, 以下 SQL 形式不支持(reinterpret Array 为 String), 已跳过: ${removed[*]}"
    fi
    SQL_TYPES=("${filtered[@]}")
fi

# ---- 引擎相关工具函数 ----

# 组装本引擎的距离表达式: clickhouse=L2Distance/cosineDistance/dotProduct；
# myscale 固定为 distance('ef_s=100', ...)(vector, q) 形式。
build_dist_expr() {
    local val_col="$1"
    local literal="$2"

    if [ "$ENGINE" = "myscale" ]; then
        local params=""
        if [ -n "$MYS_SCALE_EF" ]; then
            params="'ef_s=${MYS_SCALE_EF}'"
        fi
        if [ -n "$MYS_SEARCH_PARAMS" ]; then
            local pair
            IFS=',' read -r -a pairs <<< "$MYS_SEARCH_PARAMS"
            for pair in "${pairs[@]}"; do
                pair="$(echo "$pair" | tr -d ' ')"
                [ -z "$pair" ] && continue
                if [ -n "$params" ]; then
                    params="${params}, '${pair}'"
                else
                    params="'${pair}'"
                fi
            done
        fi
        if [ -n "$params" ]; then
            echo "distance(${params})(${val_col}, ${literal})"
        else
            echo "distance(${val_col}, ${literal})"
        fi
    else
        echo "${DISTANCE_FUNC}(${val_col}, ${literal})"
    fi
}

# 服务器是否支持 enable_vector_performance_test (决定是否在 SQL 文件头部写 SET 行)
# 0/1/未检测三种状态; 检测失败时视为不支持, 不写 SET 行。
PERF_TEST_FLAG=""
detect_perf_test_flag() {
    if [ -n "$PERF_TEST_FLAG" ]; then
        return
    fi
    local cnt
    cnt=$("$CLICKHOUSE" client --host "$HOST" --port "$PORT" \
        --query "SELECT count() FROM system.settings WHERE name='enable_vector_performance_test'" 2>/dev/null) || true
    PERF_TEST_FLAG="$(echo "${cnt:-0}" | tr -dc '0-9')"
    if [ "$PERF_TEST_FLAG" = "1" ]; then
        echo "  ✓ 服务器支持 enable_vector_performance_test, 将在 SQL 文件头部写入 SET 行"
    else
        echo "  提示: 服务器不支持 enable_vector_performance_test (per-query SETTINGS 将不会进入计划缓存探测)"
    fi
}

# 每个 profile 开始前清服务端缓存, 使首行 QPS 测量口径一致
drop_caches_for_profile() {
    local plan_cache_enabled="$1"

    "$CLICKHOUSE" client --host "$HOST" --port "$PORT" \
        --query "SYSTEM DROP QUERY CACHE" >/dev/null 2>&1 || true

    if [ "$plan_cache_enabled" != "1" ]; then
        return
    fi
    if [ "$ENGINE" = "myscale" ]; then
        "$CLICKHOUSE" client --host "$HOST" --port "$PORT" \
            --query "SYSTEM DROP QUERY PLAN CACHE" >/dev/null 2>&1 || true
    else
        "$CLICKHOUSE" client --host "$HOST" --port "$PORT" \
            --query "SYSTEM DROP VECTOR QUERY PLAN CACHE" >/dev/null 2>&1 || true
    fi
}

detect_vector_column() {
    local table="$1"
    local col
    col=$("$CLICKHOUSE" client \
        --host "$HOST" \
        --port "$PORT" \
        --query "SELECT name FROM system.columns WHERE database='default' AND table='$table' AND type LIKE 'Array(Float%)' LIMIT 1 SETTINGS use_query_cache=0" 2>/dev/null) || true
    echo "${col}"
}

detect_vector_dimension() {
    local table="$1"
    local vec_col="$2"
    local dim
    dim=$("$CLICKHOUSE" client \
        --host "$HOST" \
        --port "$PORT" \
        --query "SELECT length($vec_col) FROM $table LIMIT 1 SETTINGS use_query_cache=0" 2>/dev/null) || true
    echo "${dim:-0}"
}

# MyScale 的 distance() + 原生 benchmark loader 会把纯整数文本 "[3,2,8]" 解析为
# UInt 数组并报 UNEXPECTED_DATA_TYPE; 只有 float 字面量能进 ANN 路径。该函数给
# 整数 token 补 ".0", 使字面量解析为 Array(Float64) (与 query_forms.py 里
# json.dumps 的输出一致)。
floatify_vec_str() {
    # 仅对"整个 token 都是整数"的值补 .0; "1e-05"、"-0.5" 等原样保留
    echo "$1" | awk -F',' '{ for (i=1; i<=NF; ++i) if ($i ~ /^-?[0-9]+$/) $i = $i ".0"; print $0 }' OFS=','
}

# 将逗号分隔的 float 值转换为 IEEE 754 little-endian hex 字符串 (大写)。
# 用于 MyScale 下生成 reinterpret(unhex('...')) / reinterpret(x'...') 所需的 hex。
# 例: "1.0,2.0" -> "0000803F00000040"
float_vec_to_hex() {
    python3 -c "
import struct, sys
vals = sys.argv[1].split(',')
sys.stdout.write(struct.pack('<' + 'f' * len(vals), *(float(v) for v in vals)).hex().upper())
" "$1"
}

generate_sql_for_table() {
    local table="$1"
    local vec_col="$2"
    local dist_func="$3"
    local sort_dir="$4"
    local settings_profile="$5"
    local settings_clause="$6"

    if [ "$ENGINE" = "myscale" ]; then
        dist_func="distance(ef_s=${MYS_SCALE_EF})"
    fi
    echo "  生成SQL文件 (表=$table, 引擎=$ENGINE, profile=$settings_profile, 列=$vec_col, 距离=$dist_func, 排序=$sort_dir)..."

    for count in "${ROW_COUNTS[@]}"; do
        local tmp_vectors
        local tmp_error
        tmp_vectors=$(mktemp)
        tmp_error=$(mktemp)

        # MyScale 不支持 reinterpretAsString(Array(Float32)), 不取 hex 列;
        # raw_bytes/raw_bytes_x 所需的 hex 由 float_vec_to_hex() 从 float 值在 bash 侧计算。
        if [ "$ENGINE" = "myscale" ]; then
            sample_query="SELECT id, arrayStringConcat($vec_col, ',') FROM $table ORDER BY rand() LIMIT $count SETTINGS use_query_cache=0"
        else
            sample_query="SELECT id, arrayStringConcat($vec_col, ','), hex(reinterpretAsString($vec_col)) FROM $table ORDER BY rand() LIMIT $count SETTINGS use_query_cache=0"
        fi

        if ! "$CLICKHOUSE" client \
            --host "$HOST" \
            --port "$PORT" \
            --query "$sample_query" \
            > "$tmp_vectors" 2>"$tmp_error"; then
            echo "  错误: 表 $table 抽取 ${count} 条样本向量失败，无法生成 SQL"
            echo "  $ENGINE 错误输出:"
            tail -20 "$tmp_error" 2>/dev/null | sed 's/^/    /'
            rm -f "$tmp_vectors" "$tmp_error"
            return 1
        fi

        local actual_count
        actual_count=$(wc -l < "$tmp_vectors")
        if [ "$actual_count" -ne "$count" ]; then
            echo "  警告: 表 $table 预期 ${count} 条，实际获取 ${actual_count} 条"
        fi

        local normal_file="$SQL_DIR/${table}_${settings_profile}_normal_${count}.sql"
        > "$normal_file"
        while IFS=$'\t' read -r _id vec_str _vec_hex; do
            [ -z "$vec_str" ] && continue
            local lit_vec="$vec_str"
            if [ "$ENGINE" = "myscale" ]; then
                lit_vec=$(floatify_vec_str "$vec_str")
            fi
            echo "SELECT id, $(build_dist_expr "$vec_col" "[${lit_vec}]") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$normal_file"
        done < "$tmp_vectors"
        echo "    已生成: $normal_file ($(wc -l < "$normal_file") 条查询)"

        local cast_file="$SQL_DIR/${table}_${settings_profile}_cast_${count}.sql"
        > "$cast_file"
        while IFS=$'\t' read -r _id vec_str _vec_hex; do
            [ -z "$vec_str" ] && continue
            echo "SELECT id, $(build_dist_expr "$vec_col" "cast('[${vec_str}]','Array(Float32)')") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$cast_file"
        done < "$tmp_vectors"
        echo "    已生成: $cast_file ($(wc -l < "$cast_file") 条查询)"

        local cast_array_file="$SQL_DIR/${table}_${settings_profile}_cast_array_${count}.sql"
        > "$cast_array_file"
        while IFS=$'\t' read -r _id vec_str _vec_hex; do
            [ -z "$vec_str" ] && continue
            echo "SELECT id, $(build_dist_expr "$vec_col" "CAST([${vec_str}] AS Array(Float32))") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$cast_array_file"
        done < "$tmp_vectors"
        echo "    已生成: $cast_array_file ($(wc -l < "$cast_array_file") 条查询)"

        local type_hint_file="$SQL_DIR/${table}_${settings_profile}_type_hint_${count}.sql"
        > "$type_hint_file"
        while IFS=$'\t' read -r _id vec_str _vec_hex; do
            [ -z "$vec_str" ] && continue
            echo "SELECT id, $(build_dist_expr "$vec_col" "[${vec_str}]::Array(Float32)") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$type_hint_file"
        done < "$tmp_vectors"
        echo "    已生成: $type_hint_file ($(wc -l < "$type_hint_file") 条查询)"

        local raw_bytes_file="$SQL_DIR/${table}_${settings_profile}_raw_bytes_${count}.sql"
        > "$raw_bytes_file"
        while IFS=$'\t' read -r _id vec_str vec_hex; do
            [ -z "$vec_str" ] && continue
            if [ -z "$vec_hex" ]; then
                vec_hex=$(float_vec_to_hex "$vec_str")
            fi
            echo "SELECT id, $(build_dist_expr "$vec_col" "reinterpret(unhex('${vec_hex}'), 'Array(Float32)')") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$raw_bytes_file"
        done < "$tmp_vectors"
        echo "    已生成: $raw_bytes_file ($(wc -l < "$raw_bytes_file") 条查询)"

        local raw_bytes_x_file="$SQL_DIR/${table}_${settings_profile}_raw_bytes_x_${count}.sql"
        > "$raw_bytes_x_file"
        while IFS=$'\t' read -r _id vec_str vec_hex; do
            [ -z "$vec_str" ] && continue
            if [ -z "$vec_hex" ]; then
                vec_hex=$(float_vec_to_hex "$vec_str")
            fi
            echo "SELECT id, $(build_dist_expr "$vec_col" "reinterpret(x'${vec_hex}', 'Array(Float32)')") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$raw_bytes_x_file"
        done < "$tmp_vectors"
        echo "    已生成: $raw_bytes_x_file ($(wc -l < "$raw_bytes_x_file") 条查询)"

        local with_file="$SQL_DIR/${table}_${settings_profile}_with_${count}.sql"
        > "$with_file"
        while IFS=$'\t' read -r _id vec_str _vec_hex; do
            [ -z "$vec_str" ] && continue
            local lit_vec="$vec_str"
            if [ "$ENGINE" = "myscale" ]; then
                lit_vec=$(floatify_vec_str "$vec_str")
            fi
            echo "WITH [${lit_vec}] AS query_vector SELECT id, $(build_dist_expr "$vec_col" "query_vector") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$with_file"
        done < "$tmp_vectors"
        echo "    已生成: $with_file ($(wc -l < "$with_file") 条查询)"

        local with_cast_file="$SQL_DIR/${table}_${settings_profile}_with_cast_${count}.sql"
        > "$with_cast_file"
        while IFS=$'\t' read -r _id vec_str _vec_hex; do
            [ -z "$vec_str" ] && continue
            echo "WITH cast('[${vec_str}]','Array(Float32)') AS query_vector SELECT id, $(build_dist_expr "$vec_col" "query_vector") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$with_cast_file"
        done < "$tmp_vectors"
        echo "    已生成: $with_cast_file ($(wc -l < "$with_cast_file") 条查询)"

        local with_cast_array_file="$SQL_DIR/${table}_${settings_profile}_with_cast_array_${count}.sql"
        > "$with_cast_array_file"
        while IFS=$'\t' read -r _id vec_str _vec_hex; do
            [ -z "$vec_str" ] && continue
            echo "WITH CAST([${vec_str}] AS Array(Float32)) AS query_vector SELECT id, $(build_dist_expr "$vec_col" "query_vector") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$with_cast_array_file"
        done < "$tmp_vectors"
        echo "    已生成: $with_cast_array_file ($(wc -l < "$with_cast_array_file") 条查询)"

        local with_type_hint_file="$SQL_DIR/${table}_${settings_profile}_with_type_hint_${count}.sql"
        > "$with_type_hint_file"
        while IFS=$'\t' read -r _id vec_str _vec_hex; do
            [ -z "$vec_str" ] && continue
            echo "WITH [${vec_str}]::Array(Float32) AS query_vector SELECT id, $(build_dist_expr "$vec_col" "query_vector") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$with_type_hint_file"
        done < "$tmp_vectors"
        echo "    已生成: $with_type_hint_file ($(wc -l < "$with_type_hint_file") 条查询)"

        local with_raw_bytes_file="$SQL_DIR/${table}_${settings_profile}_with_raw_bytes_${count}.sql"
        > "$with_raw_bytes_file"
        while IFS=$'\t' read -r _id vec_str vec_hex; do
            [ -z "$vec_str" ] && continue
            if [ -z "$vec_hex" ]; then
                vec_hex=$(float_vec_to_hex "$vec_str")
            fi
            echo "WITH reinterpret(unhex('${vec_hex}'), 'Array(Float32)') AS query_vector SELECT id, $(build_dist_expr "$vec_col" "query_vector") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$with_raw_bytes_file"
        done < "$tmp_vectors"
        echo "    已生成: $with_raw_bytes_file ($(wc -l < "$with_raw_bytes_file") 条查询)"

        local with_raw_bytes_x_file="$SQL_DIR/${table}_${settings_profile}_with_raw_bytes_x_${count}.sql"
        > "$with_raw_bytes_x_file"
        while IFS=$'\t' read -r _id vec_str vec_hex; do
            [ -z "$vec_str" ] && continue
            if [ -z "$vec_hex" ]; then
                vec_hex=$(float_vec_to_hex "$vec_str")
            fi
            echo "WITH reinterpret(x'${vec_hex}', 'Array(Float32)') AS query_vector SELECT id, $(build_dist_expr "$vec_col" "query_vector") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$with_raw_bytes_x_file"
        done < "$tmp_vectors"
        echo "    已生成: $with_raw_bytes_x_file ($(wc -l < "$with_raw_bytes_x_file") 条查询)"

        local subquery_id_file="$SQL_DIR/${table}_${settings_profile}_subquery_id_${count}.sql"
        > "$subquery_id_file"
        while IFS=$'\t' read -r id _vec_str _vec_hex; do
            [ -z "$id" ] && continue
            echo "SELECT id, $(build_dist_expr "$vec_col" "(SELECT ${vec_col} FROM ${table} WHERE id = ${id})") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$subquery_id_file"
        done < "$tmp_vectors"
        echo "    已生成: $subquery_id_file ($(wc -l < "$subquery_id_file") 条查询)"

        local with_subquery_id_file="$SQL_DIR/${table}_${settings_profile}_with_subquery_id_${count}.sql"
        > "$with_subquery_id_file"
        while IFS=$'\t' read -r id _vec_str _vec_hex; do
            [ -z "$id" ] && continue
            echo "WITH (SELECT ${vec_col} FROM ${table} WHERE id = ${id}) AS query_vector SELECT id, $(build_dist_expr "$vec_col" "query_vector") as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K}${settings_clause};" >> "$with_subquery_id_file"
        done < "$tmp_vectors"
        echo "    已生成: $with_subquery_id_file ($(wc -l < "$with_subquery_id_file") 条查询)"

        rm -f "$tmp_vectors" "$tmp_error"
    done
}

build_settings_clause() {
    # profile 字段统一位置: f1=use_query_cache, f2,f3,f4,f5 含义随引擎不同
    #   clickhouse: f2=vector_query_plan_cache, f3=vector_only_cache_query_plan,
    #               f4=vector_query_plan_cache_only_vector, f5=vector_use_cast
    #   myscale:    f2=enable_query_plan_cache, f3=enable_cast_vector,
    #               f4=query_plan_cache_only_vector, f5=only_cache_query_plan
    local f1="$1"
    local f2="$2"
    local f3="$3"
    local f4="$4"
    local f5="$5"

    if [ "$ENGINE" = "myscale" ]; then
        echo " SETTINGS use_query_cache=${f1}, enable_query_plan_cache=${f2}, enable_cast_vector=${f3}, query_plan_cache_only_vector=${f4}, only_cache_query_plan=${f5}"
    else
        echo " SETTINGS use_query_cache=${f1}, vector_query_plan_cache=${f2}, vector_only_cache_query_plan=${f3}, vector_query_plan_cache_only_vector=${f4}, vector_use_cast=${f5}"
    fi
}

# 生成当前 profile 的会话级 SET 行 (与 run.py 客户端 apply_query_plan_cache_settings
# 的装配方式一致)。计划缓存等开关必须在会话级设置: 内联 SETTINGS 子句会为每条查询
# 增加一次解析开销, 实测使 plan_cache QPS 反而低于 off; 会话级 SET 只在连接建立时付一次,
# plan_cache 相对 off 有 ~+20% 的真实收益。
session_set_line_for_profile() {
    local f1="$1"
    local f2="$2"
    local f3="$3"
    local f4="$4"
    local f5="$5"

    if [ "$ENGINE" = "myscale" ]; then
        echo "SET use_query_cache = ${f1}, enable_query_plan_cache = ${f2}, enable_cast_vector = ${f3}, query_plan_cache_only_vector = ${f4}, only_cache_query_plan = ${f5}"
    else
        echo "SET use_query_cache = ${f1}, vector_query_plan_cache = ${f2}, vector_only_cache_query_plan = ${f3}, vector_query_plan_cache_only_vector = ${f4}, vector_use_cast = ${f5}"
    fi
}

validate_settings_groups() {
    local group profile_name f1 f2 f3 f4 f5
    for group in "${SETTINGS_GROUPS[@]}"; do
        IFS='|' read -r profile_name f1 f2 f3 f4 f5 <<< "$group"
        # 通用约束: 面向向量的开关 (f4=vector_query_plan_cache_only_vector / query_plan_cache_only_vector)
        # 依赖总开关 f2 已开启
        if [ "$f4" = "1" ] && [ "$f2" != "1" ]; then
            echo "错误: settings profile '$profile_name' 非法: 第4个开关(仅向量缓存)=1 需要第2个开关(计划缓存)=1" >&2
            exit 1
        fi
        # myscale 约束: only_cache_query_plan (f5) 依赖 enable_query_plan_cache (f2)
        if [ "$ENGINE" = "myscale" ] && [ "$f5" = "1" ] && [ "$f2" != "1" ]; then
            echo "错误: settings profile '$profile_name' 非法: only_cache_query_plan=1 需要 enable_query_plan_cache=1" >&2
            exit 1
        fi
        # clickhouse 特有约束: vector_only_cache_query_plan 依赖 vector_query_plan_cache
        if [ "$ENGINE" = "clickhouse" ] && [ "$f3" = "1" ] && [ "$f2" != "1" ]; then
            echo "错误: settings profile '$profile_name' 非法: vector_only_cache_query_plan=1 需要 vector_query_plan_cache=1" >&2
            exit 1
        fi
    done
}

parse_total_qps() {
    local stderr_output="$1"
    echo "$stderr_output" | grep -oP 'QPS:\s*\K[0-9.]+' | awk '{sum += $1} END {printf "%.3f", sum}'
}

# MyScale 版本索引检查: system.vector_indices 看状态, EXPLAIN 命中断言 ReadWithHybridSearch
check_vector_index_myscale() {
    local table="$1"
    local vec_col="$2"

    local index_status
    index_status=$("$CLICKHOUSE" client \
        --host "$HOST" \
        --port "$PORT" \
        --query "SELECT name, type, status FROM system.vector_indices WHERE database='default' AND table='$table' SETTINGS use_query_cache=0" 2>/dev/null) || true

    if [ -z "$index_status" ]; then
        echo "  ⚠ 表 $table: system.vector_indices 无记录 (MyScale 未建立向量索引)"
        echo "    查询将使用全表扫描 (ReadFromMergeTree)"
        return 1
    fi

    local index_name index_type status
    index_name=$(echo "$index_status" | awk '{print $1}')
    index_type=$(echo "$index_status" | awk '{print $2}')
    status=$(echo "$index_status" | awk '{print $3}')

    if [ "$status" != "Built" ]; then
        echo "  ⚠ 表 $table: 向量索引未构建完成 (name=$index_name, type=$index_type, status=$status)"
        echo "    请等待索引构建完成后再测试"
        return 1
    fi

    local sample_vec
    sample_vec=$("$CLICKHOUSE" client \
        --host "$HOST" \
        --port "$PORT" \
        --query "SELECT arrayStringConcat($vec_col, ',') FROM $table LIMIT 1 SETTINGS use_query_cache=0" 2>/dev/null) || true

    if [ -z "$sample_vec" ]; then
        echo "  ⚠ 表 $table: 无法获取样本向量进行 EXPLAIN 检测"
        echo "    索引信息: name=$index_name type=$index_type status=$status"
        return 1
    fi

    local dist_expr
    dist_expr=$(build_dist_expr "$vec_col" "cast('[${sample_vec}]','Array(Float32)')")
    local explain_output
    explain_output=$("$CLICKHOUSE" client \
        --host "$HOST" \
        --port "$PORT" \
        --query "EXPLAIN SELECT id, ${dist_expr} as dis FROM ${table} ORDER BY dis ASC LIMIT 10 SETTINGS use_query_cache=0" 2>/dev/null) || true

    if echo "$explain_output" | grep -q "ReadWithHybridSearch"; then
        echo "  ✓ 表 $table: 向量索引生效 (EXPLAIN 命中 ReadWithHybridSearch)"
        echo "    索引信息: name=$index_name type=$index_type status=$status"
        return 0
    fi

    echo "  ⚠ 表 $table: 向量索引存在但该查询未走混合检索 (ReadWithHybridSearch)"
    echo "    索引信息: name=$index_name type=$index_type status=$status"
    echo "    可能原因: 距离函数/维度与索引不匹配, 或查询语句形式不正确"
    echo "    EXPLAIN 输出:"
    echo "$explain_output" | head -15 | sed 's/^/      /'
    return 1
}

check_vector_index() {
    local table="$1"
    local vec_col="$2"
    local dist_func="$3"

    if [ "$ENGINE" = "myscale" ]; then
        check_vector_index_myscale "$table" "$vec_col"
        return $?
    fi

    local index_info
    index_info=$("$CLICKHOUSE" client \
        --host "$HOST" \
        --port "$PORT" \
        --query "SELECT name, type, data_compressed_bytes FROM system.data_skipping_indices WHERE database='default' AND table='$table' SETTINGS use_query_cache=0" 2>/dev/null) || true

    if [ -z "$index_info" ]; then
        echo "  ⚠ 表 $table: 未找到向量索引 (system.data_skipping_indices 无记录)"
        echo "    查询将使用全表扫描 (Read type: Default)"
        return 1
    fi

    local index_name index_type compressed_bytes
    index_name=$(echo "$index_info" | awk '{print $1}')
    index_type=$(echo "$index_info" | awk '{print $2}')
    compressed_bytes=$(echo "$index_info" | awk '{print $3}')

    if [ "$compressed_bytes" = "0" ] || [ -z "$compressed_bytes" ]; then
        echo "  ⚠ 表 $table: 向量索引已定义但未构建 (name=$index_name, type=$index_type, size=0)"
        echo "    可能需要执行: ALTER TABLE $table MATERIALIZE SKIP INDEX vector_index"
        return 1
    fi

    local pending_mutations
    pending_mutations=$("$CLICKHOUSE" client \
        --host "$HOST" \
        --port "$PORT" \
        --query "SELECT count() FROM system.mutations WHERE database='default' AND table='$table' AND is_done=0 SETTINGS use_query_cache=0" 2>/dev/null) || true
    pending_mutations=${pending_mutations:-0}

    if [ "$pending_mutations" -gt 0 ]; then
        echo "  ⚠ 表 $table: 向量索引正在构建中 (pending_mutations=$pending_mutations)"
        return 1
    fi

    local sample_vec
    sample_vec=$("$CLICKHOUSE" client \
        --host "$HOST" \
        --port "$PORT" \
        --query "SELECT arrayStringConcat($vec_col, ',') FROM $table LIMIT 1 SETTINGS use_query_cache=0" 2>/dev/null) || true

    if [ -n "$sample_vec" ]; then
        local explain_output
        explain_output=$("$CLICKHOUSE" client \
            --host "$HOST" \
            --port "$PORT" \
            --query "EXPLAIN indexes=1 SELECT id, ${dist_func}(${vec_col}, cast('[${sample_vec}]','Array(Float32)')) as dis FROM ${table} ORDER BY dis ASC LIMIT 10 SETTINGS use_query_cache=0" 2>/dev/null) || true

        if echo "$explain_output" | grep -q "Name: ${index_name}" && echo "$explain_output" | grep -q "Description: ${index_type}"; then
            local skip_granules
            skip_granules=$(echo "$explain_output" | awk '
                found && /Granules:/ {gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print; exit}
                $0 ~ "Name: " idx {found=1}
            ' idx="$index_name")
            echo "  ✓ 表 $table: 向量索引生效 (EXPLAIN indexes=1 命中 Skip index)"
            echo "    索引信息: name=$index_name type=$index_type size=${compressed_bytes}B"
            if [ -n "$skip_granules" ]; then
                echo "    $skip_granules"
            fi
            return 0
        else
            echo "  ⚠ 表 $table: 向量索引存在但当前查询未命中 Skip index"
            echo "    索引信息: name=$index_name type=$index_type size=${compressed_bytes}B"
             echo "    可能原因: 距离函数与索引定义不一致，查询向量类型未识别，或索引未覆盖数据"
            echo "    EXPLAIN 输出:"
            echo "$explain_output" | head -25 | sed 's/^/      /'
            return 1
        fi
    fi

    echo "  ? 表 $table: 无法获取样本向量进行 EXPLAIN 检测"
    echo "    索引信息: name=$index_name type=$index_type size=${compressed_bytes}B"
    return 2
}

# ============ 主逻辑 ============

echo "=============================================="
echo " ClickHouse / MyScale 向量查询基准测试"
echo "=============================================="
echo "引擎:   $ENGINE"
echo "二进制: $CLICKHOUSE"
echo "目标:   $HOST:$PORT"
echo "时长:   ${TIMELIMIT}s / 次"
echo "重复:   $REPEAT 次 (去掉最大最小值取均值)"
echo "并发:   ${CONCURRENCIES[*]}"
echo "类型:   ${SQL_TYPES[*]}"
echo "行数:   ${ROW_COUNTS[*]}"
echo "浮点解析 precise_float_parsing: ${PRECISE_FLOAT_PARSING_VALUES[*]}"
echo "输出:   $OUTPUT_CSV"
echo "=============================================="
echo ""

if [ ! -x "$CLICKHOUSE" ]; then
    echo "错误: 找不到 clickhouse 可执行文件: $CLICKHOUSE"
    exit 1
fi

# ---- Step 1: 输入表名 ----
echo ">>> Step 1: 输入测试表名"
echo ""

TABLES=()

if [ $# -gt 0 ]; then
    for t in "$@"; do
        TABLES+=("$t")
    done
    echo "从命令行参数获取表名: ${TABLES[*]}"
else
    echo "当前数据库中的表 (含 Array(Float) 列):"
    "$CLICKHOUSE" client --host "$HOST" --port "$PORT" \
        --query "SELECT DISTINCT table FROM system.columns WHERE database='default' AND type LIKE 'Array(Float%)' ORDER BY table SETTINGS use_query_cache=0" 2>/dev/null | sed 's/^/  /'
    echo ""
    read -p "请输入表名 (多个表用空格分隔): " table_input
    if [ -z "$table_input" ]; then
        echo "错误: 未输入表名"
        exit 1
    fi
    read -ra TABLES <<< "$table_input"
fi

echo ""

declare -A TABLE_VEC_COL
declare -A TABLE_DIM

for table in "${TABLES[@]}"; do
    vec_col=$(detect_vector_column "$table")
    if [ -z "$vec_col" ]; then
        echo "错误: 表 '$table' 中未找到 Array(Float*) 类型的向量列"
        exit 1
    fi
    dim=$(detect_vector_dimension "$table" "$vec_col")
    TABLE_VEC_COL["$table"]="$vec_col"
    TABLE_DIM["$table"]="$dim"
    echo "  表: $table  向量列: $vec_col  维度: $dim"
done

echo ""

# ---- Step 2: 选择距离函数 ----
echo ">>> Step 2: 选择距离函数"
echo ""
if [ "$ENGINE" = "myscale" ]; then
    echo "  (MyScale: 检索统一走 distance('ef_s=…')(vector, q) 家族, 命中 ANN 索引;"
    echo "   实际度量由建表时的向量索引 (HNSWFLAT) 决定, 下方选择仅用于标注与排序方向,"
    echo "   与 benchmark/run.py 的 MyScale 客户端行为一致)"
else
    echo "  (ClickHouse: 使用 L2Distance/cosineDistance 等标量距离函数, 由向量查询计划缓存分支加速)"
fi
echo "  1) cosineDistance (余弦距离, 默认)"
echo "  2) L2Distance (欧氏距离)"
echo "  3) dotProduct (点积, 排序方向为 DESC)"
echo ""
read -p "请选择 (1/2/3, 默认 1): " dist_choice
dist_choice=${dist_choice:-1}

case "$dist_choice" in
    2) DISTANCE_FUNC="L2Distance"; SORT_DIR="ASC" ;;
    3) DISTANCE_FUNC="dotProduct"; SORT_DIR="DESC" ;;
    *) DISTANCE_FUNC="cosineDistance"; SORT_DIR="ASC" ;;
esac

if [ "$ENGINE" = "myscale" ]; then
    # MyScale 检索函数名固定为 distance, 实际度量取索引上配置的
    DISTANCE_FUNC="distance"
    if [ "$dist_choice" = "3" ]; then
        echo "    注意: MyScale distance() 要求非 IP 度量按 ASC 排序; 仅当表的向量索引为 IP 度量时 ORDER BY dis DESC 才可用, 否则服务端会报错"
    fi
fi

echo "  距离函数: $DISTANCE_FUNC  排序方向: $SORT_DIR"
echo ""

# ---- Step 2.5: 检测向量索引 ----
echo ">>> Step 2.5: 检测向量索引状态"
echo ""

declare -A TABLE_INDEX_STATUS
HAS_INDEX_ISSUE=false
for table in "${TABLES[@]}"; do
    vec_col="${TABLE_VEC_COL[$table]}"
    if check_vector_index "$table" "$vec_col" "$DISTANCE_FUNC"; then
        TABLE_INDEX_STATUS["$table"]="index_active"
    else
        TABLE_INDEX_STATUS["$table"]="full_scan"
        HAS_INDEX_ISSUE=true
    fi
done

if [ "$HAS_INDEX_ISSUE" = true ]; then
    echo ""
    echo "  ⚠ 部分表的向量索引未生效，查询将使用全表扫描，性能可能较差"
    read -p "  是否继续测试? (y/N): " continue_choice
    if [[ ! "$continue_choice" =~ ^[Yy]$ ]]; then
        echo "已取消测试"
        exit 0
    fi
fi

echo ""

# ---- Step 3: 准备配置分组 ----
echo ">>> Step 3: 准备测试配置分组"
echo ""

validate_settings_groups
echo "配置分组:"
for settings_group in "${SETTINGS_GROUPS[@]}"; do
    IFS='|' read -r profile_name val_use_query_cache val_query_plan_cache val_cast_or_only_plan val_only_vector val_fifth <<< "$settings_group"
    if [ "$ENGINE" = "myscale" ]; then
        echo "  $profile_name: use_query_cache=$val_use_query_cache, enable_query_plan_cache=$val_query_plan_cache, enable_cast_vector=$val_cast_or_only_plan, query_plan_cache_only_vector=$val_only_vector, only_cache_query_plan=$val_fifth"
    else
        echo "  $profile_name: use_query_cache=$val_use_query_cache, vector_query_plan_cache=$val_query_plan_cache, vector_only_cache_query_plan=$val_cast_or_only_plan, vector_query_plan_cache_only_vector=$val_only_vector, vector_use_cast=$val_fifth"
    fi
done
echo ""

# ---- Step 4: 生成SQL文件 ----
echo ">>> Step 4: 生成SQL文件"
echo ""

mkdir -p "$SQL_DIR"

for settings_group in "${SETTINGS_GROUPS[@]}"; do
    IFS='|' read -r profile_name val_use_query_cache val_query_plan_cache val_cast_or_only_plan val_only_vector val_fifth <<< "$settings_group"
    settings_clause=$(build_settings_clause "$val_use_query_cache" "$val_query_plan_cache" "$val_cast_or_only_plan" "$val_only_vector" "$val_fifth")

    for table in "${TABLES[@]}"; do
        vec_col="${TABLE_VEC_COL[$table]}"

        sql_files_exist=true
        for sql_type in "${SQL_TYPES[@]}"; do
            for row_count in "${ROW_COUNTS[@]}"; do
                sql_file="$SQL_DIR/${table}_${profile_name}_${sql_type}_${row_count}.sql"
                if [ ! -f "$sql_file" ]; then
                    sql_files_exist=false
                    break 2
                fi
            done
        done

        if [ "$sql_files_exist" = true ]; then
            echo "  表 $table profile=$profile_name: SQL文件已存在，跳过生成"
        else
            generate_sql_for_table "$table" "$vec_col" "$DISTANCE_FUNC" "$SORT_DIR" "$profile_name" "$settings_clause"
        fi
    done
done

echo ""

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

detect_perf_test_flag

# ---- Step 5: 预热服务端 ----
echo ">>> Step 5: 预热服务端 (${WARMUP_TIMELIMIT}秒)"
echo ""

warmup_sql_file=""
for table in "${TABLES[@]}"; do
    for settings_group in "${SETTINGS_GROUPS[@]}"; do
        IFS='|' read -r profile_name _ <<< "$settings_group"
        for sql_type in "${SQL_TYPES[@]}"; do
            for row_count in "${ROW_COUNTS[@]}"; do
                candidate="$SQL_DIR/${table}_${profile_name}_${sql_type}_${row_count}.sql"
                if [ -f "$candidate" ]; then
                    warmup_sql_file="$candidate"
                    break 4
                fi
            done
        done
    done
done

if [ -z "$warmup_sql_file" ]; then
    echo "警告: 没有找到可用的 SQL 文件，跳过预热"
else
    echo "使用 $warmup_sql_file 进行预热..."
    warmup_run_file="$TMPDIR/warmup_run.sql"
    if [ "$PERF_TEST_FLAG" = "1" ]; then
        # 会话级打开 enable_vector_performance_test, 使查询自带的 SETTINGS 子句
        # 能进入服务端查询计划缓存探测; 因此关闭 --randomize 保证 SET 行先于查询执行
        { echo "SET enable_vector_performance_test = 1"; cat "$warmup_sql_file"; } > "$warmup_run_file"
        randomize_opts=()
    else
        cp "$warmup_sql_file" "$warmup_run_file"
        randomize_opts=(--randomize)
    fi
    set +e
    "$CLICKHOUSE" benchmark \
        --host "$HOST" \
        --port "$PORT" \
        --concurrency 1 \
        --timelimit "$WARMUP_TIMELIMIT" \
        --delay 0 \
        "${randomize_opts[@]}" \
        --iterations 0 \
        --precise_float_parsing "${PRECISE_FLOAT_PARSING_VALUES[0]}" \
        -- \
        < "$warmup_run_file" \
        > /dev/null 2>&1
    set -e
    echo "预热完成"
fi
echo ""

# ---- Step 6: 运行基准测试 ----
echo ">>> Step 6: 运行基准测试"
echo ""

# 结果 CSV 首列加上 engine, 便于 ck 与 myscale 结果在同一文件里区分
NEW_HEADER="engine,table_name,distance_func,index_status,settings_profile"
if [ "$ENGINE" = "myscale" ]; then
    NEW_HEADER="${NEW_HEADER},use_query_cache,enable_query_plan_cache,enable_cast_vector,query_plan_cache_only_vector,only_cache_query_plan"
else
    NEW_HEADER="${NEW_HEADER},use_query_cache,vector_query_plan_cache,vector_only_cache_query_plan,vector_query_plan_cache_only_vector,vector_use_cast"
fi
NEW_HEADER="${NEW_HEADER},precise_float_parsing,sql_type,row_count,concurrency"
for i in $(seq 1 $REPEAT); do
    NEW_HEADER="${NEW_HEADER},run_${i}"
done
NEW_HEADER="${NEW_HEADER},qps_avg,qps_min,qps_max"

# 输出目录可能不存在 (默认相对路径 ../results/), 先建目录再写文件
mkdir -p "$(dirname "$OUTPUT_CSV")"

if [ ! -f "$OUTPUT_CSV" ]; then
    echo "$NEW_HEADER" > "$OUTPUT_CSV"
    echo "创建新的 CSV 文件: $OUTPUT_CSV"
else
    existing_header=$(head -1 "$OUTPUT_CSV")
    if [ "$existing_header" != "$NEW_HEADER" ]; then
        backup_file="${OUTPUT_CSV}.$(date '+%Y%m%d%H%M%S').bak"
        cp "$OUTPUT_CSV" "$backup_file"
        echo "CSV 表头不匹配，已备份旧文件到: $backup_file"
        if echo "$existing_header" | grep -q '^engine,'; then
            # engine 已在首列但列结构不同 (通常是混用了另一个引擎的结果文件), 无法对齐
            echo "$NEW_HEADER" > "$OUTPUT_CSV"
            echo "无法自动迁移 (引擎/列结构不匹配)，已创建新表头"
        elif echo "$existing_header" | grep -q '^settings_profile'; then
            echo "迁移旧数据: 为每行添加 engine=clickhouse,table_name,distance_func,index_status 列..."
            tmp_csv=$(mktemp)
            echo "$NEW_HEADER" > "$tmp_csv"
            tail -n +2 "$OUTPUT_CSV" | while IFS= read -r line; do
                migrated="local_768d_test,cosineDistance,full_scan,${line}"
                echo "$migrated" | awk -F',' 'BEGIN {OFS=","} {for (i=1; i<=9; ++i) printf "%s%s", $i, OFS; printf "not_recorded"; for (i=10; i<=NF; ++i) printf "%s%s", OFS, $i; printf "\n"}' | sed 's/^/clickhouse,/' >> "$tmp_csv"
            done
            mv "$tmp_csv" "$OUTPUT_CSV"
            echo "迁移完成"
        elif echo "$existing_header" | grep -q '^table_name,settings_profile'; then
            echo "迁移旧数据: 为每行添加 engine,distance_func,index_status 列..."
            tmp_csv=$(mktemp)
            echo "$NEW_HEADER" > "$tmp_csv"
            tail -n +2 "$OUTPUT_CSV" | while IFS= read -r line; do
                migrated="${line/,/,cosineDistance,full_scan,}"
                echo "$migrated" | awk -F',' 'BEGIN {OFS=","} {for (i=1; i<=9; ++i) printf "%s%s", $i, OFS; printf "not_recorded"; for (i=10; i<=NF; ++i) printf "%s%s", OFS, $i; printf "\n"}' | sed 's/^/clickhouse,/' >> "$tmp_csv"
            done
            mv "$tmp_csv" "$OUTPUT_CSV"
            echo "迁移完成"
        elif echo "$existing_header" | grep -q '^table_name,distance_func,index_status,settings_profile'; then
            echo "迁移旧数据: 为每行添加 engine 列与 precise_float_parsing=not_recorded 列..."
            tmp_csv=$(mktemp)
            echo "$NEW_HEADER" > "$tmp_csv"
            tail -n +2 "$OUTPUT_CSV" | awk -F',' 'BEGIN {OFS=","} {for (i=1; i<=9; ++i) printf "%s%s", $i, OFS; printf "not_recorded"; for (i=10; i<=NF; ++i) printf "%s%s", OFS, $i; printf "\n"}' | sed 's/^/clickhouse,/' >> "$tmp_csv"
            mv "$tmp_csv" "$OUTPUT_CSV"
            echo "迁移完成"
        else
            echo "$NEW_HEADER" > "$OUTPUT_CSV"
            echo "无法自动迁移，已创建新表头"
        fi
    fi
    echo "追加到现有 CSV 文件: $OUTPUT_CSV ($(wc -l < "$OUTPUT_CSV") 行)"
fi
echo ""

echo "开始测试 ($(date '+%Y-%m-%d %H:%M:%S'))"
echo ""

for settings_group in "${SETTINGS_GROUPS[@]}"; do
    IFS='|' read -r profile_name val_use_query_cache val_query_plan_cache val_cast_or_only_plan val_only_vector val_fifth <<< "$settings_group"

    # 每个 profile 从空缓存开始, 保证首行 QPS 口径一致
    drop_caches_for_profile "$val_query_plan_cache"

    for table in "${TABLES[@]}"; do
        echo "╔══════════════════════════════════════════════════════════════╗"
        echo "║ 引擎: $ENGINE"
        echo "║ 表: $table"
        echo "║   向量列: ${TABLE_VEC_COL[$table]}"
        echo "║   维度: ${TABLE_DIM[$table]}"
        echo "║   距离函数: $DISTANCE_FUNC"
        echo "║   配置 Profile: $profile_name"
        if [ "$ENGINE" = "myscale" ]; then
            echo "║   use_query_cache=$val_use_query_cache"
            echo "║   enable_query_plan_cache=$val_query_plan_cache"
            echo "║   enable_cast_vector=$val_cast_or_only_plan"
            echo "║   query_plan_cache_only_vector=$val_only_vector"
            echo "║   only_cache_query_plan=$val_fifth"
        else
            echo "║   use_query_cache=$val_use_query_cache"
            echo "║   vector_query_plan_cache=$val_query_plan_cache"
            echo "║   vector_only_cache_query_plan=$val_cast_or_only_plan"
            echo "║   vector_query_plan_cache_only_vector=$val_only_vector"
            echo "║   vector_use_cast=$val_fifth"
        fi
        echo "╚══════════════════════════════════════════════════════════════╝"
        echo ""

        for precise_float_parsing in "${PRECISE_FLOAT_PARSING_VALUES[@]}"; do
            echo "  precise_float_parsing=$precise_float_parsing"
            for sql_type in "${SQL_TYPES[@]}"; do
                for row_count in "${ROW_COUNTS[@]}"; do
                    sql_file="$SQL_DIR/${table}_${profile_name}_${sql_type}_${row_count}.sql"

                    if [ ! -f "$sql_file" ]; then
                        echo "警告: SQL 文件不存在，跳过: $sql_file"
                        continue
                    fi

                    actual_queries=$(wc -l < "$sql_file")
                    if [ "$actual_queries" -eq 0 ]; then
                        echo "警告: SQL 文件为空，跳过: $sql_file"
                        continue
                    fi

                    # 生成本文件的运行副本: 计划缓存等开关走会话级 SET (与 run.py 一致),
                    # 头部写本 profile 的 SET 行, 并用 sed 剥掉每条查询行尾内联的
                    # SETTINGS 子句, 使发到服务端的 SQL 是纯查询文本 (缓存按同一文本命中)。
                    # 关闭 --randomize: 保证 SET 行与该文件后续查询在同一条会话上按序执行。
                    run_file="$TMPDIR/run_${table}_${profile_name}_${sql_type}_${row_count}.sql"
                    profile_set_line=$(session_set_line_for_profile \
                        "$val_use_query_cache" "$val_query_plan_cache" \
                        "$val_cast_or_only_plan" "$val_only_vector" \
                        "$val_fifth")
                    {
                        if [ "$PERF_TEST_FLAG" = "1" ]; then
                            echo "SET enable_vector_performance_test = 1"
                        fi
                        echo "$profile_set_line"
                        sed 's/ SETTINGS .*$//' "$sql_file"
                    } > "$run_file"
                    randomize_opts=()

                    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                    echo " 表: $table  Profile: $profile_name  precise_float_parsing=$precise_float_parsing  SQL: ${sql_type}_${row_count}.sql ($actual_queries 条查询)"
                    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

                    for concurrency in "${CONCURRENCIES[@]}"; do
                        echo ""
                        echo "  并发数: $concurrency (重复 $REPEAT 次)"

                        qps_values=()

                        for run in $(seq 1 $REPEAT); do
                            stderr_file="$TMPDIR/stderr_${table}_${profile_name}_precise${precise_float_parsing}_${sql_type}_${row_count}_${concurrency}_${run}.log"

                            echo -n "    第 ${run}/${REPEAT} 次... "

                            set +e
                            "$CLICKHOUSE" benchmark \
                                --host "$HOST" \
                                --port "$PORT" \
                                --concurrency "$concurrency" \
                                --timelimit "$TIMELIMIT" \
                                --delay 0 \
                                "${randomize_opts[@]}" \
                                --iterations 0 \
                                --precise_float_parsing "$precise_float_parsing" \
                                -- \
                                < "$run_file" \
                                2>"$stderr_file" \
                                > /dev/null
                            exit_code=$?
                            set -e

                            if [ $exit_code -ne 0 ]; then
                                echo "错误 (exit=$exit_code)，记录 QPS=0"
                                echo "  stderr 尾部:"
                                tail -5 "$stderr_file" 2>/dev/null | sed 's/^/    /'
                                qps_values+=(0)
                                continue
                            fi

                            qps=$(parse_total_qps "$(cat "$stderr_file")")

                            if [ -z "$qps" ] || [ "$qps" = "0.000" ]; then
                                echo "警告: 无法解析 QPS，记录为 0"
                                head -20 "$stderr_file" | sed 's/^/    /'
                                qps_values+=(0)
                            else
                                echo "QPS = $qps"
                                qps_values+=("$qps")
                            fi
                        done

                        sorted_qps=$(printf '%s\n' "${qps_values[@]}" | sort -n)
                        count=${#qps_values[@]}

                        if [ "$count" -le 2 ]; then
                            qps_avg=$(printf '%s\n' "${qps_values[@]}" | awk '{sum+=$1; n++} END {printf "%.3f", sum/n}')
                            qps_min=$(printf '%s\n' "${qps_values[@]}" | sort -n | head -1)
                            qps_max=$(printf '%s\n' "${qps_values[@]}" | sort -n | tail -1)
                        else
                            qps_min=$(echo "$sorted_qps" | head -1)
                            qps_max=$(echo "$sorted_qps" | tail -1)
                            qps_avg=$(echo "$sorted_qps" | sed '1d;$d' | awk '{sum+=$1; n++} END {printf "%.3f", sum/n}')
                        fi

                        echo "  ─────────────────────────────────────"
                        echo "  结果: 均值=$qps_avg  最小=$qps_min  最大=$qps_max"
                        echo ""

                        if [ "$ENGINE" = "myscale" ]; then
                        csv_line="${ENGINE},${table},${DISTANCE_FUNC},${TABLE_INDEX_STATUS[$table]},${profile_name},${val_use_query_cache},${val_query_plan_cache},${val_cast_or_only_plan},${val_only_vector},${val_fifth},${precise_float_parsing},${sql_type},${row_count},${concurrency}"
                    else
                        csv_line="${ENGINE},${table},${DISTANCE_FUNC},${TABLE_INDEX_STATUS[$table]},${profile_name},${val_use_query_cache},${val_query_plan_cache},${val_cast_or_only_plan},${val_only_vector},${val_fifth},${precise_float_parsing},${sql_type},${row_count},${concurrency}"
                    fi
                        for v in "${qps_values[@]}"; do
                            csv_line="${csv_line},${v}"
                        done
                        for _ in $(seq $((count + 1)) $REPEAT); do
                            csv_line="${csv_line},"
                        done
                        csv_line="${csv_line},${qps_avg},${qps_min},${qps_max}"
                        echo "$csv_line" >> "$OUTPUT_CSV"
                    done

                    echo ""
                done
            done
        done
    done
done

echo "=============================================="
echo " 测试完成! ($(date '+%Y-%m-%d %H:%M:%S'))"
echo " 结果文件: $OUTPUT_CSV"
echo "=============================================="
echo " "
echo "--- CSV 内容预览 (最后20行) ---"
tail -20 "$OUTPUT_CSV" | column -t -s',' 2>/dev/null || tail -20 "$OUTPUT_CSV"