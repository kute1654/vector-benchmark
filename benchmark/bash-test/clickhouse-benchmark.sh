#!/bin/bash
#
# ClickHouse 向量查询性能基准测试脚本
# 支持录入表名，根据表名自动生成SQL，测试结果写入CSV
#
# 用法:
#   ./clickhouse-benchmark.sh [表名1] [表名2] ...
#   不带参数时交互式输入表名
#
# 测试维度: table × settings_profile × sql_type(normal/cast) × row_count(1/10/100/1000) × concurrency
#

set -euo pipefail

# ============ 配置 ============
CLICKHOUSE="/home/ClickHouse/build/programs/clickhouse"
HOST="127.0.0.1"
PORT="9000"
TIMELIMIT=30
WARMUP_TIMELIMIT=10
REPEAT=5
OUTPUT_CSV="../results/clickhouse-benchmark-results.csv"
SQL_DIR="sql-bench"
TOP_K=10

SQL_TYPES=(normal cast)
ROW_COUNTS=(1 10 100 1000)
CONCURRENCIES=(1 2 4 8 16 32)

# ============ 工具函数 ============

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

generate_sql_for_table() {
    local table="$1"
    local vec_col="$2"
    local dist_func="$3"
    local sort_dir="$4"

    echo "  生成SQL文件 (表=$table, 列=$vec_col, 距离=$dist_func, 排序=$sort_dir)..."

    for count in "${ROW_COUNTS[@]}"; do
        local tmp_vectors
        tmp_vectors=$(mktemp)

        "$CLICKHOUSE" client \
            --host "$HOST" \
            --port "$PORT" \
            --query "SELECT arrayStringConcat($vec_col, ',') FROM $table ORDER BY rand() LIMIT $count SETTINGS use_query_cache=0" \
            > "$tmp_vectors" 2>/dev/null

        local actual_count
        actual_count=$(wc -l < "$tmp_vectors")
        if [ "$actual_count" -ne "$count" ]; then
            echo "  警告: 表 $table 预期 ${count} 条，实际获取 ${actual_count} 条"
        fi

        local normal_file="$SQL_DIR/${table}_normal_${count}.sql"
        > "$normal_file"
        while IFS= read -r vec_str; do
            [ -z "$vec_str" ] && continue
            echo "SELECT id, ${dist_func}(${vec_col}, [${vec_str}]) as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K};" >> "$normal_file"
        done < "$tmp_vectors"
        echo "    已生成: $normal_file ($(wc -l < "$normal_file") 条查询)"

        local cast_file="$SQL_DIR/${table}_cast_${count}.sql"
        > "$cast_file"
        while IFS= read -r vec_str; do
            [ -z "$vec_str" ] && continue
            echo "SELECT id, ${dist_func}(${vec_col}, cast('[${vec_str}]','Array(Float32)')) as dis FROM ${table} ORDER BY dis ${sort_dir} LIMIT ${TOP_K};" >> "$cast_file"
        done < "$tmp_vectors"
        echo "    已生成: $cast_file ($(wc -l < "$cast_file") 条查询)"

        rm -f "$tmp_vectors"
    done
}

parse_total_qps() {
    local stderr_output="$1"
    echo "$stderr_output" | grep -oP 'QPS:\s*\K[0-9.]+' | awk '{sum += $1} END {printf "%.3f", sum}'
}

check_vector_index() {
    local table="$1"
    local vec_col="$2"
    local dist_func="$3"

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
            --query "EXPLAIN SELECT id, ${dist_func}(${vec_col}, [${sample_vec}]) as dis FROM ${table} ORDER BY dis ASC LIMIT 10 SETTINGS use_query_cache=0" 2>/dev/null) || true

        local read_type
        read_type=$(echo "$explain_output" | grep -oP 'Read type:\s*\K\S+' || true)

        if [ -n "$read_type" ]; then
            if echo "$read_type" | grep -qi "default"; then
                echo "  ⚠ 表 $table: 向量索引存在但未生效! EXPLAIN 显示 Read type: $read_type (全表扫描)"
                echo "    索引信息: name=$index_name type=$index_type size=${compressed_bytes}B"
                echo "    可能原因: 距离函数与索引类型不匹配，或索引未覆盖所有数据分区"
                return 1
            else
                echo "  ✓ 表 $table: 向量索引生效! Read type: $read_type"
                echo "    索引信息: name=$index_name type=$index_type size=${compressed_bytes}B"
                return 0
            fi
        else
            echo "  ? 表 $table: 无法从 EXPLAIN 解析 Read type"
            echo "    索引信息: name=$index_name type=$index_type size=${compressed_bytes}B"
            echo "    EXPLAIN 输出:"
            echo "$explain_output" | head -15 | sed 's/^/      /'
            return 2
        fi
    fi

    echo "  ? 表 $table: 无法获取样本向量进行 EXPLAIN 检测"
    echo "    索引信息: name=$index_name type=$index_type size=${compressed_bytes}B"
    return 2
}

# ============ 主逻辑 ============

echo "=============================================="
echo " ClickHouse 向量查询基准测试"
echo "=============================================="
echo "二进制: $CLICKHOUSE"
echo "目标:   $HOST:$PORT"
echo "时长:   ${TIMELIMIT}s / 次"
echo "重复:   $REPEAT 次 (去掉最大最小值取均值)"
echo "并发:   ${CONCURRENCIES[*]}"
echo "类型:   ${SQL_TYPES[*]}"
echo "行数:   ${ROW_COUNTS[*]}"
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

# ---- Step 3: 输入配置参数 ----
echo ">>> Step 3: 请输入测试配置参数"
echo ""

read -p "配置名称 (profile_name, 用于 CSV 输出): " profile_name
if [ -z "$profile_name" ]; then
    profile_name="default"
    echo "  使用默认名称: $profile_name"
fi

read -p "use_query_cache (0/1, 默认 0): " val_use_query_cache
val_use_query_cache=${val_use_query_cache:-0}

read -p "vector_query_plan_cache (0/1, 默认 0): " val_vector_query_plan_cache
val_vector_query_plan_cache=${val_vector_query_plan_cache:-0}

read -p "vector_only_cache_query_plan (0/1, 默认 0): " val_vector_only_cache_query_plan
val_vector_only_cache_query_plan=${val_vector_only_cache_query_plan:-0}

read -p "vector_query_plan_cache_only_vector (0/1, 默认 0): " val_vector_query_plan_cache_only_vector
val_vector_query_plan_cache_only_vector=${val_vector_query_plan_cache_only_vector:-0}

read -p "vector_use_cast (0/1, 默认 0): " val_vector_use_cast
val_vector_use_cast=${val_vector_use_cast:-0}

echo ""
echo "配置参数:"
echo "  profile_name=$profile_name"
echo "  use_query_cache=$val_use_query_cache"
echo "  vector_query_plan_cache=$val_vector_query_plan_cache"
echo "  vector_only_cache_query_plan=$val_vector_only_cache_query_plan"
echo "  vector_query_plan_cache_only_vector=$val_vector_query_plan_cache_only_vector"
echo "  vector_use_cast=$val_vector_use_cast"
echo ""

# ---- Step 4: 生成SQL文件 ----
echo ">>> Step 4: 生成SQL文件"
echo ""

mkdir -p "$SQL_DIR"

for table in "${TABLES[@]}"; do
    vec_col="${TABLE_VEC_COL[$table]}"

    sql_files_exist=true
    for sql_type in "${SQL_TYPES[@]}"; do
        for row_count in "${ROW_COUNTS[@]}"; do
            sql_file="$SQL_DIR/${table}_${sql_type}_${row_count}.sql"
            if [ ! -f "$sql_file" ]; then
                sql_files_exist=false
                break 2
            fi
        done
    done

    if [ "$sql_files_exist" = true ]; then
        echo "  表 $table: SQL文件已存在，跳过生成"
    else
        generate_sql_for_table "$table" "$vec_col" "$DISTANCE_FUNC" "$SORT_DIR"
    fi
done

echo ""

# ---- Step 5: 预热服务端 ----
echo ">>> Step 5: 预热服务端 (${WARMUP_TIMELIMIT}秒)"
echo ""

warmup_sql_file=""
for table in "${TABLES[@]}"; do
    for sql_type in "${SQL_TYPES[@]}"; do
        for row_count in "${ROW_COUNTS[@]}"; do
            candidate="$SQL_DIR/${table}_${sql_type}_${row_count}.sql"
            if [ -f "$candidate" ]; then
                warmup_sql_file="$candidate"
                break 3
            fi
        done
    done
done

if [ -z "$warmup_sql_file" ]; then
    echo "警告: 没有找到可用的 SQL 文件，跳过预热"
else
    echo "使用 $warmup_sql_file 进行预热..."
    set +e
    "$CLICKHOUSE" benchmark \
        --host "$HOST" \
        --port "$PORT" \
        --concurrency 1 \
        --timelimit "$WARMUP_TIMELIMIT" \
        --delay 0 \
        --randomize \
        --iterations 0 \
        -- \
        < "$warmup_sql_file" \
        > /dev/null 2>&1
    set -e
    echo "预热完成"
fi
echo ""

# ---- Step 6: 运行基准测试 ----
echo ">>> Step 6: 运行基准测试"
echo ""

NEW_HEADER="table_name,distance_func,index_status,settings_profile,use_query_cache,vector_query_plan_cache,vector_only_cache_query_plan,vector_query_plan_cache_only_vector,vector_use_cast,sql_type,row_count,concurrency"
for i in $(seq 1 $REPEAT); do
    NEW_HEADER="${NEW_HEADER},run_${i}"
done
NEW_HEADER="${NEW_HEADER},qps_avg,qps_min,qps_max"

if [ ! -f "$OUTPUT_CSV" ]; then
    echo "$NEW_HEADER" > "$OUTPUT_CSV"
    echo "创建新的 CSV 文件: $OUTPUT_CSV"
else
    existing_header=$(head -1 "$OUTPUT_CSV")
    if [ "$existing_header" != "$NEW_HEADER" ]; then
        backup_file="${OUTPUT_CSV}.$(date '+%Y%m%d%H%M%S').bak"
        cp "$OUTPUT_CSV" "$backup_file"
        echo "CSV 表头不匹配，已备份旧文件到: $backup_file"
        if echo "$existing_header" | grep -q '^settings_profile'; then
            echo "迁移旧数据: 为每行添加 table_name,distance_func,index_status 列..."
            tmp_csv=$(mktemp)
            echo "$NEW_HEADER" > "$tmp_csv"
            tail -n +2 "$OUTPUT_CSV" | while IFS= read -r line; do
                echo "local_768d_test,cosineDistance,full_scan,${line}" >> "$tmp_csv"
            done
            mv "$tmp_csv" "$OUTPUT_CSV"
            echo "迁移完成"
        elif echo "$existing_header" | grep -q '^table_name,settings_profile'; then
            echo "迁移旧数据: 为每行添加 distance_func,index_status 列..."
            tmp_csv=$(mktemp)
            echo "$NEW_HEADER" > "$tmp_csv"
            tail -n +2 "$OUTPUT_CSV" | while IFS= read -r line; do
                echo "${line/,/,cosineDistance,full_scan,}" >> "$tmp_csv"
            done
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

TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo "开始测试 ($(date '+%Y-%m-%d %H:%M:%S'))"
echo ""

for table in "${TABLES[@]}"; do
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║ 表: $table"
    echo "║   向量列: ${TABLE_VEC_COL[$table]}"
    echo "║   维度: ${TABLE_DIM[$table]}"
    echo "║   距离函数: $DISTANCE_FUNC"
    echo "║   配置 Profile: $profile_name"
    echo "║   use_query_cache=$val_use_query_cache"
    echo "║   vector_query_plan_cache=$val_vector_query_plan_cache"
    echo "║   vector_only_cache_query_plan=$val_vector_only_cache_query_plan"
    echo "║   vector_query_plan_cache_only_vector=$val_vector_query_plan_cache_only_vector"
    echo "║   vector_use_cast=$val_vector_use_cast"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo ""

    for sql_type in "${SQL_TYPES[@]}"; do
        for row_count in "${ROW_COUNTS[@]}"; do
            sql_file="$SQL_DIR/${table}_${sql_type}_${row_count}.sql"

            if [ ! -f "$sql_file" ]; then
                echo "警告: SQL 文件不存在，跳过: $sql_file"
                continue
            fi

            actual_queries=$(wc -l < "$sql_file")
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo " 表: $table  SQL: ${sql_type}_${row_count}.sql ($actual_queries 条查询)"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

            for concurrency in "${CONCURRENCIES[@]}"; do
                echo ""
                echo "  并发数: $concurrency (重复 $REPEAT 次)"

                qps_values=()

                for run in $(seq 1 $REPEAT); do
                    stderr_file="$TMPDIR/stderr_${table}_${profile_name}_${sql_type}_${row_count}_${concurrency}_${run}.log"

                    echo -n "    第 ${run}/${REPEAT} 次... "

                    set +e
                    "$CLICKHOUSE" benchmark \
                        --host "$HOST" \
                        --port "$PORT" \
                        --concurrency "$concurrency" \
                        --timelimit "$TIMELIMIT" \
                        --delay 0 \
                        --randomize \
                        --iterations 0 \
                        -- \
                        < "$sql_file" \
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

                csv_line="${table},${DISTANCE_FUNC},${TABLE_INDEX_STATUS[$table]},${profile_name},${val_use_query_cache},${val_vector_query_plan_cache},${val_vector_only_cache_query_plan},${val_vector_query_plan_cache_only_vector},${val_vector_use_cast},${sql_type},${row_count},${concurrency}"
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

echo "=============================================="
echo " 测试完成! ($(date '+%Y-%m-%d %H:%M:%S'))"
echo " 结果文件: $OUTPUT_CSV"
echo "=============================================="
echo ""
echo "--- CSV 内容预览 (最后20行) ---"
tail -20 "$OUTPUT_CSV" | column -t -s',' 2>/dev/null || tail -20 "$OUTPUT_CSV"