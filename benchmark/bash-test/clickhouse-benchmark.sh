#!/bin/bash
#
# ClickHouse 向量查询性能基准测试脚本
# 对不同并发数(1,2,4,8,16,32)分别运行 benchmark 5次，
# 去掉最大值和最小值后取均值，结果追加写入 CSV 文件。
#
# 测试维度: settings_profile × sql_type(normal/cast) × row_count(1/10/100/1000) × concurrency
#

set -euo pipefail

# ============ 配置 ============
CLICKHOUSE="/home/ClickHouse/build/programs/clickhouse"
HOST="127.0.0.1"
PORT="9000"
TIMELIMIT=30          # 每次测试持续秒数
WARMUP_TIMELIMIT=10   # 预热持续秒数
REPEAT=5              # 每种并发测试次数
OUTPUT_CSV="../results/clickhouse-benchmark-results.csv"
SQL_DIR="sql-bench"
GENERATE_SCRIPT="generate-sql-files.sh"

# SQL 类型和行数
SQL_TYPES=(normal cast)
ROW_COUNTS=(1)

# 并发数列表
CONCURRENCIES=(1)

# ============ 配置参数（运行时由用户输入） ============
# profile_name: 配置名称，用于 CSV 输出
# use_query_cache: 是否使用查询缓存 (0/1)
# vector_query_plan_cache: 是否使用向量查询计划缓存 (0/1)
# vector_only_cache_query_plan: 是否仅缓存查询计划 (0/1)
# vector_query_plan_cache_only_vector: 是否仅缓存向量查询计划 (0/1)
# vector_use_cast: 是否使用 cast 模式 (0/1)

# ============ 工具函数 ============

# 从 clickhouse benchmark 的 stderr 输出中提取所有 QPS 值并求和
parse_total_qps() {
    local stderr_output="$1"
    echo "$stderr_output" | grep -oP 'QPS:\s*\K[0-9.]+' | awk '{sum += $1} END {printf "%.3f", sum}'
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

# 检查 clickhouse 可执行文件
if [ ! -x "$CLICKHOUSE" ]; then
    echo "错误: 找不到 clickhouse 可执行文件: $CLICKHOUSE"
    exit 1
fi

# ---- Step 1: 生成 SQL 文件 ----
echo ">>> Step 1: 检查 SQL 文件"
echo ""

# 检查是否所有 SQL 文件都已存在
sql_files_exist=true
for sql_type in "${SQL_TYPES[@]}"; do
    for row_count in "${ROW_COUNTS[@]}"; do
        sql_file="$SQL_DIR/${sql_type}_${row_count}.sql"
        if [ ! -f "$sql_file" ]; then
            sql_files_exist=false
            break 2
        fi
    done
done

if [ "$sql_files_exist" = true ]; then
    echo "所有 SQL 文件已存在，跳过生成"
    echo "文件列表:"
    ls -lh "$SQL_DIR"/*.sql 2>/dev/null | sed 's/^/  /'
else
    echo "SQL 文件不存在或不完整，开始生成..."
    echo ""

    if [ ! -f "$GENERATE_SCRIPT" ]; then
        echo "错误: 找不到 SQL 生成脚本: $GENERATE_SCRIPT"
        exit 1
    fi

    bash "$GENERATE_SCRIPT"
fi
echo ""

# ---- Step 2: 用户输入配置参数 ----
echo ">>> Step 2: 请输入测试配置参数"
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

# ---- Step 3: 预热服务端 ----
echo ">>> Step 3: 预热服务端 (${WARMUP_TIMELIMIT}秒)"
echo ""

# 使用第一个可用的 SQL 文件进行预热
warmup_sql_file=""
for sql_type in "${SQL_TYPES[@]}"; do
    for row_count in "${ROW_COUNTS[@]}"; do
        candidate="$SQL_DIR/${sql_type}_${row_count}.sql"
        if [ -f "$candidate" ]; then
            warmup_sql_file="$candidate"
            break 2
        fi
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
        --use_query_cache="$val_use_query_cache" \
        --vector_query_plan_cache="$val_vector_query_plan_cache" \
        --vector_only_cache_query_plan="$val_vector_only_cache_query_plan" \
        --vector_query_plan_cache_only_vector="$val_vector_query_plan_cache_only_vector" \
        --vector_use_cast="$val_vector_use_cast" \
        < "$warmup_sql_file" \
        > /dev/null 2>&1
    set -e
    echo "预热完成"
fi
echo ""

# ---- Step 4: 运行基准测试 ----
echo ">>> Step 4: 运行基准测试"
echo ""

# CSV 追加模式 — 只在文件不存在时写入表头
if [ ! -f "$OUTPUT_CSV" ]; then
    header="settings_profile,use_query_cache,vector_query_plan_cache,vector_only_cache_query_plan,vector_query_plan_cache_only_vector,vector_use_cast,sql_type,row_count,concurrency"
    for i in $(seq 1 $REPEAT); do
        header="${header},run_${i}"
    done
    header="${header},qps_avg,qps_min,qps_max"
    echo "$header" > "$OUTPUT_CSV"
    echo "创建新的 CSV 文件: $OUTPUT_CSV"
else
    echo "追加到现有 CSV 文件: $OUTPUT_CSV ($(wc -l < "$OUTPUT_CSV") 行)"
fi
echo ""

# 临时目录
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

echo "开始测试 ($(date '+%Y-%m-%d %H:%M:%S'))"
echo ""

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║ 配置 Profile: $profile_name"
echo "║   use_query_cache=$val_use_query_cache"
echo "║   vector_query_plan_cache=$val_vector_query_plan_cache"
echo "║   vector_only_cache_query_plan=$val_vector_only_cache_query_plan"
echo "║   vector_query_plan_cache_only_vector=$val_vector_query_plan_cache_only_vector"
echo "║   vector_use_cast=$val_vector_use_cast"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

for sql_type in "${SQL_TYPES[@]}"; do
    for row_count in "${ROW_COUNTS[@]}"; do
        sql_file="$SQL_DIR/${sql_type}_${row_count}.sql"

        if [ ! -f "$sql_file" ]; then
            echo "警告: SQL 文件不存在，跳过: $sql_file"
            continue
        fi

        actual_queries=$(wc -l < "$sql_file")
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo " SQL: ${sql_type}_${row_count}.sql ($actual_queries 条查询)"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

        for concurrency in "${CONCURRENCIES[@]}"; do
            echo ""
            echo "  并发数: $concurrency (重复 $REPEAT 次)"

            qps_values=()

            for run in $(seq 1 $REPEAT); do
                stderr_file="$TMPDIR/stderr_${profile_name}_${sql_type}_${row_count}_${concurrency}_${run}.log"

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
                    --use_query_cache="$val_use_query_cache" \
                    --vector_query_plan_cache="$val_vector_query_plan_cache" \
                    --vector_only_cache_query_plan="$val_vector_only_cache_query_plan" \
                    --vector_query_plan_cache_only_vector="$val_vector_query_plan_cache_only_vector" \
                    --vector_use_cast="$val_vector_use_cast" \
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

            # 计算去掉最大最小值后的均值
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

            # 追加写入 CSV
            csv_line="${profile_name},${val_use_query_cache},${val_vector_query_plan_cache},${val_vector_only_cache_query_plan},${val_vector_query_plan_cache_only_vector},${val_vector_use_cast},${sql_type},${row_count},${concurrency}"
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

echo "=============================================="
echo " 测试完成! ($(date '+%Y-%m-%d %H:%M:%S'))"
echo " 结果文件: $OUTPUT_CSV"
echo "=============================================="
echo ""
echo "--- CSV 内容预览 (最后20行) ---"
tail -20 "$OUTPUT_CSV" | column -t -s',' 2>/dev/null || tail -20 "$OUTPUT_CSV"
