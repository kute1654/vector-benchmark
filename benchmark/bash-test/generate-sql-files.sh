#!/bin/bash
#
# 从 local_768d_test 表中随机抽取 1/10/100/1000 个向量，
# 生成对应的 SQL 查询文件（normal 和 cast 两种模式）。
#

set -euo pipefail

CLICKHOUSE="/home/ClickHouse/build/programs/clickhouse"
HOST="127.0.0.1"
PORT="9000"
TABLE="local_768d_test"
OUTPUT_DIR="sql-bench"

# 需要生成的向量数量
ROW_COUNTS=(1 10 100 1000)

# 创建输出目录
mkdir -p "$OUTPUT_DIR"

echo "=============================================="
echo " SQL 文件生成器"
echo "=============================================="
echo "表:     $TABLE"
echo "输出:   $OUTPUT_DIR"
echo "数量:   ${ROW_COUNTS[*]}"
echo "=============================================="
echo ""

for count in "${ROW_COUNTS[@]}"; do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " 生成 ${count} 条向量的 SQL 文件..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # 从表中随机取 count 个向量，输出为逗号分隔的字符串
    # 使用 SETTINGS use_query_cache=0 避免 rand() 非确定性报错
    tmp_vectors=$(mktemp)
    "$CLICKHOUSE" client \
        --host "$HOST" \
        --port "$PORT" \
        --query "SELECT arrayStringConcat(vector, ',') FROM $TABLE ORDER BY rand() LIMIT $count SETTINGS use_query_cache=0" \
        > "$tmp_vectors"

    actual_count=$(wc -l < "$tmp_vectors")
    if [ "$actual_count" -ne "$count" ]; then
        echo "警告: 预期 ${count} 条，实际获取 ${actual_count} 条"
    fi

    # ---- 生成 normal 模式 SQL ----
    normal_file="$OUTPUT_DIR/normal_${count}.sql"
    > "$normal_file"
    while IFS= read -r vec_str; do
        echo "SELECT id, cosineDistance(vector, [${vec_str}]) as dis FROM $TABLE ORDER BY dis ASC LIMIT 10;" >> "$normal_file"
    done < "$tmp_vectors"
    echo "  已生成: $normal_file ($(wc -l < "$normal_file") 条查询)"

    # ---- 生成 cast 模式 SQL ----
    cast_file="$OUTPUT_DIR/cast_${count}.sql"
    > "$cast_file"
    while IFS= read -r vec_str; do
        echo "SELECT id, cosineDistance(vector, cast('[${vec_str}]','Array(Float32)')) as dis FROM $TABLE ORDER BY dis ASC LIMIT 10;" >> "$cast_file"
    done < "$tmp_vectors"
    echo "  已生成: $cast_file ($(wc -l < "$cast_file") 条查询)"

    rm -f "$tmp_vectors"
    echo ""
done

echo "=============================================="
echo " 生成完成! 列出所有文件:"
echo "=============================================="
ls -lh "$OUTPUT_DIR"/*.sql
