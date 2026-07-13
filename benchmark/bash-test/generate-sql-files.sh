#!/bin/bash
#
# 从指定表中随机抽取 1/10/100/1000 个向量，
# 生成对应的 SQL 查询文件（normal 和 cast 两种模式）。
#
# 用法:
#   ./generate-sql-files.sh [表名] [距离函数] [排序方向]
#   默认: 表名=local_768d_test  距离函数=cosineDistance  排序方向=ASC
#

set -euo pipefail

CLICKHOUSE="/home/ClickHouse/build/programs/clickhouse"
HOST="127.0.0.1"
PORT="9000"
TABLE="${1:-local_768d_test}"
DISTANCE_FUNC="${2:-cosineDistance}"
SORT_DIR="${3:-ASC}"
OUTPUT_DIR="sql-bench"

ROW_COUNTS=(1 10 100 1000)
TOP_K=10

mkdir -p "$OUTPUT_DIR"

echo "=============================================="
echo " SQL 文件生成器"
echo "=============================================="
echo "表:       $TABLE"
echo "距离函数: $DISTANCE_FUNC"
echo "排序方向: $SORT_DIR"
echo "输出:     $OUTPUT_DIR"
echo "数量:     ${ROW_COUNTS[*]}"
echo "=============================================="
echo ""

vec_col=$("$CLICKHOUSE" client \
    --host "$HOST" \
    --port "$PORT" \
    --query "SELECT name FROM system.columns WHERE database='default' AND table='$TABLE' AND type LIKE 'Array(Float%)' LIMIT 1 SETTINGS use_query_cache=0" 2>/dev/null) || true

if [ -z "$vec_col" ]; then
    echo "错误: 表 '$TABLE' 中未找到 Array(Float*) 类型的向量列"
    exit 1
fi

echo "检测到向量列: $vec_col"
echo ""

for count in "${ROW_COUNTS[@]}"; do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " 生成 ${count} 条向量的 SQL 文件..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    tmp_vectors=$(mktemp)
    "$CLICKHOUSE" client \
        --host "$HOST" \
        --port "$PORT" \
        --query "SELECT arrayStringConcat($vec_col, ',') FROM $TABLE ORDER BY rand() LIMIT $count SETTINGS use_query_cache=0" \
        > "$tmp_vectors"

    actual_count=$(wc -l < "$tmp_vectors")
    if [ "$actual_count" -ne "$count" ]; then
        echo "警告: 预期 ${count} 条，实际获取 ${actual_count} 条"
    fi

    normal_file="$OUTPUT_DIR/${TABLE}_normal_${count}.sql"
    > "$normal_file"
    while IFS= read -r vec_str; do
        [ -z "$vec_str" ] && continue
        echo "SELECT id, ${DISTANCE_FUNC}(${vec_col}, [${vec_str}]) as dis FROM $TABLE ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};" >> "$normal_file"
    done < "$tmp_vectors"
    echo "  已生成: $normal_file ($(wc -l < "$normal_file") 条查询)"

    cast_file="$OUTPUT_DIR/${TABLE}_cast_${count}.sql"
    > "$cast_file"
    while IFS= read -r vec_str; do
        [ -z "$vec_str" ] && continue
        echo "SELECT id, ${DISTANCE_FUNC}(${vec_col}, cast('[${vec_str}]','Array(Float32)')) as dis FROM $TABLE ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};" >> "$cast_file"
    done < "$tmp_vectors"
    echo "  已生成: $cast_file ($(wc -l < "$cast_file") 条查询)"

    rm -f "$tmp_vectors"
    echo ""
done

echo "=============================================="
echo " 生成完成! 列出所有文件:"
echo "=============================================="
ls -lh "$OUTPUT_DIR"/*.sql