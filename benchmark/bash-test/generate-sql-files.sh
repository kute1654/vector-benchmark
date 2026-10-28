#!/bin/bash
#
# 从指定表中随机抽取 1/10/100/1000 个向量，
# 生成对应的 SQL 查询文件（normal 和 cast 两种模式）。
#
# 用法:
#   ./generate-sql-files.sh [表名] [距离函数] [排序方向]
#   默认: 表名=local_768d_test  距离函数=cosineDistance  排序方向=ASC
#
# ---- 用途 ----
#   轻量版 SQL 生成器: 连接 ClickHouse 随机采样向量, 为每个 ROW_COUNTS 值生成
#   两个查询文件 (normal: 数组字面量; cast: 字符串再 cast 成 Array(Float32)),
#   供 clickhouse-benchmark 直接使用。
#   说明: 本脚本是纯 bash 实现, 不调用 Python; benchmark/generate_sql_files.py 是
#   另一套独立的 Python 生成器 (支持更多引擎与 SQL 形态), 两者互不依赖。
#
# ---- 参数 ----
#   $1  表名 (须位于 default 库, 且含 Array(Float*) 列)
#   $2  距离函数名 (原样拼进 SQL, 如 cosineDistance / L2Distance / dotProduct)
#   $3  排序方向 (ASC / DESC; 点积通常用 DESC)
#   所有参数都有默认值, 缺省时使用下面 TABLE/DISTANCE_FUNC/SORT_DIR 的默认值。
#
# ---- 环境变量 ----
#   无。可执行文件路径与连接地址直接写死在下方常量里, 需要改动时编辑文件。
#
# ---- 依赖命令 ----
#   clickhouse (client 子命令); 其余仅用到 bash 内建与 coreutils
#   (mktemp / wc / rm / ls)。不需要 awk 或 python3。
#
# ---- 输出文件 ----
#   工作目录下的 sql-bench/ 目录 (相对当前工作目录, 不是脚本所在目录):
#     sql-bench/<表名>_normal_<N>.sql   字面量形态, N = 1/10/100/1000
#     sql-bench/<表名>_cast_<N>.sql     字符串 cast 形态
#   每个文件内一行一条 SELECT, 行尾带分号。
#
# ---- 退出码 ----
#   0  全部生成成功
#   1  表不存在或找不到 Array(Float*) 向量列; set -e 下查询/写文件失败也会非 0 退出
#

# 说明: -e 让任一命令失败即退出; -u 禁止引用未定义变量; pipefail 让管道任一环失败
# 都算失败 (本脚本管道较少, 但保持与其它 bench 脚本一致)。
set -euo pipefail

# ---- 配置 ----
# clickhouse 可执行文件与 native 协议地址 (9000 端口)
CLICKHOUSE="/home/ClickHouse/build/programs/clickhouse"
HOST="127.0.0.1"
PORT="9000"
# ${1:-默认值}: 参数未提供或为空时取默认值
TABLE="${1:-local_768d_test}"
DISTANCE_FUNC="${2:-cosineDistance}"
SORT_DIR="${3:-ASC}"
# 输出目录相对于「执行脚本时的当前工作目录」, 因此建议在脚本所在目录下运行
OUTPUT_DIR="sql-bench"

# 每个文件包含的查询条数 (即采样向量个数) 的取值列表
ROW_COUNTS=(1 10 100 1000)
# 每条查询的 LIMIT k
TOP_K=10

# -p 允许目录已存在
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

# ---- 检测向量列 ----
# 在 system.columns 里找该表第一个 Array(Float*) 类型列, 作为距离函数的输入列。
# 末尾的 SETTINGS use_query_cache=0 关闭查询缓存, 避免拿到过期的元数据结果;
# 2>/dev/null 抑制错误输出, 失败时由下面的空值判断统一报错 (|| true 防止 set -e 提前退出)。
vec_col=$("$CLICKHOUSE" client \
    --host "$HOST" \
    --port "$PORT" \
    --query "SELECT name FROM system.columns WHERE database='default' AND table='$TABLE' AND type LIKE 'Array(Float%)' LIMIT 1 SETTINGS use_query_cache=0" 2>/dev/null) || true

# -z 判空: 没查到列 (表不存在 / 无数组列 / 连接失败) 都走这里
if [ -z "$vec_col" ]; then
    echo "错误: 表 '$TABLE' 中未找到 Array(Float*) 类型的向量列"
    exit 1
fi

echo "检测到向量列: $vec_col"
echo ""

# 遍历每个采样条数, 分别生成 normal / cast 两个文件
for count in "${ROW_COUNTS[@]}"; do
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo " 生成 ${count} 条向量的 SQL 文件..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

    # 临时文件保存采样结果: 一行一个向量, 元素用逗号连接。
    # 注意: 本脚本未注册 trap, 中途失败时 mktemp 文件可能残留在 /tmp。
    tmp_vectors=$(mktemp)
    # ORDER BY rand() LIMIT n: 随机抽样 n 行; arrayStringConcat 把数组拼成 "0.1,0.2,..."
    "$CLICKHOUSE" client \
        --host "$HOST" \
        --port "$PORT" \
        --query "SELECT arrayStringConcat($vec_col, ',') FROM $TABLE ORDER BY rand() LIMIT $count SETTINGS use_query_cache=0" \
        > "$tmp_vectors"

    # wc -l 统计实际行数, 与预期不符时只告警不中断 (表行数可能少于 count, 或存在空数组)
    actual_count=$(wc -l < "$tmp_vectors")
    if [ "$actual_count" -ne "$count" ]; then
        echo "警告: 预期 ${count} 条，实际获取 ${actual_count} 条"
    fi

    # ---- normal 形态: 向量直接写成数组字面量 [1,2,...] ----
    # > 先截断 (新建或清空) 目标文件
    normal_file="$OUTPUT_DIR/${TABLE}_normal_${count}.sql"
    > "$normal_file"
    # IFS= read -r: 保留行首尾空白、不解释反斜杠, 适合读数值串;
    # 最后一行无换行符时 read 仍返回非 0, 但内容已写入 vec_str, 这里靠 -z 判断空行。
    while IFS= read -r vec_str; do
        [ -z "$vec_str" ] && continue
        echo "SELECT id, ${DISTANCE_FUNC}(${vec_col}, [${vec_str}]) as dis FROM $TABLE ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};" >> "$normal_file"
    done < "$tmp_vectors"
    echo "  已生成: $normal_file ($(wc -l < "$normal_file") 条查询)"

    # ---- cast 形态: 先拼成字符串 '[...]', 再由 ClickHouse cast 成 Array(Float32) ----
    # 用于对比「字面量数组」与「字符串解析 + cast」两条路径的性能差异。
    cast_file="$OUTPUT_DIR/${TABLE}_cast_${count}.sql"
    > "$cast_file"
    while IFS= read -r vec_str; do
        [ -z "$vec_str" ] && continue
        echo "SELECT id, ${DISTANCE_FUNC}(${vec_col}, cast('[${vec_str}]','Array(Float32)')) as dis FROM $TABLE ORDER BY dis ${SORT_DIR} LIMIT ${TOP_K};" >> "$cast_file"
    done < "$tmp_vectors"
    echo "  已生成: $cast_file ($(wc -l < "$cast_file") 条查询)"

    # 本轮的临时文件用完即删
    rm -f "$tmp_vectors"
    echo ""
done

# -h 人类可读大小; 若目录下没有 .sql 文件, 通配符不匹配会让 ls 报错 (set -e 下退出)
echo "=============================================="
echo " 生成完成! 列出所有文件:"
echo "=============================================="
ls -lh "$OUTPUT_DIR"/*.sql