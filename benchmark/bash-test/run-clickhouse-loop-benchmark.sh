#!/bin/bash
# ClickHouse/MyScale 基准循环编排脚本 (在 xxb-ubuntu22-ck 容器内运行)
#
# 对 5 个数据表 × 2 种随机抽样数量, 依次执行:
#   1) 记录结果 CSV 当前行数, 清空时序日志
#   2) 循环 RUNS(默认10) 次:
#        bash clickhouse-benchmark.sh -t <表序号> -n <抽样条数>
#   3) 10 轮全部结束后, 执行一次:
#        python analyze_myscale_log.py --db <引擎> --batch <本轮CSV新增行数> \
#               --out results/csv/<引擎>-<表名>-log-analysis-<条数>.csv
#      注意: 分析输出按 --out 覆盖写, 必须等全部轮次跑完再执行才不漏数据;
#      analyze_myscale_log.py 按结果 CSV 行与日志 SET 簇尾部对齐切批,
#      --batch 用"组合前后 CSV 行数差"精确定位本轮组合的行, 天然跳过旧组合数据。
#
# 用法:
#   ./run-clickhouse-loop-benchmark.sh                       # 全量: 5 表 × 2 条数 × 10 轮
#   ./run-clickhouse-loop-benchmark.sh -t "1 5" -n "1 1000" -r 3
#   ENGINE=myscale ./run-clickhouse-loop-benchmark.sh        # 改跑 MyScale
#   CH_DIST=2 ./run-clickhouse-loop-benchmark.sh             # 距离函数选 2=L2Distance
#
# 交互注水: 基准脚本的两处 read 按顺序消费下面两行 stdin
#   第1行 -> Step 2 距离函数 (空=按表自动选择 datasets.json 的 distance 字段;
#            可用 CH_DIST=1/2/3 强制覆盖)
#   第2行 -> Step 2.5 索引未生效确认 (默认 y=继续; 可用 CH_CONFIRM 覆盖)
#
# 依赖: 容器内以 root 运行(清空日志需要写 /var/log 权限)。
set -uo pipefail

TABLE_NUMS=(1 2 3 4 5)                  # 数据表序号 = clickhouse-benchmark.sh -l 的编号
ROW_COUNTS=(1 1000)                     # 随机抽样向量条数
RUNS=10                                 # 每个组合循环的基准次数
ENGINE="${ENGINE:-clickhouse}"          # clickhouse | myscale
CH_DIST="${CH_DIST:-}"                  # Step 2 输入(空=按表自动匹配 datasets.json)
CH_CONFIRM="${CH_CONFIRM:-y}"           # Step 2.5 输入(默认继续)
BENCH="${BENCH:-/home/vector-benchmark/benchmark/bash-test/clickhouse-benchmark.sh}"
ANALYZE="${ANALYZE:-/home/vector-benchmark/benchmark/analyze_myscale_log.py}"
RESULTS_DIR="${RESULTS_DIR:-/home/vector-benchmark/benchmark/results/csv}"
PYTHON="${PYTHON:-$(command -v python || command -v python3)}"

# 引擎相关路径: 时序日志 / 结果 CSV / 分析器 --db
# 注意: clickhouse 的查询时序日志在 clickhouse-server.log (需服务端 <level>trace</level>),
#       .err.log 只收错误, 分析器读不到数据; 结果 CSV 是连字符文件名 (与基准脚本一致)。
case "$ENGINE" in
    clickhouse)
        LOG_PATH="/var/log/clickhouse-server/clickhouse-server.err.log"
        RESULT_CSV="$RESULTS_DIR/clickhouse-benchmark-results.csv" ;;
    myscale)
        LOG_PATH="/var/log/myscale-server/myscale-server.err.log"
        RESULT_CSV="$RESULTS_DIR/myscale-benchmark-results.csv" ;;
    *) echo "错误: 未知 ENGINE=$ENGINE (可选 clickhouse/myscale)" >&2; exit 1 ;;
esac

# 表序号 -> 表名(用于分析输出命名); 序号与 -l 列表对应
declare -A TABLE_NAMES=(
    [1]=Benchmark_128_10m
    [2]=Benchmark_1536_1m
    [3]=Benchmark_256_290k
    [4]=Benchmark_768_1m
    [5]=Benchmark_960_1m
)

usage() {
    local self; self=$(basename "$0")
    cat >&2 <<EOF
用法: $self [选项]

选项:
  -t <序号序列>   数据表序号, 空格/逗号分隔 (默认 "1 2 3 4 5")
  -n <数值序列>   随机抽样条数, 空格/逗号分隔 (默认 "1 1000")
  -r <次数>       每个组合循环的基准次数 (默认 10)
  -h              显示本帮助

环境变量: ENGINE(clickhouse|myscale)/CH_DIST/CH_CONFIRM/RUNS/LOG_PATH/
BENCH/ANALYZE/RESULTS_DIR/PYTHON 可覆盖默认值; 其余变量 (TIMELIMIT/REPEAT/
MYS_SCALE_EF 等) 原样透传给 clickhouse-benchmark.sh。
EOF
    exit "${1:-0}"
}

# ---- 命令行参数解析 (覆盖默认表序号/抽样条数/循环次数) ----
while [ "$#" -gt 0 ]; do
    case "$1" in
        -t) [ -n "${2:-}" ] || { echo "错误: -t 缺少序号序列" >&2; usage 1; }
            IFS=', ' read -r -a TABLE_NUMS <<< "$2"; shift 2 ;;
        -n) [ -n "${2:-}" ] || { echo "错误: -n 缺少数值序列" >&2; usage 1; }
            IFS=', ' read -r -a ROW_COUNTS <<< "$2"; shift 2 ;;
        -r) [ -n "${2:-}" ] || { echo "错误: -r 缺少次数" >&2; usage 1; }
            RUNS="$2"; shift 2 ;;
        -h|--help) usage 0 ;;
        *)  echo "未知选项: $1" >&2; usage 1 ;;
    esac
done

[ -f "$BENCH" ] || { echo "错误: 基准脚本不存在: $BENCH" >&2; exit 1; }
[ -f "$ANALYZE" ] || { echo "错误: 分析脚本不存在: $ANALYZE" >&2; exit 1; }
[ -n "$PYTHON" ] || { echo "错误: 未找到 python 解释器" >&2; exit 1; }

csv_rows() { [ -f "$RESULT_CSV" ] && wc -l < "$RESULT_CSV" | tr -d ' ' || echo 0; }

FAILS=0
START_ALL=$(date +%s)
echo ">>> 编排开始: 引擎=$ENGINE 表序号=[${TABLE_NUMS[*]}] 抽样条数=[${ROW_COUNTS[*]}] 每组合 ${RUNS} 轮"

for t in "${TABLE_NUMS[@]}"; do
    tname="${TABLE_NAMES[$t]:-}"
    if [ -z "$tname" ]; then
        echo "  (warn) 表序号 $t 不在映射表中, 分析输出将用 result 命名; 可在脚本内 TABLE_NAMES 补充"
        tname="result"
    fi
    for n in "${ROW_COUNTS[@]}"; do
        echo ""
        echo "===== 组合: 表$t ($tname) 抽样条数=$n ====="
        combo_start=$(date +%s)
        rows_pre=$(csv_rows)

        # 1) 清空时序日志 (本组合 RUNS 轮日志从零开始累计)
        if [ -f "$LOG_PATH" ]; then
            if ! truncate -s 0 "$LOG_PATH" 2>/dev/null && ! : > "$LOG_PATH" 2>/dev/null; then
                echo "错误: 无法清空日志 $LOG_PATH (需要 root)" >&2
                exit 1
            fi
            echo "已清空日志: $LOG_PATH"
        else
            echo "(日志尚不存在, 首轮运行时创建: $LOG_PATH)"
        fi

        # 2) 循环 RUNS 次基准 (分析输出会覆盖, 故等全部轮次跑完后再执行)
        for i in $(seq 1 "$RUNS"); do
            echo ""
            echo "----- 第 $i/$RUNS 轮 | 表$t($tname) n=$n -----"
            # stdin 注水: 第1行=距离函数选择, 第2行=索引未生效确认
            if printf '%s\n%s\n' "$CH_DIST" "$CH_CONFIRM" | bash "$BENCH" -t "$t" -n "$n"; then
                echo "  基准运行完成"
            else
                echo "  (warn) 基准运行失败(退出码 $?)" >&2
                FAILS=$((FAILS+1))
            fi
        done

        # 3) 全部轮次结束后一次性解析: --batch=本轮组合新增 CSV 行数, 精确对齐日志尾部
        rows_post=$(csv_rows)
        batch_n=$(( rows_post - rows_pre ))
        out_csv="$RESULTS_DIR/${ENGINE}-${tname}-log-analysis-${n}.csv"
        echo ""
        if [ "$batch_n" -le 0 ]; then
            echo "  (warn) 结果 CSV 未新增行($rows_pre -> $rows_post), 跳过日志分析" >&2
            FAILS=$((FAILS+1))
        elif "$PYTHON" "$ANALYZE" --db "$ENGINE" --log "$LOG_PATH" --csv "$RESULT_CSV" \
                 --batch "$batch_n" --out "$out_csv" >/dev/null; then
            echo "  分析完成: 批次=$batch_n, 输出 $out_csv"
        else
            echo "  (warn) 日志分析失败" >&2
            FAILS=$((FAILS+1))
        fi
        echo "===== 组合完成: $tname n=$n, 耗时 $(( $(date +%s) - combo_start ))s ====="
    done
done

echo ""
echo ">>> 编排结束: 总耗时 $(( $(date +%s) - START_ALL ))s, 失败步骤数 $FAILS"
exit 0
