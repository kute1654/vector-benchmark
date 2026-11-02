#!/bin/bash
# pgvector 基准循环编排脚本 (在 xxb-ubuntu22-ck 容器内运行)
#
# 对 5 个数据表 × 2 种随机抽样数量, 依次执行:
#   1) 清空 PG 服务日志 (/usr/local/pgsql/data/log/postgresql.log)
#   2) 循环 RUNS(默认10) 次:
#        bash pgvector-benchmark.sh -t <表序号> -n <抽样条数>
#   3) 10 轮全部结束后, 执行一次:
#        python gen_step_time_report.py -t <表名> -n <抽样条数>
#      注意: 报告脚本每次执行会覆盖同名输出, 必须等全部轮次跑完再执行,
#      否则只能解析到已跑的部分轮次、漏掉数据。
#      输出 results/csv/pgvector_log_analysis-<表名>-<条数>.csv 与对应占比图
#
# 用法:
#   ./run-pgvector-loop-benchmark.sh                       # 全量: 5 表 × 2 条数 × 10 轮
#   ./run-pgvector-loop-benchmark.sh -t "1 7" -n "1 1000" -r 3
#   RUNS=2 TIMELIMIT=1 REPEAT=1 ./run-pgvector-loop-benchmark.sh   # 快速冒烟(env 透传给基准脚本)
#
# 依赖: 容器内以 root 运行(清空日志需要写 /usr/local/pgsql/data/log 权限)。
set -uo pipefail

TABLE_NUMS=(1 2 3 4 7)                  # 数据表序号 = pgvector-benchmark.sh -l 的编号
ROW_COUNTS=(1 1000)                     # 随机抽样向量条数
RUNS=10                                 # 每个组合循环的基准次数
PGLOG="${PGLOG:-/usr/local/pgsql/data/log/postgresql.log}"
BENCH="${BENCH:-/home/vector-benchmark/benchmark/bash-test/pgvector-benchmark.sh}"
REPORT="${REPORT:-/home/vector-benchmark/benchmark/gen_step_time_report.py}"
PYTHON="${PYTHON:-$(command -v python || command -v python3)}"

# 表序号 -> 表名(用于报告输出文件命名); 序号与 -l 列表对应
declare -A TABLE_NAMES=(
    [1]=benchmark_1536_1m
    [2]=benchmark_256_290k
    [3]=benchmark_768_1m
    [4]=benchmark_960_1m
    [7]=benchmark_sift_128
)

usage() {
    local self; self=$(basename "$0")
    cat >&2 <<EOF
用法: $self [选项]

选项:
  -t <序号序列>   数据表序号, 空格/逗号分隔 (默认 "1 2 3 4 7")
  -n <数值序列>   随机抽样条数, 空格/逗号分隔 (默认 "1 1000")
  -r <次数>       每个组合循环的基准次数 (默认 10)
  -h              显示本帮助

环境变量: RUNS/PGLOG/BENCH/REPORT/PYTHON 可覆盖默认值; 其余变量
(TIMELIMIT/REPEAT/WARMUP_TIMELIMIT 等) 原样透传给 pgvector-benchmark.sh。
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

[ -x "$BENCH" ] || { echo "错误: 基准脚本不可执行: $BENCH" >&2; exit 1; }
[ -f "$REPORT" ] || { echo "错误: 报告脚本不存在: $REPORT" >&2; exit 1; }
[ -n "$PYTHON" ] || { echo "错误: 未找到 python 解释器" >&2; exit 1; }

FAILS=0
START_ALL=$(date +%s)
echo ">>> 编排开始: 表序号=[${TABLE_NUMS[*]}] 抽样条数=[${ROW_COUNTS[*]}] 每组合 ${RUNS} 轮"

for t in "${TABLE_NUMS[@]}"; do
    tname="${TABLE_NAMES[$t]:-}"
    if [ -z "$tname" ]; then
        echo "  (warn) 表序号 $t 不在映射表中, 报告文件名将用 unknown; 可在脚本内 TABLE_NAMES 补充"
        tname="unknown"
    fi
    for n in "${ROW_COUNTS[@]}"; do
        echo ""
        echo "===== 组合: 表$t ($tname) 抽样条数=$n ====="
        combo_start=$(date +%s)

        # 1) 清空 PG 服务日志 (本轮组合的 10 轮日志将从零开始累计)
        if [ -f "$PGLOG" ]; then
            if ! truncate -s 0 "$PGLOG" 2>/dev/null && ! : > "$PGLOG" 2>/dev/null; then
                echo "错误: 无法清空日志 $PGLOG (需要 root)" >&2
                exit 1
            fi
            echo "已清空日志: $PGLOG"
        else
            echo "(日志尚不存在, 首轮运行时创建: $PGLOG)"
        fi

        # 2) 循环 RUNS 次基准 (报告脚本会覆盖输出, 故等全部轮次跑完后再执行)
        for i in $(seq 1 "$RUNS"); do
            echo ""
            echo "----- 第 $i/$RUNS 轮 | 表$t($tname) n=$n -----"
            if bash "$BENCH" -t "$t" -n "$n"; then
                echo "  基准运行完成"
            else
                echo "  (warn) 基准运行失败(退出码 $?)" >&2
                FAILS=$((FAILS+1))
            fi
        done

        # 3) 全部轮次结束后一次性解析日志生成报告 (按批次平均合并 RUNS 轮)
        echo ""
        if "$PYTHON" "$REPORT" -t "$tname" -n "$n"; then
            echo "  报告生成完成 (合并累计 $RUNS 轮日志)"
        else
            echo "  (warn) 报告生成失败" >&2
            FAILS=$((FAILS+1))
        fi
        echo "===== 组合完成: $tname n=$n, 耗时 $(( $(date +%s) - combo_start ))s ====="
    done
done

echo ""
echo ">>> 编排结束: 总耗时 $(( $(date +%s) - START_ALL ))s, 失败步骤数 $FAILS"
exit 0
