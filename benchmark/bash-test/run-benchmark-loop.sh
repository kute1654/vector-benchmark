#!/bin/bash
# 统一基准循环编排脚本 (pgvector / clickhouse / myscale, 在 xxb-ubuntu22-ck 容器内运行)
#
# 把 run-pgvector-loop-benchmark.sh 与 run-clickhouse-loop-benchmark.sh 合并成一个脚本:
# 对每个数据库类型, 先确保服务端在运行, 再按"表 × 抽样条数"组合循环执行基准脚本
# (rounds 见 combo_rounds: n=1 跑 RUNS 轮不同单条向量, n>1 只跑 1 轮)。
# 日志分析默认关闭(也不清空服务日志), 避免覆盖已有分析数据;
# 需要时加 --with-analysis 开启。
#
# 各类型的执行内容 (含与原脚本的差异):
#   pgvector:
#     1) (仅 --with-analysis) 清空 PG 服务日志 (PGLOG)
#     2) 循环 rounds 次 (n=1 时 rounds=RUNS; n>1 时 rounds=1): bash pgvector-benchmark.sh -t <表序号> -n <抽样条数>
#        n=1 时第 i 轮传 SQL_SET=$i: 第 i 轮固定用第 i 组单条 SQL (缺则建、有则用),
#        共 RUNS 组不同向量, 免去每轮重新构建 SQL 的耗时; n>1 时该文件已含 n 条
#        不同查询, 重复多轮没有意义, 只跑 1 轮
#     3) (仅 --with-analysis) 全部轮次后执行一次(只出汇总 CSV, 不出图):
#          python analyze_pgvector_log.py --log <PGLOG> --batches all --aggregate \
#                 --engine <表名> --out results/csv/pgvector_log_analysis-<表名>-<条数>.csv
#   clickhouse / myscale:
#     1) (仅 --with-analysis) 记录结果 CSV 行数, 清空时序日志
#     2) 循环 rounds 次 (n=1 时 rounds=RUNS; n>1 时 rounds=1):
#          printf '<距离函数>\n<确认>\n' | ENGINE=<引擎> bash clickhouse-benchmark.sh -t <序号> -n <条数>
#        n=1 的组合直接跳过 (单条抽样测试仅 pgvector 执行, CK/MS 构建 SQL 耗时)
#     3) (仅 --with-analysis) 全部轮次后执行一次:
#          python analyze_myscale_log.py --db <引擎> --log <日志> --csv <结果CSV> \
#                 --batch <本轮CSV新增行数> --out results/csv/<引擎>-<表名>-log-analysis-<条数>.csv
#     分析输出按 --out 覆盖写(会覆盖同名旧文件, 故默认关闭); 必须等全部轮次跑完
#     再执行才不漏数据; --batch 用"组合前后 CSV 行数差"精确定位本轮组合的行,
#     天然跳过旧组合数据。
#
# 服务端启停 (默认开启, 命令取自 /home/command.txt):
#   pgvector   -> su - postgres -c "pg_ctl -D /usr/local/pgsql/data start"   端口 5432
#   clickhouse -> /home/ClickHouse/build/programs/clickhouse-server \
#                   --config-file /etc/clickhouse-server/config.xml
#   myscale    -> /home/clickhouse/build/programs/clickhouse-server \
#                   --config-file /etc/myscale-server/config.xml
#   注意: ClickHouse 与 MyScale 是同一份代码、同一组端口(9000/8123/9009/9181/9234),
#   二者互斥; 启动前先停掉对方, 且要等进程消失、Keeper 端口也释放后再启动,
#   否则会报 "Cannot create interserver listener on port 9234"。
#   ClickHouse 由 clickhouse-watchdog 拉起 server, 停止时按 config 路径整组终止
#   (先 TERM 后必要时 KILL), 避免 watchdog 把 server 重新拉起来。
#   用 --no-server 可跳过服务端管理(假定已由外部启动)。
#
# 交互注水 (仅 CK/MS): clickhouse-benchmark.sh 的两处 read 按顺序消费下面两行 stdin
#   第1行 -> Step 2 距离函数 (空=按表自动选择 datasets.json 的 distance 字段;
#            可用 CH_DIST=1/2/3 强制覆盖)
#   第2行 -> Step 2.5 索引未生效确认 (默认 y=继续; 可用 CH_CONFIRM 覆盖)
#
# 表序号默认值按类型不同(两套脚本各自维护, 合并不改动语义):
#   pgvector   : 1 2 3 4 7      (benchmark_1536_1m / _256_290k / _768_1m / _960_1m / _sift_128)
#   clickhouse : 1 2 3 4 5      (Benchmark_128_10m / _1536_1m / _256_290k / _768_1m / _960_1m)
#   myscale    : 同 clickhouse
#   用 --tables 可统一覆盖所有类型的表序号。
#
# 用法:
#   ./run-benchmark-loop.sh                                  # 全量: 3 类型 × 默认表 × 2 条数
#                                                            #   n=1 每组合 10 轮, n=1000 每组合 1 轮
#   ./run-benchmark-loop.sh -t "pgvector" -r 2               # 只跑 pgvector, n=1 每组合 2 轮 (n>1 仍为 1 轮)
#   ./run-benchmark-loop.sh -t "clickhouse myscale" -tables "1 3" -n "1"
#   ENGINE 无关: CK/MS 由 -t 决定, CH_DIST/CH_CONFIRM 由环境变量控制
#   ./run-benchmark-loop.sh --no-server                      # 不管理服务端(外部已启动)
#
# 依赖: 容器内以 root 运行(清空日志、pg_ctl/su - postgres 与启停 CK/MS 需要权限)。
set -uo pipefail

ENGINE_TYPES=(clickhouse myscale pgvector)   # 数据库类型
PG_TABLE_NUMS=(1 2 3 4 7)                   # pgvector 默认表序号(pgvector-benchmark.sh -l)
CH_TABLE_NUMS=(1 2 3 4 5)                   # CK/MS 默认表序号(clickhouse-benchmark.sh -l)
ROW_COUNTS=(1 1000)                         # 随机抽样向量条数
USER_TABLES=()                              # --tables 覆盖; 空=按类型用上面的默认值
RUNS="${RUNS:-10}"                          # 每个组合循环的基准次数
MANAGE_SERVER=1                             # 1=按类型启停服务端; --no-server 置 0
DO_ANALYSIS=0                               # 1=组合后做日志分析(不出图); --with-analysis 置 1

# CK/MS 交互注水: 距离函数选择 / 索引未生效确认
CH_DIST="${CH_DIST:-}"                      # 空=按表自动匹配 datasets.json
CH_CONFIRM="${CH_CONFIRM:-y}"

PYTHON="${PYTHON:-$(command -v python || command -v python3)}"
RESULTS_DIR="${RESULTS_DIR:-/home/vector-benchmark/benchmark/results/csv}"

# ---- pgvector 相关路径 ----
PG_BENCH="${PG_BENCH:-/home/vector-benchmark/benchmark/bash-test/pgvector-benchmark.sh}"
PG_ANALYZE="${PG_ANALYZE:-/home/vector-benchmark/benchmark/analyze_pgvector_log.py}"
PGLOG="${PGLOG:-/usr/local/pgsql/data/log/postgresql.log}"

# ---- ClickHouse / MyScale 相关路径 ----
CH_BENCH="${CH_BENCH:-/home/vector-benchmark/benchmark/bash-test/clickhouse-benchmark.sh}"
CH_ANALYZE="${CH_ANALYZE:-/home/vector-benchmark/benchmark/analyze_myscale_log.py}"
# 查询时序日志实际写在 stderr 日志(.err.log)里; 用 clickhouse-server.log 常因日志
# 滚动只对齐到少量批次。CK 与 MS 的分析都读 .err.log。
CK_LOG="${CK_LOG:-/var/log/clickhouse-server/clickhouse-server.err.log}"
CK_RESULT_CSV="${CK_RESULT_CSV:-$RESULTS_DIR/clickhouse-benchmark-results.csv}"
MS_LOG="${MS_LOG:-/var/log/myscale-server/myscale-server.err.log}"
MS_RESULT_CSV="${MS_RESULT_CSV:-$RESULTS_DIR/myscale-benchmark-results.csv}"

# 表序号 -> 表名(用于报告/分析输出命名); 序号与各自基准脚本 -l 的列表对应
declare -A PG_TABLE_NAMES=(
    [1]=benchmark_1536_1m
    [2]=benchmark_256_290k
    [3]=benchmark_768_1m
    [4]=benchmark_960_1m
    [7]=benchmark_sift_128
)
declare -A CH_TABLE_NAMES=(
    [1]=Benchmark_128_10m
    [2]=Benchmark_1536_1m
    [3]=Benchmark_256_290k
    [4]=Benchmark_768_1m
    [5]=Benchmark_960_1m
)

# ---- 服务端路径/端口 (取值见 /home/command.txt) ----
PG_CTL="${PG_CTL:-/usr/local/pgsql/bin/pg_ctl}"
PG_DATA="${PG_DATA:-/usr/local/pgsql/data}"
PG_PORT="${PG_PORT:-5432}"
CK_BIN="${CK_BIN:-/home/ClickHouse/build/programs/clickhouse-server}"
CK_CONF="${CK_CONF:-/etc/clickhouse-server/config.xml}"
MS_BIN="${MS_BIN:-/home/clickhouse/build/programs/clickhouse-server}"
MS_CONF="${MS_CONF:-/etc/myscale-server/config.xml}"
CH_PORT="${CH_PORT:-9000}"                  # CK 与 MS 共用; 8123 亦复用
CK_STDOUT="${CK_STDOUT:-/var/log/clickhouse-server/stdout.log}"
MS_STDOUT="${MS_STDOUT:-/var/log/myscale-server/stdout.log}"
START_TIMEOUT="${START_TIMEOUT:-180}"       # 服务端启动后等待端口就绪的秒数
STOP_TIMEOUT="${STOP_TIMEOUT:-90}"          # 停止后等待端口释放的秒数

usage() {
    local self; self=$(basename "$0")
    cat >&2 <<EOF
用法: $self [选项]

选项:
  -t <类型序列>     数据库类型, 空格/逗号分隔 (默认 "pgvector clickhouse myscale")
      --tables <序号> 表序号序列, 空格/逗号分隔 (默认按类型: pgvector "1 2 3 4 7",
                     clickhouse/myscale "1 2 3 4 5")
  -n <条数序列>     随机抽样向量条数, 空格/逗号分隔 (默认 "1 1000")
  -r <次数>         每个组合循环的基准次数 (默认 10)
      --no-server   不管理服务端(假定已由外部启动); 默认按类型自动启停
      --with-analysis 组合结束后执行日志分析(只出汇总 CSV, 不出图; 会清空服务日志, 默认关闭)
  -h                显示本帮助

环境变量: RUNS/PYTHON/CH_DIST/CH_CONFIRM/RESULTS_DIR 可覆盖默认值;
基准脚本与分析脚本路径: PG_BENCH/PG_ANALYZE/PGLOG/CH_BENCH/CH_ANALYZE/
CK_LOG/CK_RESULT_CSV/MS_LOG/MS_RESULT_CSV;
服务端路径/端口/超时: PG_CTL/PG_DATA/PG_PORT/CK_BIN/CK_CONF/MS_BIN/MS_CONF/
CH_PORT/CK_STDOUT/MS_STDOUT/START_TIMEOUT/STOP_TIMEOUT 均可覆盖。
其余变量(TIMELIMIT/REPEAT/WARMUP_TIMELIMIT/MYS_SCALE_EF/SQL_REGEN 等)原样透传给基准脚本。
SQL_REGEN=1(默认)每次运行重新生成 SQL; SQL_REGEN=0 复用已有文件。
EOF
    exit "${1:-0}"
}

# ---- 命令行参数解析 (覆盖默认类型/表序号/条数/循环次数) ----
while [ "$#" -gt 0 ]; do
    case "$1" in
        -t) [ -n "${2:-}" ] || { echo "错误: -t 缺少类型序列" >&2; usage 1; }
            IFS=', ' read -r -a ENGINE_TYPES <<< "$2"; shift 2 ;;
        --tables|-b) [ -n "${2:-}" ] || { echo "错误: --tables 缺少序号序列" >&2; usage 1; }
            IFS=', ' read -r -a USER_TABLES <<< "$2"; shift 2 ;;
        -n) [ -n "${2:-}" ] || { echo "错误: -n 缺少数值序列" >&2; usage 1; }
            IFS=', ' read -r -a ROW_COUNTS <<< "$2"; shift 2 ;;
        -r) [ -n "${2:-}" ] || { echo "错误: -r 缺少次数" >&2; usage 1; }
            RUNS="$2"; shift 2 ;;
        --no-server) MANAGE_SERVER=0; shift ;;
        --with-analysis) DO_ANALYSIS=1; shift ;;
        -h|--help) usage 0 ;;
        *)  echo "未知选项: $1" >&2; usage 1 ;;
    esac
done

[ -f "$PG_BENCH" ] || { echo "错误: pgvector 基准脚本不存在: $PG_BENCH" >&2; exit 1; }
[ -f "$CH_BENCH" ] || { echo "错误: clickhouse 基准脚本不存在: $CH_BENCH" >&2; exit 1; }
if [ "$DO_ANALYSIS" -eq 1 ]; then
    [ -f "$PG_ANALYZE" ] || { echo "错误: pgvector 日志分析脚本不存在: $PG_ANALYZE" >&2; exit 1; }
    [ -f "$CH_ANALYZE" ] || { echo "错误: CK/MS 分析脚本不存在: $CH_ANALYZE" >&2; exit 1; }
    [ -n "$PYTHON" ] || { echo "错误: 未找到 python 解释器" >&2; exit 1; }
fi
case "$RUNS" in
    ''|*[!0-9]*) echo "错误: -r 必须是正整数: $RUNS" >&2; exit 1 ;;
esac
[ "$RUNS" -ge 1 ] || { echo "错误: -r 必须 >= 1: $RUNS" >&2; exit 1; }

# --------------------------------------------------------------------------- #
# 服务端启停 (命令取自 /home/command.txt)
# --------------------------------------------------------------------------- #
# 端口探测用 bash 的 /dev/tcp: 连得上即视为该端口有服务在监听。
port_open() {
    (exec 3<>"/dev/tcp/127.0.0.1/$1") >/dev/null 2>&1
}

wait_port_open() {
    local port=$1 timeout=$2 i=0
    while [ "$i" -lt "$timeout" ]; do
        port_open "$port" && return 0
        sleep 1; i=$((i+1))
    done
    return 1
}

# 等服务端彻底停止: 进程全部消失 且 端口释放。
# 只等 9000 不够: Keeper 的 9181/raft 9234 会比 tcp/http 端口晚释放, 若此时
# 立刻启动另一个(CK/MS 共用这组端口)会报 "Address already in use on port 9234"。
wait_ch_stopped() {
    local conf=$1 port=$2 timeout=$3 i=0
    while [ "$i" -lt "$timeout" ]; do
        if [ -z "$(ch_pids "$conf")" ] && ! port_open "$port"; then
            return 0
        fi
        sleep 1; i=$((i+1))
    done
    return 1
}

# PostgreSQL: pg_ctl status 返回 0 即运行中
pg_running() { su - postgres -c "$PG_CTL -D $PG_DATA status" >/dev/null 2>&1; }

start_pg() {
    echo "  启动 PostgreSQL (数据目录 $PG_DATA)"
    if ! su - postgres -c "$PG_CTL -D $PG_DATA start -w" >/dev/null 2>&1; then
        echo "  (error) pg_ctl start 失败, 请查看 $PG_DATA/log 下的日志" >&2
        return 1
    fi
    wait_port_open "$PG_PORT" "$START_TIMEOUT" || {
        echo "  (error) PostgreSQL 端口 $PG_PORT 未就绪" >&2; return 1; }
    echo "  PostgreSQL 端口 $PG_PORT 已就绪"
}

# ClickHouse/MyScale: 进程按 "--config-file <conf>" 精确匹配, 避免误匹配到
# 命令行中恰好包含配置路径的其它进程(watchdog 与 server 的 argv 都含该参数)。
ch_pids() { pgrep -f -- "--config-file $1" 2>/dev/null || true; }

# 是否已在提供服务: 以端口监听为准(端口未开即视为未就绪, 交由调用方处理)
ch_running() { port_open "${1:-$CH_PORT}"; }

stop_ch_like() {
    local name=$1 conf=$2 port=$3 pids
    pids=$(ch_pids "$conf")
    if [ -z "$pids" ]; then
        echo "  $name 未运行, 无需停止"
        return 0
    fi
    echo "  停止 $name (PID: $(echo "$pids" | tr '\n' ' '))"
    # watchdog 与 server 的 argv 都含 --config-file <conf>, 一起 TERM;
    # watchdog 先退出后就不会再把 server 拉起来
    pkill -TERM -f -- "--config-file $conf" 2>/dev/null || true
    if ! wait_ch_stopped "$conf" "$port" "$STOP_TIMEOUT"; then
        echo "  (warn) $name 在 ${STOP_TIMEOUT}s 内未完全停止, 强制 KILL" >&2
        pkill -KILL -f -- "--config-file $conf" 2>/dev/null || true
        wait_ch_stopped "$conf" "$port" 20 \
            || echo "  (warn) $name 仍有残留进程或端口 $port 仍被占用" >&2
    fi
    echo "  $name 已停止"
}

start_ch_like() {
    local name=$1 bin=$2 conf=$3 port=$4 stdout_file=$5
    echo "  启动 $name: $bin --config-file $conf"
    # 后台启动并脱离终端; stdout/stderr 追加到服务端自己的 stdout 日志便于排错
    nohup "$bin" --config-file "$conf" >> "$stdout_file" 2>&1 &
    if ! wait_port_open "$port" "$START_TIMEOUT"; then
        echo "  (error) $name 在 ${START_TIMEOUT}s 内未就绪, 日志尾部($stdout_file):" >&2
        tail -n 20 "$stdout_file" >&2 2>/dev/null || true
        return 1
    fi
    echo "  $name 端口 $port 已就绪"
}

# 确保指定类型的服务端在运行; ClickHouse 与 MyScale 互斥(同一组端口), 先停对方
ensure_server() {
    case "$1" in
        pgvector)
            if pg_running; then
                echo "  PostgreSQL 已在运行 (端口 $PG_PORT)"
            else
                start_pg || return 1
            fi
            ;;
        clickhouse)
            stop_ch_like myscale "$MS_CONF" "$CH_PORT"
            if ch_running "$CH_PORT"; then
                echo "  ClickHouse 已在运行 (端口 $CH_PORT)"
            else
                # 端口未开但仍有残留进程(启动卡住/上次异常退出) -> 先清干净再启动
                if [ -n "$(ch_pids "$CK_CONF")" ]; then
                    echo "  (warn) 发现 ClickHouse 残留进程但端口未就绪, 先停止"
                    stop_ch_like clickhouse "$CK_CONF" "$CH_PORT"
                fi
                start_ch_like clickhouse "$CK_BIN" "$CK_CONF" "$CH_PORT" "$CK_STDOUT" || return 1
            fi
            ;;
        myscale)
            stop_ch_like clickhouse "$CK_CONF" "$CH_PORT"
            if ch_running "$CH_PORT"; then
                echo "  MyScale 已在运行 (端口 $CH_PORT)"
            else
                if [ -n "$(ch_pids "$MS_CONF")" ]; then
                    echo "  (warn) 发现 MyScale 残留进程但端口未就绪, 先停止"
                    stop_ch_like myscale "$MS_CONF" "$CH_PORT"
                fi
                start_ch_like myscale "$MS_BIN" "$MS_CONF" "$CH_PORT" "$MS_STDOUT" || return 1
            fi
            ;;
        *)
            echo "错误: 未知数据库类型 $1 (可选 pgvector/myscale/clickhouse)" >&2
            return 1
            ;;
    esac
}

# --------------------------------------------------------------------------- #
# 组合执行
# --------------------------------------------------------------------------- #
FAILS=0
OKS=0

# 清空日志; 失败返回 1(由调用方计入 FAILS 并跳过该组合, 不中断整个编排)
clear_log() {
    local log=$1
    if [ ! -f "$log" ]; then
        echo "(日志尚不存在, 首轮运行时创建: $log)"
        return 0
    fi
    if ! truncate -s 0 "$log" 2>/dev/null && ! : > "$log" 2>/dev/null; then
        echo "错误: 无法清空日志 $log (需要 root)" >&2
        return 1
    fi
    echo "已清空日志: $log"
}

# 结果 CSV 当前行数(文件不存在视为 0)
csv_rows() {
    [ -f "$1" ] && wc -l < "$1" | tr -d ' ' || echo 0
}

# 每个组合实际执行的轮数。
# 参数: $1 = 抽样向量条数 n。
# n=1 时每轮只用 1 条向量, 需要 RUNS 轮才能覆盖 RUNS 个不同向量;
# n>1 时单个 SQL 文件已含 n 条不同查询(如 1000 条), 重复跑多轮没有意义,
# 反而把耗时放大 RUNS 倍, 因此固定只跑 1 轮。
combo_rounds() {
    if [ "$1" = "1" ]; then
        echo "$RUNS"
    else
        echo 1
    fi
}

# pgvector 组合: [清日志] -> rounds 轮 pgvector-benchmark.sh -> [一次性日志分析]
# (方括号步骤仅 --with-analysis 时执行; 只出汇总 CSV, 不出图)
run_pgvector_combo() {
    local num=$1 n=$2 tname=$3 i rounds out_csv
    rounds=$(combo_rounds "$n")
    if [ "$DO_ANALYSIS" -eq 1 ]; then
        clear_log "$PGLOG" || { FAILS=$((FAILS+1)); return; }
    fi
    for i in $(seq 1 "$rounds"); do
        echo ""
        echo "----- 第 $i/$rounds 轮 | 表$num($tname) n=$n -----"
        # 单条抽样(n=1): 第 i 轮传 SQL_SET=$i 固定组号文件名, 共 RUNS 组不同向量;
        # 默认 SQL_REGEN=1 每轮重新抽样生成(不复用)
        if [ "$n" = "1" ] && SQL_SET="$i" bash "$PG_BENCH" -t "$num" -n "$n"; then
            echo "  基准运行完成"
            OKS=$((OKS+1))
        elif [ "$n" != "1" ] && bash "$PG_BENCH" -t "$num" -n "$n"; then
            echo "  基准运行完成"
            OKS=$((OKS+1))
        else
            echo "  (warn) 基准运行失败(退出码 $?)" >&2
            FAILS=$((FAILS+1))
        fi
    done
    # 全部轮次结束后一次性解析累计多轮日志(按格子平均合并), 只写汇总 CSV; 默认关闭。
    if [ "$DO_ANALYSIS" -eq 1 ]; then
        echo ""
        out_csv="$RESULTS_DIR/pgvector_log_analysis-${tname}-${n}.csv"
        if "$PYTHON" "$PG_ANALYZE" --log "$PGLOG" --batches all --aggregate \
             --engine "$tname" --out "$out_csv"; then
            echo "  日志分析完成: $out_csv"
        else
            echo "  (warn) 日志分析失败" >&2
            FAILS=$((FAILS+1))
        fi
    fi
}

# CK/MS 组合: [记 CSV 行数 -> 清日志] -> rounds 轮 clickhouse-benchmark.sh -> [一次性分析]
# (方括号步骤仅 --with-analysis 时执行)
run_ch_combo() {
    local eng=$1 num=$2 n=$3 tname=$4
    local log_path csv_path rows_pre rows_post batch_n out_csv i rounds
    case "$eng" in
        clickhouse) log_path=$CK_LOG;  csv_path=$CK_RESULT_CSV ;;
        myscale)    log_path=$MS_LOG;  csv_path=$MS_RESULT_CSV ;;
    esac

    rows_pre=0
    rounds=$(combo_rounds "$n")
    if [ "$DO_ANALYSIS" -eq 1 ]; then
        rows_pre=$(csv_rows "$csv_path")
        clear_log "$log_path" || { FAILS=$((FAILS+1)); return; }
    fi
    for i in $(seq 1 "$rounds"); do
        echo ""
        echo "----- 第 $i/$rounds 轮 | 表$num($tname) n=$n -----"
        # stdin 注水: 第1行=距离函数选择, 第2行=索引未生效确认
        if printf '%s\n%s\n' "$CH_DIST" "$CH_CONFIRM" | \
                ENGINE="$eng" bash "$CH_BENCH" -t "$num" -n "$n"; then
            echo "  基准运行完成"
            OKS=$((OKS+1))
        else
            echo "  (warn) 基准运行失败(退出码 $?)" >&2
            FAILS=$((FAILS+1))
        fi
    done

    # 全部轮次结束后一次性解析: --batch=本轮组合新增 CSV 行数, 精确对齐日志尾部;
    # 默认关闭, 避免覆盖已有分析输出
    if [ "$DO_ANALYSIS" -eq 1 ]; then
        rows_post=$(csv_rows "$csv_path")
        batch_n=$(( rows_post - rows_pre ))
        # 表头变化(如 REPEAT 变了)会让 clickhouse-benchmark.sh 的 ensure_csv 把旧 CSV
        # 改名备份并新建文件, 行数差因此变成负数; 此时新文件只含本轮数据, 按它的
        # 数据行数(去掉表头)分析, 不跳过。
        if [ "$batch_n" -le 0 ] && [ "$rows_post" -gt 1 ]; then
            batch_n=$(( rows_post - 1 ))
            echo "  (info) 结果 CSV 已重建($rows_pre -> $rows_post 行), 按新文件 $batch_n 行分析" >&2
        fi
        out_csv="$RESULTS_DIR/${eng}-${tname}-log-analysis-${n}.csv"
        echo ""
        if [ "$batch_n" -le 0 ]; then
            echo "  (warn) 结果 CSV 未新增行($rows_pre -> $rows_post), 跳过日志分析" >&2
            FAILS=$((FAILS+1))
        elif "$PYTHON" "$CH_ANALYZE" --db "$eng" --log "$log_path" --csv "$csv_path" \
                 --batch "$batch_n" --out "$out_csv" >/dev/null; then
            echo "  分析完成: 批次=$batch_n, 输出 $out_csv"
        else
            echo "  (warn) 日志分析失败" >&2
            FAILS=$((FAILS+1))
        fi
    fi
}

START_ALL=$(date +%s)
if [ "$DO_ANALYSIS" -eq 1 ]; then
    echo ">>> 编排开始: 类型=[${ENGINE_TYPES[*]}] 抽样条数=[${ROW_COUNTS[*]}] 每组合 n=1 跑 ${RUNS} 轮 / n>1 跑 1 轮 | 日志分析: 开启"
else
    echo ">>> 编排开始: 类型=[${ENGINE_TYPES[*]}] 抽样条数=[${ROW_COUNTS[*]}] 每组合 n=1 跑 ${RUNS} 轮 / n>1 跑 1 轮 | 日志分析: 关闭 (加 --with-analysis 开启)"
fi

for eng in "${ENGINE_TYPES[@]}"; do
    case "$eng" in
        pgvector)          tables=("${PG_TABLE_NUMS[@]}") ;;
        clickhouse|myscale) tables=("${CH_TABLE_NUMS[@]}") ;;
        *) echo "错误: 未知数据库类型 $eng (可选 pgvector/myscale/clickhouse)" >&2
           FAILS=$((FAILS+1)); continue ;;
    esac
    # --tables 显式给出时统一覆盖所有类型的默认表序号
    [ "${#USER_TABLES[@]}" -gt 0 ] && tables=("${USER_TABLES[@]}")

    # 每个类型开始前准备服务端 (CK/MS 同端口, 会自动停掉对方)
    if [ "$MANAGE_SERVER" -eq 1 ]; then
        echo ""
        echo "===== 服务端准备: $eng ====="
        if ! ensure_server "$eng"; then
            echo "错误: $eng 服务端准备失败, 跳过该类型全部组合" >&2
            FAILS=$((FAILS+1))
            continue
        fi
    fi

    for num in "${tables[@]}"; do
        if [ "$eng" = "pgvector" ]; then
            tname="${PG_TABLE_NAMES[$num]:-}"
        else
            tname="${CH_TABLE_NAMES[$num]:-}"
        fi
        if [ -z "$tname" ]; then
            echo "  (warn) 表序号 $num 不在 $eng 的映射表中, 输出命名将用占位名; 可在脚本内补充"
            if [ "$eng" = "pgvector" ]; then tname="unknown"; else tname="result"; fi
        fi
        for n in "${ROW_COUNTS[@]}"; do
            # 单条抽样(n=1)仅 pgvector 执行; CK/MS 构建 SQL 耗时且单条抽样收益低, 跳过
            if [ "$eng" != "pgvector" ] && [ "$n" = "1" ]; then
                echo "(跳过 $eng n=1: 单条抽样测试仅 pgvector 执行)"
                continue
            fi
            echo ""
            echo "===== 组合: $eng 表$num($tname) 抽样条数=$n ====="
            combo_start=$(date +%s)
            if [ "$eng" = "pgvector" ]; then
                run_pgvector_combo "$num" "$n" "$tname"
            else
                run_ch_combo "$eng" "$num" "$n" "$tname"
            fi
            echo "===== 组合完成: $tname n=$n, 耗时 $(( $(date +%s) - combo_start ))s ====="
        done
    done
done

echo ""
echo ">>> 编排结束: 总耗时 $(( $(date +%s) - START_ALL ))s, 成功 $OKS 次, 失败 $FAILS 次"
[ "$FAILS" -eq 0 ] || exit 1
exit 0
