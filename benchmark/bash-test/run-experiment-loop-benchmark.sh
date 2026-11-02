#!/bin/bash
# run.py 实验循环编排脚本 (在 xxb-ubuntu22-ck 容器内运行)
#
# 对 3 类数据库 × 5 个数据集引擎, 逐条执行:
#   python run.py --engine-type <类型> --engines <类型>-<后缀> --skip-upload
# 每个引擎组合循环 RUNS(默认10) 次; 失败不中断, 末尾汇总失败数。
#
# 覆盖的 15 个引擎(与 configurations/config.json 的 name 一一对应):
#   pgvector : sift-128-euclidean / nytimes-256-angular / hnsw-laion-768-1m-ip /
#              hnsw-gist-960-euclidean-l2 / dbpedia-openai-1000k-angular
#   myscale  : 同上 5 个后缀
#   clickhouse: 同上 5 个后缀
#
# 服务端启停 (默认开启, 命令取自 /home/command.txt):
#   每个类型开始前确保对应服务端在运行:
#     pgvector   -> su - postgres -c "pg_ctl -D /usr/local/pgsql/data start" (端口 5432)
#     clickhouse -> /home/ClickHouse/build/programs/clickhouse-server \
#                     --config-file /etc/clickhouse-server/config.xml (端口 9000/8123)
#     myscale    -> /home/clickhouse/build/programs/clickhouse-server \
#                     --config-file /etc/myscale-server/config.xml (端口 9000/8123)
#   注意: ClickHouse 与 MyScale 是同一份代码、同一组端口(9000/8123/9009), 二者
#   互斥; 启动前会先停掉对方并等待端口释放, 再启动目标服务端并等待端口就绪。
#   ClickHouse 由 clickhouse-watchdog 拉起 server, 停止时按 config 路径整组终止
#   (先 TERM 后必要时 KILL), 避免 watchdog 把 server 重新拉起来。
#   用 --no-server 可跳过服务端管理(假定已由外部启动)。
#
# 说明: 本脚本只负责执行 run.py (--skip-upload 假定表与索引已就绪), 不做服务端
# 日志清理, 也不做日志解析; 需要时序分析时另行执行 analyze_*.py。
#
# 用法:
#   ./run-experiment-loop-benchmark.sh                      # 全量: 3 类型 × 5 引擎 × 10 轮
#   ./run-experiment-loop-benchmark.sh -t "pgvector" -r 2   # 只跑 pgvector, 每引擎 2 轮
#   ./run-experiment-loop-benchmark.sh -s "sift-128-euclidean dbpedia-openai-1000k-angular"
#   EXTRA_ARGS="--host 10.0.0.2 --port 5432" ./run-experiment-loop-benchmark.sh
#   ./run-experiment-loop-benchmark.sh --no-server          # 不管理服务端(外部已启动)
#
# 依赖: 容器内以 root 运行(pg_ctl/su - postgres 与启停 clickhouse 需要权限), 且
#       run.py 的依赖已安装。
set -uo pipefail

ENGINE_TYPES=(pgvector myscale clickhouse)   # 数据库类型 = --engine-type 取值
# 数据集引擎后缀; 实际引擎名 = <类型>-<后缀>, 与 config.json 的 name 字段一致
SUFFIXES=(
    sift-128-euclidean
    nytimes-256-angular
    hnsw-laion-768-1m-ip
    hnsw-gist-960-euclidean-l2
    dbpedia-openai-1000k-angular
)
RUNS=10                                     # 每个引擎组合循环执行次数
RUNPY="${RUNPY:-/home/vector-benchmark/benchmark/run.py}"
PYTHON="${PYTHON:-$(command -v python || command -v python3)}"
EXTRA_ARGS="${EXTRA_ARGS:-}"                # 额外透传给 run.py 的参数(如 --host/--port)
MANAGE_SERVER=1                             # 1=按类型启停服务端; --no-server 置 0

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
  -t <类型序列>   数据库类型, 空格/逗号分隔 (默认 "pgvector myscale clickhouse")
  -s <后缀序列>   引擎后缀(不含 <类型>- 前缀), 空格/逗号分隔 (默认 5 个数据集)
  -r <次数>       每个引擎组合循环执行次数 (默认 10)
      --no-server 不管理服务端(假定已由外部启动); 默认按类型自动启停
  -h              显示本帮助

环境变量: RUNS/RUNPY/PYTHON/EXTRA_ARGS 可覆盖默认值; EXTRA_ARGS 原样透传给 run.py。
服务端路径/端口/超时: PG_CTL/PG_DATA/PG_PORT/CK_BIN/CK_CONF/MS_BIN/MS_CONF/
CH_PORT/CK_STDOUT/MS_STDOUT/START_TIMEOUT/STOP_TIMEOUT 均可覆盖。
EOF
    exit "${1:-0}"
}

# ---- 命令行参数解析 (覆盖默认类型/后缀/循环次数) ----
while [ "$#" -gt 0 ]; do
    case "$1" in
        -t) [ -n "${2:-}" ] || { echo "错误: -t 缺少类型序列" >&2; usage 1; }
            IFS=', ' read -r -a ENGINE_TYPES <<< "$2"; shift 2 ;;
        -s) [ -n "${2:-}" ] || { echo "错误: -s 缺少后缀序列" >&2; usage 1; }
            IFS=', ' read -r -a SUFFIXES <<< "$2"; shift 2 ;;
        -r) [ -n "${2:-}" ] || { echo "错误: -r 缺少次数" >&2; usage 1; }
            RUNS="$2"; shift 2 ;;
        --no-server) MANAGE_SERVER=0; shift ;;
        -h|--help) usage 0 ;;
        *)  echo "未知选项: $1" >&2; usage 1 ;;
    esac
done

[ -f "$RUNPY" ] || { echo "错误: run.py 不存在: $RUNPY" >&2; exit 1; }
[ -n "$PYTHON" ] || { echo "错误: 未找到 python 解释器" >&2; exit 1; }
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

FAILS=0
OKS=0
START_ALL=$(date +%s)
echo ">>> 编排开始: 类型=[${ENGINE_TYPES[*]}] 后缀=[${SUFFIXES[*]}] 每引擎 ${RUNS} 轮"

for t in "${ENGINE_TYPES[@]}"; do
    # 每个类型开始前准备服务端 (CK/MS 同端口, 会自动停掉对方)
    if [ "$MANAGE_SERVER" -eq 1 ]; then
        echo ""
        echo "===== 服务端准备: $t ====="
        if ! ensure_server "$t"; then
            echo "错误: $t 服务端准备失败, 跳过该类型全部引擎" >&2
            FAILS=$((FAILS+1))
            continue
        fi
    fi
    for s in "${SUFFIXES[@]}"; do
        engine="${t}-${s}"
        echo ""
        echo "===== 引擎: $engine (类型=$t) ====="
        combo_start=$(date +%s)
        for i in $(seq 1 "$RUNS"); do
            echo ""
            echo "----- 第 $i/$RUNS 轮 | $engine -----"
            # EXTRA_ARGS 需按空格拆分成多个参数, 故不加引号
            # shellcheck disable=SC2086
            if "$PYTHON" "$RUNPY" --engine-type "$t" --engines "$engine" --skip-upload $EXTRA_ARGS; then
                echo "  实验运行完成"
                OKS=$((OKS+1))
            else
                echo "  (warn) 实验运行失败(退出码 $?)" >&2
                FAILS=$((FAILS+1))
            fi
        done
        echo "===== 引擎完成: $engine, 耗时 $(( $(date +%s) - combo_start ))s ====="
    done
done

echo ""
echo ">>> 编排结束: 总耗时 $(( $(date +%s) - START_ALL ))s, 成功 $OKS 次, 失败 $FAILS 次"
[ "$FAILS" -eq 0 ] || exit 1
exit 0
