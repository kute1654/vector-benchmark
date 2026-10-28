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
# ClickHouse 专属配置 (环境变量):
#   CK_EF_S            HNSW 检索候选集大小 (ef_s), 通过会话级 SET
#                      hnsw_candidate_list_size_for_search 装配 (与 run.py 客户端
#                      apply_query_plan_cache_settings 一致)。默认空=不设置。
#                      注意: 与 MyScale 不同, ClickHouse 的 ef_s 不是 distance()
#                      函数的参数, 而是写在运行副本头部的会话级 SET 行里。
#
# 说明: 计划缓存/结果缓存开关必须用会话级 `SET` 装配 (与 run.py 客户端
#   apply_query_plan_cache_settings 一致)。逐条查询内联的 `SETTINGS ...` 子句在
#   enable_vector_performance_test 打开时每次执行都要被重新解析, 会拖慢 ~1ms 的
#   ANN 查询、抵消缓存收益。因此脚本在运行副本头部写入本 profile 的会话级 SET 行,
#   并把 SQL 行尾的内联 SETTINGS 子句剥掉 (磁盘上的 .sql 生成文件保持不变);
#   关闭 --randomize 保证 SET 行先于查询在同一条会话上执行。
#   服务器支持 enable_vector_performance_test 时另写一行该会话开关 (不支持自动省略)。
#
# 并行字符串→向量转换测试 (MyScale 计划缓存命中时回填查询向量的新增功能):
#   并行模式 (mode) 是 SETTINGS_GROUPS 每行的第 7 个字段, 格式:
#       "profile名|use_query_cache|enable_query_plan_cache|enable_cast_vector|query_plan_cache_only_vector|only_cache_query_plan|mode"
#   第 7 字段可省略 (等价于 0)。0=原串行; 1=块式并行 (连续等分); 2=轮转式并行 (按偏移跨步)。
#   因为 mode 属于 profile, 结果 CSV 里就直接按 profile 区分 (另有 string_to_vector_mode 列)。
#   例: "plan_cache_blocked|0|1|0|0|0|1" —— 同一组缓存开关下的 mode=1 变体。
#
#   STRING_TO_VECTOR_THREADS           并行线程数, 一个固定值, 对所有 profile 生效 (默认 1; 0=自动=硬件并发)。
#                                      只在 mode=1/2 且批大小 >=2 时才会真正开线程。
#   VERIFY_PARALLEL_MODES              1=基准测试前先做「各 mode 变体结果一致性」校验, 默认 0
#                                      (同一组缓存开关下的每个 mode 变体跑两遍取第二遍输出做 cmp; 同时用
#                                       system.events.QueryPlanCacheHits 断言第二遍确实命中了计划缓存 ——
#                                       0 命中说明没走到被测代码, 会明确标为假通过)
#   VERIFY_SAMPLE_QUERIES              校验时每表取前 N 条查询比对, 默认 20
#
#   例: VERIFY_PARALLEL_MODES=1 STRING_TO_VECTOR_THREADS=8 ./clickhouse-benchmark.sh 表名
#
#   默认 profile 列表里, 只有 plan_cache 这一组派生了 mode=1/2 两个变体
#   (plan_cache_blocked / plan_cache_cyclic), 其余 profile 都是 mode=0 —— 对比 mode
#   只需要一组缓存开关, 所以没必要把整张表乘三倍。想换别的开关组合, 照着上面格式
#   加一行即可 (SETTINGS_GROUPS_OVERRIDE 也能整体替换)。
#   mode / threads 只写进运行副本头部的会话级 SET 行 (与其它缓存开关一致), 不写进
#   sql-bench 下的生成文件: 生成文件在运行时本来就会被剥掉行尾 SETTINGS 子句, 且同一组
#   开关下各 mode 变体共用同一份查询文本, 没必要重复生成。
#   注意 (两处会把 mode 记成 0, 均为如实处理, 不是静默降级):
#     - 服务器没有这两个 setting (未打补丁的 MyScale / 上游 clickhouse): 若同一组开关下
#       已有 mode=0 的 profile, 这些空转的 mode 变体会被直接移除;
#     - 该 profile 没开计划缓存: 转换函数根本不会被调用, mode 归零后照常测量。
#   注意 (解读结果时): 若 mode=1/2 的 QPS 与 mode=0 完全相同, 常见原因是该查询只带
#   一个向量字面量, 批大小为 1 时会按设计退回串行 (thread_count<2), 并不代表转换
#   逻辑出错; 要看出并行差异需要一条含多个向量字面量的查询。
#
# 测试维度: engine × table × settings_profile(含 string_to_vector 并行 mode) × precise_float_parsing × sql_type(normal/cast/cast_array/type_hint/raw_bytes/raw_bytes_x/with/with_cast/with_cast_array/with_type_hint/with_raw_bytes/with_raw_bytes_x/subquery_id/with_subquery_id) × row_count × concurrency (线程数 STRING_TO_VECTOR_THREADS 为固定值, 不是维度)
#
# 延迟指标 (CSV 末尾三列, 单位毫秒):
#   p95_time_ms / p99_time_ms  直接解析 `clickhouse benchmark` 结束报告里的
#                              "95%"/"99%" 百分位行 (该行单位是 sec., 这里换算成毫秒)。
#   mean_time_ms               优先解析 MyScale fork 报告里的 "Avg Latency" 行;
#                              上游 ClickHouse 不输出均值延迟, 此时按 concurrency / QPS
#                              推导 (并发>1 时是近似平均服务时间, 不是精确测量值)。
#   每次 repeat 各解析一份, 写入 CSV 的是各次运行去掉最大/最小值后的均值
#   (与 qps_avg 同口径); 某次运行失败时不参与延迟统计, 全部失败则记 0.000。
#   注意: 上游 ClickHouse 的百分位只打印到 3 位小数 (秒), 即分辨率约 1ms。
#
# ---- 用途与整体流程 ----
#   1) Step 1    读取表名 (命令行参数, 或交互式输入)
#   2) Step 2    选择距离函数与排序方向
#   3) Step 2.5  检测向量索引是否真正生效 (未生效时询问是否继续)
#   4) Step 3    校验 SETTINGS_GROUPS 里每个 profile 的开关组合是否合法
#   5) Step 4    按 表 × profile × sql_type × row_count 采样生成 .sql 文件
#   6) Step 5    每张表预热一次 (warmup)
#   7) Step 5.5  (可选) 并行 mode 变体结果一致性校验
#   8) Step 6    逐组合运行 clickhouse-benchmark, 解析 QPS/延迟并追加写入 CSV
#
# ---- 参数 ----
#   $1..$n   表名 (可多个); 不传则交互式输入。表须位于 default 库。
#
# ---- 环境变量 ----
#   ENGINE                         clickhouse | myscale, 默认 clickhouse
#   CLICKHOUSE                     clickhouse 可执行文件路径 (默认值随 ENGINE 不同)
#   MYS_SCALE_EF                   MyScale distance() 的 ef_s (默认 100)
#   MYS_SEARCH_PARAMS              MyScale 额外检索参数, 逗号分隔
#   CK_EF_S                        ClickHouse 会话级 hnsw_candidate_list_size_for_search
#   TIMELIMIT                      单次 benchmark 时长 (秒, 默认 10)
#   WARMUP_TIMELIMIT               每表预热时长 (秒, 默认 60; 小于 60 会被抬到 60)
#   REPEAT                         每个组合重复次数 (默认 10)
#   OUTPUT_CSV                     结果 CSV 路径 (默认 results/csv/<engine>-benchmark-results.csv)
#   SQL_DIR                        生成 SQL 的目录 (默认 bash-test/sql-bench/clickhouse-query-forms)
#   TOP_K                          ORDER BY ... LIMIT 的 k (默认 10)
#   STRING_TO_VECTOR_THREADS       并行字符串→向量转换线程数 (默认 1; 0=自动取硬件并发)
#   VERIFY_PARALLEL_MODES          1=基准测试前做 mode 变体一致性校验 (默认 0)
#   VERIFY_SAMPLE_QUERIES          校验时每表比对前 N 条查询 (默认 20)
#   SQL_TYPES_OVERRIDE             空格分隔, 整体替换 SQL_TYPES 数组
#   ROW_COUNTS_OVERRIDE            空格分隔, 整体替换 ROW_COUNTS 数组
#   CONCURRENCIES_OVERRIDE         空格分隔, 整体替换 CONCURRENCIES 数组
#   PRECISE_FLOAT_PARSING_OVERRIDE 空格分隔, 整体替换 PRECISE_FLOAT_PARSING_VALUES
#   SETTINGS_GROUPS_OVERRIDE       空格分隔, 整体替换 SETTINGS_GROUPS
#                                  (profile 内部用 '|' 分隔, 本身不含空格, 可直接按空格切分)
#
# ---- 依赖命令 ----
#   clickhouse (client / benchmark 子命令), awk, sed, grep, sort, wc, mktemp;
#   python3 仅 MyScale 的 raw_bytes 形态需要把 float 转 IEEE754 hex 时调用;
#   column 仅用于末尾预览, 缺失时回退为 tail。
#
# ---- 输出文件 ----
#   $OUTPUT_CSV                                             每个组合一行的结果
#   $SQL_DIR/<table>_<profile>_<sql_type>_<count>.sql       采样生成的查询文本
#   运行副本 (头部含会话级 SET 行) 写在 mktemp 临时目录, 退出时由 trap 递归删除
#
# ---- CSV 列含义 (按表头顺序) ----
#   engine, table_name, distance_func, index_status (兼容保留列, 恒写 index_active),
#   settings_profile, 5 个缓存开关列 (列名随引擎不同), precise_float_parsing,
#   string_to_vector_mode, string_to_vector_threads, sql_type, row_count, concurrency,
#   run_1..run_<REPEAT> (每次运行的 QPS; 失败或无法解析记 0),
#   qps_avg (个数>2 时去掉最大/最小后的均值), qps_min, qps_max,
#   mean_time_ms, p95_time_ms, p99_time_ms —— 这三列单位均为毫秒。
#
# ---- 退出码 ----
#   0  正常完成; 或在「索引未生效, 是否继续」提示中回答非 y (主动取消)
#   1  参数/配置错误、clickhouse 不可执行、表或向量列不存在、SQL 生成失败、
#      profile 校验失败等; set -e 下任何未显式放行的命令失败同样以非 0 退出
#   clickhouse benchmark 单次运行失败不会中止脚本 (该次 QPS 记 0 后继续)。
#
# ---- 失败重试 ----
#   每个组合固定跑 REPEAT 次; 单次失败只记录 QPS=0 并继续下一次, 不重跑已成功次数。
#   延迟列只采纳「成功且能解析出百分位」的运行; 一次都没有时记 0.000。
#
# ---- 重要实现约定 ----
#   缓存开关走会话级 SET 行 (运行副本头部), 生成的 .sql 文件里仍保留内联 SETTINGS,
#   运行时用 sed 剥掉; 磁盘文件内容不变, 便于与 run.py/其它工具复用同一份 SQL。
#

# -e: 任一命令返回非 0 立即退出 (可预期的失败点用 `|| true` 或 `set +e` 显式放行)
# -u: 引用未定义变量即报错 (因此可选环境变量统一用 "${VAR:-默认值}" 提供默认值)
# -o pipefail: 管道中任一环节失败即整体判失败, 避免只看最后一个命令的退出码
set -euo pipefail

# ============ 配置 ============
# 引擎选择; 决定二进制默认路径、profile 字段语义、SET 行内容与 CSV 列名
ENGINE="${ENGINE:-clickhouse}"
if [ "$ENGINE" = "myscale" ]; then
    CLICKHOUSE="${CLICKHOUSE:-/home/clickhouse/build/programs/clickhouse}"
else
    CLICKHOUSE="${CLICKHOUSE:-/home/ClickHouse/build/programs/clickhouse}"
fi
# 服务端 native 协议地址 (9000 端口), client 与 benchmark 都连这里
HOST="127.0.0.1"
PORT="9000"
MYS_SCALE_EF="${MYS_SCALE_EF:-100}"
MYS_SEARCH_PARAMS="${MYS_SEARCH_PARAMS:-}"
CK_EF_S="${CK_EF_S:-}"
# 单次测量/预热时长 (秒) 与重复次数
TIMELIMIT="${TIMELIMIT:-10}"
WARMUP_TIMELIMIT="${WARMUP_TIMELIMIT:-60}"
REPEAT="${REPEAT:-10}"
# 脚本所在目录的绝对路径: 先 cd 到脚本目录再 pwd, 因此不依赖调用者的当前工作目录;
# BASH_SOURCE[0] 是脚本自身路径, dirname 取目录部分。
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ "$ENGINE" = "myscale" ]; then
    OUTPUT_CSV="${OUTPUT_CSV:-${SCRIPT_DIR}/../results/csv/myscale-benchmark-results.csv}"
else
    OUTPUT_CSV="${OUTPUT_CSV:-${SCRIPT_DIR}/../results/csv/clickhouse-benchmark-results.csv}"
fi
SQL_DIR="${SQL_DIR:-${SCRIPT_DIR}/sql-bench/clickhouse-query-forms}"
TOP_K="${TOP_K:-10}"

# 测试维度 (可被 *_OVERRIDE 环境变量整体替换):
#   SQL_TYPES   向量字面量的写法; 每个元素对应一份 <...>_<type>_<count>.sql
#   ROW_COUNTS  每个 SQL 文件包含的查询条数 (从表中随机采样多少条向量)
#   CONCURRENCIES  clickhouse-benchmark --concurrency 的取值列表
#   PRECISE_FLOAT_PARSING_VALUES  服务端 --precise_float_parsing 的取值列表
# 注意: bash 数组用括号定义, 元素以空白分隔, 展开时须写成 "${ARR[@]}" 才保留元素边界。
SQL_TYPES=(
    normal
    cast
    cast_array
    raw_bytes
    raw_bytes_x
)
ROW_COUNTS=(1000)
CONCURRENCIES=(1)
PRECISE_FLOAT_PARSING_VALUES=(1)

# 并行字符串→向量转换 (MyScale 新增功能) 的测试维度, 详见文件头说明。
# 模式 (mode) 是 SETTINGS_GROUPS 的第 7 个字段 —— 每个 profile 自带模式, 展开后
# 以 profile 的形式出现在结果里; 线程数则是一个固定值, 对所有 profile 生效。
STRING_TO_VECTOR_THREADS="${STRING_TO_VECTOR_THREADS:-1}"
VERIFY_PARALLEL_MODES="${VERIFY_PARALLEL_MODES:-0}"

if [ "$ENGINE" = "myscale" ]; then
    # MyScale profile 字段(f1..f5, 可选 f6):
    #   f1=use_query_cache, f2=enable_query_plan_cache, f3=enable_cast_vector,
    #   f4=query_plan_cache_only_vector, f5=only_cache_query_plan,
    #   f6=string_to_vector_parallel_mode (0/1/2, 省略即 0)
    # plan_cache 这一组额外派生出 mode=1 (块式) / mode=2 (轮转式) 两个 profile, 使
    # 并行字符串→向量转换可以在同一组缓存开关下与 mode=0 直接对比; 其余 profile 只跑 mode=0。
    SETTINGS_GROUPS=(
        "plan_cache|0|1|0|0|0"
        "off|0|0|0|0|0"
        # "plan_cache_blocked|0|1|0|0|0|1"
        # "plan_cache_cyclic|0|1|0|0|0|2"
        # "plan_cast|0|1|1|0|0"
        # "plan_only_vector|0|1|0|1|0"
        "only_plan_cache|0|1|0|0|1"
        # "only_plan_cast|0|1|1|0|1"
        # "only_plan_only_vector|0|1|0|1|1"
        "query_off|1|0|0|0|0"
        "query_plan_cache|1|1|0|0|0"
        # "query_plan_cast|1|1|1|0|0"
        # "query_plan_only_vector|1|1|0|1|0"
        "query_only_plan_cache|1|1|0|0|1"
        # "query_only_plan_cast|1|1|1|0|1"
        # "query_only_plan_only_vector|1|1|0|1|1"
    )
else
    # clickhouse profile 字段:
    #   f1=use_query_cache, f2=vector_query_plan_cache, f3=vector_only_cache_query_plan,
    #   f4=vector_query_plan_cache_only_vector, f5=vector_use_cast
    # (f6=string_to_vector_parallel_mode 是 MyScale 分支的字段, 上游 clickhouse 没有)
    # use_query_cache 与 vector_query_plan_cache 可同时开启;
    # f3/f4=1 仅在 f2=1 时有效。
    SETTINGS_GROUPS=(
        "ck_plan_cache|0|1|0|0|0"
        "ck_off|0|0|0|0|0"
        "ck_only_plan_cache|0|1|1|0|0"
        # "ck_plan_cast|0|1|0|0|1"
        # "ck_only_plan_cast|0|1|1|0|1"
        "result_off|1|0|0|0|0"
        "result_plan_cache|1|1|0|0|0"
        "result_only_plan_cache|1|1|1|0|0"
        # "result_plan_cast|1|1|0|0|1"
        # "result_only_plan_cast|1|1|1|0|1"
    )
fi

# MyScale 不支持子查询形式的向量参数
MYS_SCALE_UNSUPPORTED_TYPES=(subquery_id with_subquery_id)

# ============ 工具函数 ============

# 用环境变量提供的「空格分隔列表」整体替换同名 bash 数组。
# 参数: $1=目标数组变量名 (字符串), $2=覆盖值所在的环境变量名。
# 副作用: 直接改写调用者作用域中的数组 (依赖 bash 4.3+ 的 nameref)。
# 返回: 0 (覆盖值为空时不做任何事)。
# 实现要点:
#   ${!override_name:-} 是间接展开: 先取出变量名, 再取其值; 未设置时为空串。
#   `local -n target_array="$var_name"` 建立 nameref, 之后对 target_array 赋值
#   等价于给调用者传入的数组赋值 (注意它没有被声明为 local 数组, 指向外部)。
#   here-string `<<< "$override_value"` 把字符串 + 换行喂给 read;
#   read -r -a 按 IFS 空白切分成数组, -r 禁止反斜杠转义。
apply_list_override() {
    local var_name="$1"
    local override_name="$2"
    local override_value="${!override_name:-}"

    if [ -n "$override_value" ]; then
        local -n target_array="$var_name"
        read -r -a target_array <<< "$override_value"
    fi
}

# 依次应用各维度的覆盖; 未设置对应 *_OVERRIDE 时数组保持上面的默认值
apply_list_override SQL_TYPES SQL_TYPES_OVERRIDE
apply_list_override ROW_COUNTS ROW_COUNTS_OVERRIDE
apply_list_override CONCURRENCIES CONCURRENCIES_OVERRIDE
apply_list_override PRECISE_FLOAT_PARSING_VALUES PRECISE_FLOAT_PARSING_OVERRIDE
apply_list_override SETTINGS_GROUPS SETTINGS_GROUPS_OVERRIDE

# 校验 STRING_TO_VECTOR_THREADS 必须是非负整数。
# 参数: 无 (读取全局变量)。副作用: 非法时向 stderr 打印错误并 exit 1。
# `=~ ^[0-9]+$` 是 bash 正则匹配; 用 ! 取反。
validate_string_to_vector_threads() {
    if ! [[ "$STRING_TO_VECTOR_THREADS" =~ ^[0-9]+$ ]]; then
        echo "错误: STRING_TO_VECTOR_THREADS 应为非负整数 (当前: '$STRING_TO_VECTOR_THREADS')" >&2
        exit 1
    fi
}

# 并行转换只在计划缓存命中、需要把参数文本还原成 Field 时才被调用, 所以在
# 「服务器不支持该 setting」或「profile 没开计划缓存」时, profile 里写的 mode 是空转。
# 这种 profile 会污染对比结果 (CSV 上写着 mode=1 其实走的是串行路径), 因此在测量前处理掉:
#   - 服务器不支持: 若同一组缓存开关下已有 mode=0 的 profile, 直接删掉这些空转 profile,
#     免得白跑一遍完全相同的配置 (拿未打补丁的 MyScale 当对照时就会走到这里);
#   - 支持但该 profile 没开计划缓存: 把 mode 归零, 保留这个 profile 但如实记录为 0。
# 参数: 无 (读取全局 SETTINGS_GROUPS / STRING_TO_VECTOR_FLAG)。
# 副作用: 覆写全局数组 SETTINGS_GROUPS, 并打印被删除/归零的 profile 名称。
# 返回: 0。
# 实现要点:
#   `local -a kept=()` 声明函数内数组 (空初始化), 用 kept+=("...") 追加元素;
#   `IFS='|' read -r name f1 ... <<< "$group"` 以 '|' 为分隔符把一行 profile 拆成字段,
#   here-string 自带末尾换行, 且缺少的可选字段会保持为空 (之后用 ${f6:-0} 兜底);
#   `grep -qxF` 中 -q 静默, -x 要求整行匹配, -F 按字面量而非正则匹配。
normalize_string_to_vector_modes() {
    local group name f1 f2 f3 f4 f5 f6 tuple
    local -a kept=()
    local -a seen_zero_tuples=()

    if [ "$STRING_TO_VECTOR_FLAG" != "1" ]; then
        # 先收集所有 mode=0 profile 的缓存开关组合, 作为「已有等价基线」的依据
        for group in "${SETTINGS_GROUPS[@]}"; do
            IFS='|' read -r name f1 f2 f3 f4 f5 f6 <<< "$group"
            [ "${f6:-0}" = "0" ] && seen_zero_tuples+=("${f1}|${f2}|${f3}|${f4}|${f5}")
        done
    fi

    local dropped=""
    local zeroed=""
    for group in "${SETTINGS_GROUPS[@]}"; do
        IFS='|' read -r name f1 f2 f3 f4 f5 f6 <<< "$group"
        f6="${f6:-0}"
        if [ "$f6" = "0" ]; then
            kept+=("$group")
            continue
        fi
        tuple="${f1}|${f2}|${f3}|${f4}|${f5}"
        if [ "$STRING_TO_VECTOR_FLAG" != "1" ]; then
            # 服务器没有这个 setting: 有等价基线就删 (基线自己一定在 kept 里, 不会删空),
            # 否则退化成 mode=0 照常测
            if printf '%s\n' "${seen_zero_tuples[@]}" | grep -qxF "$tuple"; then
                dropped="$dropped $name"
                continue
            fi
            kept+=("${name}|${f1}|${f2}|${f3}|${f4}|${f5}|0")
            zeroed="$zeroed $name"
        elif [ "$f2" != "1" ]; then
            # 没开计划缓存: mode 不会生效, 如实记为 0
            kept+=("${name}|${f1}|${f2}|${f3}|${f4}|${f5}|0")
            zeroed="$zeroed $name"
        else
            kept+=("$group")
        fi
    done

    # 用重建后的数组整体替换原数组 (数组赋值必须整体进行, 不能逐个删元素)
    SETTINGS_GROUPS=("${kept[@]}")
    if [ -n "$dropped" ]; then
        echo "  提示: 服务器不支持 string_to_vector_parallel_mode, 已移除等价的并行 profile:${dropped}"
    fi
    if [ -n "$zeroed" ]; then
        echo "  提示: 这些 profile 的 mode 不会生效 (缺少计划缓存或服务器不支持该 setting), 已按 mode=0 记录:${zeroed}"
    fi
}

# 尽早执行参数校验: 非数字的线程数在后续算术比较里会直接报错, 这里给出明确提示
validate_string_to_vector_threads

# ENGINE 只接受两个取值; *) 分支必须显式 exit 1, 否则错误配置会被当成 clickhouse 继续跑
case "$ENGINE" in
    clickhouse|myscale) ;;
    *) echo "错误: ENGINE 只支持 clickhouse 或 myscale (当前: $ENGINE)" >&2; exit 1 ;;
esac

# MyScale 不支持把 Array 重新解释成 String 的子查询形式, 从 SQL_TYPES 中过滤掉。
# 过滤手法: 把不支持列表用空格包起来, 再用 `*" $t "*` 做子串匹配 (避免 "id" 误匹配 "subquery_id")。
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
    # ${#removed[@]} 是数组元素个数; -gt 0 即非空
    if [ ${#removed[@]} -gt 0 ]; then
        echo "注意: ENGINE=myscale, 以下 SQL 形式不支持(reinterpret Array 为 String), 已跳过: ${removed[*]}"
    fi
    SQL_TYPES=("${filtered[@]}")
fi

validate_string_to_vector_threads

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
# 参数: $1=向量列名, $2=查询向量字面量/表达式 (调用方已拼好)。
# 返回: 通过 stdout 输出表达式字符串 (供 $(...) 捕获); 不改全局状态。
# 说明: MyScale 的检索参数写在 distance(...) 的括号里, 多个参数以逗号拼接,
#       每个参数各自带单引号, 形如 distance('ef_s=100', 'k=20')(vec, q)。
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
            # 逗号分隔的额外参数 -> 数组; 逐个去掉所有空格 (tr -d ' '), 空项跳过
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
# 参数: 无。副作用: 首次调用探测服务器并设置全局 PERF_TEST_FLAG; 之后直接复用。
# 返回: 0 (探测不到时也返回 0, 只把标志置为 "0")。
PERF_TEST_FLAG=""
detect_perf_test_flag() {
    # 已探测过 (非空) 就跳过, 避免每个组合都多一次客户端往返
    if [ -n "$PERF_TEST_FLAG" ]; then
        return
    fi
    local cnt
    # system.settings 有该行则 count()=1; 命令末尾 `|| true` 让连接失败/查询报错
    # 不会在 set -e 下终止脚本 (失败时 cnt 为空, 下面按 "0" 处理)。
    cnt=$("$CLICKHOUSE" client --host "$HOST" --port "$PORT" \
        --query "SELECT count() FROM system.settings WHERE name='enable_vector_performance_test'" 2>/dev/null) || true
    # tr -dc '0-9' 只保留数字, 顺带去掉可能的空白/换行; 空值用 ${cnt:-0} 兜底
    PERF_TEST_FLAG="$(echo "${cnt:-0}" | tr -dc '0-9')"
    if [ "$PERF_TEST_FLAG" = "1" ]; then
        echo "  ✓ 服务器支持 enable_vector_performance_test, 将在 SQL 文件头部写入 SET 行"
    else
        echo "  提示: 服务器不支持 enable_vector_performance_test (per-query SETTINGS 将不会进入计划缓存探测)"
    fi
}

# 服务器是否支持并行字符串→向量转换的两个 setting
# (string_to_vector_parallel_mode / string_to_vector_parallel_threads)。
# 不支持时收敛为基线组合 0:0, 使本脚本仍可直接跑在未含该功能的服务器上。
# 0/1/未检测三种状态; 检测失败时视为不支持。
# 参数: 无。副作用: 首次调用设置全局 STRING_TO_VECTOR_FLAG; 幂等。
# 返回: 0。只查 mode setting 即可, threads 与本 setting 同版本引入。
STRING_TO_VECTOR_FLAG=""
detect_string_to_vector_support() {
    if [ -n "$STRING_TO_VECTOR_FLAG" ]; then
        return
    fi
    local cnt
    cnt=$("$CLICKHOUSE" client --host "$HOST" --port "$PORT" \
        --query "SELECT count() FROM system.settings WHERE name='string_to_vector_parallel_mode'" 2>/dev/null) || true
    STRING_TO_VECTOR_FLAG="$(echo "${cnt:-0}" | tr -dc '0-9')"

    if [ "$STRING_TO_VECTOR_FLAG" = "1" ]; then
        echo "  ✓ 服务器支持 string_to_vector_parallel_mode/threads, profile 里配的并行模式会真正生效"
    else
        echo "  提示: 服务器不支持 string_to_vector_parallel_mode/threads, profile 里的并行模式会退化为 mode=0"
    fi
}

# 每个 profile 开始前清服务端缓存, 使首行 QPS 测量口径一致
# 参数: $1=该 profile 的计划缓存开关 (f2, "1"=开)。
# 副作用: 向服务端发送 SYSTEM DROP ... 语句; 失败被忽略 (|| true)。
# 返回: 0。
# 说明: 查询结果缓存 (use_query_cache) 总是清; 计划缓存按引擎不同用不同语句,
#       只在计划缓存开启时才清 (关闭时本来也没有可清的条目)。
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

# 在 default 库中找出表的第一列 Array(Float*) 向量列名。
# 参数: $1=表名。返回: 通过 stdout 输出列名; 找不到 (或查询失败) 输出空串。
# 说明: LIMIT 1 保证只取一列; 查询里 SETTINGS use_query_cache=0 避免这次元数据
#       查询本身污染结果缓存/计划缓存。
detect_vector_column() {
    local table="$1"
    local col
    col=$("$CLICKHOUSE" client \
        --host "$HOST" \
        --port "$PORT" \
        --query "SELECT name FROM system.columns WHERE database='default' AND table='$table' AND type LIKE 'Array(Float%)' LIMIT 1 SETTINGS use_query_cache=0" 2>/dev/null) || true
    echo "${col}"
}

# 取向量列维度: 读第一行的 length(vec_col)。
# 参数: $1=表名, $2=向量列名。返回: stdout 输出维度数字; 失败或空表时输出 0。
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
# 参数: $1=逗号分隔的向量字符串。返回: stdout 输出改写后的字符串, 不修改入参。
# awk 说明: -F',' 按逗号切字段, NF=字段数; $i ~ /^-?[0-9]+$/ 判断整段是否为整数
#   (可选负号); 命中则给字段追加 ".0"; 最后 print $0 会按 OFS=',' 重新拼接整行。
floatify_vec_str() {
    # 仅对"整个 token 都是整数"的值补 .0; "1e-05"、"-0.5" 等原样保留
    echo "$1" | awk -F',' '{ for (i=1; i<=NF; ++i) if ($i ~ /^-?[0-9]+$/) $i = $i ".0"; print $0 }' OFS=','
}

# 将逗号分隔的 float 值转换为 IEEE 754 little-endian hex 字符串 (大写)。
# 用于 MyScale 下生成 reinterpret(unhex('...')) / reinterpret(x'...') 所需的 hex。
# 例: "1.0,2.0" -> "0000803F00000040"
# 参数: $1=逗号分隔的 float 字符串。返回: stdout 输出大写 hex; python3 失败时非 0。
# 说明: python3 -c 的脚本里 '<' 表示小端、'f'*n 表示 n 个 float32;
#   字符串由 bash 双引号包裹, $ 在 python 代码中没有出现, 不会被 bash 展开;
#   "$1" 作为 sys.argv[1] 传入, 避免把数据拼进代码本身。
float_vec_to_hex() {
    python3 -c "
import struct, sys
vals = sys.argv[1].split(',')
sys.stdout.write(struct.pack('<' + 'f' * len(vals), *(float(v) for v in vals)).hex().upper())
" "$1"
}

# 为一 (表, profile) 组合生成所有 SQL 形态的查询文件。
# 参数:
#   $1=表名  $2=向量列名  $3=距离函数 (MyScale 下会被覆盖为 distance)
#   $4=排序方向 ASC/DESC  $5=profile 名  $6=内联 SETTINGS 子句 (含前导空格)
# 副作用: 在 $SQL_DIR 下为 ROW_COUNTS 中每个 count 各写 13 个 .sql 文件;
#   文件名固定为 <table>_<profile>_<sql_type>_<count>.sql。
#   采样结果与错误输出写在 mktemp 临时文件中, 正常结束/失败返回前都会删除。
# 返回: 0 成功; 1 采样查询失败 (调用方在 set -e 下会因此终止脚本)。
# 说明: 本函数会把 SQL_TYPES 里列出的所有形态都生成一遍 (即使某些形态不在本次
#   SQL_TYPES 列表中); 真正跑哪些形态由主循环的 SQL_TYPES 决定。
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
        # mktemp 生成唯一临时文件; 错误输出单独存一份, 便于失败时只展示尾部
        tmp_vectors=$(mktemp)
        tmp_error=$(mktemp)

        # MyScale 不支持 reinterpretAsString(Array(Float32)), 不取 hex 列;
        # raw_bytes/raw_bytes_x 所需的 hex 由 float_vec_to_hex() 从 float 值在 bash 侧计算。
        if [ "$ENGINE" = "myscale" ]; then
            sample_query="SELECT id, arrayStringConcat($vec_col, ',') FROM $table ORDER BY rand() LIMIT $count SETTINGS use_query_cache=0"
        else
            sample_query="SELECT id, arrayStringConcat($vec_col, ','), hex(reinterpretAsString($vec_col)) FROM $table ORDER BY rand() LIMIT $count SETTINGS use_query_cache=0"
        fi

        # 用 `if ! command; then` 包住: 命令失败时进入分支自行清理并 return 1,
        # 而不是让 set -e 在临时文件删除前直接退出。client 默认输出 TabSeparated,
        # 每行三列: id \t 逗号分隔向量 \t hex 字节串。
        if ! "$CLICKHOUSE" client \
            --host "$HOST" \
            --port "$PORT" \
            --query "$sample_query" \
            > "$tmp_vectors" 2>"$tmp_error"; then
            echo "  错误: 表 $table 抽取 ${count} 条样本向量失败，无法生成 SQL"
            echo "  $ENGINE 错误输出:"
            # tail 取最后 20 行, sed 给每行加 4 空格缩进 (| 管道前有 pipefail)
            tail -20 "$tmp_error" 2>/dev/null | sed 's/^/    /'
            rm -f "$tmp_vectors" "$tmp_error"
            return 1
        fi

        local actual_count
        # wc -l 加 < 重定向读取行数 (不输出文件名); 样本不足时只告警继续
        actual_count=$(wc -l < "$tmp_vectors")
        if [ "$actual_count" -ne "$count" ]; then
            echo "  警告: 表 $table 预期 ${count} 条，实际获取 ${actual_count} 条"
        fi

        # `> file` 无命令时为截断/创建文件 (清空旧内容), 之后统一用 >> 追加。
        # IFS=$'\t' 只为本次 read 指定制表符分隔; -r 禁止反斜杠转义。
        #   三个变量依次接收三列; 预先构造好查询文本再 echo, 避免把命令替换写进循环条件。
        # `[ -z "$vec_str" ] && continue` 跳过空行 (表为空时仍会留下空文件)。
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
        # 循环内 read 通过 < 重定向消费文件, 子 shell 不参与, 因此循环内的变量修改对外可见
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

        # raw_bytes 形态直接用服务端算好的 hex (第三列); 若为空 (MyScale 只取两列),
        # 则在 bash 侧用 float_vec_to_hex 把 float 文本转成 IEEE754 小端 hex。
        # 注意这里第三个 read 变量名是 vec_hex (不带下划线), 与上面的 _vec_hex 写法不同。
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

        # 子查询形态不让向量出现在 SQL 文本里, 而是用样本行的 id 回表取向量;
        # 因此只读取第一列 id, 后两列用 _vec_str/_vec_hex 占位忽略 (下划线只是命名习惯)。
        # id 直接拼进 SQL, 要求样本表的 id 是数值类型 (不加引号)。
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

        # 每个 count 用完即删临时文件; -f 忽略文件不存在
        rm -f "$tmp_vectors" "$tmp_error"
    done
}

# 生成写进 .sql 行尾的内联 SETTINGS 子句 (以空格开头, 便于直接拼在 LIMIT 后)。
# 参数: $1..$5 = profile 的 5 个开关值 (顺序见下方注释)。
# 返回: stdout 输出子句; 纯字符串拼接, 无副作用。
# 注意: 运行时会用 sed 把这个子句剥掉, 改由会话级 SET 装配; 这里保留是为了
#   磁盘上的 .sql 文件自包含、可被其它工具直接执行。
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
# 第 6/7 个参数是并行字符串→向量转换的 mode/threads; 服务器不支持时不会写进 SET 行。
# 参数: $1..$5=缓存开关值, $6=mode (默认 0), $7=threads (默认 0)。
# 返回: stdout 输出一行不带分号的 `SET ...` (由调用方决定是否补分号/换行)。
session_set_line_for_profile() {
    local f1="$1"
    local f2="$2"
    local f3="$3"
    local f4="$4"
    local f5="$5"
    local sv_mode="${6:-0}"
    local sv_threads="${7:-0}"

    local sv_suffix=""
    if [ "$STRING_TO_VECTOR_FLAG" = "1" ]; then
        sv_suffix=", string_to_vector_parallel_mode = ${sv_mode}, string_to_vector_parallel_threads = ${sv_threads}"
    fi

    if [ "$ENGINE" = "myscale" ]; then
        echo "SET use_query_cache = ${f1}, enable_query_plan_cache = ${f2}, enable_cast_vector = ${f3}, query_plan_cache_only_vector = ${f4}, only_cache_query_plan = ${f5}${sv_suffix}"
    else
        # CK_EF_S 只在 ClickHouse 分支生效: 追加 HNSW 候选集大小
        local ef_suffix=""
        if [ -n "$CK_EF_S" ]; then
            ef_suffix=", hnsw_candidate_list_size_for_search = ${CK_EF_S}"
        fi
        echo "SET use_query_cache = ${f1}, vector_query_plan_cache = ${f2}, vector_only_cache_query_plan = ${f3}, vector_query_plan_cache_only_vector = ${f4}, vector_use_cast = ${f5}${ef_suffix}${sv_suffix}"
    fi
}

# 校验每个 profile 的字段数与开关依赖关系, 非法配置直接退出。
# 参数: 无 (遍历全局 SETTINGS_GROUPS)。副作用: 非法时向 stderr 打印并以 exit 1 终止。
# 返回: 0 (全部合法)。空数组时循环不执行, 同样返回 0。
validate_settings_groups() {
    local group profile_name f1 f2 f3 f4 f5 f6
    for group in "${SETTINGS_GROUPS[@]}"; do
        # 按 '|' 拆分; 第 7 字段可省略 (此时 f6 为空)
        IFS='|' read -r profile_name f1 f2 f3 f4 f5 f6 <<< "$group"
        # 可选第 7 字段: 字符串→向量转换的并行模式 (0=串行, 1=块式, 2=轮转式)
        if [ -n "$f6" ]; then
            case "$f6" in
                0|1|2) ;;
                *) echo "错误: settings profile '$profile_name' 非法: 第7个字段(并行模式)只支持 0/1/2 (当前: '$f6')" >&2; exit 1 ;;
            esac
        fi
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

# 从 benchmark 的 stderr 报告里提取总 QPS。
# 参数: $1=benchmark 的完整 stderr 文本。返回: stdout 输出 3 位小数的 QPS;
#   没有匹配 (报告为空/格式变化) 时输出 0.000。
# 说明: grep -oP 用 PCRE 的 \K (丢弃左侧已匹配内容), 只输出 "QPS:" 后面的数字;
#   多连接时报告里会有多行 "QPS:", awk 全部累加得到总 QPS。
#   注意: 函数最后一条命令是 awk, 即使 grep 无匹配 (退出码 1) 函数仍返回 0。
parse_total_qps() {
    local stderr_output="$1"
    echo "$stderr_output" | grep -oP 'QPS:\s*\K[0-9.]+' | awk '{sum += $1} END {printf "%.3f", sum}'
}

# 解析 `clickhouse benchmark` 结束报告里指定分位的延迟, 统一换算成毫秒。
# 单连接的输出形如 (制表符分隔):
#   95%		0.010 sec.
# 多个 --host/--port 时同一行会有多列 (每连接一个值), 这里取各列平均;
# 单位按值后面的 "sec."/"ms." 判断 (MyScale fork 支持 --use-millisecond-time)。
# 解析失败时输出空串。
# 参数: $1=stderr 文本, $2=分位数数字 (如 95)。返回: stdout 输出毫秒 (3 位小数) 或空串。
# awk 说明: -v 传入 p="95%"; `$1 == p` 精确匹配该行首列; 行内是
#   "数值 单位 数值 单位 ..." 成对出现, 故 i 每次 +2; 单位不以 ms 开头则按秒 ×1000。
#   空串与 0 不同: 调用方用 [ -n ... ] 判断本次是否纳入延迟统计。
parse_percentile_ms() {
    local stderr_output="$1"
    local percentile="$2"
    printf '%s\n' "$stderr_output" | awk -v p="${percentile}%" '
        $1 == p {
            for (i = 2; i + 1 <= NF; i += 2) {
                v = $i;
                if ($(i + 1) !~ /^ms/) v *= 1000;
                sum += v; n++;
            }
        }
        END { if (n > 0) printf "%.3f", sum / n }
    '
}

# 解析平均延迟 (毫秒)。MyScale fork 的 benchmark 会额外输出一行
#   Avg Latency	0.010 sec.
# 上游 ClickHouse 没有这一行, 此时按 concurrency / QPS 推导 (平均服务时间,
# 并发>1 时含排队, 是近似值; 并发=1 时即 1/QPS)。
# 参数: $1=stderr 文本, $2=并发数, $3=本次 QPS。
# 返回: stdout 输出毫秒 (3 位小数); QPS<=0 时输出 0.000。
# 实现: 先尝试解析 "Avg Latency" 行 (多连接时同样成对取值、取平均);
#   解析结果非空就直接返回, 否则用 awk BEGIN 块做算术 (不读输入文件)。
parse_mean_time_ms() {
    local stderr_output="$1"
    local concurrency="$2"
    local qps="$3"
    local parsed
    parsed=$(printf '%s\n' "$stderr_output" | awk '
        $1 == "Avg" && $2 == "Latency" {
            for (i = 3; i + 1 <= NF; i += 2) {
                v = $i;
                if ($(i + 1) !~ /^ms/) v *= 1000;
                sum += v; n++;
            }
        }
        END { if (n > 0) printf "%.3f", sum / n }
    ')
    if [ -n "$parsed" ]; then
        echo "$parsed"
        return
    fi
    awk -v c="$concurrency" -v q="$qps" 'BEGIN { if (q > 0) printf "%.3f", c * 1000 / q; else printf "0.000" }'
}

# 对一组数值取均值 (保留 3 位小数): 个数 >2 时去掉最大/最小值, 与 qps_avg 同口径。
# 空数组 (例如每次都失败, 没有可用延迟) 返回 0.000。
# 参数: $1..$n=数值 (可缺省)。返回: stdout 输出 3 位小数的均值。
# 说明: $# 是位置参数个数; "$@" 展开为独立的参数, 逐项放进 values 数组。
#   个数 >2 时先 sort -n 升序, 再用 sed '1d;$d' 删除第一行和最后一行
#   (即最小/最大值), 对剩余值求均值 —— 与 qps_avg 的统计口径一致。
aggregate_mean() {
    local -a values=()
    if [ "$#" -gt 0 ]; then
        values=("$@")
    fi
    if [ "${#values[@]}" -eq 0 ]; then
        echo "0.000"
        return
    fi
    if [ "${#values[@]}" -le 2 ]; then
        printf '%s\n' "${values[@]}" | awk '{s += $1; n++} END {printf "%.3f", s / n}'
    else
        # sed 的 1d 删第 1 行, $d 删最后一行; 两者用分号写在同一脚本里
        printf '%s\n' "${values[@]}" | sort -n | sed '1d;$d' | awk '{s += $1; n++} END {printf "%.3f", s / n}'
    fi
}

# MyScale 版本索引检查: system.vector_indices 看状态, EXPLAIN 命中断言 ReadWithHybridSearch
# 参数: $1=表名, $2=向量列名。
# 返回: 0=索引就绪且 EXPLAIN 命中混合检索; 1=无索引/未构建/未命中; 查不到样本时也返回 1。
# 副作用: 只读查询 + 打印诊断信息, 不修改服务端状态。
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

    # system.vector_indices 查询结果按 TabSeparated 输出, 默认分列;
    # 这里按空白取前三个字段 (name/type/status), 只取第一行记录。
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

# ClickHouse 版本索引检查: data_skipping_indices 是否有已构建的向量索引,
# 再用 EXPLAIN indexes=1 断言查询确实命中该 Skip index。
# 参数: $1=表名, $2=向量列名, $3=距离函数 (用于构造 EXPLAIN 查询)。
# 返回: 0=命中; 1=无索引/未构建/有未完成 mutation/EXPLAIN 未命中;
#   2=拿不到样本向量, 无法执行 EXPLAIN (调用方只按成功/失败二分, 2 也视为失败)。
# MyScale 分支直接转交 check_vector_index_myscale 并原样返回其退出码。
check_vector_index() {
    local table="$1"
    local vec_col="$2"
    local dist_func="$3"

    if [ "$ENGINE" = "myscale" ]; then
        check_vector_index_myscale "$table" "$vec_col"
        # $? 是上一条命令 (MyScale 检查) 的退出码, 这里原样向上传递
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

    # 按空白取第一条索引记录的 name/type/占用字节数
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

        # 两个 grep -q 都必须成功 (&&): 既出现该索引名, 又出现该索引类型, 才认为命中。
        # 注意 set -o pipefail 下 grep 无匹配会让管道返回 1, 所以不能单独依赖管道退出码。
        if echo "$explain_output" | grep -q "Name: ${index_name}" && echo "$explain_output" | grep -q "Description: ${index_type}"; then
            local skip_granules
            # awk 程序后的 `idx="$index_name"` 是变量赋值操作数 (等价于 -v idx=...),
            # 在读取输入前生效; $0 ~ "Name: " idx 是字符串拼接后做正则匹配。
            # 找到索引名后, 下一处 "Granules:" 行去掉首尾空白并打印, 然后 exit。
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
echo "并行转换: 模式随 profile 配置 (SETTINGS_GROUPS 第7字段), threads=${STRING_TO_VECTOR_THREADS}"
echo "输出:   $OUTPUT_CSV"
echo "=============================================="
echo ""

# -x 检查文件存在且有执行权限; 提前失败比跑到一半才发现更清晰
if [ ! -x "$CLICKHOUSE" ]; then
    echo "错误: 找不到 clickhouse 可执行文件: $CLICKHOUSE"
    exit 1
fi

# ---- Step 1: 输入表名 ----
echo ">>> Step 1: 输入测试表名"
echo ""

# 表名统一收进数组: 有命令行参数就逐个追加, 否则交互式读取后按空白切分。
# "$@" 加引号可保留每个参数原样 (含空格), 与 "$*" 不同。
TABLES=()

if [ $# -gt 0 ]; then
    for t in "$@"; do
        TABLES+=("$t")
    done
    echo "从命令行参数获取表名: ${TABLES[*]}"
else
    echo "当前数据库中的表 (含 Array(Float) 列):"
    # 先列出候选表帮助用户输入; sed 给每行加两个空格缩进
    "$CLICKHOUSE" client --host "$HOST" --port "$PORT" \
        --query "SELECT DISTINCT table FROM system.columns WHERE database='default' AND type LIKE 'Array(Float%)' ORDER BY table SETTINGS use_query_cache=0" 2>/dev/null | sed 's/^/  /'
    echo ""
    # read -p 打印提示并从 stdin 读一行; read -ra 把该行按空白切分进数组 (a=array)
    read -p "请输入表名 (多个表用空格分隔): " table_input
    if [ -z "$table_input" ]; then
        echo "错误: 未输入表名"
        exit 1
    fi
    read -ra TABLES <<< "$table_input"
fi

echo ""

# 关联数组 (declare -A): 以表名为 key 缓存向量列名与维度, 避免后续反复查询 system 表
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
# 交互选择 1/2/3; ${dist_choice:-1} 让直接回车时默认选 1。
# case 的 *) 分支同时覆盖 "1" 和空值/非法输入, 统一退回 cosineDistance。
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
# 索引检测结果只用于提示与 TABLE_INDEX_STATUS 记录; 注意写入 CSV 的 index_status
# 列并不使用这里的结果 (见 Step 6 的说明, 恒写 index_active)。
echo ">>> Step 2.5: 检测向量索引状态"
echo ""

declare -A TABLE_INDEX_STATUS
HAS_INDEX_ISSUE=false
for table in "${TABLES[@]}"; do
    vec_col="${TABLE_VEC_COL[$table]}"
    # 只要退出码非 0 (含 check_vector_index 的 return 2) 就进入 else, 视为不可用
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
    # -n 非空; 正则 ^[Yy]$ 只接受单字符 y/Y, 其余 (含直接回车) 都算取消
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

# 先做合法性校验 (mode 取值、开关依赖), 避免跑到一半才因配置错误退出
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

# -p 允许目录已存在, 并递归创建父目录
mkdir -p "$SQL_DIR"

# 这里只读前 6 个字段 (第 7 字段 mode 不影响 SQL 文本, 只影响会话 SET 行)
for settings_group in "${SETTINGS_GROUPS[@]}"; do
    IFS='|' read -r profile_name val_use_query_cache val_query_plan_cache val_cast_or_only_plan val_only_vector val_fifth <<< "$settings_group"
    settings_clause=$(build_settings_clause "$val_use_query_cache" "$val_query_plan_cache" "$val_cast_or_only_plan" "$val_only_vector" "$val_fifth")

    for table in "${TABLES[@]}"; do
        vec_col="${TABLE_VEC_COL[$table]}"

        # 只有该 (表, profile) 下所有 sql_type × row_count 的文件都存在时才跳过生成,
        # 避免曾经用较小的 SQL_TYPES/ROW_COUNTS 跑过一遍后缺文件却误判为已生成。
        # break 2 一次跳出 sql_type 与 row_count 两层循环, 回到 table 循环。
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

# 临时目录: 存放带会话 SET 头的运行副本、benchmark stderr、校验输出等。
# trap ... EXIT 在脚本任何正常/异常退出时执行; 双引号让 $TMPDIR 在注册时即展开,
# 避免 trap 执行时变量已失效。注意脚本顶部没有覆盖用户已有的 TMPDIR 语义,
# 这里直接复用同名变量存放本次运行的私有目录。
TMPDIR=$(mktemp -d)
trap "rm -rf $TMPDIR" EXIT

# 只探测一次, 后续通过 PERF_TEST_FLAG / STRING_TO_VECTOR_FLAG 复用结果
detect_perf_test_flag
detect_string_to_vector_support

# profile 里写的 mode 在「服务器不支持」或「没开计划缓存」时不会生效, 测量前先如实处理
normalize_string_to_vector_modes

# 汇总提示: effective=真正会走并行的 profile; sv0=其余按 mode=0 跑的 profile。
# ${VAR:+...} 是「VAR 非空才展开」的惯用法, 用来在字符串之间补一个空格分隔。
string_to_vector_effective_modes=""
string_to_vector_sv0_profiles=""
for settings_group in "${SETTINGS_GROUPS[@]}"; do
    IFS='|' read -r _ep_name _ep_f1 _ep_f2 _ep_f3 _ep_f4 _ep_f5 _ep_f6 <<< "$settings_group"
    if [ "${_ep_f6:-0}" != "0" ]; then
        string_to_vector_effective_modes="${string_to_vector_effective_modes:+${string_to_vector_effective_modes} }${_ep_name}=${_ep_f6}"
    else
        string_to_vector_sv0_profiles="${string_to_vector_sv0_profiles:+${string_to_vector_sv0_profiles} }${_ep_name}"
    fi
done
if [ -n "$string_to_vector_effective_modes" ]; then
    echo "  并行转换 mode (计划缓存开启的 profile): ${string_to_vector_effective_modes}, threads=${STRING_TO_VECTOR_THREADS}"
    echo "  其余 profile 为 mode=0:${string_to_vector_sv0_profiles:-(无)}"
else
    echo "  并行转换: 所有 profile 都是 mode=0 (未启用并行字符串→向量转换)"
fi
if [ "$VERIFY_PARALLEL_MODES" = "1" ]; then
    if [ "$STRING_TO_VECTOR_FLAG" = "1" ]; then
        echo "  并行转换一致性校验: 开启 (基准测试前先比对各模式查询结果)"
    else
        echo "  并行转换一致性校验: 已请求但服务器不支持该 setting, 跳过"
    fi
fi
if [ -n "$string_to_vector_effective_modes" ]; then
    echo "  注意: 若 mode=1/2 的 QPS 与 mode=0 完全相同, 先确认该查询是否只带一个向量字面量 ——"
    echo "        批大小为 1 时会按设计退回串行 (thread_count<2), 并行模式不生效属于预期, 不是 bug。"
    # 取最大并发, 用于估算转换线程总数是否超订
    max_concurrency=0
    for c in "${CONCURRENCIES[@]}"; do
        # `[ 条件 ] && 赋值` 在条件不成立时返回 1, 但 && 列表左侧失败不会触发 set -e
        [ "$c" -gt "$max_concurrency" ] && max_concurrency="$c"
    done
    # $(( ... )) 做整数乘/比较; threads=0 表示自动, 不参与超订估算
    if [ "$STRING_TO_VECTOR_THREADS" -gt 0 ] && [ $((STRING_TO_VECTOR_THREADS * max_concurrency)) -gt 32 ]; then
        echo "  注意: threads(${STRING_TO_VECTOR_THREADS}) × concurrency(${max_concurrency}) = $((STRING_TO_VECTOR_THREADS * max_concurrency)) 个转换线程, 可能超出本机核数造成超订,"
        echo "        观察到的 QPS 差异里会混入调度开销; 建议把 threads 控制在并发数以内做对比。"
    fi
fi
echo ""

# ---- Step 5: 预热服务端 ----
# 预热按"每张表一次"执行：对每张表选其一个 SQL 文件预热一次，把索引加载进
# 内存。时长取配置 WARMUP_TIMELIMIT，但最少 60 秒（保证预热足够充分）。
# 预热只与表相关（把该表索引进内存），因此每张表只需预热一次，后续该表的
# 所有 profile / sql_type / 并发组合共享同一份已预热的索引。
# 数值下限保护: 2>/dev/null 吞掉「非数字时 test 报错」的输出;
# 条件成立 (0 <= 值 < 60) 时抬到 60 秒, 保证预热足够充分。非数字则保持原值。
warmup_timelimit=$WARMUP_TIMELIMIT
if [ -n "$warmup_timelimit" ] && [ "$warmup_timelimit" -ge 0 ] 2>/dev/null && [ "$warmup_timelimit" -lt 60 ]; then
    warmup_timelimit=60
fi
echo ">>> Step 5: 预热服务端 (每张表各 ${warmup_timelimit}秒)"
echo ""

for table in "${TABLES[@]}"; do
    warmup_sql_file=""
    # 取第一个存在的 SQL 文件即可: 预热只关心把该表数据/索引读进内存,
    # 与 profile/sql_type/并发无关。
    # 注意: 此处嵌套顺序是 table > sql_type > settings_group > row_count, break 4
    # 会连最外层 for table 一起结束 (即实际只预热第一张表); 原逻辑如此, 本注释不改代码。
    for sql_type in "${SQL_TYPES[@]}"; do
        for settings_group in "${SETTINGS_GROUPS[@]}"; do
            # 只需要 profile 名, 其余字段用 _ 占位接收并忽略
            IFS='|' read -r profile_name _ <<< "$settings_group"
            for row_count in "${ROW_COUNTS[@]}"; do
                candidate="$SQL_DIR/${table}_${profile_name}_${sql_type}_${row_count}.sql"
                if [ -f "$candidate" ]; then
                    warmup_sql_file="$candidate"
                    break 4
                fi
            done
        done
    done

    if [ -z "$warmup_sql_file" ]; then
        echo "警告: 表 $table 没有找到可用的 SQL 文件，跳过该表预热"
        continue
    fi
    echo "使用 $warmup_sql_file 预热表 $table ..."
    warmup_run_file="$TMPDIR/warmup_run_${table}.sql"
    if [ "$PERF_TEST_FLAG" = "1" ]; then
        # 会话级打开 enable_vector_performance_test, 使查询自带的 SETTINGS 子句
        # 能进入服务端查询计划缓存探测; 因此关闭 --randomize 保证 SET 行先于查询执行
        # { ...; } 分组重定向: 把 echo 的 SET 行与 cat 的 SQL 一起写入同一文件。
        { echo "SET enable_vector_performance_test = 1"; cat "$warmup_sql_file"; } > "$warmup_run_file"
        # 空数组: 展开后不传任何参数 (bash 4.4+ 在 set -u 下也安全)
        randomize_opts=()
    else
        cp "$warmup_sql_file" "$warmup_run_file"
        randomize_opts=(--randomize)
    fi
    # 预热失败不应终止整个基准测试, 因此临时关闭 -e 捕获退出码 (这里不检查)
    # `--` 之前是 benchmark 自己的选项, 之后从 stdin 读取 SQL; --iterations 0 表示
    # 不限次数、只用 --timelimit 控制时长。
    set +e
    "$CLICKHOUSE" benchmark \
        --host "$HOST" \
        --port "$PORT" \
        --concurrency 1 \
        --timelimit "$warmup_timelimit" \
        --delay 0 \
        "${randomize_opts[@]}" \
        --iterations 0 \
        --precise_float_parsing "${PRECISE_FLOAT_PARSING_VALUES[0]}" \
        -- \
        < "$warmup_run_file" \
        > /dev/null 2>&1
    set -e
    echo "表 $table 预热完成"
done
echo ""

# ---- Step 5.5: 并行转换结果一致性校验 (可选, VERIFY_PARALLEL_MODES=1) ----
# 并行转换只在「计划缓存命中、把参数文本还原成 Field」时才被调用, 所以校验必须让
# 每条查询跑两遍: 第一遍写缓存, 第二遍命中并走被测代码。各组合的会话 SET 行不同,
# 因此各自是独立的缓存 key, 互不干扰。
#
# 一致 ≠ 校验有效: 若计划缓存一次都没命中 (例如会话级 SET 没在缓存探测前生效),
# 第二遍走的还是普通路径, 此时各组合「结果一致」是废话。所以下面用 system.events
# 的 QueryPlanCacheHits 累计计数器量出第二遍的真实命中数, 命中为 0 直接报错。
# 读取服务端累计的计划缓存命中计数器 (system.events)。
# 参数: 无。返回: stdout 输出整数值; 查询失败/为空时输出 0。
# 用法: 在跑一批查询前后各读一次, 差值即这批查询的真实命中次数。
read_plan_cache_hits() {
    local value
    value=$("$CLICKHOUSE" client --host "$HOST" --port "$PORT" \
        --query "SELECT value FROM system.events WHERE name = 'QueryPlanCacheHits'" 2>/dev/null)
    echo "${value:-0}"
}
if [ "$VERIFY_PARALLEL_MODES" = "1" ] && [ "$STRING_TO_VECTOR_FLAG" = "1" ]; then
    echo ">>> Step 5.5: 并行转换结果一致性校验"
    echo ""
    verify_sample="${VERIFY_SAMPLE_QUERIES:-20}"

    # 比对的对象是「同一组缓存开关、只有 mode 不同」的 profile (即同一 profile 的 mode 变体),
    # 所以先取第一个开了计划缓存的 profile, 用它前 5 个字段作为比对键, 把同键的 profile 都找出来。
    verify_key=""
    verify_base_spec=""
    for settings_group in "${SETTINGS_GROUPS[@]}"; do
        IFS='|' read -r vp_name vp_f1 vp_f2 vp_f3 vp_f4 vp_f5 _vp_f6 <<< "$settings_group"
        if [ "$vp_f2" = "1" ]; then
            verify_key="${vp_f1}|${vp_f2}|${vp_f3}|${vp_f4}|${vp_f5}"
            verify_base_spec="$settings_group"
            break
        fi
    done

    verify_variants=()
    if [ -n "$verify_key" ]; then
        for settings_group in "${SETTINGS_GROUPS[@]}"; do
            IFS='|' read -r vp_name vp_f1 vp_f2 vp_f3 vp_f4 vp_f5 _vp_f6 <<< "$settings_group"
            [ "${vp_f1}|${vp_f2}|${vp_f3}|${vp_f4}|${vp_f5}" = "$verify_key" ] && verify_variants+=("$settings_group")
        done
    fi

    if [ -z "$verify_key" ]; then
        echo "  警告: SETTINGS_GROUPS 里没有打开计划缓存的 profile, 无法校验 (并行转换只在缓存命中时生效)"
    elif [ ${#verify_variants[@]} -lt 2 ]; then
        echo "  提示: 同一组缓存开关下只有 1 个 profile, 没有可比的 mode 变体, 跳过;"
        echo "        在 SETTINGS_GROUPS 里给同一组开关再加几行、只在第 7 字段写 1/2 即可对比, 例如:"
        echo "          \"plan_cache_blocked|0|1|0|0|0|1\""
        echo "          \"plan_cache_cyclic|0|1|0|0|0|2\""
    else
        echo "  校验: 同一组缓存开关下的 ${#verify_variants[@]} 个 mode 变体, 每个取前 ${verify_sample} 条查询, 跑两遍取第二遍输出"
        echo ""

        for table in "${TABLES[@]}"; do
            # 只用基准 profile 的 SQL 文件: mode 只写进会话级 SET 行, 不写进生成的 SQL,
            # 所以同一组缓存开关下各 mode 变体的查询文本完全一致。
            IFS='|' read -r vp_name vp_f1 vp_f2 vp_f3 vp_f4 vp_f5 _vp_f6 <<< "$verify_base_spec"
            verify_sql_file="$SQL_DIR/${table}_${vp_name}_normal_${ROW_COUNTS[0]}.sql"
            if [ ! -f "$verify_sql_file" ]; then
                echo "  ⚠ 表 $table: 找不到校验用 SQL 文件, 跳过: $verify_sql_file"
                continue
            fi

            reference_out=""
            reference_combo=""
            verify_mismatch=0
            verify_total_hits=0

            for settings_group in "${verify_variants[@]}"; do
                IFS='|' read -r vp_name vp_f1 vp_f2 vp_f3 vp_f4 vp_f5 vp_f6 <<< "$settings_group"
                vc_mode="${vp_f6:-0}"
                svc="${vp_name}(mode=${vc_mode})"
                verify_run_file="$TMPDIR/verify_${table}_${vp_name}_mode${vc_mode}.sql"
                {
                    # 这里用 clickhouse client --multiquery, 它按分号切分语句, 所以每条 SET
                    # 都必须带 ';'; 查询行的 ';' 排在行尾内联 SETTINGS 之后, 被剥掉时会一并
                    # 吃掉, 因此 sed 的替换结果直接补一个 ';' 而不是留空。
                    if [ "$PERF_TEST_FLAG" = "1" ]; then
                        echo "SET enable_vector_performance_test = 1;"
                    fi
                    # 计划缓存探测在 AST 解析之前: 只有开启轻量预扫 (上面这行 SET) 的会话,
                    # 会话级 SET 才会在探测前生效, 从而让第二遍真的命中缓存。
                    printf '%s;\n' "$(session_set_line_for_profile \
                        "$vp_f1" "$vp_f2" "$vp_f3" "$vp_f4" "$vp_f5" "$vc_mode" "$STRING_TO_VECTOR_THREADS")"
                    # 正则 " SETTINGS .*$" 匹配行尾的子句并整体替换成 ';' (multiquery 需要分号);
                    # head -n 只取前 N 条, 控制校验耗时。
                    sed 's/ SETTINGS .*$/;/' "$verify_sql_file" | head -n "$verify_sample"
                } > "$verify_run_file"

                verify_out_file="$TMPDIR/verify_${table}_${vp_name}_mode${vc_mode}.out"
                verify_err_file="$TMPDIR/verify_${table}_${vp_name}_mode${vc_mode}.err"
                verify_probe_err="$TMPDIR/verify_${table}_${vp_name}_mode${vc_mode}.probe.err"

                # 第一遍: 写缓存 (结果丢弃)。同时用它的报错探测本 fork 的既有问题 ——
                # 会话级 enable_query_plan_cache=1 会让普通查询报
                # "ReadFromStorage cannot be cloned without pipe_builder"; 真要撞上说明
                # 这个环境上该校验不适用, 明确跳过, 而不是把失败当成「结果不一致」。
                "$CLICKHOUSE" client --host "$HOST" --port "$PORT" --multiquery \
                    < "$verify_run_file" > /dev/null 2>"$verify_probe_err" || true
                if grep -q "cannot be cloned without pipe_builder" "$verify_probe_err" 2>/dev/null; then
                    echo "  ⚠ 表 $table: 本 fork 上会话级 enable_query_plan_cache=1 会使普通查询报 pipe_builder 错误, 校验不适用, 跳过"
                    verify_mismatch=2
                    break
                fi

                # 第二遍: 命中缓存 -> 走字符串转向量/参数还原。
                # 用 system.events 的 QueryPlanCacheHits 累计计数器量出这一遍的真实命中数:
                # 一致但 0 命中 = 没走到被测代码, 是假通过。
                verify_hits_before=$(read_plan_cache_hits)
                if ! "$CLICKHOUSE" client --host "$HOST" --port "$PORT" --multiquery \
                    < "$verify_run_file" > "$verify_out_file" 2>"$verify_err_file"; then
                    echo "  ✗ 表 $table 变体 ${svc}: 查询执行失败"
                    tail -3 "$verify_err_file" 2>/dev/null | sed 's/^/      /'
                    verify_mismatch=1
                    continue
                fi
                # $(( ... )) 是算术展开; 计数器只会单调增, 差值为本遍真实命中数
                verify_hits=$(( $(read_plan_cache_hits) - verify_hits_before ))
                verify_total_hits=$((verify_total_hits + verify_hits))
                if [ "$verify_hits" -le 0 ]; then
                    echo "  ⚠ 表 $table 变体 ${svc}: 本遍计划缓存 0 命中, 结果比对未覆盖参数还原路径"
                fi

                if [ -z "$reference_out" ]; then
                    reference_out="$verify_out_file"
                    reference_combo="$svc"
                    echo "  表 $table 变体 ${svc}: 记录为基准 ($(wc -l < "$verify_out_file") 行输出, 命中 ${verify_hits})"
                # cmp -s 按字节静默比较两个输出文件, 相同返回 0 (不打印差异)
                elif cmp -s "$reference_out" "$verify_out_file"; then
                    echo "  ✓ 表 $table 变体 ${svc}: 与基准 ${reference_combo} 结果一致 (命中 ${verify_hits})"
                else
                    echo "  ✗ 表 $table 变体 ${svc}: 与基准 ${reference_combo} 结果不一致!"
                    # diff 发现差异时退出码为 1; 用 ( ... || true) 包进子 shell 并吞掉非 0,
                    # 避免 set -e/pipefail 把「发现差异」当成脚本错误直接退出。
                    (diff "$reference_out" "$verify_out_file" 2>/dev/null || true) | head -10 | sed 's/^/      /'
                    verify_mismatch=1
                fi
            done

            if [ "$verify_mismatch" = "2" ]; then
                # `:` 是内建空操作, 仅用于占位, 保持 if/elif 分支结构
                : # 已在上面提示不适用
            elif [ "$verify_mismatch" != "0" ]; then
                echo "  ✗ 表 $table: 各 mode 变体结果存在差异, 请先排查转换逻辑再采信 QPS 数据"
            elif [ -n "$reference_out" ] && [ "$verify_total_hits" -le 0 ]; then
                echo "  ⚠ 表 $table: 各变体结果一致, 但全程计划缓存 0 命中 —— 未覆盖参数还原路径, 不能据此判定实现正确"
            elif [ -n "$reference_out" ]; then
                echo "  ✓ 表 $table: 各 mode 变体结果一致 (第二遍共计划缓存命中 ${verify_total_hits} 次, 已覆盖参数还原路径)"
            fi
            echo ""
        done
    fi
    echo ""
fi

# ---- Step 6: 运行基准测试 ----
echo ">>> Step 6: 运行基准测试"
echo ""

# 结果 CSV 首列加上 engine, 便于 ck 与 myscale 结果在同一文件里区分。
# 表头按固定顺序拼出: 维度列 -> REPEAT 个 run_N -> 汇总列。
# CSV 采用最朴素的行内拼接, 字段值 (表名/profile 名) 中不能含逗号或换行。
NEW_HEADER="engine,table_name,distance_func,index_status,settings_profile"
if [ "$ENGINE" = "myscale" ]; then
    NEW_HEADER="${NEW_HEADER},use_query_cache,enable_query_plan_cache,enable_cast_vector,query_plan_cache_only_vector,only_cache_query_plan"
else
    NEW_HEADER="${NEW_HEADER},use_query_cache,vector_query_plan_cache,vector_only_cache_query_plan,vector_query_plan_cache_only_vector,vector_use_cast"
fi
NEW_HEADER="${NEW_HEADER},precise_float_parsing,string_to_vector_mode,string_to_vector_threads,sql_type,row_count,concurrency"
# 为每次重复各生成一列 run_1..run_<REPEAT>; seq 1 N 输出递增的数字
for i in $(seq 1 $REPEAT); do
    NEW_HEADER="${NEW_HEADER},run_${i}"
done
NEW_HEADER="${NEW_HEADER},qps_avg,qps_min,qps_max,mean_time_ms,p95_time_ms,p99_time_ms"

# 输出目录可能不存在 (默认相对路径 ../results/), 先建目录再写文件
# dirname 取路径的目录部分; 命令替换嵌套时用双引号包住整体
mkdir -p "$(dirname "$OUTPUT_CSV")"

if [ ! -f "$OUTPUT_CSV" ]; then
    # 首次运行: 写入表头
    echo "$NEW_HEADER" > "$OUTPUT_CSV"
    echo "创建新的 CSV 文件: $OUTPUT_CSV"
else
    # 已有文件: 表头一致就继续追加; 不一致则按历史版本逐个尝试迁移。
    # 迁移前先带时间戳备份 (date '+%Y%m%d%H%M%S'), 防止误伤已有数据。
    existing_header=$(head -1 "$OUTPUT_CSV")
    if [ "$existing_header" != "$NEW_HEADER" ]; then
        backup_file="${OUTPUT_CSV}.$(date '+%Y%m%d%H%M%S').bak"
        cp "$OUTPUT_CSV" "$backup_file"
        echo "CSV 表头不匹配，已备份旧文件到: $backup_file"
        if echo "$existing_header" | grep -q ',precise_float_parsing,sql_type,row_count,concurrency'; then
            # 本次改动前的结构: 在 precise_float_parsing 之后补两列并行转换维度
            # 做法: 新建临时文件 -> 写新表头 -> 用 awk 在每行第 11 列后插入两个
            # not_recorded, 末尾再 sed 补三列延迟 -> mv 原子替换原文件。
            # awk 里 BEGIN{OFS=","} 改变输出分隔符; printf 逐字段输出以避免逗号转义问题。
            echo "迁移旧数据: 为每行插入 string_to_vector_mode/threads=not_recorded 列..."
            tmp_csv=$(mktemp)
            echo "$NEW_HEADER" > "$tmp_csv"
            tail -n +2 "$OUTPUT_CSV" | awk -F',' 'BEGIN {OFS=","} {for (i=1; i<=11; ++i) printf "%s%s", $i, OFS; printf "not_recorded,not_recorded"; for (i=12; i<=NF; ++i) printf "%s%s", OFS, $i; printf "\n"}' | sed 's/$/,not_recorded,not_recorded,not_recorded/' >> "$tmp_csv"
            mv "$tmp_csv" "$OUTPUT_CSV"
            echo "迁移完成"
        elif echo "$existing_header" | grep -q ',qps_avg,qps_min,qps_max$'; then
            # 上一版结构: 已有全部维度列, 只缺末尾三个延迟列
            # sed 's/$/.../' 在每行行尾追加文本 (同样用临时文件 + mv 替换)
            echo "迁移旧数据: 为每行补 mean_time_ms/p95_time_ms/p99_time_ms=not_recorded 三列..."
            tmp_csv=$(mktemp)
            echo "$NEW_HEADER" > "$tmp_csv"
            tail -n +2 "$OUTPUT_CSV" | sed 's/$/,not_recorded,not_recorded,not_recorded/' >> "$tmp_csv"
            mv "$tmp_csv" "$OUTPUT_CSV"
            echo "迁移完成"
        elif echo "$existing_header" | grep -q '^engine,'; then
            # engine 已在首列但列结构不同 (通常是混用了另一个引擎的结果文件), 无法对齐
            # 这种情况下直接重建表头 (旧数据不再保留在文件里, 但仍在上面的 .bak 中)
            echo "$NEW_HEADER" > "$OUTPUT_CSV"
            echo "无法自动迁移 (引擎/列结构不匹配)，已创建新表头"
        elif echo "$existing_header" | grep -q '^settings_profile'; then
            # 最老的结构: 行首没有 engine/table/distance/index 四列。
            # 补常量 local_768d_test/cosineDistance/full_scan, 再插入 not_recorded 占位,
            # 最后行首补 engine=clickhouse、行尾补三列延迟。
            echo "迁移旧数据: 为每行添加 engine=clickhouse,table_name,distance_func,index_status 列..."
            tmp_csv=$(mktemp)
            echo "$NEW_HEADER" > "$tmp_csv"
            # while read 逐行处理; IFS= read -r line 保留行首尾空白与反斜杠
            tail -n +2 "$OUTPUT_CSV" | while IFS= read -r line; do
                migrated="local_768d_test,cosineDistance,full_scan,${line}"
                echo "$migrated" | awk -F',' 'BEGIN {OFS=","} {for (i=1; i<=9; ++i) printf "%s%s", $i, OFS; printf "not_recorded"; for (i=10; i<=NF; ++i) printf "%s%s", OFS, $i; printf "\n"}' | sed 's/^/clickhouse,/' | sed 's/$/,not_recorded,not_recorded,not_recorded/' >> "$tmp_csv"
            done
            mv "$tmp_csv" "$OUTPUT_CSV"
            echo "迁移完成"
        elif echo "$existing_header" | grep -q '^table_name,settings_profile'; then
            # 次老结构: 已有 table_name 但缺 distance_func/index_status。
            # ${line/,/,cosineDistance,full_scan,} 只替换第一个逗号 (bash 模式替换)。
            echo "迁移旧数据: 为每行添加 engine,distance_func,index_status 列..."
            tmp_csv=$(mktemp)
            echo "$NEW_HEADER" > "$tmp_csv"
            tail -n +2 "$OUTPUT_CSV" | while IFS= read -r line; do
                migrated="${line/,/,cosineDistance,full_scan,}"
                echo "$migrated" | awk -F',' 'BEGIN {OFS=","} {for (i=1; i<=9; ++i) printf "%s%s", $i, OFS; printf "not_recorded"; for (i=10; i<=NF; ++i) printf "%s%s", OFS, $i; printf "\n"}' | sed 's/^/clickhouse,/' | sed 's/$/,not_recorded,not_recorded,not_recorded/' >> "$tmp_csv"
            done
            mv "$tmp_csv" "$OUTPUT_CSV"
            echo "迁移完成"
        elif echo "$existing_header" | grep -q '^table_name,distance_func,index_status,settings_profile'; then
            # 该结构只缺 engine 首列与 precise_float_parsing 占位列
            echo "迁移旧数据: 为每行添加 engine 列与 precise_float_parsing=not_recorded 列..."
            tmp_csv=$(mktemp)
            echo "$NEW_HEADER" > "$tmp_csv"
            tail -n +2 "$OUTPUT_CSV" | awk -F',' 'BEGIN {OFS=","} {for (i=1; i<=9; ++i) printf "%s%s", $i, OFS; printf "not_recorded"; for (i=10; i<=NF; ++i) printf "%s%s", OFS, $i; printf "\n"}' | sed 's/^/clickhouse,/' | sed 's/$/,not_recorded,not_recorded,not_recorded/' >> "$tmp_csv"
            mv "$tmp_csv" "$OUTPUT_CSV"
            echo "迁移完成"
        else
            # 无法识别的表头: 直接覆盖为新表头 (旧文件已备份)
            echo "$NEW_HEADER" > "$OUTPUT_CSV"
            echo "无法自动迁移，已创建新表头"
        fi
    fi
    echo "追加到现有 CSV 文件: $OUTPUT_CSV ($(wc -l < "$OUTPUT_CSV") 行)"
fi
echo ""

echo "开始测试 ($(date '+%Y-%m-%d %H:%M:%S'))"
echo ""

# mode / threads 只写进运行副本头部的会话级 SET 行, 不写进生成的 SQL。因此每个
# profile (含 plan_cache 的 mode=1/2 变体) 各自是独立的计划缓存 key, 互不命中;
# 每个 profile 开始前清缓存, 首行 QPS 口径一致。
#
# 循环嵌套顺序 (从外到内): sql_type -> profile -> table -> precise_float_parsing
#   -> row_count -> concurrency -> repeat。每个最内层组合写 CSV 一行。
for sql_type in "${SQL_TYPES[@]}"; do
    echo "############################################################"
    echo "# SQL 类型: $sql_type  (外层循环: 先跑完该类型下的所有 profile)"
    echo "############################################################"
    echo ""
    for settings_group in "${SETTINGS_GROUPS[@]}"; do
        IFS='|' read -r profile_name val_use_query_cache val_query_plan_cache val_cast_or_only_plan val_only_vector val_fifth val_sv_mode <<< "$settings_group"
        val_sv_mode="${val_sv_mode:-0}"

        if [ "$val_sv_mode" != "0" ]; then
            echo "############################################################"
            echo "# profile: ${profile_name}  string_to_vector_parallel_mode=${val_sv_mode}, threads=${STRING_TO_VECTOR_THREADS}"
            echo "############################################################"
            echo ""
        fi


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
                for row_count in "${ROW_COUNTS[@]}"; do
                    # 每个批量查询从空缓存开始, 保证首行 QPS 口径一致
                    drop_caches_for_profile "$val_query_plan_cache"
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
                    # { ...; } > file 把多条命令的输出合成一份运行副本 (不是子 shell,
                    # 因此内部变量/退出码对外可见)。
                    run_file="$TMPDIR/run_${table}_${profile_name}_${sql_type}_${row_count}_sv${val_sv_mode}.sql"
                    profile_set_line=$(session_set_line_for_profile \
                        "$val_use_query_cache" "$val_query_plan_cache" \
                        "$val_cast_or_only_plan" "$val_only_vector" \
                        "$val_fifth" "$val_sv_mode" "$STRING_TO_VECTOR_THREADS")
                    {
                        if [ "$PERF_TEST_FLAG" = "1" ]; then
                            echo "SET enable_vector_performance_test = 1"
                        fi
                        echo "$profile_set_line"
                        # 去掉行尾 " SETTINGS ...;" (含分号), 只留纯查询文本;
                        # clickhouse-benchmark 按行读取查询, 不依赖行尾分号。
                        sed 's/ SETTINGS .*$//' "$sql_file"
                    } > "$run_file"
                    randomize_opts=()

                    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
                    echo " 表: $table  Profile: $profile_name  precise_float_parsing=$precise_float_parsing  string_to_vector=mode${val_sv_mode}/threads${STRING_TO_VECTOR_THREADS}  SQL: ${sql_type}_${row_count}.sql ($actual_queries 条查询)"
                    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

                    for concurrency in "${CONCURRENCIES[@]}"; do
                        echo ""
                        echo "  并发数: $concurrency (重复 $REPEAT 次)"

                        # 四组样本分别收集: 每次运行的 QPS 与三项延迟 (毫秒)。
                        # 延迟数组只追加「成功解析出百分位」的那几次运行。
                        qps_values=()
                        mean_ms_values=()
                        p95_ms_values=()
                        p99_ms_values=()

                        # seq 生成 1..REPEAT 的整数序列; 未加引号以便按空白拆成单词
                        for run in $(seq 1 $REPEAT); do
                            stderr_file="$TMPDIR/stderr_${table}_${profile_name}_precise${precise_float_parsing}_${sql_type}_${row_count}_${concurrency}_${run}.log"

                            # echo -n 不换行, 让结果打印在同一行
                            echo -n "    第 ${run}/${REPEAT} 次... "

                            # benchmark 失败时不能触发 set -e: 先关 -e, 跑完立刻保存 $? 再开 -e。
                            # $? 必须在 set -e 与任何其它命令之前读取, 否则会被覆盖。
                            # stderr 重定向到文件供解析, stdout 丢弃。
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
                                # 失败也占一个样本 (0), 保证 run_N 列与重复次数对齐
                                qps_values+=(0)
                                continue
                            fi

                            stderr_output=$(cat "$stderr_file")
                            qps=$(parse_total_qps "$stderr_output")

                            if [ -z "$qps" ] || [ "$qps" = "0.000" ]; then
                                echo "警告: 无法解析 QPS，记录为 0"
                                head -20 "$stderr_file" | sed 's/^/    /'
                                qps_values+=(0)
                            else
                                mean_ms=$(parse_mean_time_ms "$stderr_output" "$concurrency" "$qps")
                                p95_ms=$(parse_percentile_ms "$stderr_output" 95)
                                p99_ms=$(parse_percentile_ms "$stderr_output" 99)
                                # 只有 p95/p99 都解析成功才把三项延迟一起纳入统计,
                                # 避免不同指标用了不同的样本集合
                                if [ -n "$p95_ms" ] && [ -n "$p99_ms" ]; then
                                    mean_ms_values+=("$mean_ms")
                                    p95_ms_values+=("$p95_ms")
                                    p99_ms_values+=("$p99_ms")
                                fi
                                # ${var:-?} 在变量为空时显示 ? (解析失败的情况)
                                echo "QPS = $qps (mean=${mean_ms}ms, p95=${p95_ms:-?}ms, p99=${p99_ms:-?}ms)"
                                qps_values+=("$qps")
                            fi
                        done

                        # 汇总 QPS: 与延迟同口径 —— 样本数 >2 时去掉一个最大、一个最小值后取均值;
                        # <=2 时直接平均 (去掉极值就没有样本了)。sort -n 按数值排序, 不能按字典序。
                        sorted_qps=$(printf '%s\n' "${qps_values[@]}" | sort -n)
                        count=${#qps_values[@]}

                        if [ "$count" -le 2 ]; then
                            qps_avg=$(printf '%s\n' "${qps_values[@]}" | awk '{sum+=$1; n++} END {printf "%.3f", sum/n}')
                            qps_min=$(printf '%s\n' "${qps_values[@]}" | sort -n | head -1)
                            qps_max=$(printf '%s\n' "${qps_values[@]}" | sort -n | tail -1)
                        else
                            # head -1/tail -1 直接取已排序结果的极值; sed 删除首行与末行
                            qps_min=$(echo "$sorted_qps" | head -1)
                            qps_max=$(echo "$sorted_qps" | tail -1)
                            qps_avg=$(echo "$sorted_qps" | sed '1d;$d' | awk '{sum+=$1; n++} END {printf "%.3f", sum/n}')
                        fi

                        mean_time_ms=$(aggregate_mean "${mean_ms_values[@]}")
                        p95_time_ms=$(aggregate_mean "${p95_ms_values[@]}")
                        p99_time_ms=$(aggregate_mean "${p99_ms_values[@]}")

                        echo "  ─────────────────────────────────────"
                        echo "  结果: 均值=$qps_avg  最小=$qps_min  最大=$qps_max"
                        echo "  延迟 (ms): mean=$mean_time_ms  p95=$p95_time_ms  p99=$p99_time_ms  (有效 ${#p95_ms_values[@]}/${REPEAT} 次)"
                        echo ""

                        # 说明: CSV 不再写 index_status 的检测结果。该列此前会把因索引进度
                        # (pending_mutations 等临时状态)而误判为 full_scan 的表写成误导值,
                        # 任何基于 "index_active" 的过滤都会因此丢掉本应有效的数据。
                        # 本列仅作历史表头兼容, 统一记作 index_active(实测的 QPS 才是有效数据)。
                        # 拼一行 CSV: 维度列 + 每次运行的 QPS + 汇总列。
                        # 两个分支目前输出的字段完全相同; 保留按引擎分支是为了将来
                        # 某一引擎需要不同列时的扩展点。字段未做引号/转义, 依赖取值中无逗号。
                        if [ "$ENGINE" = "myscale" ]; then
                        csv_line="${ENGINE},${table},${DISTANCE_FUNC},index_active,${profile_name},${val_use_query_cache},${val_query_plan_cache},${val_cast_or_only_plan},${val_only_vector},${val_fifth},${precise_float_parsing},${val_sv_mode},${STRING_TO_VECTOR_THREADS},${sql_type},${row_count},${concurrency}"
                        else
                            csv_line="${ENGINE},${table},${DISTANCE_FUNC},index_active,${profile_name},${val_use_query_cache},${val_query_plan_cache},${val_cast_or_only_plan},${val_only_vector},${val_fifth},${precise_float_parsing},${val_sv_mode},${STRING_TO_VECTOR_THREADS},${sql_type},${row_count},${concurrency}"
                        fi
                        # 依次追加每次运行的 QPS (失败样本已在数组里记 0)
                        for v in "${qps_values[@]}"; do
                            csv_line="${csv_line},${v}"
                        done
                        # 若样本数不足 REPEAT (正常流程不会, 数组总是追加), 用空字段补齐 run 列;
                        # seq 的起点大于终点时不输出任何内容, 循环自然跳过。
                        for _ in $(seq $((count + 1)) $REPEAT); do
                            csv_line="${csv_line},"
                        done
                        csv_line="${csv_line},${qps_avg},${qps_min},${qps_max},${mean_time_ms},${p95_time_ms},${p99_time_ms}"
                        # 追加模式写 CSV; 表头已在此之前确保存在
                        echo "$csv_line" >> "$OUTPUT_CSV"
                    done    # for concurrency

                    echo ""
                done    # for row_count
            done    # for precise_float_parsing
        done    # for table
    done    # for settings_group in SETTINGS_GROUPS
done    # for sql_type in SQL_TYPES

echo "=============================================="
echo " 测试完成! ($(date '+%Y-%m-%d %H:%M:%S'))"
echo " 结果文件: $OUTPUT_CSV"
echo "=============================================="
echo " "
echo "--- CSV 内容预览 (最后20行) ---"
# column -t 按列对齐, -s',' 指定逗号分隔; 系统没有 column 时 (或执行失败) 回退为原文输出。
# 注意 set -o pipefail 下, 管道失败会触发 || 分支, 因此两种情况都能得到可读结果。
tail -20 "$OUTPUT_CSV" | column -t -s',' 2>/dev/null || tail -20 "$OUTPUT_CSV"