# vector-benchmark 设计文档

统一的向量数据库性能基准测试框架，支持 **pgvector**、**ClickHouse**、**MyScale** 与 **PolarDB-pg (PASE)**。
框架由**三类**测试脚本组成：① **Python 层**（`run.py` / `query_forms.py`）负责创建数据库 / 建索引 /
QPS 与召回率，并作为 **pgvector / PolarDB-pg (PASE)** 的 QPS 权威口径；② **原生 sh 层**
（`bash-test/clickhouse-benchmark.sh`）用原生工具 `clickhouse-benchmark` 测量向量检索 QPS，并作为
**ClickHouse / MyScale** 的 QPS 权威口径；③ **pgbench 交叉校验层**（`bash-test/pgvector-benchmark.sh`、
`bash-test/polardb-pase-query-forms-benchmark.sh`）独立校验 pgvector / PolarDB 各 SQL 形式与缓存模式，
并为 pgbench 无法提供的单语句延迟补充探针。三者共同覆盖多种 SQL 字面量形式与查询计划 / 结果缓存档位的
横向对比。

- 语言：中文 | [English](../../README.md)
- 客户端使用说明：[README（中文）](../../README.zh-CN.md) / [README（English）](../../README.md)
- 数据集格式：[dataset.md](./dataset.md)

---

## 1. 背景与目标

向量数据库厂商 / 开源实现众多，其 SQL 方言、向量字面量写法、索引类型与缓存机制差异很大。评估一个
向量数据库的吞吐（QPS）与准确性（召回率）需要解决两个问题：

1. **可比口径**：不同数据库对同一次基准测试应使用等价的查询语义，避免因 SQL 写法差异导致结果失真。
2. **可复现性**：压测脚本、数据集、缓存档位应能被配置化地固定下来，从而在多次运行、多台机器之间对齐。

本框架的设计目标：

- **引擎无关**：通过"引擎抽象 + 客户端实现"两层结构，把通用基准逻辑与具体数据库差异解耦。
- **配置驱动**：一次实验 = 一组 JSON 配置（引擎 × 数据集 × 参数），通过笛卡尔积自动展开为多组测量。
- **按引擎选择测量口径**：Python 层负责建库 / 建索引 / 召回率，并作为 pgvector、PolarDB-pg (PASE)
  的 QPS 权威口径（pgbench 无法测计划缓存真实 QPS）；sh 层用原生 `clickhouse-benchmark` 给出
  ClickHouse / MyScale 的 QPS（Python 驱动开销大、数值偏低）；pgbench 脚本保留为 pgvector /
  PolarDB 的独立交叉校验，并用 `psql \timing` 探针补充 pgbench 不提供的单语句延迟。
- **面向 SQL 形式与缓存**：显式覆盖不同向量字面量写法（CAST / 数组 / raw bytes / WITH 等）与
  查询计划缓存 / 结果缓存的组合，支持对缓存友好型查询形态的专项评估。

---

## 2. 系统架构总览

框架整体分为五层，自上而下：

```
         +--------------------------------------------------------------+
         |                    CLI / 入口层                                |
         |  run.py  |  python -m benchmark (__main__.py)  |  Task        |
         +-----------------------------+--------------------------------+
                                       |
         +-----------------------------v--------------------------------+
         |                    配置 / 数据层                               |
         |  config_read.py  configurations/*.json  datasets.json         |
         |  dataset.py  dataset_config.py  dataset_reader/*              |
         +-----------------------------+--------------------------------+
                                       |
         +-----------------------------v--------------------------------+
         |                       引擎抽象层                               |
         |  engine/base.py (BaseEngine: QPS / Recall 测量)               |
         |  engine/base_client/*  (Configure / Search / Upload / Parser) |
         |  engine/clients/*       (各数据库实现)                        |
         |  engine/clients/client_factory.py  (客户端工厂 + 笛卡尔积)     |
         +-----------------------------+--------------------------------+
                                       |
         +-----------------------------v--------------------------------+
         |                SQL 形式 / 缓存专项层                           |
         |  generate_sql_files.py  query_forms.py                       |
         +-----------------------------+--------------------------------+
                                       |
         +-----------------------------v--------------------------------+
         |      sh QPS 测试层 + pgbench 交叉校验层 + 结果分析层           |
         |  bash-test/clickhouse-benchmark.sh                            |
         |  bash-test/pgvector-benchmark.sh                              |
         |  bash-test/polardb-pase-query-forms-benchmark.sh              |
         |  results/csv/*.csv  results/json/*.json  results/charts/      |
         +--------------------------------------------------------------+
```

- **Python 基准路径（建库 / 建索引 + QPS + 召回率）**：`config_read → Dataset → ClientFactory →
  BaseClient`；并作为 pgvector、PolarDB-pg (PASE) 的 QPS 权威口径。
- **SQL 形式 / 缓存专项路径**：`generate_sql_files → query_forms`。
- **sh QPS 路径（ClickHouse / MyScale）**：先用 Python 路径建表、建索引、导入数据，再由
  `bash-test/clickhouse-benchmark.sh`（原生 `clickhouse-benchmark`）直接测量，作为这两者的
  QPS 权威口径。
- **pgbench 交叉校验路径（pgvector / PolarDB）**：`bash-test` 下的两个 pgbench 脚本按
  "一脚本 N 条 SQL，QPS = tps × N" 独立复测 Python 数值；pgvector 脚本另用 `psql \timing`
  逐条重放同一脚本，补出单语句 `mean/p95/p99` 延迟。
- **日志分析路径（可选）**：`analyze_pgvector_log.py` / `analyze_myscale_log.py` /
  `analyze_polardb_log.py` 解析数据库服务端日志，拆解 parse / analyze / plan / exec 与
  vector-in 转换耗时。

---

## 3. 目录结构

```
vector-benchmark/
├── benchmark/                          # Python 基准测试框架（主包）
│   ├── run.py                          #   主基准入口（MyscaleBench，Nuitka 编译目标）
│   ├── query_forms.py                  #   SQL 形式 × 缓存档位 QPS 对比
│   ├── generate_sql_files.py           #   生成 bash 测试用 SQL 文件
│   ├── plan_cache_correctness_invalidation.py # 计划缓存正确性/失效测试套件
│   ├── gen_qps_report.py               #   由结果 CSV 生成 QPS 图
│   ├── gen_step_time_report.py         #   由结果 CSV 生成分阶段耗时图
│   ├── generate_quora_embeddings.py    #   生成 Quora 文本嵌入（辅助脚本）
│   ├── analyze_pgvector_log.py         #   pgvector 服务端日志分析
│   ├── analyze_myscale_log.py          #   MyScale 服务端日志分析
│   ├── analyze_polardb_log.py          #   PolarDB PASE 服务端日志分析
│   ├── config_read.py                  #   配置读取（引擎配置 / 数据集配置）
│   ├── dataset.py                      #   数据集管理（下载 / 读取器分派）
│   ├── dataset_config.py               #   数据集 / 查询配置数据模型
│   ├── cli_output.py                   #   CLI 输出格式化工具
│   ├── __main__.py                     #   python -m benchmark 子命令入口
│   ├── __init__.py                     #   包导出 + 根目录/关键目录解析
│   ├── datasets/                       #   数据集
│   │   ├── datasets.json               #     数据集注册表
│   │   └── downloads/                  #     已下载的数据文件
│   ├── configurations/                 #   实验配置（JSON 数组）
│   │   └── config.json                 #     默认实验配置（单数组，多实验）
│   ├── engine/                         #   引擎抽象层
│   │   ├── base.py                     #     BaseEngine（QPS / 召回率通用测量）
│   │   ├── pgvector.py / polardb.py / clickhouse.py / myscale.py
│   │   ├── base_client/                #     客户端基类
│   │   │   ├── client.py               #       BaseClient（编排上传/建索引/查询）
│   │   │   ├── configure.py            #       BaseConfigurator（建表 / 建索引）
│   │   │   ├── search.py               #       BaseSearcher（查询 + session_settings）
│   │   │   ├── upload.py               #       BaseUploader（数据导入）
│   │   │   ├── parser.py               #       BaseClientParser（结果解析）
│   │   │   └── distances.py / utils.py
│   │   └── clients/                    #     各数据库客户端实现
│   │       ├── client_factory.py       #       客户端工厂（参数笛卡尔积展开）
│   │       └── pgvector/ clickhouse/ myscale/ polardb/
│   ├── dataset_reader/                 #   数据集读取器（按 type 分派）
│   │   ├── base_reader.py              #     读取器基类
│   │   ├── ann_h5_reader.py            #     ann-benchmarks HDF5（h5）
│   │   ├── ann_compound_reader.py      #     复合归档（tar）
│   │   ├── json_reader.py              #     JSONL（jsonl）
│   │   ├── gz_tsv_reader.py            #     BeIR 文本检索（gz_tsv）
│   │   └── utils.py
│   ├── results/                        #   基准结果（gitignore）
│   │   ├── csv/                        #     CSV 汇总（基准 + 日志分析）
│   │   ├── json/                       #     每次运行的详细 JSON
│   │   ├── charts/                     #     gen_*_report.py 生成的图表
│   │   └── bak/                        #     列 schema 变更时的 CSV 备份轮转
│   ├── docs/                           #   框架文档
│   │   ├── design.md                   #     设计文档（本文）
│   │   ├── dataset.md                  #     数据集格式说明
│   │   └── plan_cache_correctness_invalidation.md # 计划缓存正确性套件说明
│   └── bash-test/                      #   sh / pgbench 测试
│       ├── clickhouse-benchmark.sh     #   ClickHouse / MyScale 原生 QPS（权威口径）
│       ├── pgvector-benchmark.sh       #   pgvector pgbench + 单语句延迟探针（交叉校验）
│       ├── polardb-pase-query-forms-benchmark.sh # PolarDB PASE SQL 形式/缓存交叉校验
│       ├── generate-sql-files.sh       #   采样生成 ClickHouse 测试 SQL
│       └── sql-bench/                  #   生成的 SQL 文件（gitignore）
│
├── docs/                               # 查询形式 / 计划缓存对比分析文档（gitignore）
├── build_nuitka.sh                     # Nuitka 打包脚本（x86_64）
├── build_nuitka_arm.sh                 # Nuitka 打包脚本（ARM64）
├── README.md / README.zh-CN.md         # 使用说明（中 / 英）
```

---

## 4. 核心模块说明

### 4.1 入口与 CLI

#### `run.py`（主基准入口）

`run.py` 是主实验入口，也是 Nuitka 打包为 `myscale-bench` 的编译目标。它完成了：

1. **配置加载与匹配**：由 `config_read.read_engine_configs()` 读取 `configurations/*.json`
   （JSON 数组），`read_dataset_config()` 读取 `datasets/datasets.json`；`--engines` / `--datasets`
   支持通配符过滤，`--engine-type` 在匹配结果上按 `engine` 字段再过滤（只过滤、不改写配置，
   避免通配符误命中其它引擎族）。
2. **目标任务配对**：把选中的 engine 配置与数据集配置配对，处理"配置未声明 dataset""dataset
   不存在""dataset 未命中"等边界情况。
3. **参数覆盖**：命令行 `--host` / `--port` 覆盖连接参数；未覆盖时使用配置值，配置也缺失时回退
   到各引擎默认端口（pgvector=5432，polardb=5433，ClickHouse/MyScale=9000）。
4. **执行**：对每个 target，通过 `ClientFactory.build_client()` 构造客户端，创建 `Dataset`，
   `dataset.download()` 后调用 `client.run_experiment(...)`，支持 `--skip-upload`、
   `--skip-search`、`--recall-only`。

#### `__main__.py`（`python -m benchmark` 子命令）

提供轻量子命令入口，避免拉起整个 `benchmark/__init__.py`：

- `python -m benchmark generate_sql_files ...`：生成 SQL 基准文件。
- `python -m benchmark query_forms ...`：执行 SQL 形式 × 缓存档位对比。

### 4.2 配置层

- `config_read.py`：合并 `configurations/*.json`（每个文件为 JSON 数组，元素按 `name` 去重；
  附 `_source_file` 元信息）；读取数据集注册表。
- `dataset_config.py`：`Dataset` 配置与 `QueryConfig` 的数据模型（dataclass），统一声明向量 /
  文本 / 混合数据集的字段。

### 4.3 数据层

- `dataset.py`：`Dataset.download()` 支持本地存在即跳过、URL 下载等；`get_reader(normalize)`
  按 `type` 从 `READER_TYPE` 映射分派读取器：
  `{"h5": AnnH5Reader, "jsonl": JSONReader, "tar": AnnCompoundReader, "gz_tsv": GzTsvReader}`。
- `dataset_reader/`：各格式读取器。`ann_h5_reader.py` 读取 ann-benchmarks 标准 HDF5
  （`train` / `test` / `neighbors` / `distances`），`gz_tsv_reader.py` 读取 BeIR 风格文本检索集，
  便于全文 / 混合检索场景。

### 4.4 引擎抽象层

#### `BaseEngine`（`engine/base.py`）

面向脚本化压测的抽象基类，定义 `connect`、`create_table`、`create_index`、`insert_vectors`、
`query`、`server_version` 等抽象方法，并内置通用测量逻辑：

- `measure_qps(...)`：多线程并发压测，先 warmup，再在 `duration` 内统计请求数与错误数，
  返回 `(qps, error_count)`。
- `measure_recall(...)`：对查询向量计算 `recall@k`（结果 id 与 ground truth 前 k 的交集占比）。

`engine/__init__.py` 维护 `ENGINE_REGISTRY`：`pgvector / polardb / clickhouse / myscale`。

#### 客户端客户端树（`base_client` + `clients`）

除了脚本化的 `BaseEngine`，框架还有一套**面向驱动库的客户端**体系，每个引擎由四个组件构成：

| 组件 | 基类 | 职责 |
|------|------|------|
| 客户端编排 | `BaseClient`（`client.py`） | 串联 configure / upload / search，执行一次实验 |
| 建表/建索引 | `BaseConfigurator`（`configure.py`） | `clean()`、表的 `recreate()`、`configure()` |
| 数据导入 | `BaseUploader`（`upload.py`） | 把读取器产出的向量写入数据库 |
| 查询 | `BaseSearcher`（`search.py`） | 执行查询序列；支持 `session_settings` 的 SET 装配 |
| 结果解析 | `BaseClientParser`（`parser.py`） | 把查询结果解析为 `(id, distance)` |

各引擎在 `engine/clients/<engine>/` 提供对应实现；ClickHouse 家族（`clickhouse` / `myscale`）
通过 `clickhouse-connect` / `clickhouse-driver` 访问，PostgreSQL 家族（`pgvector` / `polardb`）
通过 `psycopg2` 访问。`engine/clients/client_factory.py` 的 `ClientFactory` 负责：

- 参数**笛卡尔积展开**：配置中值为数组的字段自动求积，生成多个测量组合。
- 客户端装配：根据配置选择对应引擎的 `Configurator / Uploader / Searcher`。

### 4.5 SQL 形式与缓存专项层

#### `generate_sql_files.py`

针对每个引擎生成"SQL 查询形式矩阵"，覆盖不同的向量字面量写法，用于苹果对苹果地比较哪种 SQL
形式 QPS 更高：

- **pgvector**：`text_literal / text_cast / vector_fn / array_int_cast / array_real_cast /
  array_cast / array_to_vector_fn / halfvec_literal / with_text_literal / with_array_cast /
  with_vector_fn / subquery_id / with_subquery_id`。
- **PolarDB PASE**：`text_pase_op_id / pase_fn_text_default / hash_op_* / with_pase_* / …`。
- **ClickHouse / MyScale**：`normal / cast / cast_array / type_hint / raw_bytes / raw_bytes_x /
  with_*` 等。

`sample_vectors_pg` / `sample_vectors_clickhouse` 从目标表直接采样向量作为查询载荷。

#### `query_forms.py`

在目标表建表/建索引完成后，用同一批采样查询池跑遍所有已注册 SQL 形式 × 缓存档位，输出一行/条的
CSV。缓存档位：

- **ClickHouse / MyScale**：`off / result-cache / plan-cache / plan-cast / plan-only-vector /
  plan-result-cache`，通过查询自身的 `SETTINGS ...` 子句表达；服务端需开启
  `enable_vector_performance_test`，每个档位开始时清空服务端缓存
  （`SYSTEM DROP QUERY CACHE` 等）。
- **PostgreSQL 家族**：`direct / result-cache / prepared-plan / prepared-result-cache`，
  计划缓存用 prepared statement，结果缓存用每连接有界结果缓存。

`_check_endpoint_conflicts` 会拒绝在同一 host:port 上同时测量 ClickHouse 与 MyScale
（两者监听同端口），要求分次顺序执行。

### 4.6 测试脚本分类与分工

框架由**三类**测试脚本组成，共用同一套 SQL 形式注册表与 CSV 列约定；每类引擎族的权威 QPS
口径是固定且唯一的（下表），其余脚本仅作交叉校验：

| 类别 | 脚本 | 权威口径 |
|------|------|----------|
| Python | `run.py`、`query_forms.py` | pgvector / PolarDB 的 QPS + 召回率；全部引擎的建表/建索引/数据导入 |
| 原生 sh | `bash-test/clickhouse-benchmark.sh` | ClickHouse / MyScale 的 QPS |
| pgbench 交叉校验 | `bash-test/pgvector-benchmark.sh`、`bash-test/polardb-pase-query-forms-benchmark.sh` | 独立校验 pgvector / PolarDB 各 SQL 形式（含计划缓存模式）的行为 |

**Python 类 —— 建库 + 建索引 + QPS + 召回率（`run.py` + `query_forms.py`）**

- 职责：创建数据库、建表、建索引、导入数据，随后测量 QPS 与召回率（含 `--recall-only`、
  `--skip-upload` 等专项）。
- 适用引擎：**pgvector / PolarDB-pg (PASE)** 的 QPS 以本类为权威口径。原因：这两者依赖的
  pgbench 无法测量"查询计划缓存"的真实 QPS —— 计划缓存收益须由同一后端会话内驱动的 prepared
  statement / 静态 SQL 包装函数来体现，而 Python 客户端恰好能精确控制会话级 SET 与 prepared
  statement，因此查询计划 / 结果缓存维度的 QPS 必须走 Python。

**sh 类 —— 向量检索 QPS（`bash-test/clickhouse-benchmark.sh`，配套 `generate-sql-files.sh`）**

- 职责：借助数据库原生工具 `clickhouse-benchmark` 直接测量向量检索 QPS，一套脚本含 N 条查询，
  `QPS = tps × N`。
- 适用引擎：**ClickHouse / MyScale** 的 QPS 以本类为权威口径。原因：用 Python 驱动测量时，
  Python 脚本的额外开销会显著拉低 QPS 数值。
- 支持计划 / 结果缓存档位（会话级 SET 装配，剥离行尾内联 `SETTINGS`），并于运行副本头部写入
  会话级 SET；支持 MyScale 并行 string→vector 模式（mode 0/1/2）与线程数。
- 缓存组合由 `SETTINGS_GROUPS` 指定，每项为
  `profile|use_query_cache|vector_query_plan_cache|vector_only_cache_query_plan|vector_query_plan_cache_only_vector|vector_use_cast`；
  MyScale 另有 `enable_cast_vector` / `only_cache_query_plan` / `query_plan_cache_only_vector`。
  `CK_EF_S` 设置会话级 `hnsw_candidate_list_size_for_search`。脚本从 `clickhouse benchmark`
  原始输出解析出 `mean_time_ms` / `p95_time_ms` / `p99_time_ms` 一并写入 CSV。

**pgbench 交叉校验类 —— pgvector / PolarDB（`bash-test/pgvector-benchmark.sh`、
`bash-test/polardb-pase-query-forms-benchmark.sh`）**

- 职责：按"一脚本 N 条 SQL，QPS = tps × N"独立复测 Python 数值，并遍历各引擎计划缓存模式
  （`raw/prepare_auto/prepare_custom/prepare_generic` 或 PolarDB 的
  `off/plan_cache/result_cache/plan_and_result_cache`）× 结果缓存开关。
- pgbench 只按*事务*（整份 N 条查询脚本）统计延迟，因此 pgvector 脚本额外实现单语句延迟探针：
  用 `psql -q` + `\timing on` 在单个 `BEGIN/COMMIT` 内逐条重放同一脚本，合并各客户端样本后
  以最近秩法计算 `mean_time_ms` / `p95_time_ms` / `p99_time_ms`。采样总量由
  `LATENCY_PROBE_STATEMENTS`（默认 1000，`0` 关闭并写 `not_recorded`）控制，
  吞吐由 `TIMELIMIT` / `WARMUP_TIMELIMIT` / `REPEAT` 控制。

计划缓存 / 结果缓存档位的精确编码（以代码为准）见下表；`use_query_cache=1` 时 pgvector 打开
`pgvector.result_cache*`、PolarDB 打开 `pase_result_cache*`，ClickHouse/MyScale 打开服务端查询缓存。

| 档位 | pgvector | PolarDB PASE | ClickHouse / MyScale |
|------|----------|--------------|----------------------|
| `0` | 不 `PREPARE`，内联文本，每次重新规划 | 同 pgvector | 缓存关闭 |
| `1` | `PREPARE` + `plan_cache_mode=auto`，类型化绑定 | `PREPARE` + `plan_cache_mode=auto`，类型化绑定 | 缓存开启 |
| `2` | `PREPARE` + `force_custom_plan`，类型化绑定 | `PREPARE` + `force_generic_plan`，文本绑定（`$1::pase`） | 缓存 + plan-only / cast 变体 |
| `3` | `PREPARE` + `force_generic_plan`，类型化绑定 | `PREPARE` + `force_generic_plan`，pase 类型化绑定（`$1`） | — |

### 4.7 结果分析与输出

所有 CSV 统一写入 `benchmark/results/csv/`，JSON 写入 `benchmark/results/json/`，图表写入
`benchmark/results/charts/`，列 schema 变更前的 CSV 轮转到 `benchmark/results/bak/`。

| 产出 | 来源 | 文件 |
|------|------|------|
| PostgreSQL 族 Python 结果 | `run.py` / `query_forms.py` | `pgvector_benchmark_results.csv`（含 `sql_type`、`vector_in_parse_mode`、`use_cache`、`use_query_cache`） |
| ClickHouse Python 结果 | `run.py` | `clickhouse_benchmark_results.csv` |
| MyScale Python 结果 | `run.py` | `benchmark_results.csv` |
| ClickHouse / MyScale sh 结果 | `clickhouse-benchmark.sh` | `clickhouse-benchmark-results.csv` / `myscale-benchmark-results.csv` |
| pgvector pgbench + 延迟探针 | `pgvector-benchmark.sh` | `pgvector-benchmark-results.csv` |
| PolarDB pgbench 交叉校验 | `polardb-pase-query-forms-benchmark.sh` | `vector-query-forms-polardb-results.csv` |
| 服务端日志汇总 | `analyze_*_log.py` | `<engine>-<table>-log-analysis*.csv`、`pgvector_log_analysis-*.csv`、`polardb_log_analysis-*.csv` |
| 详细运行记录 | `run.py` | `json/{experiment}-search-{id}-{ts}.json` |

延迟列口径：Python 客户端写 `mean_time` / `p95_time` / `p99_time`（单位**秒**）；sh 与 pgbench
脚本写 `mean_time_ms` / `p95_time_ms` / `p99_time_ms`（单位**毫秒**）；未采集时写 `not_recorded`。
CSV 头缺失列时由 `_ensure_csv_schema` 自动迁移，避免错列覆盖。

`gen_qps_report.py` / `gen_step_time_report.py` 读取 `results/csv/` 下的 CSV 生成图表，
`analyze_*_log.py` 解析服务端日志（`pase_stmt:` / prepared statement / plan cache MISS|HIT 等），
按 SQL 类型 × 缓存档位 × 结果缓存开关汇总 parse / analyze / plan / exec / vector-in 耗时。

---

## 5. 关键设计决策

### 5.1 引擎无关的双层抽象

脚本化引擎（`BaseEngine`）与驱动库客户端（`base_client/clients`）并存的缘由：两类能力分别在
不同使用场景被需要——`BaseEngine` 适合在脚本 / 测试里快速计算 QPS 与召回率；驱动库客户端被
`run.py` 用作正式实验，承载建表、建索引、导入、查询与缓存装配。这样既能快速原型验证，又能精确复现实验。

### 5.2 配置笛卡尔积展开

配置中任意字段取数组时，`ClientFactory._expand_params` 对其求笛卡尔积，自动把一次配置展开为
多个测量组合（如 `parallel` 与 `ef_s` 的组合），保证多参数扫描无需手写脚本。

### 5.3 session_settings（SET 装配）

`search_params.session_settings` 允许在每次查询前对会话执行 `SET`，从而在不重连的前提下调整
`enable_seqscan`、`ivfflat.probes`、`pase.enable`、`hnsw_candidate_list_size_for_search` 等，
显著简化"同一连接不同参数"的多轮测试。

### 5.4 缓存档位的可观测性

缓存收益只有在"同一会话、冷启动一次、档位正交"时才能被正确度量。`query_forms.py` 为每个档位
清空服务端缓存，`bash-test` 脚本在每个副本头部写入会话级 `SET` 并剥离行内 `SETTINGS`（避免
`enable_vector_performance_test` 引发的重复解析拖慢 ANN 查询），从而让"缓存命中"收益可复现、可断言
（如用 `system.events.QueryPlanCacheHits` 断言确实命中）。

### 5.5 按引擎选择 QPS 测量口径

QPS 测量口径**按引擎族固定选择**，而非一律用 Python 或一律用原生工具：

- **pgvector / PolarDB-pg (PASE)**：pgbench 无法测量"查询计划缓存"的真实 QPS，故 QPS 用
  Python 客户端（`run.py` / `query_forms.py`）测定；Python 驱动开销在此相对可控，且能精确装配
  会话级 SET 与 prepared statement。
- **ClickHouse / MyScale**：Python 脚本的额外开销较大，会把真实 QPS 显著拉低，故 QPS 用原生
  `clickhouse-benchmark`（`clickhouse-benchmark.sh`）测定。

两类权威口径共享同一套 SQL 形式注册表与 CSV 列 schema；Python 同时还负责所有引擎的建库建索引、
导入与召回率；pgbench 脚本作为独立的第三方校验，避免单一测量路径的系统性偏差。

### 5.6 单语句延迟探针

pgbench 的 `latency` 以事务（整份 N 条 SQL 脚本）为单位，无法直接给出单条查询的 `p95` / `p99`。
`pgvector-benchmark.sh` 因此不改造 pgbench，而是用 `psql` 在单个事务内逐条重放同一脚本并开启
`\timing`，从输出中提取每条语句的 `Time:` 样本；各并发客户端的样本合并后取 mean / p95 / p99
（最近秩）。探针与吞吐测量解耦：`LATENCY_PROBE_STATEMENTS=0` 时不采集、CSV 写 `not_recorded`，
避免在只想看 QPS 的场景上付出额外时间。psql 走简单协议，因此该延迟反映的是内联 SQL 形态，
与 `run.py` 的 prepared 形态相互独立——这正是交叉校验需要的。

---

## 6. 一次实验的数据流

以 `python run.py --engines pgvector-sift-128-euclidean` 为例：

```
1. read_engine_configs()  读取 configurations/*.json，按 --engines 过滤
2. read_dataset_config()  读取 datasets/datasets.json，按 --datasets 过滤
3. 配对 engine 与 dataset → targets
4. 对每个 target：
   a. ClientFactory.build_client(...) → pgvector 客户端
   b. Dataset(dataset_config); dataset.download()（本地存在则跳过）
   c. client.run_experiment(dataset, skip_upload, recall_only, skip_search)
        ├─ Configurator.configure(...)   建表 / 建索引
        ├─ Uploader.upload(...)          导入 train 向量
        ├─ Searcher.search(...)          按查询池测 QPS / 召回率
        └─ 结果写 results/csv/*.csv、results/json/*.json
```

随后，QPS 测量口径按引擎归属：
对 ClickHouse / MyScale，用 `bash-test/clickhouse-benchmark.sh` 对同一张表做原生 QPS 测量；
pgvector / PolarDB 的 QPS 已由上文 Python 查询阶段给出。

---

## 7. 支持的引擎

| 引擎 | 家族/驱动 | 默认端口 | 主要索引 | 距离度量 | QPS 测量 | 单语句延迟测量 |
|------|-----------|----------|----------|----------|----------|----------------|
| pgvector | psycopg2 | 5432 | `hnsw`、`ivfflat` | l2 / ip / cosine | Python（run.py / query_forms.py） | pgvector-benchmark.sh 的 `psql \timing` 探针（交叉校验） |
| PolarDB-pg (PASE) | psycopg2 | 5433 | `pase_hnsw`、`pase_ivfflat` | l2 / ip / cosine | Python（run.py / query_forms.py） | pgbench 事务级（`vector-query-forms-*-results.csv`） |
| ClickHouse | clickhouse-connect / clickhouse-driver | 9000 (tcp) / 8123 (http) | `HNSWFLAT`、`HNSW`、`VECTOR_SIMILARITY`、`ANNOY`、`USEARCH`、`FLAT` | l2 / dot / cosine | clickhouse-benchmark.sh | clickhouse-benchmark 原始输出解析 |
| MyScale | clickhouse-connect / clickhouse-driver | 9000 (tcp) / 8123 (http) | `HNSWFLAT`、`MSTG`、`MSRQ` | l2 / dot / cosine | clickhouse-benchmark.sh | clickhouse-benchmark 原始输出解析 |

---

## 8. 扩展指南

**新增一个引擎**大体分四步：

1. 在 `engine/clients/<engine>/` 下实现 `config.py`（默认参数/常量）、`configure.py`
   （`BaseConfigurator`）、`upload.py`（`BaseUploader`）、`search.py`（`BaseSearcher`）。
2. 在 `engine/clients/client_factory.py` 中引入并注册以上四个组件。
3. 若该引擎族需 sh 类 QPS 测试（ClickHouse / MyScale 一族），仿照
   `bash-test/clickhouse-benchmark.sh` 新增脚本，并在 README 的"支持的引擎"登记；
   pgvector / PolarDB 一族走 Python 路径（`query_forms.py`）即可，如需 pgbench 交叉校验再仿照
   `bash-test/pgvector-benchmark.sh` 扩展（含 `psql \timing` 延迟探针）。
4. 在 `configurations/` 添加该引擎的实验配置，在 `datasets/datasets.json` 配置对应数据集。

若新增**数据集格式**，在 `dataset_reader/` 实现读取器，并在 `dataset.py` 的 `READER_TYPE` 映射
和 `dataset_config.py` 中登记。

---

## 9. 测试与校验

- `plan_cache_correctness_invalidation.py`：计划缓存"正确性与失效"套件，覆盖 ClickHouse
  `VectorQueryPlanCache` 与 MyScale `QueryPlanCache`（命中后结果正确性、DDL/DML 失效边界、
  缓存键维度、容量淘汰），设计说明见
  [plan_cache_correctness_invalidation.md](./plan_cache_correctness_invalidation.md)。
- 结果 CSV 头自动迁移（`_ensure_csv_schema`），保证旧运行结果与新列 schema 兼容。
- `query_forms._check_endpoint_conflicts` 防止 ClickHouse / MyScale 同端口误测。
- 基准运行时对引擎 / 数据集不兼容情形（`IncompatibilityError`）跳过并提示，而不是中断整个实验。

---

## 10. 打包

使用 Nuitka 将 `run.py` 编译为独立可执行 `myscale-bench`：

- `build_nuitka.sh`（manylinux_2_28_x86_64）→ `dist/myscale-bench-linux-x86_64.tar.gz`
- `build_nuitka_arm.sh`（Ubuntu 22.04 / ARM64）→ `dist-arm/myscale-bench-linux-aarch64.tar.gz`

打包后通过 `get_root_dir()` 的启发式规则（Nuitka onefile 环境变量 / `argv[0]` / 工作目录）
定位 `datasets/`、`configurations/`、`results/` 目录，保证编译产物可移植运行。

---

## 附录 A：生成产物与忽略项

以下路径由运行时或辅助脚本生成，均已在 `.gitignore` 中忽略，**不入库**：

| 路径 | 产生者 | 说明 |
|------|--------|------|
| `benchmark/results/csv/` | `run.py`、`query_forms.py`、`bash-test/*.sh`、`analyze_*_log.py` | 基准 / 日志分析 CSV 汇总 |
| `benchmark/results/json/` | `run.py` | 每次运行一份详细 JSON |
| `benchmark/results/charts/` | `gen_qps_report.py`、`gen_step_time_report.py` | 图表输出 |
| `benchmark/results/bak/` | `_ensure_csv_schema`、sh 脚本写前备份 | 表头迁移或覆盖前的带时间戳备份 |
| `benchmark/bash-test/sql-bench/*/` | `generate_sql_files.py`、`generate-sql-files.sh` | pgbench / clickhouse-benchmark 用 SQL 文件 |
| `benchmark/datasets/downloads/` | 手动下载 | HDF5 等数据集文件 |
| `tmp/`（仓库根） | 各脚本的临时输入输出 | 如 `tmp/query-forms-targets.json` |
| `docs/`（仓库根） | 分析文档草稿 | 根 `.gitignore` 的 `/docs` 已忽略 |

遗留候选（本次 review 发现，未自动删除，需人工确认后清理）：

| 文件 | 说明 |
|------|------|
| `benchmark/results/pgvector-benchmark-results.csv` | 旧目录布局遗留（当前脚本写 `results/csv/`），内容已被 `results/csv/` 覆盖 |
| `benchmark/datasets/downloads/hackernews_part_1_of_1.parquet` | 约 56 GB，未在 `datasets.json` 注册，框架也没有 parquet 读取器，无法被使用 |

## 附录 B：脚本留存约定

三类测试脚本都在用，**没有可清除的脚本**。QPS 权威口径按引擎族划分（见 §4.6），
pgbench 脚本不是"被 Python 取代的遗留物"，而是 Python 数值之外的独立交叉校验：

| 脚本 | 类别 | 作用 |
|------|------|------|
| `run.py` | Python | 建库 / 建索引 / 导入 + QPS 与召回率；pgvector、PolarDB 的 QPS 权威口径 |
| `query_forms.py` | Python | SQL 形式 × 缓存档位 QPS 矩阵 |
| `generate_sql_files.py` | Python | 生成各引擎基准 SQL 文件 |
| `bash-test/clickhouse-benchmark.sh` | sh | ClickHouse / MyScale 原生 QPS（权威口径）及延迟统计 |
| `bash-test/generate-sql-files.sh` | sh | 为 clickhouse-benchmark 采样生成 SQL |
| `bash-test/pgvector-benchmark.sh` | sh · pgbench | pgvector 交叉校验 + `psql \timing` 单语句延迟探针 |
| `bash-test/polardb-pase-query-forms-benchmark.sh` | sh · pgbench | PolarDB PASE 各 SQL 形式 / 计划缓存模式的交叉校验 |

支撑模块：`dataset.py`、`config_read.py`、`dataset_config.py`、`engine/**`、`dataset_reader/**`。
校验工具：`plan_cache_correctness_invalidation.py`（计划缓存正确性与失效套件，见 §9）。

> 早期文档曾把两个 pgbench 脚本列为"可清除"，并把 `verify_string_to_vector_engine.py` 列为校验工具；
> 该 Python 校验脚本已不存在，其职责由 `plan_cache_correctness_invalidation.py` 承担。