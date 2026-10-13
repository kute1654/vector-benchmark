# vector-benchmark 设计文档

统一的向量数据库性能基准测试框架，支持 **pgvector**、**ClickHouse**、**MyScale** 与 **PolarDB-pg (PASE)**。
本框架只保留**两类**测试脚本：① **Python 层**（`run.py`）负责创建数据库 / 建索引 / QPS 与召回率，
并作为 **pgvector / PolarDB-pg (PASE)** 的 QPS 权威口径；② **sh 层**（`bash-test/clickhouse-benchmark.sh`）
用原生工具 `clickhouse-benchmark` 测量向量检索 QPS，并作为 **ClickHouse / MyScale** 的 QPS 权威口径。
两者共同覆盖多种 SQL 字面量形式与查询计划 / 结果缓存档位的横向对比。

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
  ClickHouse / MyScale 的 QPS（Python 驱动开销大、数值偏低）。
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
         |            sh QPS 测试层 + 结果分析层（ClickHouse / MyScale）  |
         |  bash-test/clickhouse-benchmark.sh  results/*.{csv,json}     |
         +--------------------------------------------------------------+
```

- **Python 基准路径（建库 / 建索引 + QPS + 召回率）**：`config_read → Dataset → ClientFactory →
  BaseClient`；并作为 pgvector、PolarDB-pg (PASE) 的 QPS 权威口径。
- **SQL 形式 / 缓存专项路径**：`generate_sql_files → query_forms`。
- **sh QPS 路径（ClickHouse / MyScale）**：先用 Python 路径建表、建索引、导入数据，再由
  `bash-test/clickhouse-benchmark.sh`（原生 `clickhouse-benchmark`）直接测量，作为这两者的
  QPS 权威口径。
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
│   │   ├── config.json                 #     默认实验配置
│   │   └── templates/                  #     配置模板（需复制到 configurations/ 使用）
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
│   ├── results/                        #   基准结果（CSV / JSON）
│   ├── docs/                           #   框架文档
│   │   ├── design.md                   #     设计文档（本文）
│   │   └── dataset.md                  #     数据集格式说明
│   └── bash-test/                      #   sh 类 QPS 测试（ClickHouse / MyScale）
│       ├── clickhouse-benchmark.sh     #   ClickHouse / MyScale 原生 QPS 压测
│       ├── pgvector-benchmark.sh       #   [可清除] pgvector（pgbench，见附录 B）
│       ├── polardb-pase-query-forms-benchmark.sh  # [可清除] PolarDB PASE（pgbench，见附录 B）
│       ├── generate-sql-files.sh       #   采样生成测试 SQL
│       ├── verify_string_to_vector_engine.py     #  MyScale 并行 string→vector 校验
│       └── sql-bench/                  #   生成的 SQL 文件（gitignore）
│
├── docs/                               # 查询形式 / 计划缓存对比分析文档
├── results/                            # 基准结果汇总 CSV
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
   支持通配符过滤。
2. **目标任务配对**：把选中的 engine 配置与数据集配置配对，处理"配置未声明 dataset""dataset
   不存在""dataset 未命中"等边界情况。
3. **参数覆盖**：命令行 `--host` / `--port` 覆盖连接参数；未覆盖时保留各引擎默认端口
   （pgvector=5432，polardb=5433，ClickHouse/MyScale=9000）。
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

框架只保留**两类**测试脚本，职责与适用引擎完全不同：

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

`verify_string_to_vector_engine.py` 是 MyScale 开发的端到端校验工具（Python），不属于上述两类
压测脚本：它驱动真实引擎代码校验并行 string→vector 的 SET 装配、模式一致性、计划缓存命中断言与
CSV 列迁移。

### 4.7 结果分析与输出

- Python 路径结果写入 `benchmark/results/`：`benchmark_results.csv`（MyScale 口径）与
  `pgvector_benchmark_results.csv`（PostgreSQL 口径），列 schema 与 `bash-test` 的 CSV 对齐；
  另有 `{experiment}-search-{id}-{ts}.json` 详细结果。CSV 头缺失列时自动迁移，避免错列覆盖。
- sh 路径（ClickHouse / MyScale）结果写入 `results/` 汇总 CSV。
- `analyze_*_log.py` 解析服务端日志（`pase_stmt:` / prepared statement / plan cache MISS|HIT 等），
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

两种口径共享同一套 SQL 形式注册表与 CSV 列 schema；Python 同时还负责所有引擎的建库建索引、导入
与召回率。

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
        └─ 结果写 results/*.csv / *.json
```

随后，QPS 测量口径按引擎归属：
对 ClickHouse / MyScale，用 `bash-test/clickhouse-benchmark.sh` 对同一张表做原生 QPS 测量；
pgvector / PolarDB 的 QPS 已由上文 Python 查询阶段给出。

---

## 7. 支持的引擎

| 引擎 | 家族/驱动 | 默认端口 | 主要索引 | 距离度量 | QPS 测量 |
|------|-----------|----------|----------|----------|----------|
| pgvector | psycopg2 | 5432 | `hnsw`、`ivfflat` | l2 / ip / cosine | Python（run.py / query_forms.py） |
| PolarDB-pg (PASE) | psycopg2 | 5433 | `pase_hnsw`、`pase_ivfflat` | l2 / ip / cosine | Python（run.py / query_forms.py） |
| ClickHouse | clickhouse-connect / clickhouse-driver | 9000 (tcp) / 8123 (http) | `HNSWFLAT`、`HNSW`、`VECTOR_SIMILARITY`、`ANNOY`、`USEARCH`、`FLAT` | l2 / dot / cosine | clickhouse-benchmark.sh |
| MyScale | clickhouse-connect / clickhouse-driver | 9000 (tcp) / 8123 (http) | `HNSWFLAT`、`MSTG`、`MSRQ` | l2 / dot / cosine | clickhouse-benchmark.sh |

---

## 8. 扩展指南

**新增一个引擎**大体分四步：

1. 在 `engine/clients/<engine>/` 下实现 `config.py`（默认参数/常量）、`configure.py`
   （`BaseConfigurator`）、`upload.py`（`BaseUploader`）、`search.py`（`BaseSearcher`）。
2. 在 `engine/clients/client_factory.py` 中引入并注册以上四个组件。
3. 若该引擎族需 sh 类 QPS 测试（ClickHouse / MyScale 一族），仿照
   `bash-test/clickhouse-benchmark.sh` 新增脚本，并在 README 的"支持的引擎"登记；
   pgvector / PolarDB 一族走 Python 路径（`query_forms.py`）即可。
4. 在 `configurations/` 添加该引擎的实验配置，在 `datasets/datasets.json` 配置对应数据集。

若新增**数据集格式**，在 `dataset_reader/` 实现读取器，并在 `dataset.py` 的 `READER_TYPE` 映射
和 `dataset_config.py` 中登记。

---

## 9. 测试与校验

- `verify_string_to_vector_engine.py`：MyScale 并行 string→vector 核心链路（SET 装配、模式一致性、
  计划缓存命中、CSV 迁移）的端到端校验。
- 结果 CSV 头自动迁移（`_ensure_csv_schema`），保证旧运行结果与新列 schema 兼容。
- `query_forms._check_endpoint_conflicts` 防止 ClickHouse / MyScale 同端口误测。
- 基准运行时对引擎 / 数据集不兼容情形（`IncompatibilityError`）跳过并提示，而不是中断整个实验。

---

## 附录 A：临时 / 未用文件清单

以下文件为调试、备份或开发中的中间产物，**未被任何生产代码引用**，均予保留以便继续调试。
若需清理，可参照下表按类别删除：

### A. 开发中/未启用的缓存功能变体

对应 `search.py` / `client.py` 的"查询结果缓存 / 查询计划缓存"实验性实现，仅作版本留档，正式代码未引用。

| 文件 | 说明 |
|------|------|
| `engine/base_client/client.py.query_cache` | BaseClient 查询结果缓存变体 |
| `engine/base_client/client.py.query_plan_cache` | BaseClient 查询计划缓存变体 |
| `engine/clients/myscale/search.py.query_cache` | MyScale Searcher 结果缓存变体 |
| `engine/clients/myscale/search.py.query_plan_cache` | MyScale Searcher 计划缓存变体 |

### B. 配置文件备份与变体

| 文件 | 说明 |
|------|------|
| `configurations/polardb-sift-128.json.bak` | PolarDB 配置备份（旧版） |
| `configurations/pgvector-sift-128.json.bak` | pgvector 配置备份（旧版） |
| `configurations/clickhouse-hnsw.json.bak` | ClickHouse 配置备份（旧版） |
| `configurations/myscale-hnsw-laion-768-1m-ip.json.bak` | MyScale 配置备份（旧版） |
| `configurations/pgvector-hnsw-laion-768-1m-ip.json.bak` | pgvector 配置备份（旧版） |
| `configurations/myscale-hnsw-laion-768-1m-ip.json.query_cache` | MyScale 配置·结果缓存变体 |
| `configurations/myscale-hnsw-laion-768-1m-ip.json.query_plan_cache` | MyScale 配置·计划缓存变体 |

### C. 调试/探针输出

日志分析与解析流程产生的中间输出，供复现调试。

| 文件 | 说明 |
|------|------|
| `benchmark/probe_tmp.py` | 临时探针脚本（读取 `analyze_polardb_log.py` 中间结果） |
| `benchmark/probe_res.txt` | 探针输出·搜索结果 |
| `benchmark/probe_stdout.txt` | 探针输出·标准输出 |
| `benchmark/probe_sql.txt` | 探针输出·SQL 语句 |
| `benchmark/probe_count.txt` | 探针输出·数量统计 |
| `benchmark/probe_trans.txt` | 探针输出·事务日志 |
| `benchmark/dbg_text_pase_op_strtof.log` | PASE `strtof` 调试日志 |

### D. 未使用的临时数据集

| 文件 | 说明 |
|------|------|
| `datasets/downloads/hackernews_part_1_of_1.parquet` | 未被任何代码引用的临时数据文件（若已删除则忽略） |

> 清理建议：确认无需再调试后，可删除 C 组探针文件与 D 组临时数据集；A/B 组变体与备份可在核对
> 与正式配置差异后一并移除。

## 附录 B：按测试脚本分类可清除的文件

依据"只保留两类测试脚本 —— ① Python（建库 + 建索引 + QPS + 召回率）；② sh（向量检索 QPS）"
的原则，结合各引擎的 QPS 测量口径归属，以下遗留脚本不再需要，可清除：

### B-1. 可清除（pgbench 脚本）

pgvector 与 PolarDB 的 QPS 已改由 Python 承担（pgbench 无法测量查询计划缓存的真实 QPS），
因此面向 pgbench 的 sh 脚本失去存在意义。

| 文件 | 类别 | 清除原因 |
|------|------|----------|
| `bash-test/pgvector-benchmark.sh` | sh · pgbench | pgvector 的 QPS 改由 Python（run.py / query_forms.py）测量 |
| `bash-test/polardb-pase-query-forms-benchmark.sh` | sh · pgbench | PolarDB PASE 的 QPS 改由 Python（run.py / query_forms.py）测量 |

连带清理：`bash-test/sql-bench/` 中仅为 pgbench 生成的 SQL 文件（ClickHouse/MyScale 所用 SQL
仍由 `generate-sql-files.sh` 生成，不受影响）。

### B-2. 保留（两类脚本）

#### Python 类

| 文件 | 保留原因 |
|------|----------|
| `benchmark/run.py` | 建库/建索引/导入 + QPS 与召回率；pgvector、PolarDB 的 QPS 权威口径 |
| `benchmark/query_forms.py` | SQL 形式 × 缓存档位 QPS；承载 pgvector / PolarDB 的计划缓存测量 |
| `benchmark/generate_sql_files.py` | 生成各引擎 SQL 基准文件 |
| `benchmark/dataset.py`、`config_read.py`、`dataset_config.py`、`engine/**`、`dataset_reader/**` | Python 类的支撑模块 |

#### sh 类

| 文件 | 保留原因 |
|------|----------|
| `bash-test/clickhouse-benchmark.sh` | ClickHouse / MyScale 的原生 QPS 压测（Python 开销大、数值偏低） |
| `bash-test/generate-sql-files.sh` | 为 clickhouse-benchmark 采样生成 SQL |
| `bash-test/verify_string_to_vector_engine.py` | MyScale 并行 string→vector 端到端校验（开发校验工具） |

---

## 10. 打包

使用 Nuitka 将 `run.py` 编译为独立可执行 `myscale-bench`：

- `build_nuitka.sh`（manylinux_2_28_x86_64）→ `dist/myscale-bench-linux-x86_64.tar.gz`
- `build_nuitka_arm.sh`（Ubuntu 22.04 / ARM64）→ `dist-arm/myscale-bench-linux-aarch64.tar.gz`

打包后通过 `get_root_dir()` 的启发式规则（Nuitka onefile 环境变量 / `argv[0]` / 工作目录）
定位 `datasets/`、`configurations/`、`results/` 目录，保证编译产物可移植运行。