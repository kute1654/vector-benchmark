# vector-benchmark

统一的向量数据库性能基准测试框架，支持 **pgvector**、**ClickHouse**、**MyScale** 和 **PolarDB-pg (PASE)**。
架构与设计说明见 [设计文档](benchmark/docs/design.md)。

语言：中文 | [English](README.md)

## 目录结构

```
vector-benchmark/
├── benchmark/                          # Python 基准测试框架（主包）
│   ├── run.py                          #   主基准入口（MyscaleBench，Nuitka 编译目标）
│   ├── query_forms.py                  #   SQL 形式 × 缓存档位 QPS 对比
│   ├── generate_sql_files.py           #   生成 bash 测试用 SQL 文件
│   ├── analyze_pgvector_log.py         #   pgvector 服务端日志分析
│   ├── analyze_myscale_log.py          #   MyScale 服务端日志分析
│   ├── analyze_polardb_log.py          #   PolarDB PASE 服务端日志分析
│   ├── config_read.py                  #   配置读取（引擎/数据集）
│   ├── dataset.py                      #   数据集管理（下载/读取器分派）
│   ├── dataset_config.py               #   数据集配置模型
│   ├── cli_output.py                   #   CLI 输出格式化
│   ├── __init__.py                     #   包导出 + 目录解析
│   ├── requirements.txt                #   Python 依赖
│   ├── datasets/                       #   数据集
│   │   ├── datasets.json               #     数据集注册表
│   │   └── downloads/                  #     下载的数据文件
│   ├── configurations/                 #   实验配置（JSON 数组）
│   │   ├── config.json                 #     默认实验配置
│   │   └── templates/                  #     配置模板（复制到 configurations/ 使用）
│   ├── engine/                         #   引擎抽象层
│   │   ├── base.py                     #     BaseEngine（QPS/召回率测量）
│   │   ├── pgvector.py / polardb.py / clickhouse.py / myscale.py
│   │   ├── base_client/                #     客户端基类
│   │   │   ├── client.py               #       BaseClient（编排上传/建索引/查询）
│   │   │   ├── configure.py            #       BaseConfigurator（建表/建索引）
│   │   │   ├── search.py               #       BaseSearcher（查询 + session_settings）
│   │   │   ├── upload.py               #       BaseUploader（数据导入）
│   │   │   ├── parser.py               #       BaseClientParser（结果解析）
│   │   │   └── distances.py / utils.py
│   │   └── clients/                    #     各数据库客户端实现
│   │       ├── client_factory.py       #       客户端工厂（笛卡尔积展开）
│   │       └── pgvector/ clickhouse/ myscale/ polardb/
│   ├── dataset_reader/                 #   数据集读取器
│   │   ├── base_reader.py              #     读取器基类
│   │   ├── ann_h5_reader.py            #     ann-benchmarks HDF5（h5）
│   │   ├── ann_compound_reader.py      #     复合归档（tar）
│   │   ├── json_reader.py              #     JSONL（jsonl）
│   │   ├── gz_tsv_reader.py            #     BeIR 文本检索（gz_tsv）
│   │   └── utils.py
│   ├── results/                        #   测试结果（CSV/JSON）
│   ├── docs/                           #   框架文档
│   │   ├── design.md                   #     设计文档
│   │   └── dataset.md                  #     数据集格式说明
│   └── bash-test/                      #   sh 类 QPS 测试（ClickHouse / MyScale）
│       ├── clickhouse-benchmark.sh     #   ClickHouse/MyScale 原生 QPS 压测
│       ├── pgvector-benchmark.sh       #   [可清除] pgvector（pgbench，见 design.md 附录 B）
│       ├── polardb-pase-query-forms-benchmark.sh # [可清除] PolarDB PASE（pgbench，见 design.md 附录 B）
│       ├── generate-sql-files.sh       #   采样生成测试 SQL
│       ├── verify_string_to_vector_engine.py   #  MyScale string→vector 校验
│       └── sql-bench/                  #   生成的 SQL 文件
│
├── docs/                               # 查询形式 / 计划缓存分析文档
├── results/                            # Shell 基准结果汇总 CSV
├── README.md                           # 英文版
├── README.zh-CN.md                     # 中文版
├── build_nuitka.sh                     # Nuitka 构建脚本（x86_64）
└── build_nuitka_arm.sh                 # Nuitka 构建脚本（ARM64）
```

## 快速开始

### 1. 安装依赖

```bash
cd vector-benchmark/benchmark
pip install -r requirements.txt
```

### 2. 准备数据集

将 HDF5 数据集文件放到 `benchmark/datasets/downloads/` 目录下，并在 `benchmark/datasets/datasets.json` 中配置对应条目。

```bash
# 示例：下载 ann-benchmarks 数据集
cd benchmark/datasets/downloads
wget https://ann-benchmarks.com/sift-128-euclidean.hdf5
wget https://ann-benchmarks.com/gist-960-euclidean.hdf5
```

### 3. 配置实验

实验配置文件为 JSON 数组，存放在 `configurations/` 目录下。每个元素定义一次实验：

```json
[
  {
    "name": "pgvector-sift-128-euclidean",
    "engine": "pgvector",
    "dataset": "sift-128-euclidean",
    "connection_params": {
      "host": "127.0.0.1",
      "port": 5432,
      "user": "postgres",
      "password": "123456",
      "database": "postgres",
      "table": "benchmark_sift_128"
    },
    "upload_params": {
      "index_type": "hnsw",
      "index_params": { "m": 16, "ef_construction": 200 },
      "parallel": 16,
      "batch_size": 256,
      "search_number": 10
    },
    "search_params": {
      "parallel": [1, 4, 8],
      "top": 10,
      "test_duration": 20,
      "params": { "ef_s": [40, 100, 200] },
      "session_settings": {
        "enable_seqscan": "off",
        "ivfflat.probes": 10
      }
    }
  }
]
```

### 4. 运行基准测试

```bash
cd benchmark

# 运行所有匹配通配符的配置
python run.py --engines "pgvector-*" --host 127.0.0.1 --port 5432

# 运行指定配置
python run.py --engines pgvector-sift-128-euclidean

# 跳过数据上传（仅运行查询）
python run.py --engines pgvector-sift-128-euclidean --skip-upload

# 仅测试召回率（不测试 QPS）
python run.py --engines pgvector-sift-128-euclidean --recall-only
```

### 5. 比较全部 SQL 形式与缓存配置

为四个目标表完成建表和建索引后，可以运行一个矩阵覆盖所有已注册的查询形式。需要先准备一个
JSON 目标配置文件（包含目标表的连接参数与表名；若包含多个引擎，建议一次只测一个，参考
`query_forms.py` 的 `--config` 参数说明），例如：

```bash
cd ..
python -m benchmark query_forms \
  --config query-forms/targets.json \
  --query-count 100 \
  --concurrency 4 \
  --duration 20 \
  --output results/query-forms-qps.csv
```

输出按 `数据库 × 缓存配置 × SQL 类型` 写入 CSV。ClickHouse/MyScale 使用服务端查询结果缓存和向量查询计划缓存；PostgreSQL 兼容数据库的计划缓存场景使用 prepared statement，结果缓存场景使用每个连接内的有界结果缓存。

## 配置文件格式

### 顶层字段

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 是 | 实验唯一标识（通过 `--engines` 使用） |
| `engine` | string | 是 | 引擎类型：`pgvector`、`clickhouse`、`myscale`、`polardb` |
| `dataset` | string | 是 | 数据集名称（对应 `datasets.json` 的 `name` 字段） |
| `connection_params` | object | 是 | 数据库连接参数 |
| `upload_params` | object | 是 | 建表、建索引、导入数据参数 |
| `search_params` | object | 是 | 查询参数 |

### connection_params

数据库连接配置。不同引擎的默认值：

| 引擎 | 默认端口 | 默认用户 | 协议 |
|------|----------|----------|------|
| pgvector | 5432 | postgres | 无 |
| ClickHouse | 9000 | default | tcp |
| MyScale | 9000 | default | tcp |
| PolarDB-pg | 5433 | postgres | 无 |

```json
"connection_params": {
  "host": "127.0.0.1",
  "port": 5432,
  "user": "postgres",
  "password": "123456",
  "database": "postgres",
  "table": "benchmark_sift_128",
  "protocol": "tcp"
}
```

### upload_params

建表、建索引、导入数据的参数：

| 参数 | 类型 | 说明 |
|------|------|------|
| `index_type` | string | 索引算法（如 `hnsw`、`ivfflat`、`HNSWFLAT`、`MSTG`） |
| `index_params` | object | 索引相关参数（如 `m`、`ef_construction`、`ef_c`） |
| `parallel` | int | 数据导入并发线程数 |
| `batch_size` | int | 每批写入的行数 |
| `search_number` | int | 导入后查询轮数 |
| `use_cache` | int[] | 启用 prepared statement 模式（pgvector/PolarDB） |
| `use_query_cache` | int[] | 启用查询结果缓存，并与查询计划缓存组合测试 |
| `optimize` | bool | 导入后优化表（ClickHouse） |
| `enable_query_plan_cache` | int[] | 启用查询计划缓存（MyScale） |

### search_params

查询基准测试阶段的参数：

| 参数 | 类型 | 说明 |
|------|------|------|
| `parallel` | int[] | 并发查询客户端数 |
| `top` | int | 返回最近邻数（K 值） |
| `test_duration` | int | 测试持续时间（秒） |
| `params` | object | 引擎特定查询参数（如 `ef_s`） |
| `session_settings` | object | 每次查询前执行的 SET 命令 |

### 数组参数自动展开

配置文件中的数组值会通过笛卡尔积自动展开为多组测试：

```json
"parallel": [1, 4, 8],
"params": { "ef_s": [40, 100, 200] }
```

将生成 3 × 3 = 9 组测试组合。

### session_settings

在 `search_params` 下配置 `session_settings`，可在每次查询前执行 SET 命令来调整数据库参数，无需重新连接。

**pgvector 示例：**
```json
"session_settings": {
  "enable_seqscan": "off",
  "ivfflat.probes": 10
}
```

**ClickHouse 示例：**
```json
"session_settings": {
  "hnsw_candidate_list_size_for_search": 100
}
```

**PolarDB 示例：**
```json
"session_settings": {
  "enable_seqscan": "off",
  "pase.enable": "on"
}
```

## 支持的数据库引擎

### pgvector

| 属性 | 值 |
|------|-----|
| 索引类型 | `hnsw`、`ivfflat` |
| 距离函数 | `l2`、`ip`、`cosine` |
| 默认端口 | 5432 |
| 连接方式 | psycopg2 |
| QPS 测量 | Python（run.py / query_forms.py） |

### ClickHouse

| 属性 | 值 |
|------|-----|
| 索引类型 | `HNSWFLAT`、`HNSW`、`VECTOR_SIMILARITY`、`ANNOY`、`USEARCH`、`FLAT` |
| 距离函数 | `l2`、`dot`、`cosine` |
| 默认端口 | 9000 (tcp) / 8123 (http) |
| 连接方式 | clickhouse-driver (tcp) / clickhouse-connect (http) |
| QPS 测量 | clickhouse-benchmark.sh |

### MyScale

| 属性 | 值 |
|------|-----|
| 索引类型 | `HNSWFLAT`、`MSTG`、`MSRQ` |
| 距离函数 | `l2`、`dot`、`cosine` |
| 默认端口 | 9000 (tcp) / 8123 (http) |
| 连接方式 | clickhouse-driver (tcp) / clickhouse-connect (http) |
| QPS 测量 | clickhouse-benchmark.sh |

### PolarDB-pg (PASE)

| 属性 | 值 |
|------|-----|
| 索引类型 | `hnsw`（pase_hnsw）、`ivfflat`（pase_ivfflat） |
| 距离函数 | `l2`、`ip`、`cosine` |
| 默认端口 | 5433 |
| 连接方式 | psycopg2 |
| QPS 测量 | Python（run.py / query_forms.py） |

## 测试模式

框架只保留**两类**测试脚本：

### Python 测试（建库 / 建索引 + QPS + 召回率）

由 `run.py`（及 `query_forms.py`）驱动，负责创建数据库、建表、建索引、导入数据，
并测量 QPS 与召回率：

- **duration 模式**：在指定时间内持续发送查询，测量 QPS
- **count 模式**：执行固定数量查询，测量延迟和召回率
- **recall-only 模式**：单进程遍历所有测试查询，仅输出召回率指标

**pgvector、PolarDB-pg (PASE)** 的 QPS 以本路径（Python）为权威口径——因为 pgbench 无法测试
查询计划缓存的真实 QPS，而 Python 客户端能精确装配会话级 SET 与 prepared statement。

### sh 测试（ClickHouse / MyScale 的向量检索 QPS）

使用数据库原生工具 `clickhouse-benchmark` 直接测量向量检索 QPS。Python 驱动测量时会因脚本额外
开销大而拉低 QPS 数值，因此 **ClickHouse / MyScale** 的 QPS 以本路径为权威口径：

```bash
cd benchmark/bash-test

# ClickHouse
./clickhouse-benchmark.sh benchmark_sift_128

# MyScale
ENGINE=myscale ./clickhouse-benchmark.sh benchmark_sift_128
```

> 先通过 `python run.py`（上传阶段）完成目标表的建表、建索引与数据导入，再执行上述 sh 脚本。
> 脚本用法详见 `clickhouse-benchmark.sh` 头部注释。

## Shell 建表与索引操作

建表、建索引与数据导入统一由 `python run.py` 的上传阶段完成（见 `upload_params` 配置）。
也可按如下方式手动创建索引：

```sql
-- pgvector HNSW 索引
CREATE INDEX ON benchmark_sift_128_1k USING hnsw (vector vector_l2_ops)
    WITH (m = 16, ef_construction = 200);

-- pgvector IVFFlat 索引
CREATE INDEX ON benchmark_sift_128_1k USING ivfflat (vector vector_l2_ops)
    WITH (lists = 100);

-- PolarDB PASE HNSW 索引
CREATE INDEX ON benchmark_sift_128_1k USING pase_hnsw (vector)
    WITH (dim = 128, base_nb_num = 16, ef_build = 40, ef_search = 100, base64_encoded = 0);
```

## 查看结果

```bash
# 查看 CSV 汇总结果
cd benchmark/results
cat benchmark_results.csv | column -t -s,

# 查看 JSON 详细结果
ls -la *search*.json
cat pgvector-sift-128-euclidean-search-*.json | python -m json.tool

# 查看 Shell 测试结果
cat results/vector-query-forms-pgvector-results.csv
```

## 常用操作速查

| 操作 | 命令 |
|------|------|
| 查看所有表 | `psql -h 127.0.0.1 -p 5432 -U postgres -c "\dt"` |
| 查看表行数 | `psql -h 127.0.0.1 -p 5432 -U postgres -c "SELECT count(*) FROM benchmark_sift_128_1k"` |
| 查看索引 | `psql -h 127.0.0.1 -p 5432 -U postgres -c "\di benchmark_sift_128_1k*"` |
| 查看执行计划 | `psql -h 127.0.0.1 -p 5432 -U postgres -c "EXPLAIN (ANALYZE, BUFFERS) SELECT ..."` |
| 删除表 | `psql -h 127.0.0.1 -p 5432 -U postgres -c "DROP TABLE IF EXISTS benchmark_sift_128_1k"` |
| 查看 ClickHouse 表 | `clickhouse-client -q "SHOW TABLES"` |
| 查看 ClickHouse 索引 | `clickhouse-client -q "SELECT name, type FROM system.data_skipping_indices WHERE table='benchmark_sift_128'"` |

## 故障排查

### 索引未生效

检查执行计划，确认是否使用了索引扫描：

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, (vector <-> '[1,2,3,...]'::vector) AS dis
FROM benchmark_sift_128_1k
ORDER BY dis ASC LIMIT 10;
```

如果看到 `Seq Scan` 而非 `Index Scan`，需要：
- 确保索引已创建
- 设置 `SET enable_seqscan = off`
- 或通过 `session_settings` 配置

### QPS 过低

- 检查 `ef_search` 参数是否过大
- 确认并发数合理
- 使用 Shell 脚本测试排除 Python 网络开销影响

### 连接失败

- 检查 `connection_params` 中的 host/port 配置
- 确认数据库已启动
- 对于 ClickHouse，检查 `protocol` 配置（tcp/http）

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--engines` | `*` | 实验名称（通配符匹配 `configurations/*.json` 的 `name` 字段） |
| `--datasets` | `*` | 数据集名称（通配符匹配 `datasets.json` 的 `name` 字段） |
| `--host` | `127.0.0.1` | 数据库服务器地址 |
| `--port` | `9000` | 数据库服务器端口 |
| `--skip-upload` | `false` | 跳过数据上传和索引构建 |
| `--recall-only` | `false` | 仅运行召回率/指标评估，遍历所有测试查询 |

## 结果输出

结果保存在 `benchmark/results/` 目录下：
- `benchmark_results.csv` — 汇总的基准测试结果
- `{experiment_name}-search-{id}-{timestamp}.json` — 每个实验的详细结果

## 打包

使用 Nuitka 构建独立可执行文件：

- **x86_64**（manylinux_2_28_x86_64）：`./build_nuitka.sh` → `dist/myscale-bench-linux-x86_64.tar.gz`
- **ARM64**（Ubuntu 22.04）：`./build_nuitka_arm.sh` → `dist-arm/myscale-bench-linux-aarch64.tar.gz`

最低 GLIBC 版本要求：
- x86_64：GLIBC 2.14
- ARM64：GLIBC 2.34

## 内存建议

- 对于 laion-768-1m-ip 数据集，建议至少 4GB 内存。
- 若上传阶段发生 OOM，请调小 `upload_params.parallel` 和 `upload_params.batch_size`。
- 若查询阶段发生 OOM，请调小 `search_params.parallel` 和 `datasets.json` 中对应数据集的 `queries_pool_size`。