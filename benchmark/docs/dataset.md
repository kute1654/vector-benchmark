# 允许的数据集格式

基准测试通过 `datasets/datasets.json` 读取数据集配置，并根据 `type` 选择不同的数据读取器：
- `h5`：向量检索（ANN）数据集
- `gz_tsv`：文本检索 / 混合检索（BeIR 风格）数据集

## 1. vector_search dataset

### 1.1 h5

#### 1.1.1 datasets.json 示例

```json
{
  "name": "laion-768-1m-ip",
  "result_group": "vector_search",
  "vector_size": 768,
  "vector_count": 1000000,
  "queries_pool_size": 1000,
  "distance": "dot",
  "type": "h5",
  "group_name": "laion-768-1m-ip",
  "tag": "no-filter",
  "path": "downloaded/laion-1m-test-ip.hdf5",
  "link": ""
}
```

#### 1.1.2 hdf5 文件示例

HDF5 文件必须在根路径 `/` 下包含以下数据集。这些数据集共同定义了底库向量、查询向量以及预计算的基准结果。

| 数据集名称 | 描述 | 维度（Dimensions） | 数据类型（HDF5 Type） | 必选 |
|---|---|---|---|---|
| `train` | 底库向量：用于插入数据库/构建索引的原始向量数据。 | `(N_train, Dim)` | `H5T_IEEE_F32LE`（float32） | 是 |
| `test` | 查询向量：用于执行搜索测试的向量。 | `(N_test, Dim)` | `H5T_IEEE_F32LE`（float32） | 是 |
| `neighbors` | 最近邻索引：test 中每个向量对应的 K 个最近邻在 train 中的行索引。 | `(N_test, K)` | `H5T_STD_U32LE`（uint32） | 是 |
| `distances` | 最近邻距离：test 中每个向量对应的 K 个最近邻的精确距离值。 | `(N_test, K)` | `H5T_IEEE_F32LE`（float32） | 否* |

```bash
h5dump -H laion-1m-test-ip.hdf5
```

```text
HDF5 "laion-1m-test-ip.hdf5" {
GROUP "/" {
   DATASET "distances" {
      DATATYPE  H5T_IEEE_F32LE
      DATASPACE  SIMPLE { ( 10000, 5000 ) / ( 10000, 5000 ) }
   }
   DATASET "neighbors" {
      DATATYPE  H5T_STD_U32LE
      DATASPACE  SIMPLE { ( 10000, 5000 ) / ( 10000, 5000 ) }
   }
   DATASET "test" {
      DATATYPE  H5T_IEEE_F32LE
      DATASPACE  SIMPLE { ( 10000, 768 ) / ( 10000, 768 ) }
   }
   DATASET "train" {
      DATATYPE  H5T_IEEE_F32LE
      DATASPACE  SIMPLE { ( 1000000, 768 ) / ( 1000000, 768 ) }
   }
}
}
```

在测试时，会自动选择 `train` 作为底库向量建表。然后再使用 `test` 作为查询向量，并使用 `neighbors` 计算召回率。

## 2. text_search dataset

### 2.1 gz_tsv

#### 2.1.1 数据集介绍

测试数据集为 BeIR/quora

数据集链接：
- `https://huggingface.co/datasets/BeIR/quora-qrels`
- `https://huggingface.co/datasets/BeIR/quora`

下载上述链接的三个文件：`corpus.jsonl.gz`、`queries.jsonl.gz`、`dev.tsv`。即可进行测试。

#### 2.1.2 datasets.json 示例

```json
{
  "name": "quora-mini-text-dev",
  "result_group": "text_search",
  "corpus_count": 1000,
  "queries_pool_size": 100,
  "type": "gz_tsv",
  "path": "downloaded/quora-mini",
  "corpus_file": "corpus.jsonl.gz",
  "queries_file": "queries.jsonl.gz",
  "qrels_file": "dev.tsv",
  "query_cols": ["body"],
  "schema": { "body": "string" }
}
```

字段含义与约束
- `name`：数据集唯一名称（用于 `--datasets` 选择）
- `result_group`：文本搜索必须为 `text_search`
- `type`：数据集类型，必须为 `gz_tsv`
- `path`：数据集目录（相对 `myscale-bench/datasets/`）
- `corpus_file`：语料库文件名（相对 `path`）
- `queries_file`：查询文件名（相对 `path`）
- `qrels_file`：查询相关性文件名（相对 `path`）
- `corpus_count`：语料库文档总数
- `queries_pool_size`：实际参与压测的查询数量上限
- `query_cols`：要查询的列，将来会构造全文索引
  - 约束：列名必须在 `schema` 中出现，且类型必须为 `string`
- `schema`：写入表的字段声明（必填）
  - 当前版本最小要求：至少包含 `query_cols` 覆盖到的列
  - 类型约定：`string` 表示文本列

#### 2.1.3 文件示例

##### 2.1.3.1 corpus.jsonl.gz（语料库）文件格式

- 文件类型：gzip 压缩的 JSON Lines
- 每行一个 JSON 对象，表示一条文档
- 必须字段：
  - `id`：整数，文档唯一 id（需与 `qrels_file` 的 `corpus-id` 对齐）
  - `body`：字符串，文档正文（字段名必须与 `schema/query_cols` 里的列名一致；本例为 `body`）
- 可选字段：
  - 任何其它字段均可存在（是否写入表由 `schema` 决定）

示例行：

```json
{"id": 0, "body": "this is the document text"}
```

##### 2.1.3.2 queries.jsonl.gz（查询）文件格式

- 文件类型：gzip 压缩的 JSON Lines
- 每行一个 JSON 对象，表示一条查询
- 必须字段：
  - `id`：整数，query 唯一 id（需与 `qrels_file` 的 `query-id` 对齐）
  - `text`：字符串，query 文本

示例行：

```json
{"id": 1185869, "text": "what is ... ?"}
```

##### 2.1.3.3 dev.tsv（qrels / relevance）文件格式

- 文件类型：TSV（tab 分隔文本）
- 第一行表头必须为：

```text
query-id<TAB>corpus-id<TAB>score
```

- 每行一条标注：
  - `query-id`：查询 id（整数）
  - `corpus-id`：文档 id（整数）
  - `score`：相关性分数（数值；`score > 0` 视为相关）

示例：

```text
query-id    corpus-id    score
1185869     0            1
1185868     16           1
```

## 3. hybrid_search dataset

### 3.1 gz_tsv

#### 3.1.1 datasets.json 示例

```json
{
  "name": "quora-mini-hybrid-dev",
  "result_group": "hybrid_search",
  "vector_size": 384,
  "corpus_count": 1000,
  "queries_pool_size": 100,
  "distance": "cosine",
  "type": "gz_tsv",
  "path": "downloaded/quora-mini",
  "corpus_file": "corpus.jsonl.gz",
  "queries_file": "queries.jsonl.gz",
  "qrels_file": "dev.tsv",
  "corpus_embedding_file": "corpus_vectors.jsonl.gz",
  "queries_embedding_file": "queries_vectors.jsonl.gz",
  "query_cols": ["body"],
  "schema": { "body": "string" }
}
```

字段含义与约束
- `name`：数据集唯一名称（用于 `--datasets` 选择）
- `result_group`：混合搜索必须为 `hybrid_search`
- `type`：数据集类型，必须为 `gz_tsv`
- `path`：数据集目录（相对 `myscale-bench/datasets/`）
- `corpus_file`：语料库文件名（相对 `path`）
- `queries_file`：查询文件名（相对 `path`）
- `corpus_embedding_file`：语料库对应的 embedding 文件名（相对 `path`）
- `queries_embedding_file`：查询对应的 embedding 文件名（相对 `path`）
- `qrels_file`：查询相关性文件名（相对 `path`）
- `vector_size`：向量维度（必须与 embedding 文件内向量维度一致）
- `corpus_count`：语料库文档总数
- `queries_pool_size`：实际参与压测的查询数量上限
- `query_cols`：要查询的列，将来会构造全文索引
  - 约束：列名必须在 `schema` 中出现，且类型必须为 `string`
- `schema`：写入表的字段声明（必填）
  - 当前版本最小要求：至少包含 `query_cols` 覆盖到的列
  - 类型约定：`string` 表示文本列

#### 3.1.2 文件示例

##### 3.1.2.1 corpus.jsonl.gz（语料库）文件格式

- 文件类型：gzip 压缩的 JSON Lines
- 每行一个 JSON 对象，表示一条文档
- 必须字段：
  - `id`：整数，文档唯一 id（需与 `qrels_file` 的 `corpus-id` 对齐）
  - `body`：字符串，文档正文（字段名必须与 `schema/query_cols` 里的列名一致；本例为 `body`）
- 可选字段：
  - 任何其它字段均可存在（是否写入表由 `schema` 决定）

示例行：

```json
{"id": 0, "body": "this is the document text"}
```

##### 3.1.2.2 queries.jsonl.gz（查询）文件格式

- 文件类型：gzip 压缩的 JSON Lines
- 每行一个 JSON 对象，表示一条查询
- 必须字段：
  - `id`：整数，query 唯一 id（需与 `qrels_file` 的 `query-id` 对齐）
  - `text`：字符串，query 文本

示例行：

```json
{"id": 1185869, "text": "what is ... ?"}
```

##### 3.1.2.3 dev.tsv（相关性关系文件）文件格式

- 文件类型：TSV（tab 分隔文本）
- 第一行表头必须为：

```text
query-id<TAB>corpus-id<TAB>score
```

- 每行一条标注：
  - `query-id`：查询 id（整数）
  - `corpus-id`：文档 id（整数）
  - `score`：相关性分数（数值；`score > 0` 视为相关）

示例：

```text
query-id    corpus-id    score
1185869     0            1
1185868     16           1
```

##### 3.1.2.4 corpus_embedding_file（语料向量）文件格式

- 文件类型：gzip 压缩的 JSON Lines
- 每行一个 JSON 对象，表示一条文档向量
- 必须字段：
  - `id`：整数，文档唯一 id（需与 `corpus.jsonl.gz` 的 `id` 对齐）
  - `vector`：浮点数组，文档 embedding 向量
- 说明：
  - 向量维度必须与 `datasets.json` 中该数据集的 `vector_size` 一致

示例行：

```json
{"id": 0, "vector": [0.12, -0.33, 0.98]}
```

##### 3.1.2.5 queries_embedding_file（查询向量）文件格式

- 文件类型：gzip 压缩的 JSON Lines
- 每行一个 JSON 对象，表示一条查询向量
- 必须字段：
  - `id`：整数，query 唯一 id（需与 `queries.jsonl.gz` 的 `id` 对齐）
  - `vector`：浮点数组，查询 embedding 向量
- 说明：
  - 向量维度必须与 `datasets.json` 中该数据集的 `vector_size` 一致

示例行：

```json
{"id": 1185869, "vector": [0.07, 0.41, -0.26]}
```
