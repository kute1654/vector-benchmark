# MyScale Benchmark

用于快速性能验证（POC）的基准测试工具

语言：中文 | [English](README.md)

## 快速开始（向量搜索）

### 1）解压

```bash
tar -xzf myscale-bench-linux-x86_64.tar.gz
cd myscale-bench
```

### 2）准备数据集

将数据集文件放到 `datasets/` 目录下，并在 `datasets/datasets.json` 中为对应数据集条目配置正确的 `path`。

说明：
- 如果 `link` 不可用或为空，需要你自行下载数据集，并将 `path` 指向数据集文件所在位置。

### 3）配置实验

实验配置文件为 JSON，存放在以下目录：
- `configurations/templates/`：已提供 MSTG、HNSW 等多种测试配置模板，可用于参考或复制后修改。
- `configurations/`：用于放置你要实际运行的测试配置文件（将模板复制/修改后放到这里）。

程序只会读取 `configurations/` 目录下的配置文件；`configurations/templates/` 下的模板不会被自动搜索/加载。

数据集名称说明：
- 实验配置文件中使用到的数据集名称，来自 `datasets/datasets.json` 中对应条目的 `name` 字段。

#### 实验配置 JSON 参数

示例模板：
- `myscale-bench/configurations/templates/myscale-hnsw-laion-768-5m-ip.json`

该文件是一个 JSON 数组；数组中的每个元素表示一次实验配置。

参数介绍：

命令行：
- `--engines`：`configurations/` 下实验配置 JSON 文件中的 `name` 字段。
- `--datasets`：要执行的数据集名称，会执行 `configurations/` 下所有 `dataset` 为该名称的配置。
- *`--host`*：IP，默认值 `127.0.0.1`。
- *`--port`*：端口号，默认值 `9000`。
- *`--skip-upload`*：跳过数据上传和索引构建过程，默认值 `False`。
 - *`--recall-only`*（可选）：只测试数据集的评测指标（例如 recall/MRR）。配合 `--engines` 或 `--datasets` 使用时，会在单进程下，对每个匹配配置在所有测试 query 上跑一遍搜索并输出 metric，忽略 `datasets/datasets.json` 中该数据集的 `queries_pool_size` 限制。是否重建/重传数据仍由 `--skip-upload` 控制。

配置文件（实验 JSON）：
- `name`：测试配置的唯一标识名称。
- `dataset`：指定测试所使用的源数据集名称。
- `upload_params`：
  - `index_type`：向量索引算法类型（如 `HNSWFLAT`, `MSTG`）。
  - *`index_params`*：索引构建参数（以 HNSWFLAT 为例，其他索引类型有各自的参数）：
    - *`m`*：HNSW 图中节点的最大边数，影响召回率与内存占用。
    - *`ef_c`*：索引构建时的动态候选列表大小，影响索引质量与构建耗时。
  - *`parallel`*：数据导入时的并发线程数。默认值 `16`。
  - *`batch_size`*：单次写入请求包含的数据行数。默认值 `256`。对于数据量较大的数据集，可以适当调大 `batch_size`（并配合调整 `parallel`），以减少单表产生的 part 数量，在提升导入速度的同时降低后台合并压力。
  - *`mstg_disk_mode`*：MSTG 索引类型可选参数，设置为 `1` 时使用磁盘模式。默认值 `0`。
- `search_params`：
  - `parallel`：压力测试时的并发查询客户端数。
  - `top`：每次搜索返回的最近邻结果数量（K 值）。
  - `test_duration`：压力测试的总持续时间（秒）。
  - `params`：搜索参数（如 `alpha`、`ef_s` 等）。当出现多个参数时，会按笛卡尔积组合展开执行。
- *`connection_params`*：
  - *`protocol`*：数据库通讯协议（如 TCP）。默认值 `TCP`。
  - *`user`*：数据库访问用户名。默认值 `default`。
  - *`password`*：数据库访问密码。默认值 `""`。
  - *`table`*：表名，默认值 `Benchmark`。

#### 预热阶段说明

在正式进入 SEARCH 阶段之前，程序会先执行一个简短的预热（warmup）：
- 会从展开后的所有 `search_params` 组合中，选择最后一组参数；
- 使用与正式测试相同的查询逻辑，运行一次预热搜索，预热时间大约为 2 秒。

对于体量特别大的数据集，第一次查询往往需要将向量索引或相关数据加载到内存，这一步可能远大于 2 秒。此时：
- 预热阶段看起来会“卡住”，实际上是在等待数据库完成数据加载；
- 基准工具会持续等待数据库返回结果，而不是立即失败；
- 预热阶段的单次查询默认最长等待时间为 1800 秒。

如果需要修改预热阶段的最长等待时间，可以在实验配置的 `connection_params` 中设置：

```json
"connection_params": {
  "warmup_timeout_s": 1800
}
```

该参数仅影响预热阶段的等待上限，正式 SEARCH 阶段仍然使用常规的超时设置。

### 4）运行

```bash
./myscale-bench --engines myscale-mstg-laion-768-1m-ip --host 127.0.0.1 --port 9000
```

可选覆盖项：
- 使用 `--host`（以及 `--port`）覆盖服务端地址。
- 可使用 `--datasets` 选择某一数据集对应的所有测试配置并全部执行。
- `--engines` 与 `--datasets` 后面只能填写一个参数，但支持通配符以选中多个测试配置（例如 `--datasets "laion-768-*-ip"`）。
- 在 `--engines` 或 `--datasets` 中使用通配符时，建议使用双引号包裹参数值（例如 `--engines "myscale-*"`、`--datasets "laion-768-*-ip"`），避免在某些 shell（如 `zsh`）中通配符被 shell 抢先展开。
- `--engines` 后面填写的是 `configurations/` 下实验配置 JSON 文件中的 `name` 字段。
- 如需跳过“数据导入 + 索引构建”两个阶段：命令行加 `--skip-upload`。

### 5）结果输出

结果会以 JSON 文件形式写入 `results/` 目录。

## 文本搜索

示例模板：
- `myscale-bench/configurations/templates/myscale-text-quora-mini-dev.json`

评测指标：
- MRR

参数介绍：
配置文件（实验 JSON）：
- `name`：测试配置的唯一标识名称。
- `dataset`：指定测试所使用的源数据集名称。
- `upload_params`：
  - `fts_idx_cols`：需要建立全文索引的列名列表（示例里是 `["body"]`）。
  - `fts_idx_params`：全文索引参数（例如 tokenizer、停用词等）。
  - *`parallel`*：数据导入时的并发线程数。默认值 `16`。
  - *`batch_size`*：单次写入请求包含的数据行数。默认值 `256`。
- `search_params`：
  - `parallel`：压力测试时的并发查询客户端数。
  - `top`：每次搜索返回的结果数量（K 值）。
  - `test_duration`：压力测试的总持续时间（秒）。
- *`connection_params`*：
  - *`protocol`*：数据库通讯协议（如 TCP）。默认值 `TCP`。
  - *`user`*：数据库访问用户名。默认值 `default`。
  - *`password`*：数据库访问密码。默认值 `""`。
  - *`table`*：表名，默认值 `Benchmark`。

运行命令（使用模板配置：`myscale-bench/configurations/templates/myscale-text-quora-mini-dev.json`）：
```bash
cp myscale-bench/configurations/templates/myscale-text-quora-mini-dev.json myscale-bench/configurations/

./myscale-bench --engines myscale-text-quora-mini-dev --host 127.0.0.1 --port 9000
```

## 混合搜索

示例模板：
- `myscale-bench/configurations/templates/myscale-mstg-hybrid-quora-mini-dev.json`

评测指标：
- MRR

参数介绍：
配置文件（实验 JSON）：
- `name`：测试配置的唯一标识名称。
- `dataset`：指定测试所使用的源数据集名称。
- `upload_params`：
  - `index_type`：向量索引算法类型（例如 `MSTG`）。
  - `mstg_disk_mode`：是否启用磁盘模式（`1` 为磁盘模式，`0` 为内存模式）。
  - `fts_idx_cols`：需要建立全文索引的列名列表（示例里是 `["body"]`）。
  - `fts_idx_params`：全文索引参数（例如 tokenizer、停用词等）。
  - *`parallel`*：数据导入时的并发线程数。默认值 `16`。
  - *`batch_size`*：单次写入请求包含的数据行数。默认值 `256`。
- `search_params`：
  - `parallel`：压力测试时的并发查询客户端数。
  - `top`：每次搜索返回的结果数量（K 值）。
  - `test_duration`：压力测试的总持续时间（秒）。
  - `params.dense*`：所有以 `dense` 为前缀的参数都会传给 `HybridSearch(...)` 的向量部分（例如 `dense_alpha`、`dense_m`、`dense_ef_s`）。
  - `params.fusion_type`：融合算法（例如 `RRF`）。
  - `params.fusion_weight`：融合权重。
  - `params.fusion_k`：融合参数 `k`（RRF 参数）。
  - `params.only_vector_search`：强制走纯向量检索（绕过 `HybridSearch(...)`），写在实验配置的 `search_params.params` 中。
  - `params.only_text_search`：强制走纯文本检索（绕过 `HybridSearch(...)`），写在实验配置的 `search_params.params` 中。
- *`connection_params`*：
  - *`protocol`*：数据库通讯协议（如 TCP）。默认值 `TCP`。
  - *`user`*：数据库访问用户名。默认值 `default`。
  - *`password`*：数据库访问密码。默认值 `""`。
  - *`table`*：表名，默认值 `Benchmark`。

运行命令（使用模板配置：`myscale-bench/configurations/templates/myscale-mstg-hybrid-quora-mini-dev.json`）：
```bash
cp myscale-bench/configurations/templates/myscale-mstg-hybrid-quora-mini-dev.json myscale-bench/configurations/

./myscale-bench --engines myscale-mstg-hybrid-quora-mini-dev --host 127.0.0.1 --port 9000
```

## 内存不足（OOM kill）

在数据导入（upload）或查询（search）阶段，如果机器内存不足，操作系统可能会触发 OOM killer 直接将进程杀死，此时通常不会看到完整的 Python 异常栈，而只是看到类似 “killed” 的信息。

- 如果在 upload 阶段发生 OOM，可在对应实验配置的 `upload_params` 中调小 `parallel` 和 `batch_size`。
- 如果在 search 阶段发生 OOM，可调小实验配置中的 `search_params.parallel`，以及 `datasets/datasets.json` 中对应数据集的 `queries_pool_size`。

注意：发生 OOM 之后，在下一次运行基准测试之前，请先检查是否还有上一次残留的 python3 进程（例如有些子进程没有被完全杀死）。可以使用 `ps aux | grep python3` 等命令查看，并在确认安全的前提下使用 `pkill -9 python3` 等命令先清理残留进程，再调小相应参数重新执行。

## 打包

- ARM（Ubuntu 22.04）：运行 `./build_nuitka_arm.sh`，产物输出到 `dist-arm/`。
- x86-64（manylinux_2_28_x86_64）：运行 `./build_nuitka.sh`，产物输出到 `dist/`。
- 打包产物 `dist-arm/myscale-bench/myscale-bench` 支持的最低 GLIBC 版本：`2.34`
- 打包产物 `dist/myscale-bench/myscale-bench` 支持的最低 GLIBC 版本：`2.14`

## 推荐内存配置

- 对于 laion-768-1m-ip 数据集，推荐使用至少 4GB 内存。
