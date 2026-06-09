# MyScale Benchmark

A customer tool for fast performance proof-of-concept (POC)

Language: English | [中文](README.zh-CN.md)

## Quick Start (Vector Search)

### 1) Extract

```bash
tar -xzf myscale-bench-linux-x86_64.tar.gz
cd myscale-bench
```

### 2) Prepare datasets

Place your dataset files under `datasets/` and make sure each dataset entry in `datasets/datasets.json` points to the correct file location via the `path` field.

Notes:
- If `link` is empty or not accessible, download the dataset manually and set `path` to its location.

### 3) Configure experiments

Experiment configuration files are JSON files stored in these directories:
- `configurations/templates/`: prebuilt configuration templates for MSTG, HNSW. Use them as references or copy-and-modify.
- `configurations/`: your runnable configs (copy templates here after editing).

The tool only loads configs from `configurations/`. Files under `configurations/templates/` are not searched/loaded automatically.

Dataset naming:
- The dataset name used in experiment configs comes from the `name` field in `datasets/datasets.json`.

#### Experiment JSON parameters

Example template:
- `myscale-bench/configurations/templates/myscale-hnsw-laion-768-5m-ip.json`

The file is a JSON array; each element describes one experiment run.

Parameters:

Command line:
- `--engines`: Experiment `name` field(s) from config JSON files under `configurations/`.
- `--datasets`: Dataset name(s) to run. All configs under `configurations/` with `dataset` matching the given name will be executed.
- *`--host`*: Server IP. Default: `127.0.0.1`.
- *`--port`*: Server port. Default: `9000`.
- *`--skip-upload`*: Skip dataset upload and index build. Default: `False`.
 - *`--recall-only`*: Only run metric evaluation (e.g. recall/MRR) on the selected datasets. Runs single-process search over all test queries for each config, ignoring the `queries_pool_size` limit from `datasets/datasets.json`. Upload behavior is still controlled by `--skip-upload`.

Config file (experiment JSON):
- `name`: Unique identifier for the experiment configuration.
- `dataset`: Source dataset name used by this experiment.
- `upload_params`:
  - *`index_type`*: Vector index algorithm type (e.g. `HNSWFLAT`, `MSTG`).
  - `index_params`: Index build parameters (HNSWFLAT example; other index types have their own params):
    - *`m`*: Max number of edges per node in HNSW graph; affects recall and memory usage.
    - *`ef_c`*: Candidate list size during index build; affects index quality and build time.
  - *`parallel`*: Concurrency (threads/workers) for data import. Default: `16`.
  - *`batch_size`*: Number of rows per insert request. Default: `256`. For large datasets, consider increasing `batch_size` (and adjusting `parallel` as needed) to reduce the number of insert operations, speed up upload, and lower background merge pressure.
  - *`mstg_disk_mode`*: MSTG disk mode flag. Use `1` to enable disk mode. Default: `0`.
- `search_params`:
  - `parallel`: Concurrent query clients during benchmarking.
  - `top`: Number of nearest neighbors to return per search (K).
  - `test_duration`: Total test duration in seconds.
  - `params`: Search parameters (e.g. `alpha`, `ef_s`). When multiple parameters are provided, they are expanded as a Cartesian product.
- *`connection_params`*:
  - *`protocol`*: Database protocol. Default: `TCP`.
  - *`user`*: Database username. Default: `default`.
  - *`password`*: Database password. Default: `""`.
  - *`table`*: Table name. Default: `Benchmark`.

#### Warmup phase

Before the main search phase, the benchmark runs a short warmup:
- It selects the last combination from all expanded `search_params` (including `params`).
- It runs a warmup search with the same query logic as the real test, but with a fixed duration of about 2 seconds.

On very large datasets, the first query may need to load vector indexes or data into memory, which can take much longer than 2 seconds. In that case:
- The warmup may appear to “hang” while the database finishes loading data.
- The benchmark waits for the server’s response instead of failing fast.
- The maximum wait time for warmup queries is 1800 seconds by default.

You can change the warmup wait limit via the experiment config:

```json
"connection_params": {
  "warmup_timeout_s": 1800
}
```

This only affects the warmup phase; the main search phase still uses the regular timeout settings.

### 4) Run

```bash
./myscale-bench --engines myscale-mstg-laion-768-1m-ip --host 127.0.0.1 --port 9000
```

Optional overrides:
- Use `--host` (and `--port`) to override the server address.
- Use `--datasets` to run all test configs that target a given dataset.
- `--engines` and `--datasets` accept only one argument, but glob patterns can match multiple test configs (e.g. `--engines "myscale-*"`).
- When using glob patterns in `--engines` or `--datasets`, wrap the value in double quotes (e.g. `--datasets "laion-768-*-ip"`) so that shells like `zsh` do not expand the pattern before it is passed to the benchmark tool.
- `--engines` expects the `name` field from the experiment JSON files under `configurations/`.
- To skip both data upload and index build stages, add `--skip-upload` to the command line.

### 5) Results

Results are written to `results/` as JSON files.

## Text Search

Example template:
- `myscale-bench/configurations/templates/myscale-text-quora-mini-dev.json`

Evaluation metric:
- MRR

Parameters:

Config file (experiment JSON):
- `name`: Unique identifier for the experiment configuration.
- `dataset`: Source dataset name used by this experiment.
- `upload_params`:
  - `fts_idx_cols`: Column names to build full-text index on (example: `["body"]`).
  - `fts_idx_params`: Full-text index parameters (e.g. tokenizer, stop words).
  - *`parallel`*: Concurrency (threads/workers) for data import. Default: `16`.
  - *`batch_size`*: Number of rows per insert request. Default: `256`.
- `search_params`:
  - `parallel`: Concurrent query clients during benchmarking.
  - `top`: Number of results to return per search (K).
  - `test_duration`: Total test duration in seconds.
- *`connection_params`*:
  - *`protocol`*: Database protocol. Default: `TCP`.
  - *`user`*: Database username. Default: `default`.
  - *`password`*: Database password. Default: `""`.
  - *`table`*: Table name. Default: `Benchmark`.

Run (using the template: `myscale-bench/configurations/templates/myscale-text-quora-mini-dev.json`):
```bash
cp myscale-bench/configurations/templates/myscale-text-quora-mini-dev.json myscale-bench/configurations/
./myscale-bench --engines myscale-text-quora-mini-dev --host 127.0.0.1 --port 9000
```

## Hybrid Search

Example template:
- `myscale-bench/configurations/templates/myscale-mstg-hybrid-quora-mini-dev.json`

Evaluation metric:
- MRR

Parameters:

Config file (experiment JSON):
- `name`: Unique identifier for the experiment configuration.
- `dataset`: Source dataset name used by this experiment.
- `upload_params`:
  - `index_type`: Vector index algorithm type (e.g. `MSTG`).
  - `mstg_disk_mode`: Enable MSTG disk mode (`1` for disk mode, `0` for memory mode).
  - `fts_idx_cols`: Column names to build full-text index on (example: `["body"]`).
  - `fts_idx_params`: Full-text index parameters (e.g. tokenizer, stop words).
  - *`parallel`*: Concurrency (threads/workers) for data import. Default: `16`.
  - *`batch_size`*: Number of rows per insert request. Default: `256`.
- `search_params`:
  - `parallel`: Concurrent query clients during benchmarking.
  - `top`: Number of results to return per search (K).
  - `test_duration`: Total test duration in seconds.
  - `params.dense*`: All params with `dense` prefix are passed to the vector part of `HybridSearch(...)` (e.g. `dense_alpha`, `dense_m`, `dense_ef_s`).
  - `params.fusion_type`: Fusion algorithm (e.g. `RRF`).
  - `params.fusion_weight`: Fusion weight.
  - `params.fusion_k`: Fusion parameter `k` (RRF parameter).
  - `params.only_vector_search`: Force vector-only search (bypass `HybridSearch(...)`). Put it under `search_params.params` in the experiment JSON.
  - `params.only_text_search`: Force text-only search (bypass `HybridSearch(...)`). Put it under `search_params.params` in the experiment JSON.
- *`connection_params`*:
  - *`protocol`*: Database protocol. Default: `TCP`.
  - *`user`*: Database username. Default: `default`.
  - *`password`*: Database password. Default: `""`.
  - *`table`*: Table name. Default: `Benchmark`.

Run (using the template: `myscale-bench/configurations/templates/myscale-mstg-hybrid-quora-mini-dev.json`):
```bash
cp myscale-bench/configurations/templates/myscale-mstg-hybrid-quora-mini-dev.json myscale-bench/configurations/

./myscale-bench --engines myscale-mstg-hybrid-quora-mini-dev --host 127.0.0.1 --port 9000
```

## Out-of-memory (OOM kill)

During the upload or search stages, if the machine does not have enough memory, the operating system may trigger the OOM killer and terminate the process. In such cases, you typically will not see a full Python traceback, but only messages like “killed” in logs or `dmesg`.

- If OOM happens during the upload stage, decrease `upload_params.parallel` and `upload_params.batch_size` in the corresponding experiment config.
- If OOM happens during the search stage, decrease `search_params.parallel` in the experiment config, and the dataset’s `queries_pool_size` value in `datasets/datasets.json`.

After an OOM kill, before running the benchmark again, check whether any `python3` processes from the previous run are still alive (for example, some child processes might not have been fully killed). You can inspect them with `ps aux | grep python3` and, once you are sure it is safe, clean them up using commands such as `pkill -9 python3`. Then lower the parameters and rerun the benchmark.

## Packaging

- ARM64 (Ubuntu 22.04): Run `./build_nuitka_arm.sh`. Output: `dist-arm/`.
- x86-64 (manylinux_2_28_x86_64): Run `./build_nuitka.sh`. Output: `dist/`.
- Minimum supported GLIBC version for `dist-arm/myscale-bench/myscale-bench`: `2.34`
- Minimum supported GLIBC version for `dist/myscale-bench/myscale-bench`: `2.14`

## Recommended Memory

- For the laion-768-1m-ip dataset, use at least 4GB of memory.
