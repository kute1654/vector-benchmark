# vector-benchmark

A unified benchmark framework for vector database performance testing. Supports **pgvector**, **ClickHouse**, **MyScale**, and **PolarDB-pg (PASE)**.
See [Design Document](benchmark/docs/design.md) for the architecture and design.

Language: English | [中文](README.zh-CN.md)

## Directory Structure

```
vector-benchmark/
├── benchmark/                          # Python benchmark framework (main package)
│   ├── run.py                          #   Main entry (MyscaleBench, Nuitka target)
│   ├── query_forms.py                  #   SQL-form × cache-profile QPS comparison
│   ├── generate_sql_files.py           #   Generate SQL benchmark files for bash tests
│   ├── plan_cache_correctness_invalidation.py # Plan-cache correctness/invalidation suite
│   ├── gen_qps_report.py               #   QPS charts from result CSVs
│   ├── gen_step_time_report.py         #   Stage-time charts from result CSVs
│   ├── generate_quora_embeddings.py    #   Quora text-embedding helper
│   ├── analyze_pgvector_log.py         #   pgvector server-log analysis
│   ├── analyze_myscale_log.py          #   MyScale server-log analysis
│   ├── analyze_polardb_log.py          #   PolarDB PASE server-log analysis
│   ├── config_read.py                  #   Config reader (engines / datasets)
│   ├── dataset.py                      #   Dataset management (download / reader dispatch)
│   ├── dataset_config.py               #   Dataset configuration model
│   ├── cli_output.py                   #   CLI output formatting
│   ├── __init__.py                     #   Package exports + directory resolution
│   ├── requirements.txt                #   Python dependencies
│   ├── datasets/                       #   Datasets
│   │   ├── datasets.json               #     Dataset registry
│   │   └── downloads/                  #     Downloaded data files
│   ├── configurations/                 #   Experiment configs (JSON arrays)
│   │   └── config.json                 #     Default experiment config (one array, many experiments)
│   ├── engine/                         #   Engine abstraction layer
│   │   ├── base.py                     #     BaseEngine (QPS / recall measurement)
│   │   ├── pgvector.py / polardb.py / clickhouse.py / myscale.py
│   │   ├── base_client/                #     Base client classes
│   │   │   ├── client.py               #       BaseClient (orchestrates upload/index/search)
│   │   │   ├── configure.py            #       BaseConfigurator (table/index creation)
│   │   │   ├── search.py               #       BaseSearcher (query + session_settings)
│   │   │   ├── upload.py               #       BaseUploader (data import)
│   │   │   ├── parser.py               #       BaseClientParser (result parsing)
│   │   │   └── distances.py / utils.py
│   │   └── clients/                    #     Database client implementations
│   │       ├── client_factory.py       #       Client factory (cartesian expansion)
│   │       └── pgvector/ clickhouse/ myscale/ polardb/
│   ├── dataset_reader/                 #   Dataset readers
│   │   ├── base_reader.py              #     Base reader
│   │   ├── ann_h5_reader.py            #     ann-benchmarks HDF5 (h5)
│   │   ├── ann_compound_reader.py      #     Compound archive (tar)
│   │   ├── json_reader.py              #     JSONL (jsonl)
│   │   ├── gz_tsv_reader.py            #     BeIR text retrieval (gz_tsv)
│   │   └── utils.py
│   ├── results/                        #   Test results (gitignored)
│   │   ├── csv/                        #     Benchmark CSV summaries
│   │   ├── json/                       #     Detailed per-run JSON results
│   │   ├── charts/                     #     Charts produced by gen_*_report.py
│   │   └── bak/                        #     Timestamped CSV rotations
│   ├── docs/                           #   Framework docs
│   │   ├── design.md                   #     Design document
│   │   ├── dataset.md                  #     Dataset format guide
│   │   └── plan_cache_correctness_invalidation.md # Plan-cache correctness suite
│   └── bash-test/                      #   Shell QPS tests
│       ├── clickhouse-benchmark.sh     #   ClickHouse/MyScale native QPS (authoritative)
│       ├── pgvector-benchmark.sh       #   pgvector pgbench + single-statement latency probe
│       ├── polardb-pase-query-forms-benchmark.sh # PolarDB PASE SQL-form / cache cross-check
│       ├── generate-sql-files.sh       #   Sample-generated ClickHouse SQL
│       └── sql-bench/                  #   Generated SQL files (gitignored)
│
├── docs/                               # Query-form / plan-cache analysis docs (gitignored)
├── README.md                           # This file (English)
├── README.zh-CN.md                     # Chinese version
├── build_nuitka.sh                     # Nuitka build script (x86_64)
└── build_nuitka_arm.sh                 # Nuitka build script (ARM64)
```

## Quick Start

### 1. Install Dependencies

```bash
cd vector-benchmark/benchmark
pip install -r requirements.txt
```

### 2. Prepare Datasets

Place HDF5 dataset files under `benchmark/datasets/downloads/` and configure entries in `benchmark/datasets/datasets.json`.

```bash
# Example: download ann-benchmarks datasets
cd benchmark/datasets/downloads
wget https://ann-benchmarks.com/sift-128-euclidean.hdf5
wget https://ann-benchmarks.com/gist-960-euclidean.hdf5
```

### 3. Configure Experiments

Experiment configuration files are JSON arrays stored in `configurations/`. Each element defines one experiment:

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
      "search_number": 1,
      "sql_type": ["text_literal", "text_cast"],
      "enable_query_plan_cache": [0, 1, 2, 3],
      "use_query_cache": [0],
      "vector_in_parse_mode": ["default"]
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

### 4. Run Benchmark

```bash
cd benchmark

# Run all configs matching a pattern
python run.py --engines "pgvector-*" --host 127.0.0.1 --port 5432

# Run a specific config
python run.py --engines pgvector-sift-128-euclidean

# Restrict a wildcard to one engine family (`--engine-type` filters, never overrides)
python run.py --engines "*" --engine-type pgvector

# Skip data upload (only run search)
python run.py --engines pgvector-sift-128-euclidean --skip-upload

# Create table + index only (used to prepare tables for the shell tests)
python run.py --engines pgvector-sift-128-euclidean --skip-search

# Recall-only mode (no QPS test)
python run.py --engines pgvector-sift-128-euclidean --recall-only
```

### 5. Compare all SQL forms and cache profiles

After the target tables have been created and indexed, run one matrix
covering every registered query form. Prepare a JSON target config file first
(connection params + table names; if it lists multiple engines, run one at a
time — ClickHouse and MyScale share the same default port), for example:

```bash
# From the repository root
python -m benchmark query_forms \
  --config tmp/query-forms-targets.json \
  --query-count 100 \
  --concurrency 4 \
  --duration 20 \
  --output benchmark/results/csv/query-forms-qps.csv
```

The output contains one row per `target × cache profile × sql_type`.
ClickHouse/MyScale use their server query-result and vector-plan cache
settings. PostgreSQL-compatible targets use prepared statements for the
plan-cache profile and a bounded per-connection result cache for the
result-cache profile. Run `python -m benchmark query_forms --help` for the
per-engine default profile list.

## Configuration File Format

### Top-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique experiment identifier (used with `--engines`) |
| `engine` | string | Yes | Engine type: `pgvector`, `clickhouse`, `myscale`, `polardb` |
| `dataset` | string | Yes | Dataset name (matches `datasets.json` `name` field) |
| `connection_params` | object | Yes | Database connection parameters |
| `upload_params` | object | Yes | Table creation, index building, and data import parameters |
| `search_params` | object | Yes | Query parameters |

### connection_params

Database connection settings. Supports all engines with different defaults:

| Engine | Default Port | Default User | Protocol |
|--------|-------------|-------------|----------|
| pgvector | 5432 | postgres | N/A |
| ClickHouse | 9000 | default | tcp |
| MyScale | 9000 | default | tcp |
| PolarDB-pg | 5433 | postgres | N/A |

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

Parameters for creating tables, building indexes, and importing data:

| Parameter | Type | Description |
|-----------|------|-------------|
| `index_type` | string | Index algorithm (e.g., `hnsw`, `ivfflat`, `HNSWFLAT`, `MSTG`) |
| `index_params` | object | Index-specific parameters (e.g., `m`, `ef_construction`, `ef_c`) |
| `parallel` | int | Number of concurrent threads for data import |
| `batch_size` | int | Rows per insert batch |
| `search_number` | int | Number of search rounds after upload |
| `sql_type` | string[] | `[sql_type]` values to sweep (query-form names, see below) |
| `enable_query_plan_cache` | int[] | Plan-cache tier; pgvector/PolarDB `0..3`, MyScale `0..2` |
| `use_query_plan_cache` | int[] | Legacy alias accepted by pgvector/PolarDB |
| `use_query_cache` | int[] | Result-cache switch (`0`/`1`), combined with the plan-cache tier |
| `vector_query_plan_cache` | int[] | ClickHouse plan-cache tier (`0..2`) |
| `vector_only_cache_query_plan`, `vector_query_plan_cache_only_vector`, `vector_use_cast` | int[] | ClickHouse cache-key variants |
| `enable_cast_vector`, `only_cache_query_plan`, `query_plan_cache_only_vector` | int[] | MyScale cache-key variants |
| `vector_in_parse_mode` | string[] | pgvector/PolarDB vector-text parser: `default`, `strtof`, `inline`, `fast_float` |
| `optimize` | bool | Optimize table after import (ClickHouse) |

Plan-cache tiers are engine-specific:

| Tier | pgvector | PolarDB PASE | ClickHouse (`vector_query_plan_cache`) / MyScale (`enable_query_plan_cache`) |
|------|----------|--------------|-------------------------------------------------------------------------------|
| `0` | No `PREPARE`; inline text, re-plan every query | Same as pgvector | Cache off |
| `1` | `PREPARE` + `plan_cache_mode=auto`, typed bind | `PREPARE` + `plan_cache_mode=auto`, typed bind | Cache on |
| `2` | `PREPARE` + `force_custom_plan`, typed bind | `PREPARE` + `force_generic_plan`, text bind (`$1::pase`) | Cache on + plan-only / cast variant |
| `3` | `PREPARE` + `force_generic_plan`, typed bind | `PREPARE` + `force_generic_plan`, pase typed bind (`$1`) | — |

For pgvector/PolarDB, `use_query_cache=1` additionally enables the session-level
result cache (`pgvector.result_cache*` / `pase_result_cache*`). For
ClickHouse/MyScale it enables the server query cache.

### search_params

Parameters for the query benchmark phase:

| Parameter | Type | Description |
|-----------|------|-------------|
| `parallel` | int[] | Concurrent query clients |
| `top` | int | Number of nearest neighbors (K) |
| `test_duration` | int | Test duration in seconds |
| `params` | object | Engine-specific search parameters (e.g., `ef_s`) |
| `session_settings` | object | Session-level SET commands applied before each query |

### Array Parameter Expansion

Array values in configuration files are automatically expanded into multiple test combinations via Cartesian product:

```json
"parallel": [1, 4, 8],
"params": { "ef_s": [40, 100, 200] }
```

produces 3 × 3 = 9 test combinations.

### session_settings

Configure `session_settings` under `search_params` to execute SET commands before each query batch. This allows adjusting database parameters without reconnecting.

**pgvector example:**
```json
"session_settings": {
  "enable_seqscan": "off",
  "ivfflat.probes": 10
}
```

**ClickHouse example:**
```json
"session_settings": {
  "hnsw_candidate_list_size_for_search": 100
}
```

**PolarDB example:**
```json
"session_settings": {
  "enable_seqscan": "off",
  "pase.enable": "on"
}
```

## Supported Engines

### pgvector

| Attribute | Value |
|-----------|-------|
| Index types | `hnsw`, `ivfflat` |
| Distance functions | `l2`, `ip`, `cosine` |
| Default port | 5432 |
| Connection | psycopg2 |
| QPS measurement | Python (run.py / query_forms.py) |

### ClickHouse

| Attribute | Value |
|-----------|-------|
| Index types | `HNSWFLAT`, `HNSW`, `VECTOR_SIMILARITY`, `ANNOY`, `USEARCH`, `FLAT` |
| Distance functions | `l2`, `dot`, `cosine` |
| Default port | 9000 (tcp) / 8123 (http) |
| Connection | clickhouse-driver (tcp) / clickhouse-connect (http) |
| QPS measurement | clickhouse-benchmark.sh |

### MyScale

| Attribute | Value |
|-----------|-------|
| Index types | `HNSWFLAT`, `MSTG`, `MSRQ` |
| Distance functions | `l2`, `dot`, `cosine` |
| Default port | 9000 (tcp) / 8123 (http) |
| Connection | clickhouse-driver (tcp) / clickhouse-connect (http) |
| QPS measurement | clickhouse-benchmark.sh |

### PolarDB-pg (PASE)

| Attribute | Value |
|-----------|-------|
| Index types | `hnsw` (pase_hnsw), `ivfflat` (pase_ivfflat) |
| Distance functions | `l2`, `ip`, `cosine` |
| Default port | 5433 |
| Connection | psycopg2 |
| QPS measurement | Python (run.py / query_forms.py) |

## Testing Modes

Three classes of test scripts share the same SQL-form registry and CSV column
conventions. The **authoritative QPS** for each engine family is fixed:

| Class | Script(s) | Authoritative for |
|-------|-----------|-------------------|
| Python | `run.py`, `query_forms.py` | pgvector / PolarDB QPS + recall; table/index creation and loading for all engines |
| Native sh | `bash-test/clickhouse-benchmark.sh` | ClickHouse / MyScale QPS |
| pgbench cross-check | `bash-test/pgvector-benchmark.sh`, `bash-test/polardb-pase-query-forms-benchmark.sh` | Independent check of pgvector / PolarDB per-SQL-form behaviour (including plan-cache modes) |

### Python Benchmark (create/index + QPS + recall)

Driven by `run.py` (and `query_forms.py`): creates the database, tables, indexes,
imports data, then measures QPS and recall:

- **Duration mode**: Sends queries continuously for a specified duration, measures QPS
- **Count mode**: Executes a fixed number of queries, measures latency and recall
- **Recall-only mode**: Runs single-process search over all test queries, outputs recall metrics only

**pgvector and PolarDB-pg (PASE)** use this Python path as the authoritative QPS
measurement — pgbench cannot measure the real QPS of the query plan cache, while
the Python client can precisely apply session-level `SET` and prepared statements.

### sh Benchmark (ClickHouse / MyScale vector-search QPS)

Uses the native `clickhouse-benchmark` tool to measure vector-search QPS directly.
Python-driven measurement is low here because the script overhead inflates latency
significantly, so **ClickHouse / MyScale** use this sh path as the authoritative QPS
measurement:

```bash
cd benchmark/bash-test

# ClickHouse
./clickhouse-benchmark.sh benchmark_sift_128

# MyScale
ENGINE=myscale ./clickhouse-benchmark.sh benchmark_sift_128
```

> Create the target table, indexes, and import data first via the `python run.py`
> upload phase, then run the sh script. See the header comments of
> `clickhouse-benchmark.sh` for usage details.

The script accepts `SETTINGS_GROUPS` entries of the form
`profile|use_query_cache|vector_query_plan_cache|vector_only_cache_query_plan|vector_query_plan_cache_only_vector|vector_use_cast`
(MyScale appends a seventh field: `enable_cast_vector`, then
`only_cache_query_plan`, `query_plan_cache_only_vector`; see the header for
the exact ordering). `CK_EF_S` sets the session-level
`hnsw_candidate_list_size_for_search`.

### pgbench cross-check (pgvector / PolarDB)

`pgvector-benchmark.sh` and `polardb-pase-query-forms-benchmark.sh` are retained
as independent cross-checks of the Python numbers. Both follow the
ClickHouse-style convention "one script = N queries, QPS = tps × N" and sweep
the per-engine plan-cache modes (§ `upload_params`) × result-cache on/off.

Because pgbench reports latency per *transaction* (one full N-query script), the
pgvector script adds a separate **single-statement latency probe**: after each
throughput measurement it replays the same script through `psql` with
`\timing on`, one statement at a time, inside a single transaction, and merges
the `Time:` samples to compute `mean_time_ms` / `p95_time_ms` / `p99_time_ms`
(nearest-rank). The probe is controlled by:

| Variable | Default | Meaning |
|----------|---------|---------|
| `LATENCY_PROBE_STATEMENTS` | `1000` | Total statements sampled per combination; `0` disables the probe and writes `not_recorded` |
| `TIMELIMIT` / `WARMUP_TIMELIMIT` / `REPEAT` | `10` / `1` / `1` | pgbench measure time, warmup time, repetitions |

```bash
cd benchmark/bash-test
./pgvector-benchmark.sh benchmark_sift_128_1k
TABLE=Benchmark_768_1m INDEX_TYPE=hnsw ./polardb-pase-query-forms-benchmark.sh
```

### Cache profile comparison (`query_forms.py`)

`query_forms.py` sweeps every registered query form × cache profile:

- **ClickHouse / MyScale**: `off`, `result-cache`, `plan-cache`, `plan-cast`,
  `plan-only-vector`, `plan-result-cache`. Profiles are expressed as inline
  `SETTINGS` on each query; the server must have
  `enable_vector_performance_test` enabled, and each profile starts by dropping
  the relevant server cache (`SYSTEM DROP QUERY CACHE`, `SYSTEM DROP VECTOR
  QUERY PLAN CACHE` / `SYSTEM DROP QUERY PLAN CACHE`).
- **pgvector / PolarDB**: `direct`, `result-cache`, `prepared-plan`,
  `prepared-result-cache`. The plan-cache profiles use prepared statements
  (`enable_query_plan_cache` tiers); the result-cache profiles use the
  session-level server result cache.

Targets on the same `host:port` are rejected because ClickHouse and MyScale
share the default port — run them sequentially.

## Shell Setup & Index Operations

Table/index creation and data import are all done through the `python run.py`
upload phase (see the `upload_params` config). You may also create indexes manually:

```sql
-- pgvector HNSW index
CREATE INDEX ON benchmark_sift_128_1k USING hnsw (vector vector_l2_ops)
    WITH (m = 16, ef_construction = 200);

-- pgvector IVFFlat index
CREATE INDEX ON benchmark_sift_128_1k USING ivfflat (vector vector_l2_ops)
    WITH (lists = 100);

-- PolarDB PASE HNSW index
CREATE INDEX ON benchmark_sift_128_1k USING pase_hnsw (vector)
    WITH (dim = 128, base_nb_num = 16, ef_build = 40, ef_search = 100, base64_encoded = 0);
```

## Viewing Results

```bash
# Python-client CSV summaries
column -t -s, benchmark/results/csv/pgvector_benchmark_results.csv
column -t -s, benchmark/results/csv/clickhouse_benchmark_results.csv
column -t -s, benchmark/results/csv/benchmark_results.csv

# Shell-test CSVs (same directory, different file names)
column -t -s, benchmark/results/csv/clickhouse-benchmark-results.csv
column -t -s, benchmark/results/csv/myscale-benchmark-results.csv
column -t -s, benchmark/results/csv/pgvector-benchmark-results.csv
column -t -s, benchmark/results/csv/vector-query-forms-polardb-results.csv

# Detailed per-run JSON
ls benchmark/results/json/
python -m json.tool benchmark/results/json/pgvector-*-search-*.json | head -50

# Charts
ls benchmark/results/charts/
python benchmark/gen_qps_report.py
```

## Quick Reference

| Operation | Command |
|-----------|---------|
| List all tables | `psql -h 127.0.0.1 -p 5432 -U postgres -c "\dt"` |
| Count rows | `psql -h 127.0.0.1 -p 5432 -U postgres -c "SELECT count(*) FROM benchmark_sift_128_1k"` |
| List indexes | `psql -h 127.0.0.1 -p 5432 -U postgres -c "\di benchmark_sift_128_1k*"` |
| View query plan | `psql -h 127.0.0.1 -p 5432 -U postgres -c "EXPLAIN (ANALYZE, BUFFERS) SELECT ..."` |
| Drop table | `psql -h 127.0.0.1 -p 5432 -U postgres -c "DROP TABLE IF EXISTS benchmark_sift_128_1k"` |
| List ClickHouse tables | `clickhouse-client -q "SHOW TABLES"` |
| List ClickHouse indexes | `clickhouse-client -q "SELECT name, type FROM system.data_skipping_indices WHERE table='benchmark_sift_128'"` |

## Troubleshooting

### Index Not Used

Check the query plan to confirm index scan is being used:

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT id, (vector <-> '[1,2,3,...]'::vector) AS dis
FROM benchmark_sift_128_1k
ORDER BY dis ASC LIMIT 10;
```

If you see `Seq Scan` instead of `Index Scan`:
- Ensure the index has been created
- Use `SET enable_seqscan = off`
- Or configure via `session_settings`

### Low QPS

- Check if `ef_search` is too large
- Verify concurrency settings are reasonable
- Use the sh path (`bash-test/clickhouse-benchmark.sh`) for ClickHouse/MyScale precise QPS
  (Python driver overhead can depress ClickHouse/MyScale QPS)

### Connection Failure

- Check `connection_params` host/port settings
- Verify the database is running
- For ClickHouse, verify `protocol` setting (tcp/http)

## Command Line Options

| Option | Default | Description |
|--------|---------|-------------|
| `--engines` | `*` | Experiment name (glob pattern matching `configurations/*.json` `name` field) |
| `--datasets` | `*` | Dataset name (glob pattern matching `datasets.json` `name` field) |
| `--engine-type` | none | Filter matched configs by `engine` (`clickhouse`, `myscale`, `pgvector`, `polardb`); never rewrites the config |
| `--host` | config value, else `127.0.0.1` | Override database server host |
| `--port` | config value, else `5432`/`5433`/`9000` | Override database server port |
| `--skip-upload` | `false` | Skip data upload and index build stages |
| `--skip-search` | `false` | Create table + index only, then exit (prepares tables for the shell tests) |
| `--recall-only` | `false` | Only run recall/metric evaluation over all test queries (ignores `queries_pool_size`) |

## Results

Results are saved under `benchmark/results/`:

- `csv/pgvector_benchmark_results.csv` — pgvector/PolarDB Python-client results
- `csv/clickhouse_benchmark_results.csv` — ClickHouse Python-client results
- `csv/benchmark_results.csv` — MyScale Python-client results
- `csv/*-benchmark-results.csv`, `csv/vector-query-forms-*-results.csv` — shell-test results
- `json/{experiment}-search-{id}-{timestamp}.json` — detailed per-experiment results
- `charts/` — charts produced by `gen_qps_report.py` / `gen_step_time_report.py`
- `bak/` — timestamped rotations of CSV files whose schema changed

Python-client latency columns (`mean_time`, `p95_time`, `p99_time`) are in
**seconds**; shell-test latency columns (`mean_time_ms`, `p95_time_ms`,
`p99_time_ms`) are in **milliseconds**. Missing measurements are written as
`not_recorded`.

## Packaging

Build standalone executables using Nuitka:

- **x86_64** (manylinux_2_28_x86_64): `./build_nuitka.sh` → `dist/myscale-bench-linux-x86_64.tar.gz`
- **ARM64** (Ubuntu 22.04): `./build_nuitka_arm.sh` → `dist-arm/myscale-bench-linux-aarch64.tar.gz`

Minimum GLIBC requirements:
- x86_64: GLIBC 2.14
- ARM64: GLIBC 2.34

## Memory Recommendations

- For the laion-768-1m-ip dataset, use at least 4GB of memory.
- If OOM occurs during upload, reduce `upload_params.parallel` and `upload_params.batch_size`.
- If OOM occurs during search, reduce `search_params.parallel` and the dataset's `queries_pool_size` in `datasets.json`.