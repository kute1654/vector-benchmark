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
│   │   ├── config.json                 #     Default experiment config
│   │   └── templates/                  #     Config templates (copy into configurations/)
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
│   ├── results/                        #   Test results (CSV/JSON)
│   ├── docs/                           #   Framework docs
│   │   ├── design.md                   #     Design document
│   │   └── dataset.md                  #     Dataset format guide
│   └── bash-test/                      #   sh QPS tests (ClickHouse / MyScale)
│       ├── clickhouse-benchmark.sh     #   ClickHouse/MyScale native QPS load
│       ├── pgvector-benchmark.sh       #   [removable] pgvector (pgbench, see design.md App. B)
│       ├── polardb-pase-query-forms-benchmark.sh # [removable] PolarDB PASE (pgbench, see design.md App. B)
│       ├── generate-sql-files.sh       #   Sample-generated SQL
│       ├── verify_string_to_vector_engine.py   #  MyScale string→vector verification
│       └── sql-bench/                  #   Generated SQL files
│
├── docs/                               # Query-form / plan-cache analysis docs
├── results/                            # Shell benchmark summary CSVs
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

### 4. Run Benchmark

```bash
cd benchmark

# Run all configs matching a pattern
python run.py --engines "pgvector-*" --host 127.0.0.1 --port 5432

# Run a specific config
python run.py --engines pgvector-sift-128-euclidean

# Skip data upload (only run search)
python run.py --engines pgvector-sift-128-euclidean --skip-upload

# Recall-only mode (no QPS test)
python run.py --engines pgvector-sift-128-euclidean --recall-only
```

### 5. Compare all SQL forms and cache profiles

After the four target tables have been created and indexed, run one matrix
covering every registered query form. Prepare a JSON target config file first
(connection params + table names; if it lists multiple engines, run one at a
time — see the `--config` docs in `query_forms.py`), for example:

```bash
cd ..
python -m benchmark query_forms \
  --config query-forms/targets.json \
  --query-count 100 \
  --concurrency 4 \
  --duration 20 \
  --output results/query-forms-qps.csv
```

The output contains one row per `engine × cache profile × sql_type`.
ClickHouse/MyScale use their server query-result and vector-plan cache
settings. PostgreSQL-compatible targets use prepared statements for the
plan-cache profile and a bounded per-connection result cache for the
result-cache profile.

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
| `use_cache` | int[] | Enable prepared statement mode (pgvector/PolarDB) |
| `use_query_cache` | int[] | Enable query-result cache; combined with the plan-cache setting |
| `optimize` | bool | Optimize table after import (ClickHouse) |
| `enable_query_plan_cache` | int[] | Enable query plan cache (MyScale) |

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

The framework keeps exactly **two** kinds of test scripts:

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
# View CSV summary
cd benchmark/results
cat benchmark_results.csv | column -t -s,

# View JSON detailed results
ls -la *search*.json
cat pgvector-sift-128-euclidean-search-*.json | python -m json.tool

# View sh (ClickHouse/MyScale) benchmark results
cat results/vector-query-forms-pgvector-results.csv
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
| `--host` | `127.0.0.1` | Database server host |
| `--port` | `9000` | Database server port |
| `--skip-upload` | `false` | Skip data upload and index build stages |
| `--recall-only` | `false` | Only run recall/metric evaluation over all test queries |

## Results

Results are saved in `benchmark/results/`:
- `benchmark_results.csv` — Aggregated benchmark results
- `{experiment_name}-search-{id}-{timestamp}.json` — Detailed per-experiment results

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