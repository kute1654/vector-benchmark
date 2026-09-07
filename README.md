# vector-benchmark

A unified benchmark framework for vector database performance testing. Supports **pgvector**, **ClickHouse**, **MyScale**, and **PolarDB-pg (PASE)**.

Language: English | [中文](README.zh-CN.md)

## Directory Structure

```
vector-benchmark/
├── benchmark/                          # Python benchmark framework
│   ├── run.py                          #   Main entry point (Nuitka compilation target)
│   ├── config_read.py                  #   Configuration file reader
│   ├── dataset.py                      #   Dataset management
│   ├── dataset_config.py               #   Dataset configuration
│   ├── cli_output.py                   #   CLI output formatting
│   ├── datasets/                       #   Datasets
│   │   ├── datasets.json               #     Dataset registry
│   │   ├── downloads/                  #     Downloaded HDF5 files
│   │   └── .gitignore
│   ├── engine/                         #   Engine abstraction layer
│   │   ├── base_client/                #     Base client classes
│   │   │   ├── base.py                 #       BaseClient (upload/build/search)
│   │   │   ├── configure.py            #       BaseConfigure (table/index creation)
│   │   │   ├── search.py               #       BaseSearcher (query + session_settings)
│   │   │   └── upload.py               #       BaseUploader (data import)
│   │   ├── clients/                    #     Database client implementations
│   │   │   ├── pgvector/               #       pgvector
│   │   │   │   ├── config.py           #         Default config constants
│   │   │   │   ├── configure.py        #         Table/index creation
│   │   │   │   ├── search.py           #         Vector search (with SET support)
│   │   │   │   └── upload.py           #         Data import
│   │   │   ├── clickhouse/             #       ClickHouse
│   │   │   │   ├── config.py
│   │   │   │   ├── configure.py
│   │   │   │   ├── search.py           #         Vector search (with SET support)
│   │   │   │   └── upload.py
│   │   │   ├── myscale/                #       MyScale
│   │   │   │   ├── config.py
│   │   │   │   ├── configure.py
│   │   │   │   ├── search.py           #         Vector search (with SET support)
│   │   │   │   └── upload.py
│   │   │   └── polardb/                #       PolarDB-pg (PASE)
│   │   │       ├── config.py
│   │   │       ├── configure.py
│   │   │       ├── search.py
│   │   │       └── upload.py
│   │   ├── client_factory.py           #     Client factory
│   │   └── __init__.py
│   ├── dataset_reader/                 #   Dataset readers
│   │   ├── base_reader.py              #     Base reader
│   │   ├── h5_reader.py                #     HDF5 format reader
│   │   └── utils.py
│   ├── results/                        #   Test results (CSV/JSON)
│   └── __init__.py                     #   Package exports
│
├── bash-test/                          # Shell-level precise QPS tests
│   ├── clickhouse-benchmark.sh         #   ClickHouse benchmark (clickhouse-benchmark)
│   ├── pgvector-query-forms-benchmark.sh # pgvector benchmark (pgbench)
│   ├── polardb-pase-query-forms-benchmark.sh # PolarDB benchmark (pgbench)
│   ├── setup-pgvector-from-h5.sh       #   pgvector HDF5 -> table setup
│   ├── setup-polardb-pase-from-h5.sh   #   PolarDB HDF5 -> table setup
│   ├── generate-sql-files.sh           #   SQL generation for benchmarking
│   └── sql-bench/                      #   Generated SQL files
│
├── configurations/                     # Experiment configuration files (JSON)
│   ├── pgvector.json                   #   pgvector test config
│   ├── clickhouse.json                 #   ClickHouse test config
│   ├── myscale.json                    #   MyScale test config
│   └── polardb.json                    #   PolarDB test config
│
├── docs/                               # Detailed documentation
│   ├── README.md                       #   English version
│   └── README.zh-CN.md                 #   Chinese version
│
├── README.md                           # This file (English)
├── README.zh-CN.md                     # Chinese version
├── requirements.txt                    # Python dependencies
├── build_nuitka.sh                     # Nuitka build script (x86_64)
└── build_nuitka_arm.sh                 # Nuitka build script (ARM64)
```

## Quick Start

### 1. Install Dependencies

```bash
cd vector-benchmark
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
covering every registered query form:

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
| Shell benchmark | pgbench |

### ClickHouse

| Attribute | Value |
|-----------|-------|
| Index types | `HNSWFLAT`, `HNSW`, `VECTOR_SIMILARITY`, `ANNOY`, `USEARCH`, `FLAT` |
| Distance functions | `l2`, `dot`, `cosine` |
| Default port | 9000 (tcp) / 8123 (http) |
| Connection | clickhouse-driver (tcp) / clickhouse-connect (http) |
| Shell benchmark | clickhouse-benchmark |

### MyScale

| Attribute | Value |
|-----------|-------|
| Index types | `HNSWFLAT`, `MSTG`, `MSRQ` |
| Distance functions | `l2`, `dot`, `cosine` |
| Default port | 9000 (tcp) / 8123 (http) |
| Connection | clickhouse-driver (tcp) / clickhouse-connect (http) |
| Shell benchmark | clickhouse-benchmark |

### PolarDB-pg (PASE)

| Attribute | Value |
|-----------|-------|
| Index types | `hnsw` (pase_hnsw), `ivfflat` (pase_ivfflat) |
| Distance functions | `l2`, `ip`, `cosine` |
| Default port | 5433 |
| Connection | psycopg2 |
| Shell benchmark | pgbench |

## Testing Modes

### Python Benchmark (Coarse QPS + Recall)

- **Duration mode**: Sends queries continuously for a specified duration, measures QPS
- **Count mode**: Executes a fixed number of queries, measures latency and recall
- **Recall-only mode**: Runs single-process search over all test queries, outputs recall metrics only

### Shell Benchmark (Precise QPS)

Uses native database benchmarking tools (pgbench / clickhouse-benchmark) for precise QPS measurement, eliminating Python network overhead:

```bash
# pgvector
cd bash-test
PSQL=/usr/local/pgsql/bin/psql PGBENCH=/usr/local/pgsql/bin/pgbench \
    REPEAT=5 TIMELIMIT=30 \
    ./pgvector-query-forms-benchmark.sh benchmark_sift_128_1k

# ClickHouse
cd bash-test
./clickhouse-benchmark.sh benchmark_sift_128

# PolarDB
cd bash-test
PSQL=/usr/local/pgsql/bin/psql PGBENCH=/usr/local/pgsql/bin/pgbench \
    REPEAT=5 TIMELIMIT=30 \
    ./polardb-pase-query-forms-benchmark.sh benchmark_sift_128_1k
```

## Shell Setup & Index Operations

### pgvector: Create Table and Import Data

```bash
cd bash-test
PSQL=/usr/local/pgsql/bin/psql ./setup-pgvector-from-h5.sh \
    ../benchmark/datasets/downloads/sift-128-euclidean.hdf5 \
    benchmark_sift_128_1k 1000 10 l2

# Arguments:
#   $1: HDF5 file path
#   $2: table name
#   $3: row count to import (train_count)
#   $4: top_k
#   $5: distance type (l2/ip/cosine)
```

### PolarDB: Create Table and Import Data

```bash
cd bash-test
PSQL=/usr/local/pgsql/bin/psql ./setup-polardb-pase-from-h5.sh \
    ../benchmark/datasets/downloads/sift-128-euclidean.hdf5 \
    benchmark_sift_128_1k 1000 10 l2
```

### Manual Index Creation

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

# View Shell benchmark results
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
- Use Shell scripts to isolate Python network overhead

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
