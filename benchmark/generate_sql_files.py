#!/usr/bin/env python3
"""
Generate SQL benchmark files for pgvector/polardb/clickhouse bash-based QPS testing.

Supports all query literal forms for each engine, enabling apples-to-apples
comparison of which SQL form yields the highest QPS.

Usage (PGVector):
    python -m benchmark generate_sql_files --engine pgvector \
        --table benchmark_sift_128_1k --query-count 1000 --top-k 10

Usage (PolarDB):
    python -m benchmark generate_sql_files --engine polardb \
        --table benchmark_sift_1m --query-count 1000 --top-k 10

Usage (ClickHouse):
    python -m benchmark generate_sql_files --engine clickhouse \
        --table Benchmark_768_1m --query-count 1000 --top-k 10

Config-driven:
    python -m benchmark generate_sql_files \
        --config ../configurations/pgvector_sift128.json
"""

import argparse
import json
import sys
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Supported query forms per engine
# ---------------------------------------------------------------------------

PGVECTOR_SQL_TYPES = [
    "text_literal",
    "text_cast",
    "vector_fn",
    "array_int_cast",
    "array_real_cast",
    "array_cast",
    "array_to_vector_fn",
    "halfvec_literal",
    "with_text_literal",
    "with_array_cast",
    "with_vector_fn",
    "subquery_id",
    "with_subquery_id",
]

POLARDB_SQL_TYPES = [
    "text_pase_op_id",
    "text_pase_op_extra",
    "text_pase_op_extra_ds",
    "pase_fn_text_default",
    "pase_fn_array_default",
    "pase_fn_array_extra",
    "pase_fn_array_ip",
    "hash_op_default",
    "hash_op_extra",
    "with_pase_fn_array",
    "with_pase_fn_text",
    "subquery_id",
    "with_subquery_id",
]

# Keep the query-form registry in one place.  The shell benchmarks historically
# used a larger list than this module, which meant that a generated benchmark
# did not cover the same SQL shapes as the shell benchmark.
CLICKHOUSE_SQL_TYPES = [
    "normal",
    "cast",
    "cast_array",
    "type_hint",
    "raw_bytes",
    "raw_bytes_x",
    "with",
    "with_cast",
    "with_cast_array",
    "with_type_hint",
    "with_raw_bytes",
    "with_raw_bytes_x",
]


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args():
    p = argparse.ArgumentParser(
        description="Generate SQL benchmark files for bash-based QPS testing"
    )
    p.add_argument(
        "--engine",
        choices=["pgvector", "polardb", "clickhouse", "myscale"],
        default="pgvector",
        help="Target database engine",
    )
    p.add_argument("--table", help="Table name (required)")
    p.add_argument("--query-count", type=int, default=1000, help="Number of query vectors")
    p.add_argument("--top-k", type=int, default=10, help="LIMIT clause")
    p.add_argument("--distance", choices=["l2", "ip", "cosine"], default="l2",
                   help="Distance metric (pgvector/clickhouse only)")
    p.add_argument("--dimension", type=int, default=0,
                   help="Vector dimension (pgvector: halfvec; clickhouse: type_hint)")
    p.add_argument("--sql-types", nargs="*",
                   help="Specific SQL forms to generate (default: all)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--user", default="postgres")
    p.add_argument("--password", default="123456")
    p.add_argument("--database", default="postgres")
    p.add_argument("--output-dir", default="../bash-test/sql-bench",
                   help="Output directory for SQL files")
    p.add_argument("--config", help="JSON config file path")
    # PolarDB-specific
    p.add_argument("--pase-extra", type=int, default=5,
                   help="PolarDB HNSW ef_search parameter")
    p.add_argument("--pase-ds", type=int, default=0,
                   help="PolarDB distance type (0=L2, 1=IP)")
    # ClickHouse-specific
    p.add_argument("--clickhouse-host", default="127.0.0.1")
    p.add_argument("--clickhouse-port", type=int, default=9000)
    return p.parse_args()


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def sample_vectors_pg(
    host: str, port: int, user: str, password: str, database: str,
    table: str, n: int, vector_col: str = "vector",
) -> List[Tuple[int, List[float]]]:
    """Sample random vectors from a PostgreSQL table."""
    import psycopg2
    with psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=database
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, {vector_col}::text FROM {table} "
                f"ORDER BY random() LIMIT {n}"
            )
            result = []
            for row_id, txt in cur.fetchall():
                clean = txt.strip("{}[]\"'")
                vals = [float(x.strip()) for x in clean.split(",") if x.strip()]
                result.append((row_id, vals))
            return result


def get_vector_dimension(
    host: str, port: int, user: str, password: str, database: str,
    table: str, vector_col: str = "vector",
) -> int:
    """Get the dimension of a vector column."""
    import psycopg2
    with psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=database
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT vector_dims({vector_col}) FROM {table} LIMIT 1"
            )
            return cur.fetchone()[0]


def sample_vectors_clickhouse(
    host: str, port: int, table: str, n: int, vector_col: str = "vector",
) -> List[Tuple[int, List[float]]]:
    """Sample random vectors from a ClickHouse table.

    Uses clickhouse-connect library (project dependency) instead of shell command.
    Falls back to clickhouse-driver if connect is unavailable.
    """
    try:
        import clickhouse_connect as cc
    except ImportError:
        raise ImportError(
            "ClickHouse sampling requires 'clickhouse-connect' or 'clickhouse-driver'. "
            "Install with: pip install clickhouse-connect"
        )

    try:
        # Try clickhouse-connect first (recommended)
        client = cc.get_client(host=host, port=port)
        rows = client.query(
            f"SELECT id, {vector_col} FROM {table} "
            f"ORDER BY rand() LIMIT {n} "
            f"SETTINGS use_query_cache=0"
        ).result_rows
        vectors = []
        for row in rows:
            row_id = int(row[0])
            vec = list(row[1]) if hasattr(row[1], '__iter__') else [float(x) for x in str(row[1]).strip('[]').split(',')]
            vectors.append((row_id, vec))
        return vectors
    except Exception:
        pass

    try:
        from clickhouse_driver import Client as CHClient
        ch_client = CHClient(host=host, port=port)
        result = ch_client.execute(
            f"SELECT id, arrayStringConcat({vector_col}, ',') FROM {table} "
            f"ORDER BY rand() LIMIT {n} SETTINGS use_query_cache=0"
        )
        vectors = []
        for row_id, vec_str in result:
            vals = [float(x.strip()) for x in str(vec_str).split(",") if x.strip()]
            vectors.append((int(row_id), vals))
        return vectors
    except ImportError:
        raise ImportError(
            "No ClickHouse driver available. Install one of:\n"
            "  pip install clickhouse-connect\n"
            "  pip install clickhouse-driver"
        )


# ---------------------------------------------------------------------------
# PGVector SQL generators
# ---------------------------------------------------------------------------

def _vec_str(vals: List[float]) -> str:
    return ",".join(str(v) for v in vals)


def _dist_op(distance: str) -> str:
    return {"l2": "<->", "ip": "<#>", "cosine": "<=>"}[distance]


def _sort_dir(distance: str) -> str:
    return "ASC" if distance in ("l2", "cosine") else "DESC"


def generate_pgvector_sql_forms(
    table: str,
    vectors: List[Tuple[int, List[float]]],
    top_k: int,
    distance: str,
    dimension: int = 0,
    sql_types: Optional[List[str]] = None,
) -> Dict[str, str]:
    """Generate all pgvector SQL query forms."""
    dist_op = _dist_op(distance)
    sort = _sort_dir(distance)
    dim = dimension or _detect_dim_from_vectors(vectors) or 128

    if sql_types is None:
        sql_types = PGVECTOR_SQL_TYPES

    # Collectors
    collectors: Dict[str, List[str]] = {t: [] for t in sql_types}

    for rid, vec in vectors:
        v_str = _vec_str(vec)
        v_arr = "{" + v_str + "}"

        if "text_literal" in collectors:
            collectors["text_literal"].append(
                f"SELECT id, (vector {dist_op} '[{v_str}]'::vector) "
                f"AS dis FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "text_cast" in collectors:
            collectors["text_cast"].append(
                f"SELECT id, (vector {dist_op} "
                f"CAST('[{v_str}]' AS vector)) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "vector_fn" in collectors:
            collectors["vector_fn"].append(
                f"SELECT id, (vector {dist_op} "
                f"vector('[{v_str}]')) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "array_int_cast" in collectors:
            collectors["array_int_cast"].append(
                f"SELECT id, (vector {dist_op} ARRAY[{v_str}]::vector) "
                f"AS dis FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "array_real_cast" in collectors:
            collectors["array_real_cast"].append(
                f"SELECT id, (vector {dist_op} "
                f"ARRAY[{v_str}]::real[]::vector) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "array_cast" in collectors:
            collectors["array_cast"].append(
                f"SELECT id, (vector {dist_op} "
                f"CAST(ARRAY[{v_str}]::real[] AS vector)) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "array_to_vector_fn" in collectors:
            collectors["array_to_vector_fn"].append(
                f"SELECT id, (vector {dist_op} "
                f"array_to_vector(ARRAY[{v_str}]::real[], {dim}, false)) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "halfvec_literal" in collectors:
            collectors["halfvec_literal"].append(
                f"SELECT id, (vector {dist_op} "
                f"'[{v_str}]'::halfvec({dim})::vector) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "with_text_literal" in collectors:
            collectors["with_text_literal"].append(
                f"WITH query_vector AS (SELECT '[{v_str}]'::vector AS v) "
                f"SELECT t.id, (t.vector {dist_op} query_vector.v) AS dis "
                f"FROM {table} AS t CROSS JOIN query_vector "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "with_array_cast" in collectors:
            collectors["with_array_cast"].append(
                f"WITH query_vector AS "
                f"(SELECT ARRAY[{v_str}]::real[]::vector AS v) "
                f"SELECT t.id, (t.vector {dist_op} query_vector.v) AS dis "
                f"FROM {table} AS t CROSS JOIN query_vector "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "with_vector_fn" in collectors:
            collectors["with_vector_fn"].append(
                f"WITH query_vector AS "
                f"(SELECT vector('[{v_str}]') AS v) "
                f"SELECT t.id, (t.vector {dist_op} query_vector.v) AS dis "
                f"FROM {table} AS t CROSS JOIN query_vector "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "subquery_id" in collectors:
            collectors["subquery_id"].append(
                f"SELECT id, (vector {dist_op} "
                f"(SELECT vector FROM {table} WHERE id = {rid})::vector) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "with_subquery_id" in collectors:
            collectors["with_subquery_id"].append(
                f"WITH query_vector AS "
                f"(SELECT vector FROM {table} WHERE id = {rid}) "
                f"SELECT t.id, (t.vector {dist_op} query_vector.vector) AS dis "
                f"FROM {table} AS t CROSS JOIN query_vector "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

    return {t: "\n".join(collectors[t]) + "\n" for t in sql_types}


# ---------------------------------------------------------------------------
# PolarDB SQL generators
# ---------------------------------------------------------------------------

def generate_polardb_sql_forms(
    table: str,
    vectors: List[Tuple[int, List[float]]],
    top_k: int,
    distance: str,
    pase_extra: int = 5,
    pase_ds: int = 0,
    sql_types: Optional[List[str]] = None,
) -> Dict[str, str]:
    """Generate all PolarDB pase SQL query forms."""
    sort = _sort_dir(distance)

    if sql_types is None:
        sql_types = POLARDB_SQL_TYPES

    collectors: Dict[str, List[str]] = {t: [] for t in sql_types}

    for rid, vec in vectors:
        v_str = _vec_str(vec)

        if "text_pase_op_id" in collectors:
            collectors["text_pase_op_id"].append(
                f"SELECT id FROM {table} ORDER BY "
                f"vector <?> '{v_str}'::pase {sort} LIMIT {top_k};"
            )

        if "text_pase_op_extra" in collectors:
            extra_str = f"{v_str}:{pase_extra}"
            collectors["text_pase_op_extra"].append(
                f"SELECT id, (vector <?> '{extra_str}'::pase) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "text_pase_op_extra_ds" in collectors:
            extra_ds_str = f"{v_str}:{pase_extra}:{pase_ds}"
            collectors["text_pase_op_extra_ds"].append(
                f"SELECT id, (vector <?> '{extra_ds_str}'::pase) AS ds "
                f"FROM {table} ORDER BY ds {sort} LIMIT {top_k};"
            )

        if "pase_fn_text_default" in collectors:
            # 陷阱: pase(text) 构造函数绑定 pase_text_i_i (base64 解码), 逗号文本会被
            # 解出垃圾维度; 文本进 pase 唯一安全路径是 'txt'::pase 输入函数。
            collectors["pase_fn_text_default"].append(
                f"SELECT id, (vector <?> '{v_str}'::pase) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "pase_fn_array_default" in collectors:
            collectors["pase_fn_array_default"].append(
                f"SELECT id, (vector <?> "
                f"pase(ARRAY[{v_str}]::float4[])) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "pase_fn_array_extra" in collectors:
            collectors["pase_fn_array_extra"].append(
                f"SELECT id, (vector <?> "
                f"pase(ARRAY[{v_str}]::float4[], {pase_extra})) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "pase_fn_array_ip" in collectors:
            collectors["pase_fn_array_ip"].append(
                f"SELECT id, (vector <?> "
                f"pase(ARRAY[{v_str}]::float4[], {pase_extra}, 1)) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "hash_op_default" in collectors:
            collectors["hash_op_default"].append(
                f"SELECT id, vector <?> "
                f"pase(ARRAY[{v_str}]::float4[]) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "hash_op_extra" in collectors:
            collectors["hash_op_extra"].append(
                f"SELECT id, vector <?> "
                f"pase(ARRAY[{v_str}]::float4[], {pase_extra}) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "with_pase_fn_array" in collectors:
            collectors["with_pase_fn_array"].append(
                f"WITH query_pase AS "
                f"(SELECT pase(ARRAY[{v_str}]::float4[]) AS p) "
                f"SELECT id, (vector <?> query_pase.p) AS dis "
                f"FROM {table} CROSS JOIN query_pase "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "with_pase_fn_text" in collectors:
            # 同上: pase('...') 是 base64 陷阱, 用 cast 走 pase_in
            collectors["with_pase_fn_text"].append(
                f"WITH query_pase AS "
                f"(SELECT '{v_str}'::pase AS p) "
                f"SELECT id, (vector <?> query_pase.p) AS dis "
                f"FROM {table} CROSS JOIN query_pase "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "subquery_id" in collectors:
            collectors["subquery_id"].append(
                f"SELECT id, (vector <?> "
                f"(SELECT pase(vector) FROM {table} WHERE id = {rid})) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "with_subquery_id" in collectors:
            collectors["with_subquery_id"].append(
                f"WITH query_pase AS "
                f"(SELECT (SELECT pase(vector) FROM {table} WHERE id = {rid}) AS v) "
                f"SELECT id, (vector <?> query_pase.v) AS dis "
                f"FROM {table} CROSS JOIN query_pase "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

    return {t: "\n".join(collectors[t]) + "\n" for t in sql_types}


# ---------------------------------------------------------------------------
# ClickHouse SQL generators
# ---------------------------------------------------------------------------

def _ch_dist_func(distance: str) -> str:
    return {"l2": "L2Distance", "ip": "cosineDistance", "cosine": "cosineDistance"}[distance]


def generate_clickhouse_sql_forms(
    table: str,
    vectors: List[Tuple[int, List[float]]],
    top_k: int,
    distance: str,
    dimension: int = 0,
    sql_types: Optional[List[str]] = None,
    engine: str = "clickhouse",
    search_params: Optional[dict] = None,
) -> Dict[str, str]:
    """Generate all ClickHouse SQL query forms.

    ``engine`` distinguishes the two ClickHouse-family servers, which index
    different function families:

    * ``clickhouse`` (the ``xxb-vectorqueryplancache`` branch) accelerates the
      plain scalar helpers ``L2Distance`` / ``cosineDistance`` in an
      ORDER BY ... LIMIT query, so the forms keep those names.
    * ``myscale`` (the ``myscaledb-oss-queryplancache`` branch) only routes a
      query through its ANN vector index when the top-level function belongs to
      the ``distance(...)`` family (the planner prefix-matches the function
      name; ``L2Distance`` etc. only produce a full-table scan).  For that
      engine the same query-vector literal spellings are therefore wrapped in
      ``distance('key=value', ...)(vector, query)``, mirroring how
      ``engine/clients/myscale/search.py::vector_search`` issues the query.
      ``search_params`` supplies the tuple arguments (e.g. ``ef_s``) and is
      ignored for any other engine.
    """
    dist_func = _ch_dist_func(distance)
    is_myscale = engine.lower() == "myscale"
    sort = _sort_dir(distance)
    dim = dimension or _detect_dim_from_vectors(vectors) or 768

    # distance() search parameters rendered as its curried tuple, e.g.
    # "('ef_s=100', 'k=20')" -- the exact form run.py sends via the MyScale
    # client.  A bare `distance(vector, q)` is used when none are configured.
    dist_params = ""
    if is_myscale and search_params:
        rendered = []
        for key, value in dict(search_params).items():
            if isinstance(value, (list, tuple)):
                value = value[0] if value else None
            if value is None:
                continue
            if isinstance(value, bool):
                value = int(value)
            rendered.append(f"'{key}={value}'")
        if rendered:
            dist_params = "(" + ", ".join(rendered) + ")"

    if sql_types is None:
        sql_types = CLICKHOUSE_SQL_TYPES

    collectors: Dict[str, List[str]] = {t: [] for t in sql_types}

    def distance_expr(query_expr: str) -> str:
        if is_myscale:
            return f"distance{dist_params}(vector, {query_expr})"
        return f"{dist_func}(vector, {query_expr})"

    def select_sql(query_expr: str, order: str = sort) -> str:
        return (
            f"SELECT id, {distance_expr(query_expr)} AS dis FROM {table} "
            f"ORDER BY dis {order} LIMIT {top_k};"
        )

    def with_sql(query_expr: str) -> str:
        return (
            f"WITH {query_expr} AS query_vector SELECT id, "
            f"{distance_expr('query_vector')} AS dis FROM {table} "
            f"ORDER BY dis {sort} LIMIT {top_k};"
        )

    for rid, vec in vectors:
        v_str = _vec_str(vec)

        if "normal" in collectors:
            collectors["normal"].append(select_sql(f"[{v_str}]"))

        if "cast" in collectors:
            collectors["cast"].append(select_sql(f"CAST([{v_str}] AS Array(Float32))"))

        if "cast_array" in collectors:
            collectors["cast_array"].append(select_sql(f"cast('[{v_str}]','Array(Float32)')"))

        if "raw_bytes" in collectors:
            collectors["raw_bytes"].append(select_sql(f"[{v_str}]::Array(Float32)"))

        if "raw_bytes_x" in collectors:
            collectors["raw_bytes_x"].append(select_sql(f"[{v_str}]"))

        if "type_hint" in collectors:
            collectors["type_hint"].append(
                select_sql(f"arrayMap(x -> toFloat32(x), [{v_str}])")
            )

        if "with" in collectors:
            collectors["with"].append(with_sql(f"[{v_str}]"))
        if "with_cast" in collectors:
            collectors["with_cast"].append(with_sql(f"CAST([{v_str}] AS Array(Float32))"))
        if "with_cast_array" in collectors:
            collectors["with_cast_array"].append(with_sql(f"cast('[{v_str}]','Array(Float32)')"))
        if "with_type_hint" in collectors:
            collectors["with_type_hint"].append(
                with_sql(f"arrayMap(x -> toFloat32(x), [{v_str}])")
            )
        if "with_raw_bytes" in collectors:
            collectors["with_raw_bytes"].append(with_sql(f"[{v_str}]::Array(Float32)"))
        if "with_raw_bytes_x" in collectors:
            collectors["with_raw_bytes_x"].append(with_sql(f"[{v_str}]"))

    return {t: "\n".join(collectors[t]) + "\n" for t in sql_types}


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _detect_dim_from_vectors(vectors: List[Tuple[int, List[float]]]) -> int:
    if vectors:
        return len(vectors[0][1])
    return 0


def _engine_to_sql_dir(engine: str) -> str:
    mapping = {
        "pgvector": "pgvector-query-forms",
        "polardb": "polardb-pase-query-forms",
        "clickhouse": "vector-query-forms",
        "myscale": "vector-query-forms",
    }
    return mapping.get(engine, engine)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    args = parse_args()

    if args.config:
        config_path = Path(args.config)
        if not config_path.exists():
            print(f"Error: config not found: {config_path}", file=sys.stderr)
            sys.exit(1)
        with open(config_path) as f:
            config = json.load(f)
        for key, value in config.items():
            key_clean = key.replace("-", "_")
            if hasattr(args, key_clean) and value is not None and not isinstance(value, list):
                setattr(args, key_clean, value)

    if not args.table:
        print("Error: --table is required", file=sys.stderr)
        sys.exit(1)

    engine = args.engine
    sql_dir = _engine_to_sql_dir(engine)
    output_dir = Path(args.output_dir) / sql_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine which SQL types to generate
    if args.sql_types:
        sql_types = args.sql_types
    else:
        type_map = {
            "pgvector": PGVECTOR_SQL_TYPES,
            "polardb": POLARDB_SQL_TYPES,
            "clickhouse": CLICKHOUSE_SQL_TYPES,
            "myscale": CLICKHOUSE_SQL_TYPES,
        }
        sql_types = type_map.get(engine, [])

    # Sample vectors
    if engine in ("pgvector", "polardb"):
        print(f"Connecting to PostgreSQL at {args.host}:{args.port}...")
        print(f"Sampling {args.query_count} vectors from {args.table}...")
        vectors = sample_vectors_pg(
            args.host, args.port, args.user, args.password,
            args.database, args.table, args.query_count,
        )
        # Auto-detect dimension if not provided
        if args.dimension <= 0:
            try:
                args.dimension = get_vector_dimension(
                    args.host, args.port, args.user, args.password,
                    args.database, args.table,
                )
                print(f"Auto-detected vector dimension: {args.dimension}")
            except Exception:
                args.dimension = _detect_dim_from_vectors(vectors)
                print(f"Fallback dimension from data: {args.dimension}")
    elif engine in ("clickhouse", "myscale"):
        print(f"Connecting to ClickHouse at {args.clickhouse_host}:{args.clickhouse_port}...")
        print(f"Sampling {args.query_count} vectors from {args.table}...")
        vectors = sample_vectors_clickhouse(
            args.clickhouse_host, args.clickhouse_port, args.table, args.query_count,
        )
        if args.dimension <= 0:
            args.dimension = _detect_dim_from_vectors(vectors)
    else:
        print(f"Error: unsupported engine {engine}", file=sys.stderr)
        sys.exit(1)

    print(f"Sampled: {len(vectors)} vectors")

    # Generate SQL forms
    if engine == "pgvector":
        forms = generate_pgvector_sql_forms(
            args.table, vectors, args.top_k, args.distance,
            args.dimension, sql_types,
        )
    elif engine == "polardb":
        forms = generate_polardb_sql_forms(
            args.table, vectors, args.top_k, args.distance,
            args.pase_extra, args.pase_ds, sql_types,
        )
    elif engine in ("clickhouse", "myscale"):
        forms = generate_clickhouse_sql_forms(
            args.table, vectors, args.top_k, args.distance,
            args.dimension, sql_types, engine=engine,
        )
    else:
        print(f"Error: unknown engine {engine}", file=sys.stderr)
        sys.exit(1)

    # Write SQL files
    for form_name, sql_content in forms.items():
        filename = f"{engine}_{args.table}_{form_name}_{args.query_count}.sql"
        filepath = output_dir / filename
        with open(filepath, "w") as f:
            f.write(sql_content)
        line_count = sql_content.strip().count("\n") + (1 if sql_content.strip() else 0)
        print(f"  Generated: {filepath} ({line_count} queries)")

    print(f"\nDone! SQL files in: {output_dir}")


if __name__ == "__main__":
    main()