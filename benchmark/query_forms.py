"""Run one query-form QPS comparison across all configured engines.

The regular benchmark measures one canonical query per engine.  This module
measures every SQL shape emitted by :mod:`generate_sql_files` using the same
sampled query pool, concurrency, duration and cache profile, so the result is
directly comparable.  Table creation and index building remain owned by
``benchmark.run``; use ``python run.py`` once per target before this command.

Cache profiles on the ClickHouse-family engines are expressed through the query's
own SETTINGS clause (e.g. ``... SETTINGS vector_query_plan_cache=1``).  For those
per-query settings to reach the server-side query-plan-cache probe, the server must
run with the ``enable_vector_performance_test`` mode on; each worker turns it on for
its session (``_session_setup``) and the server caches are cleared once per profile
(``_clear_server_caches``).  PostgreSQL-compatible engines keep using prepared
statements (``PREPARE``/``EXECUTE``) for their plan cache.

ClickHouse and MyScale listen on the same port and cannot be up at the same time, so
they are measured in separate, sequential invocations: ``--engines clickhouse`` while
the ClickHouse server runs, then ``--engines myscale`` while the MyScale server runs.
A single invocation that would measure two engines against the same host:port is
rejected up front (``_check_endpoint_conflicts``).
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

from .generate_sql_files import (
    CLICKHOUSE_SQL_TYPES,
    PGVECTOR_SQL_TYPES,
    POLARDB_SQL_TYPES,
    generate_clickhouse_sql_forms,
    generate_pgvector_sql_forms,
    generate_polardb_sql_forms,
    sample_vectors_clickhouse,
    sample_vectors_pg,
)


def _int_list(value: str) -> list[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def _load_targets(path: str) -> list[dict[str, Any]]:
    source = Path(path)
    if source.is_dir():
        targets = []
        for item in sorted(source.glob("*.json")):
            targets.extend(_load_targets(str(item)))
        return targets
    with source.open(encoding="utf-8") as fp:
        payload = json.load(fp)
    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list):
        raise ValueError(f"{path} must contain a JSON object or array")
    return [dict(item) for item in payload if isinstance(item, dict) and item.get("engine")]


def _query_without_settings(sql: str) -> str:
    return sql.rstrip().rstrip(";").rstrip()


def _setting_clause(engine: str, profile: dict[str, Any]) -> str:
    if engine in {"clickhouse", "myscale"}:
        settings = []
        for key, value in profile.items():
            if key in {"name", "mode"}:
                continue
            if isinstance(value, bool):
                value = int(value)
            if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
                value = "'" + value.replace("'", "\'") + "'"
            settings.append(f"{key}={value}")
        return (" SETTINGS " + ", ".join(settings)) if settings else ""
    return ""


def _profiles(target: dict[str, Any], requested: list[str]) -> list[dict[str, Any]]:
    configured = target.get("cache_profiles") or []
    if not configured:
        engine = str(target["engine"]).lower()
        if engine == "myscale":
            # MyScale plan-cache settings. Each profile is expressed as the server-side
            # setting names the myscaledb-oss-queryplancache branch understands. The values
            # are attached to the query's own SETTINGS clause and only take effect for the
            # plan-cache probe once `enable_vector_performance_test` is on at the session.
            configured = [
                {"name": "off", "enable_query_plan_cache": 0, "enable_cast_vector": 0, "query_plan_cache_only_vector": 0, "use_query_cache": 0},
                {"name": "result-cache", "enable_query_plan_cache": 0, "enable_cast_vector": 0, "query_plan_cache_only_vector": 0, "use_query_cache": 1},
                {"name": "plan-cache", "enable_query_plan_cache": 1, "enable_cast_vector": 0, "query_plan_cache_only_vector": 0, "use_query_cache": 0},
                {"name": "plan-cast", "enable_query_plan_cache": 1, "enable_cast_vector": 1, "query_plan_cache_only_vector": 0, "use_query_cache": 0},
                {"name": "plan-only-vector", "enable_query_plan_cache": 1, "enable_cast_vector": 0, "query_plan_cache_only_vector": 1, "use_query_cache": 0},
                {"name": "plan-result-cache", "enable_query_plan_cache": 1, "enable_cast_vector": 0, "query_plan_cache_only_vector": 0, "use_query_cache": 1},
            ]
        elif engine == "clickhouse":
            configured = [
                {"name": "off", "vector_query_plan_cache": 0, "vector_use_cast": 0, "vector_query_plan_cache_only_vector": 0, "use_query_cache": 0},
                {"name": "result-cache", "vector_query_plan_cache": 0, "vector_use_cast": 0, "vector_query_plan_cache_only_vector": 0, "use_query_cache": 1},
                {"name": "plan-cache", "vector_query_plan_cache": 1, "vector_use_cast": 0, "vector_query_plan_cache_only_vector": 0, "use_query_cache": 0},
                {"name": "plan-cast", "vector_query_plan_cache": 1, "vector_use_cast": 1, "vector_query_plan_cache_only_vector": 0, "use_query_cache": 0},
                {"name": "plan-only-vector", "vector_query_plan_cache": 1, "vector_use_cast": 0, "vector_query_plan_cache_only_vector": 1, "use_query_cache": 0},
                {"name": "plan-result-cache", "vector_query_plan_cache": 1, "vector_use_cast": 0, "vector_query_plan_cache_only_vector": 0, "use_query_cache": 1},
            ]
        else:
            configured = [
                {"name": "direct", "use_query_plan_cache": 0},
                {"name": "result-cache", "use_query_plan_cache": 0, "use_result_cache": 1},
                {"name": "prepared-plan", "use_query_plan_cache": 1},
                {"name": "prepared-result-cache", "use_query_plan_cache": 1, "use_result_cache": 1},
            ]
    if not requested:
        return [dict(x) for x in configured]
    names = set(requested)
    return [dict(x) for x in configured if str(x.get("name")) in names]


def _check_endpoint_conflicts(targets: list[dict[str, Any]]) -> None:
    """Refuse to measure two engines against the same host:port in one run.

    ClickHouse and MyScale both listen on the same port (typically 9000), so at most one
    of them can be up at a time.  A config that lists both on the same endpoint would
    silently query whichever server happens to own the port, and the other engine's
    queries would be executed with setting names its server ignores - giving fake
    plan-cache numbers.  Such engines must be measured in separate, sequential runs
    (``--engines <target>``), each against its own running server.
    """
    owner: dict[tuple[str, int], str] = {}
    for target in targets:
        conn = target.get("connection_params") or {}
        endpoint = (conn.get("host", "127.0.0.1"), int(conn.get("port", 9000)))
        engine = str(target.get("engine", "")).lower()
        other = owner.get(endpoint)
        if other is not None and other != engine:
            raise SystemExit(
                f"targets of engines '{other}' and '{engine}' both use "
                f"{endpoint[0]}:{endpoint[1]}, which the two databases cannot share at the "
                f"same time. Run them sequentially, one engine per invocation, e.g. "
                f"'--engines {other}' then '--engines {engine}', and write each result to "
                f"its own CSV."
            )
        owner[endpoint] = engine


def _session_setup(engine: str) -> list[str]:
    """Session-level SQL each worker must run once on its connection.

    The ClickHouse and MyScale branches drive the query plan cache from the query's own
    SETTINGS clause (see ``_setting_clause``). That only works while the server is in
    ``enable_vector_performance_test`` mode: without it, per-query SETTINGS clauses are
    applied after AST parsing, i.e. after the plan-cache probe has already run. The flag
    is session state (putting it in the SETTINGS clause would be circular), so every worker
    enables it on its own session before measuring.
    """
    if engine not in {"clickhouse", "myscale"}:
        return []
    return ["SET enable_vector_performance_test = 1"]


def _clear_server_caches(target: dict[str, Any], profile: dict[str, Any]) -> None:
    """Start each cache profile from an empty server cache so its first QPS row is
    measured the same way across runs."""
    engine = str(target["engine"]).lower()
    if engine not in {"clickhouse", "myscale"}:
        return
    drops = []
    if int(profile.get("use_query_cache", 0) or 0):
        drops.append("SYSTEM DROP QUERY CACHE")
    if engine == "clickhouse" and int(profile.get("vector_query_plan_cache", 0) or 0):
        drops.append("SYSTEM DROP VECTOR QUERY PLAN CACHE")
    if engine == "myscale" and int(profile.get("enable_query_plan_cache", 0) or 0):
        drops.append("SYSTEM DROP QUERY PLAN CACHE")
    if not drops:
        return
    connection = _connect(target)
    try:
        for statement in drops:
            connection.execute(statement)
    except Exception as exc:  # noqa: BLE001 - the benchmark must survive unprivileged users
        print(f"warning: failed to clear server caches for {profile.get('name', '?')}: {exc}")
    finally:
        if hasattr(connection, "close"):
            connection.close()


def _sample(target: dict[str, Any], count: int):
    engine = str(target["engine"]).lower()
    conn = target.get("connection_params") or {}
    table = conn.get("table") or target.get("table")
    if not table:
        raise ValueError(f"target {target.get('name', engine)} has no connection_params.table")
    if engine in {"pgvector", "polardb"}:
        return sample_vectors_pg(
            conn.get("host", "127.0.0.1"), int(conn.get("port", 5432)),
            conn.get("user", "postgres"), conn.get("password", ""),
            conn.get("database", "postgres"), table, count,
        )
    return sample_vectors_clickhouse(
        conn.get("host", "127.0.0.1"), int(conn.get("port", 9000)), table, count
    )


def _forms(target: dict[str, Any], vectors):
    engine = str(target["engine"]).lower()
    conn = target.get("connection_params") or {}
    table = conn["table"]
    top = int(target.get("top_k", 10))
    distance = target.get("distance", "l2")
    dimension = int(target.get("dimension", 0) or 0)
    selected = target.get("sql_types")
    if engine == "pgvector":
        return generate_pgvector_sql_forms(table, vectors, top, distance, dimension, selected or PGVECTOR_SQL_TYPES)
    if engine == "polardb":
        return generate_polardb_sql_forms(
            table, vectors, top, distance, int(target.get("pase_extra", 5)),
            int(target.get("pase_ds", 0)), selected or POLARDB_SQL_TYPES,
        )
    # The ClickHouse-family generator serves both engines: engine="clickhouse"
    # keeps the plain L2Distance/cosineDistance forms, while engine="myscale"
    # emits distance(...)(vector, q) forms and receives the search parameters
    # (ef_s etc.) nested under search_params.params exactly as run.py's MyScale
    # configuration expresses them.
    search_params = None
    if engine == "myscale":
        sp = target.get("search_params") or {}
        search_params = sp.get("params") if isinstance(sp, dict) else None
        if not isinstance(search_params, dict):
            search_params = {}
    return generate_clickhouse_sql_forms(
        table, vectors, top, distance, dimension,
        selected or CLICKHOUSE_SQL_TYPES, engine=engine, search_params=search_params,
    )


def _connect(target: dict[str, Any]):
    engine = str(target["engine"]).lower()
    conn = target.get("connection_params") or {}
    if engine in {"pgvector", "polardb"}:
        import psycopg2
        return psycopg2.connect(
            host=conn.get("host", "127.0.0.1"), port=int(conn.get("port", 5432)),
            user=conn.get("user", "postgres"), password=conn.get("password", ""),
            dbname=conn.get("database", "postgres"),
        )
    from clickhouse_driver import Client
    return Client(
        host=conn.get("host", "127.0.0.1"), port=int(conn.get("port", 9000)),
        user=conn.get("user", "default"), password=conn.get("password", ""),
        database=conn.get("database", "default"),
    )


def _execute(connection, engine: str, statement: str):
    if engine in {"pgvector", "polardb"}:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            rows = cursor.fetchall()
        connection.rollback()
        return rows
    else:
        return connection.execute(statement)


def _prepare(connection, engine: str, statements: list[str], profile: dict[str, Any]) -> list[str]:
    """Prepare sampled statements for PostgreSQL-compatible engines."""
    if engine not in {"pgvector", "polardb"} or not int(profile.get("use_query_plan_cache", 0) or 0):
        return []
    names = []
    for index, statement in enumerate(statements):
        name = f"vb_qf_{index}"
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DEALLOCATE {name}")
        except Exception:
            connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute(f"PREPARE {name} AS {statement}")
        connection.commit()
        names.append(name)
    return names


def _measure(target, sql_statements: list[str], profile: dict[str, Any], concurrency: int, duration: float):
    engine = str(target["engine"]).lower()
    statements = [_query_without_settings(sql) + _setting_clause(engine, profile) for sql in sql_statements]
    stop = threading.Event()
    counts = [0] * int(concurrency)
    errors = [0] * int(concurrency)

    def worker(slot: int):
        connection = _connect(target)
        try:
            # Enable the per-query-SETTINGS-clause plan-cache path on this session
            # (ClickHouse-family engines only; no-op for pg/polardb).
            for setup_statement in _session_setup(engine):
                connection.execute(setup_statement)
            prepared = _prepare(connection, engine, statements, profile)
        except Exception:
            # A server may reject one particular SQL shape when preparing it.
            # Keep that form in the comparison and measure its direct path.
            connection.rollback()
            prepared = []
        result_cache = {}
        use_result_cache = int(profile.get("use_result_cache", profile.get("use_query_cache", 0)) or 0)
        rng = random.Random(slot + 17)
        try:
            while not stop.is_set():
                try:
                    index = rng.randrange(len(statements))
                    statement_key = index
                    if use_result_cache and statement_key in result_cache:
                        result_cache[statement_key]
                    elif prepared:
                        with connection.cursor() as cursor:
                            cursor.execute(f"EXECUTE {prepared[index]}")
                            rows = cursor.fetchall()
                        connection.rollback()
                        if use_result_cache:
                            result_cache[statement_key] = rows
                    else:
                        rows = _execute(connection, engine, statements[index])
                        if use_result_cache:
                            result_cache[statement_key] = rows
                    counts[slot] += 1
                except Exception:
                    errors[slot] += 1
                    if engine in {"pgvector", "polardb"}:
                        connection.rollback()
        finally:
            if hasattr(connection, "close"):
                connection.close()

    with ThreadPoolExecutor(max_workers=int(concurrency)) as pool:
        futures = [pool.submit(worker, i) for i in range(int(concurrency))]
        start = time.perf_counter()
        time.sleep(max(0.01, float(duration)))
        stop.set()
        for future in futures:
            future.result()
    elapsed = max(0.001, time.perf_counter() - start)
    return sum(counts) / elapsed, sum(errors)


def run(args) -> int:
    if args.query_count <= 0:
        raise SystemExit("--query-count must be greater than zero")
    if args.concurrency <= 0:
        raise SystemExit("--concurrency must be greater than zero")
    if args.duration <= 0:
        raise SystemExit("--duration must be greater than zero")
    targets = _load_targets(args.config)
    if args.engines:
        wanted = set(args.engines.split(","))
        targets = [t for t in targets if t.get("name") in wanted or t.get("engine") in wanted]
    if not targets:
        raise SystemExit("no matching query-form targets")
    _check_endpoint_conflicts(targets)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    fields = ["timestamp", "target", "engine", "table", "cache_profile", "sql_type", "queries", "concurrency", "duration_s", "qps", "errors"]
    with output.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        for target in targets:
            vectors = _sample(target, args.query_count)
            forms = _forms(target, vectors)
            for profile in _profiles(target, args.cache_profiles):
                _clear_server_caches(target, profile)
                for sql_type, sql_text in forms.items():
                    statements = [line for line in sql_text.splitlines() if line.strip()]
                    qps, errors = _measure(target, statements, profile, args.concurrency, args.duration)
                    row = {
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "target": target.get("name", target["engine"]),
                        "engine": target["engine"],
                        "table": target.get("connection_params", {}).get("table", ""),
                        "cache_profile": profile.get("name", "custom"),
                        "sql_type": sql_type, "queries": len(statements),
                        "concurrency": args.concurrency, "duration_s": args.duration,
                        "qps": f"{qps:.3f}", "errors": errors,
                    }
                    writer.writerow(row)
                    fp.flush()
                    print(f"{row['target']} {row['cache_profile']} {sql_type}: QPS={row['qps']} errors={errors}")
    print(f"saved: {output}")
    return 0


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Compare all vector SQL query forms and cache profiles")
    parser.add_argument("--config", required=True, help="JSON target file or directory")
    parser.add_argument("--engines", default="", help="comma-separated target name or engine filter")
    parser.add_argument("--query-count", type=int, default=100, help="sampled query vectors per target")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--duration", type=float, default=10)
    parser.add_argument("--cache-profiles", nargs="*", default=[], help="profile names; default is off/result/plan/combined")
    parser.add_argument("--output", default="results/query-forms-qps.csv")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
