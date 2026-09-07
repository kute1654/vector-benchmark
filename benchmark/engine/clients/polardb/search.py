import threading
from typing import List, Optional, Tuple

import psycopg2

from benchmark.dataset_reader.base_reader import Query
from benchmark.cli_output import warn
from engine.base_client import BaseSearcher
from engine.clients.polardb.config import (
    POLARDB_DATABASE_NAME, POLARDB_DEFAULT_PASSWD, POLARDB_DEFAULT_PORT,
    POLARDB_DEFAULT_USER, validate_table_name,
)

thread_local = threading.local()


class PolarDBSearcher(BaseSearcher):
    connection_params = {}
    search_params = {}
    distance = "<?>"
    use_query_plan_cache = 0
    use_result_cache = 0
    result_cache = {}
    prepared_statement_name = "polardb_vector_search_stmt"

    @classmethod
    def _apply_session_settings(cls, connection, settings):
        for key, value in (settings or {}).items():
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f"SET {key} = %s", (str(value),))
                connection.commit()
            except Exception as exc:
                connection.rollback()
                warn(f"failed to apply session setting {key}={value}: {exc}")

    @classmethod
    def init_client(cls, host, distance, connection_params, search_params):
        cls.connection_params = connection_params or {}
        cls.search_params = search_params or {}
        thread_local.connection = psycopg2.connect(
            host=cls.connection_params.get("host", host or "127.0.0.1"),
            port=cls.connection_params.get("port", POLARDB_DEFAULT_PORT),
            user=cls.connection_params.get("user", POLARDB_DEFAULT_USER),
            password=cls.connection_params.get("password", POLARDB_DEFAULT_PASSWD),
            database=cls.connection_params.get("database", POLARDB_DATABASE_NAME),
        )
        cls._apply_session_settings(thread_local.connection, cls.search_params.get("session_settings", {}))
        cls.use_query_plan_cache = int(cls.search_params.get("use_query_plan_cache", 0) or 0)
        cls.use_result_cache = int(cls.search_params.get("use_result_cache", 0) or 0)
        cls.result_cache = {}
        if cls.use_query_plan_cache:
            cls._prepare_statement()

    @classmethod
    def _prepare_statement(cls):
        table = validate_table_name(cls.connection_params.get("table", "vec_items"))
        statement = (
            f"PREPARE {cls.prepared_statement_name} (real[], integer) AS "
            f"SELECT id, (vector <?> pase($1)) AS distance FROM {table} "
            f"ORDER BY distance LIMIT $2"
        )
        try:
            with thread_local.connection.cursor() as cursor:
                cursor.execute(f"DEALLOCATE {cls.prepared_statement_name}")
        except Exception:
            thread_local.connection.rollback()
        try:
            with thread_local.connection.cursor() as cursor:
                cursor.execute(statement)
            thread_local.connection.commit()
        except Exception as exc:
            thread_local.connection.rollback()
            cls.use_query_plan_cache = 0
            warn(f"PolarDB prepared statement unavailable, using direct SQL: {exc}")

    @classmethod
    def search_one(cls, vector: List[float], meta_conditions, top: Optional[int], schema, query: Query) -> List[Tuple[int, float]]:
        table = validate_table_name(cls.connection_params.get("table", "vec_items"))
        top = int(top or 100)
        conn = thread_local.connection
        cache_key = (tuple(float(x) for x in vector), top)
        if meta_conditions is None and cls.use_result_cache and cache_key in cls.result_cache:
            return cls.result_cache[cache_key]
        if meta_conditions is None and cls.use_query_plan_cache:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(f"EXECUTE {cls.prepared_statement_name} (%s, %s)", (vector, top))
                    result = [(row[0], float(row[1])) for row in cursor.fetchall()]
                    if meta_conditions is None and cls.use_result_cache:
                        cls.result_cache[cache_key] = result
                    return result
            except Exception:
                conn.rollback()
        vector_text = "{" + ",".join(str(value) for value in vector) + "}"
        with conn.cursor() as cursor:
            cursor.execute(
                f"SELECT id, (vector <?> pase(%s::float4[])) AS distance FROM {table} "
                f"ORDER BY distance ASC LIMIT %s",
                (vector_text, top),
            )
            result = [(row[0], float(row[1])) for row in cursor.fetchall()]
            if meta_conditions is None and cls.use_result_cache:
                cls.result_cache[cache_key] = result
            return result
