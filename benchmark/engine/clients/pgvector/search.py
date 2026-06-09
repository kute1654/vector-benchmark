import threading
import json
from typing import List, Optional, Tuple

import psycopg2

from dataset_reader.base_reader import Query
from engine.base_client import BaseSearcher
from benchmark.cli_output import warn
from engine.clients.pgvector.config import *


thread_local = threading.local()


class PGVectorSearcher(BaseSearcher):
    search_params = {}
    connection = None
    distance_op: str = None
    host: str = None
    connection_params: dict = {}
    use_query_plan_cache: int = 0
    prepared_statement_name: str = "pgvector_search_stmt"

    def __init__(self, host, connection_params, search_params):
        # Set default connection parameters for PGVector
        default_conn_params = {
            "host": "127.0.0.1",
            "port": PGVECTOR_DEFAULT_PORT,
            "user": PGVECTOR_DEFAULT_USER,
            "password": PGVECTOR_DEFAULT_PASSWD,
            "database": PGVECTOR_DATABASE_NAME,
            "table": "vec_items"
        }
        
        # Merge provided connection_params with defaults
        merged_conn_params = {**default_conn_params, **(connection_params or {})}
        
        super().__init__(host, merged_conn_params, search_params)

    def setup_search(self, host, distance, connection_params: dict, search_params: dict, dataset_config):
        pass

    @classmethod
    def init_client(
            cls, host: str, distance, connection_params: dict, search_params: dict
    ):
        cls.connection_params = connection_params
        cls.host = host
        cls.distance_op = DISTANCE_MAPPING[distance]
        cls.search_params = search_params
        # Read use_query_plan_cache from search_params (which is set by base client during cache mode iteration)
        cls.use_query_plan_cache = int(search_params.get("use_query_plan_cache", 0))
        
        # Create connection per thread
        cls.connection = psycopg2.connect(
            host=connection_params.get("host", "127.0.0.1"),
            port=connection_params.get("port", PGVECTOR_DEFAULT_PORT),
            user=connection_params.get("user", PGVECTOR_DEFAULT_USER),
            password=connection_params.get("password", PGVECTOR_DEFAULT_PASSWD),
            database=connection_params.get("database", PGVECTOR_DATABASE_NAME),
        )
        
        # If using cache (prepared statements), prepare the statement
        if cls.use_query_plan_cache == 1:
            cls._prepare_statement()

    @classmethod
    def _prepare_statement(cls):
        """Prepare the search statement for reuse"""
        table_name = validate_table_name(cls.connection_params.get("table", "vec_items"))
        # Prepare statement with placeholders for vector and limit
        # Note: We can't prepare the full WHERE clause dynamically, so we'll handle metadata conditions differently
        # For now, we'll only prepare the basic vector search without metadata conditions
        prepare_sql = f"PREPARE {cls.prepared_statement_name} (vector, integer) AS SELECT id, vector {cls.distance_op} $1 AS distance FROM {table_name} ORDER BY distance LIMIT $2"
        # print(prepare_sql)
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(prepare_sql)
                cls.connection.commit()
        except Exception as e:
            # If preparation fails, fall back to direct execution
            cls.use_query_plan_cache = 0
            warn(f"Failed to prepare statement, falling back to direct execution: {e}")

    @classmethod
    def get_connection(cls):
        return cls.connection

    @classmethod
    def vector_search(cls, vector: List[float], meta_conditions, top: Optional[int]) -> List[Tuple[int, float]]:
        table_name = validate_table_name(cls.connection_params.get("table", "vec_items"))
        
        # Convert vector to string format
        vector_str = '[' + ','.join(str(x) for x in vector) + ']'
        
        # If there are metadata conditions or we're not using cache, use direct execution
        if meta_conditions is not None or cls.use_query_plan_cache == 0:
            return cls._direct_search(vector_str, meta_conditions, top, table_name)
        else:
            # Use prepared statement for simple vector search without metadata
            return cls._prepared_search(vector_str, top)

    @classmethod
    def _direct_search(cls, vector_str: str, meta_conditions, top: Optional[int], table_name: str) -> List[Tuple[int, float]]:
        """Execute direct SQL search"""
        # Build base query
        search_str = f"SELECT id, vector {cls.distance_op} %s::vector AS distance FROM {table_name}"
        # Add metadata conditions if present
        if meta_conditions is not None:
            where_clause = cls._build_where_clause(meta_conditions)
            if where_clause:
                search_str += f" WHERE {where_clause}"
        
        # Add ordering and limit
        search_str += f" ORDER BY distance LIMIT {top}"
        
        res_list = []
        try:
            with cls.get_connection().cursor() as cursor:
                cursor.execute(search_str, (vector_str,))
                results = cursor.fetchall()
                for row in results:
                    res_list.append((row[0], float(row[1])))
        except Exception as e:
            raise RuntimeError(f"Search failed: {e}")

        return res_list

    @classmethod
    def _prepared_search(cls, vector_str: str, top: Optional[int]) -> List[Tuple[int, float]]:
        """Execute prepared statement search"""
        res_list = []
        try:
            with cls.get_connection().cursor() as cursor:
                # print(f"EXECUTE {cls.prepared_statement_name} (%s::vector, %s)")
                cursor.execute(f"EXECUTE {cls.prepared_statement_name} (%s::vector, %s)", (vector_str, top))
                
                results = cursor.fetchall()
                for row in results:
                    res_list.append((row[0], float(row[1])))
        except Exception as e:
            # If prepared statement fails, fall back to direct execution
            table_name = validate_table_name(cls.connection_params.get("table", "vec_items"))
            warn(f"Prepared statement execution failed, falling back to direct execution: {e}")
            return cls._direct_search(vector_str, None, top, table_name)

        return res_list

    @classmethod
    def _build_where_clause(cls, meta_conditions):
        """Build WHERE clause from metadata conditions"""
        if not meta_conditions:
            return ""
        
        conditions = []
        for key, value in meta_conditions.items():
            if isinstance(value, dict):
                # Handle range queries
                if 'gte' in value or 'lte' in value:
                    col_conditions = []
                    if 'gte' in value:
                        col_conditions.append(f"{key} >= {value['gte']}")
                    if 'lte' in value:
                        col_conditions.append(f"{key} <= {value['lte']}")
                    if col_conditions:
                        conditions.append(" AND ".join(col_conditions))
                else:
                    # Handle exact match for other dict values
                    conditions.append(f"{key} = '{json.dumps(value)}'")
            elif isinstance(value, list):
                # Handle IN queries
                if value:
                    placeholders = ", ".join(f"'{v}'" for v in value)
                    conditions.append(f"{key} IN ({placeholders})")
            else:
                # Handle exact match
                if isinstance(value, str):
                    conditions.append(f"{key} = '{value}'")
                else:
                    conditions.append(f"{key} = {value}")
        
        return " AND ".join(conditions) if conditions else ""

    @classmethod
    def search_one(cls, vector: List[float], meta_conditions, top: Optional[int], schema, query: Query) -> List[Tuple[int, float]]:
        return cls.vector_search(vector, meta_conditions, top)