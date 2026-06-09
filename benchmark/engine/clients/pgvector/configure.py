import psycopg2
from psycopg2 import sql
import signal
import time

from benchmark.cli_output import compact_kv, sql as sql_log, step
from engine.base_client.configure import BaseConfigurator
from engine.clients.pgvector.config import *


class PGVectorConfigurator(BaseConfigurator):
    connection = None
    table_name: str = None

    def __init__(self, host, collection_params: dict, connection_params: dict):
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
        
        super().__init__(host, collection_params, merged_conn_params)

    @classmethod
    def init_client(cls, connection_params):
        cls.connection = psycopg2.connect(
            host=connection_params.get("host", "127.0.0.1"),
            port=connection_params.get("port", PGVECTOR_DEFAULT_PORT),
            user=connection_params.get("user", PGVECTOR_DEFAULT_USER),
            password=connection_params.get("password", PGVECTOR_DEFAULT_PASSWD),
            database=connection_params.get("database", PGVECTOR_DATABASE_NAME),
        )
        # Set statement timeout to 30 seconds to prevent hanging
        with cls.connection.cursor() as cursor:
            cursor.execute("SET statement_timeout = '30s'")
        cls.table_name = validate_table_name(connection_params.get("table", "vec_items"))

    @classmethod
    def command(cls, sql_query: str, timeout_seconds: int = 30):
        """Execute SQL command with timeout protection"""
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(sql_query)
                if cursor.description:
                    return cursor.fetchall()
                cls.connection.commit()
                return None
        except psycopg2.errors.QueryCanceled as e:
            if "canceling statement due to statement timeout" in str(e):
                # Rollback the aborted transaction before raising the error
                cls.connection.rollback()
                raise RuntimeError(f"SQL command timed out after {timeout_seconds} seconds: {sql_query[:100]}...")
            else:
                # Rollback for other query canceled errors
                cls.connection.rollback()
                raise
        except psycopg2.errors.InFailedSqlTransaction as e:
            # This can happen if we try to execute after a previous failure
            # Rollback and retry the command
            cls.connection.rollback()
            try:
                with cls.connection.cursor() as cursor:
                    cursor.execute(sql_query)
                    if cursor.description:
                        return cursor.fetchall()
                    cls.connection.commit()
                    return None
            except Exception as retry_e:
                raise RuntimeError(f"Failed to execute SQL command after transaction rollback: {sql_query[:100]}... Error: {retry_e}")
        except Exception as e:
            # For any other exception, rollback the transaction
            cls.connection.rollback()
            raise RuntimeError(f"Failed to execute SQL command: {sql_query[:100]}... Error: {e}")

    def clean(self):
        pass

    @classmethod
    def sub_recreate(cls, distance, vector_size, collection_params, extra_columns_name, extra_columns_type):
        # Create table with id, vector, and extra columns
        columns = ["id INTEGER PRIMARY KEY"]
        
        if vector_size > 0:
            columns.append(f"vector vector({vector_size})")

        for col_index in range(0, len(extra_columns_name)):
            columns.append(
                f"{extra_columns_name[col_index]} {convert_H52PostgreSQLType(extra_columns_type[col_index])}"
            )
        
        columns_str = ", ".join(columns)

        # Drop table if exists
        drop_table = f"DROP TABLE IF EXISTS {cls.table_name}"
        sql_log(drop_table)
        try:
            cls.command(drop_table)
        except RuntimeError as e:
            # If DROP TABLE fails due to timeout or other issues, log and continue
            step(f"Warning: Failed to drop table {cls.table_name}: {e}")
            # The command method already handled the transaction rollback, so we can proceed

        # Create table
        create_table = f"CREATE TABLE {cls.table_name} ({columns_str})"
        sql_log(create_table)
        cls.command(create_table)
        step("recreate finished")

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):
        if vector_size and vector_size > 0:
            compact_kv(
                "configure",
                distance=distance,
                vector_size=vector_size,
                index_type=collection_params.get("index_type"),
            )
        
        # Initialize client and recreate table
        self.__class__.init_client(connection_params)
        self.sub_recreate(DISTANCE_MAPPING[distance], vector_size, collection_params, extra_columns_name, extra_columns_type)

    def execution_params(self, distance, vector_size) -> dict:
        # For cosine distance, vectors should be normalized
        return {"normalize": distance == Distance.COSINE}