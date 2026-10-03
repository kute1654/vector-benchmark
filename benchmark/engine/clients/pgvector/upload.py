import math
import time
import sys
from typing import List, Optional

import psycopg2
from psycopg2.extras import execute_values

from benchmark.cli_output import sql as sql_log, stage, status, step, set_live_line, warn
from engine.base_client import BaseUploader
from engine.base_client.utils import format_bytes
from engine.clients.pgvector.config import *


# Map distance types to PGvector operator classes
DISTANCE_TO_OPERATOR_CLASS = {
    "<->": "vector_l2_ops",      # L2 distance
    "<#>": "vector_ip_ops",      # Inner product  
    "<=>": "vector_cosine_ops"   # Cosine distance
}


class PGVectorUploader(BaseUploader):
    connection = None
    upload_params = {}
    distance_op: str = None
    table_name: str = None
    vector_size: int = 0

    def __init__(self, host, connection_params, upload_params):
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
        
        super().__init__(host, merged_conn_params, upload_params)

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        cls.connection = psycopg2.connect(
            host=connection_params.get("host", "127.0.0.1"),
            port=connection_params.get("port", PGVECTOR_DEFAULT_PORT),
            user=connection_params.get("user", PGVECTOR_DEFAULT_USER),
            password=connection_params.get("password", PGVECTOR_DEFAULT_PASSWD),
            database=connection_params.get("database", PGVECTOR_DATABASE_NAME),
        )
        cls.upload_params = upload_params
        cls.distance_op = DISTANCE_MAPPING[distance]
        cls.table_name = validate_table_name(connection_params.get("table", "vec_items"))
        cls.vector_size = vector_count  # Use vector_count as vector_size

    @classmethod
    def command(cls, sql_query: str):
        with cls.connection.cursor() as cursor:
            cursor.execute(sql_query)
            if cursor.description:
                return cursor.fetchall()
            cls.connection.commit()
            return None

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]):
        if len(ids) != len(vectors):
            raise RuntimeError("pgvector batch upload unhealthy")

        # Build column list
        col_list = ['id']
        col_list_str = 'id'
        
        has_vector = cls.vector_size > 0
        if has_vector:
            col_list.append('vector')
            col_list_str = 'id, vector'

        # Add metadata columns if present
        meta_columns = []
        if metadata and metadata[0] is not None:
            meta_columns = list(metadata[0].keys())
            for col_name in meta_columns:
                col_list.append(str(col_name))
                col_list_str += f", {col_name}"

        # Prepare data rows
        rows = []
        for i in range(len(ids)):
            row = [ids[i]]
            
            if has_vector:
                # Convert vector to string format: '[1.0,2.0,3.0]'
                # Replace NaN values with 0.0 since pgvector does not support NaN
                vector_str = '[' + ','.join('0.0' if math.isnan(x) else str(x) for x in vectors[i]) + ']'
                row.append(vector_str)
            
            if metadata[i] is not None:
                for col_name in meta_columns:
                    value = metadata[i][col_name]
                    # Handle geo coordinates as PostgreSQL POINT
                    if isinstance(value, dict) and ('lon' in value and 'lat' in value):
                        row.append(f"POINT({value['lon']}, {value['lat']})")
                    else:
                        row.append(value)
            
            rows.append(tuple(row))

        # Insert data using execute_values for better performance
        while True:
            try:
                with cls.connection.cursor() as cursor:
                    placeholders = '(' + ','.join(['%s'] * len(col_list)) + ')'
                    execute_values(
                        cursor,
                        f"INSERT INTO {cls.table_name} ({col_list_str}) VALUES %s",
                        rows,
                        template=placeholders
                    )
                    cls.connection.commit()
                break
            except Exception as e:
                step(f"pgvector upload exception: {e}")
                time.sleep(3)

    @classmethod
    def post_upload(cls, distance):
        stage("POST UPLOAD")
        
        index_type = str(
            cls.upload_params.get("_index_type")
            or cls.upload_params.get("index_type", "hnsw")
            or "hnsw"
        ).lower()
        if index_type == "hnswflat":
            index_type = "hnsw"
        index_params = cls.upload_params.get("index_params", {}) or {}

        step(f"pgvector post upload: distance={distance} metric={cls.distance_op} table={cls.table_name} index_type={index_type}")

        # Create HNSW index
        if index_type.lower() == "hnsw":
            # Default HNSW parameters. config.json uses the ck/myscale-style key
            # `ef_c`; `ef_construction` is kept as a fallback.
            m = index_params.get("m", 16)
            ef_construction = index_params.get("ef_construction", index_params.get("ef_c", 64))
            
            # Get the correct operator class for the distance type
            operator_class = DISTANCE_TO_OPERATOR_CLASS.get(cls.distance_op, "vector_l2_ops")
            
            # Build index creation SQL with proper operator class
            index_name = f"{cls.table_name}_idx"
            index_sql = (
                f"CREATE INDEX {index_name} ON {cls.table_name} "
                f"USING hnsw (vector {operator_class}) "
                f"WITH (m = {m}, ef_construction = {ef_construction})"
            )
            
            sql_log(index_sql)
            step(f"Creating HNSW index on {cls.table_name} (m={m}, ef_construction={ef_construction})...")
            step("⚠️  Press Ctrl+C to cancel (may wait for current transaction to finish)...")
            
            import threading
            import signal
            
            stop_spinner = [False]
            cancelled = [False]
            
            def show_progress():
                dots = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
                idx = [0]
                start_time = time.time()
                while not stop_spinner[0]:
                    elapsed = int(time.time() - start_time)
                    sys.stdout.write(f"\r  ⏳ Building HNSW index... {dots[idx[0] % len(dots)]} {elapsed}s (Ctrl+C to stop)")
                    sys.stdout.flush()
                    idx[0] += 1
                    time.sleep(0.1)
                sys.stdout.write("\r")
                sys.stdout.flush()
            
            def signal_handler(signum, frame):
                cancelled[0] = True
                stop_spinner[0] = True
                print("\n\n⚠️  Cancel requested! Waiting for PostgreSQL to abort...")
                print("   (This may take a few seconds)")
            
            # Register signal handler for Ctrl+C
            original_handler = signal.signal(signal.SIGINT, signal_handler)
            
            spinner_thread = threading.Thread(target=show_progress, daemon=True)
            spinner_thread.start()
            
            try:
                vector_index_begin_time = time.perf_counter()
                
                # Set a statement timeout to ensure we can eventually cancel
                # Default: 1 hour max, can be overridden by upload_params.index_timeout
                index_timeout = cls.upload_params.get("index_timeout", 3600)
                cls.command(f"SET LOCAL statement_timeout = '{index_timeout}s'")
                
                cls.command(index_sql)
                vector_index_build_time = time.perf_counter() - vector_index_begin_time
                
                if cancelled[0]:
                    step("✗ Index creation cancelled")
                    return {"cancelled": True}
                    
            except KeyboardInterrupt:
                cancelled[0] = True
                step("✗ Index creation interrupted by user")
                # Try to rollback and cleanup
                try:
                    cls.connection.rollback()
                    # Try to drop the partial index if it exists
                    cls.command(f"DROP INDEX IF EXISTS {index_name}")
                except Exception as e:
                    warn(f"Cleanup after cancellation failed: {e}")
                return {"cancelled": True, "error": "interrupted"}
                
            except Exception as e:
                if cancelled[0]:
                    step(f"✗ Index creation failed: {e}")
                else:
                    raise
                    
            finally:
                stop_spinner[0] = True
                spinner_thread.join(timeout=2)
                # Restore original signal handler
                signal.signal(signal.SIGINT, original_handler)
            
            step(f"✓ Vector index built successfully in {vector_index_build_time:.3f}s")

            # `optimize: true` (ck/myscale convention) -> ANALYZE so the planner has
            # fresh statistics for both custom and generic plans
            if cls.upload_params.get("optimize"):
                analyze_begin = time.perf_counter()
                sql_log(f"ANALYZE {cls.table_name}")
                cls.command(f"ANALYZE {cls.table_name}")
                step(f"ANALYZE finished, time: {time.perf_counter() - analyze_begin:.3f}s")

            return {
                "vector_index_build_time": vector_index_build_time,
            }

        if index_type.lower() == "ivfflat":
            operator_class = DISTANCE_TO_OPERATOR_CLASS.get(cls.distance_op, "vector_l2_ops")
            index_name = f"{cls.table_name}_ivfflat_idx"
            total = cls.command(f"SELECT count(*) FROM {cls.table_name}")[0][0]
            import math
            lists = int(index_params.get("lists", max(1, int(math.sqrt(max(1, total)) / 10))))
            index_sql = (
                f"CREATE INDEX {index_name} ON {cls.table_name} "
                f"USING ivfflat (vector {operator_class}) WITH (lists = {lists})"
            )
            sql_log(index_sql)
            begin = time.perf_counter()
            cls.command(index_sql)
            return {
                "vector_index_build_time": time.perf_counter() - begin,
                "index_type": "ivfflat",
                "lists": lists,
            }

        return {}