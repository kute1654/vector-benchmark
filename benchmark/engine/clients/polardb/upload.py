import time
from typing import List, Optional

import psycopg2
from psycopg2.extras import execute_values

from benchmark.cli_output import sql as sql_log, stage, step
from engine.base_client import BaseUploader
from engine.clients.polardb.config import (
    POLARDB_DATABASE_NAME, POLARDB_DEFAULT_PASSWD, POLARDB_DEFAULT_PORT,
    POLARDB_DEFAULT_USER, validate_table_name,
)


class PolarDBUploader(BaseUploader):
    connection = None
    upload_params = {}
    table_name = None
    vector_size = 0

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params, upload_params, extra_columns_name, extra_columns_type):
        cls.connection = psycopg2.connect(
            host=connection_params.get("host", "127.0.0.1"),
            port=connection_params.get("port", POLARDB_DEFAULT_PORT),
            user=connection_params.get("user", POLARDB_DEFAULT_USER),
            password=connection_params.get("password", POLARDB_DEFAULT_PASSWD),
            database=connection_params.get("database", POLARDB_DATABASE_NAME),
        )
        cls.upload_params = upload_params or {}
        cls.table_name = validate_table_name(connection_params.get("table", "vec_items"))
        cls.vector_size = int(cls.upload_params.get("_vector_size", 0) or 0)

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]):
        columns = ["id", "vector"]
        meta_columns = list((metadata[0] or {}).keys()) if metadata else []
        columns.extend(meta_columns)
        rows = []
        for row_id, vector, meta in zip(ids, vectors, metadata):
            values = [row_id, "{" + ",".join(str(x) for x in vector) + "}"]
            values.extend((meta or {}).get(k) for k in meta_columns)
            rows.append(tuple(values))
        for attempt in range(3):
            try:
                with cls.connection.cursor() as cursor:
                    execute_values(
                        cursor,
                        f"INSERT INTO {cls.table_name} ({', '.join(columns)}) VALUES %s",
                        rows, template="(" + ",".join(
                            ["%s", "CAST(%s AS real[])"] + ["%s"] * len(meta_columns)
                        ) + ")",
                    )
                cls.connection.commit()
                return
            except Exception:
                cls.connection.rollback()
                if attempt == 2:
                    raise
                time.sleep(1)

    @classmethod
    def post_upload(cls, distance):
        index_type = str(cls.upload_params.get("_index_type", cls.upload_params.get("index_type", "hnsw")) or "").lower()
        if not index_type or index_type == "none":
            return {}
        params = cls.upload_params.get("index_params") or {}
        index_name = f"{cls.table_name}_{index_type}_idx"
        if index_type == "hnsw":
            sql = (
                f"CREATE INDEX {index_name} ON {cls.table_name} USING pase_hnsw (vector) WITH ("
                f"dim = {cls.vector_size}, base_nb_num = {int(params.get('base_nb_num', 16))}, "
                f"ef_build = {int(params.get('ef_build', 40))}, ef_search = {int(params.get('ef_search', 100))}, "
                f"base64_encoded = 0)"
            )
        elif index_type == "ivfflat":
            lists = int(params.get("lists", 100))
            sql = (
                f"CREATE INDEX {index_name} ON {cls.table_name} USING pase_ivfflat (vector) WITH ("
                f"dimension = {cls.vector_size}, distance_type = 0, clustering_type = 1, "
                f"clustering_params = 'lists={lists}')"
            )
        else:
            raise RuntimeError(f"PolarDB does not support index_type={index_type}")
        stage("POST UPLOAD")
        sql_log(sql)
        with cls.connection.cursor() as cursor:
            cursor.execute(sql)
        cls.connection.commit()
        return {"vector_index_build_time": 0.0, "index_type": index_type}
