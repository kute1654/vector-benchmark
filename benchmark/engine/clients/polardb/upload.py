import math
import time
from typing import List, Optional

import psycopg2
from psycopg2.extras import execute_values

from benchmark.cli_output import sql as sql_log, stage, step
from engine.base_client import BaseUploader
from engine.base_client.distances import Distance
from engine.clients.polardb.config import (
    PASE_MAX_DIM, POLARDB_DATABASE_NAME, POLARDB_DEFAULT_PASSWD,
    POLARDB_DEFAULT_PORT, POLARDB_DEFAULT_USER, distance_ds,
    validate_table_name,
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
            values = [row_id, "{" + ",".join('0.0' if math.isnan(x) else str(x) for x in vector) + "}"]
            values.extend((meta or {}).get(k) for k in meta_columns)
            rows.append(tuple(values))
        for attempt in range(3):
            try:
                with cls.connection.cursor() as cursor:
                    execute_values(
                        cursor,
                        f"INSERT INTO {cls.table_name} ({', '.join(columns)}) VALUES %s",
                        rows, template="(" + ",".join(
                            # pase 向量列是 float4[], 文本 '{0.1,0.2,...}' 直接 cast
                            ["%s", "CAST(%s AS float4[])"] + ["%s"] * len(meta_columns)
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
        dim = int(cls.vector_size or 0)

        if dim > PASE_MAX_DIM:
            raise RuntimeError(
                f"PolarDB pase index requires dim <= {PASE_MAX_DIM} (PASE_MAX_DIM), got {dim}; "
                f"this dataset cannot build a pase index"
            )

        if index_type == "hnsw":
            # config.json 用 pgvector/ck 风格参数 ef_c / m; 映射到 pase reloptions:
            #   m  -> base_nb_num (HNSW 建图时每个节点的邻居数)
            #   ef_c / ef_construction -> ef_build (建图时候选集大小)
            #   ef_search          -> 构建期默认搜索 ef (查询向量文本里的 :extra 可覆盖)
            base_nb_num = int(params.get('base_nb_num', params.get('m', 16)) or 16)
            ef_build = int(params.get('ef_build', params.get('ef_c', params.get('ef_construction', 40))) or 40)
            ef_search = int(params.get('ef_search', 100) or 100)
            sql = (
                f"CREATE INDEX {index_name} ON {cls.table_name} "
                f"USING pase_hnsw (vector) WITH ("
                f"dim = {dim}, base_nb_num = {base_nb_num}, ef_build = {ef_build}, "
                f"ef_search = {ef_search}, base64_encoded = 0)"
            )
        elif index_type == "ivfflat":
            lists = int(params.get("lists", 100))
            dist_type = distance_ds(distance)   # ds: 0 = L2, 1 = inner product
            sql = (
                f"CREATE INDEX {index_name} ON {cls.table_name} "
                f"USING pase_ivfflat (vector) WITH ("
                f"dimension = {dim}, distance_type = {dist_type}, "
                f"clustering_type = 1, clustering_params = 'lists={lists}')"
            )
        else:
            raise RuntimeError(f"PolarDB does not support index_type={index_type}")
        stage("POST UPLOAD")
        sql_log(sql)
        build_begin = time.perf_counter()
        with cls.connection.cursor() as cursor:
            cursor.execute(sql)
        cls.connection.commit()
        build_time = time.perf_counter() - build_begin
        step(f"✓ Vector index built successfully in {build_time:.3f}s")
        # `optimize: true` (ck/myscale convention) -> ANALYZE so the planner has
        # fresh statistics for both custom and generic plans
        if cls.upload_params.get("optimize"):
            analyze_begin = time.perf_counter()
            sql_log(f"ANALYZE {cls.table_name}")
            with cls.connection.cursor() as cursor:
                cursor.execute(f"ANALYZE {cls.table_name}")
            cls.connection.commit()
            step(f"ANALYZE finished, time: {time.perf_counter() - analyze_begin:.3f}s")
        return {"vector_index_build_time": build_time, "index_type": index_type}