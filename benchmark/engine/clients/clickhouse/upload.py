import time
import sys
from typing import List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_driver import Client as DriverClient
import json

from benchmark.cli_output import sql, stage, status, step, set_live_line, warn
from engine.base_client import BaseUploader
from engine.base_client.utils import format_bytes
from engine.clients.clickhouse.config import *


class ClickHouseUploader(BaseUploader):
    client: Client = None
    upload_params = {}
    distance: str = None
    table_name: str = None
    protocol: str = "tcp"

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        cls.protocol = connection_params.get("protocol", "tcp")
        if cls.protocol == "tcp":
            cls.client = DriverClient(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 9000),
                user=connection_params.get("user", CLICKHOUSE_DEFAULT_USER),
                password=connection_params.get("password", CLICKHOUSE_DEFAULT_PASSWD),
                database="default",
            )
        else:
            cls.client = clickhouse_connect.get_client(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 8123),
                username=connection_params.get("user", CLICKHOUSE_DEFAULT_USER),
                password=connection_params.get("password", CLICKHOUSE_DEFAULT_PASSWD),
                database="default",
            )
        cls.upload_params = upload_params
        cls.distance = DISTANCE_MAPPING[distance]
        cls.table_name = validate_table_name(connection_params.get("table", CLICKHOUSE_DATABASE_NAME))

    @classmethod
    def command(cls, sql: str):
        if cls.protocol == "tcp":
            return cls.client.execute(sql)
        return cls.client.command(sql)

    @classmethod
    def query(cls, sql: str):
        if cls.protocol == "tcp":
            return cls.client.execute(sql)
        return cls.client.query(sql).result_rows

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]):
        if len(ids) != len(vectors):
            raise RuntimeError("clickhouse batch upload unhealthy")

        col_list = ['id']
        col_list.append('vector')

        # Getting the names of structured data columns based on the first meta information.
        if metadata[0] is not None:
            for col_name in list(metadata[0].keys()):
                col_list.append(str(col_name))

        res = []
        for i in range(0, len(ids)):
            temp_list = [ids[i]]
            temp_list.append(vectors[i])

            if metadata[i] is not None:
                for col_name in list(metadata[i].keys()):
                    value = metadata[i][col_name]
                    # Determining if the data is a dictionary type of latitude and longitude.
                    if isinstance(value, dict) and ('lon' and 'lat') in list(value.keys()):
                        # Keep the correct order of longitude and latitude.
                        temp_list.append(tuple([value.get('lon'), value.get('lat')]))
                    else:
                        temp_list.append(value)
            res.append(temp_list)

        while True:
            try:
                if cls.protocol == "tcp":
                    columns = ", ".join(col_list)
                    insert_sql = f"INSERT INTO {cls.table_name} ({columns}) VALUES"
                    cls.client.execute(insert_sql, res)
                else:
                    cls.client.insert(cls.table_name, res, column_names=col_list)
                break
            except Exception as e:
                step(f"clickhouse upload exception: {e}")
                time.sleep(3)

    @classmethod
    def post_upload(cls, distance):
        stage("POST UPLOAD")

        step(f"clickhouse post upload: distance={distance} metric={cls.distance} table={cls.table_name}")

        index_type = cls.upload_params.get("_index_type", "")
        if not index_type:
            warn("no index_type specified, skipping vector index creation")
            return {}

        # ClickHouse 26.6.1.1 向量索引语法（从源码确认）：
        # INDEX name vector TYPE vector_similarity('hnsw', 'distance_function', dimensions,
        #     [quantization, hnsw_max_connections_per_layer, hnsw_candidate_list_size_for_construction])
        #
        # 参数说明：
        # 1. method: 'hnsw'（目前只支持 hnsw）
        # 2. distance_function: 'L2Distance', 'cosineDistance', 'dotProduct'
        # 3. dimensions: 向量维度
        # 4. quantization（可选）: 'f64', 'f32', 'f16', 'bf16', 'i8', 'b1'，默认 'bf16'
        # 5. hnsw_max_connections_per_layer（可选）: 类似 HNSW 的 m 参数
        # 6. hnsw_candidate_list_size_for_construction（可选）: 类似 HNSW 的 ef_construction 参数

        # 将索引类型映射到 ClickHouse 的 vector_similarity
        supported_index_types = ["hnswflat", "hnsw", "annoy", "usearch", "flat", "vector_similarity"]
        if index_type.lower() not in supported_index_types:
            warn(f"unsupported index type for ClickHouse: {index_type}. Supported types: {supported_index_types}")
            return {}

        # 距离函数：cls.distance 已经通过 DISTANCE_MAPPING 转换为 ClickHouse 函数名
        # （L2Distance, cosineDistance, dotProduct）
        distance_func = cls.distance
        print(cls.upload_params)
        # 构建索引参数（位置参数，不是键值对）
        index_params = cls.upload_params.get("index_params") or {}
        # 获取向量维度（从 configure 阶段设置的 vector_size）
        vector_size = cls.upload_params.get("_vector_size", 0)

        # 获取 HNSW 参数
        m = index_params.get("m", 16)  # hnsw_max_connections_per_layer，默认 16
        ef_c = index_params.get("ef_c", 200)  # hnsw_candidate_list_size_for_construction，默认 200

        # 量化类型（默认 bf16）
        quantization = index_params.get("quantization", "bf16")

        if vector_size > 0:
            # 有向量维度信息，使用完整参数
            index_args = f"'hnsw', '{distance_func}', {vector_size}, '{quantization}', {m}, {ef_c}"
        else:
            # 没有向量维度信息，使用简化参数（只支持 3 个参数的情况）
            warn(f"vector_size not available, using simplified index args (3 parameters)")
            index_args = f"'hnsw', '{distance_func}'"

        # 创建向量索引定义
        # ClickHouse 26.6.1.1 使用 vector_similarity 索引类型
        # ADD INDEX 只是创建索引定义，不会立即构建索引数据
        index_create_str = (
            f"ALTER TABLE {cls.table_name} ADD INDEX vector_index "
            f"vector TYPE vector_similarity({index_args}) GRANULARITY 1000"
        )
        vector_index_begin_time = time.perf_counter()
        sql(index_create_str)
        try:
            cls.command(index_create_str)
            step(f"vector index definition added successfully")
        except Exception as e:
            warn(f"failed to add vector index: {e}")
            return {}

        # 使用 MATERIALIZE INDEX 触发实际的索引构建
        # 这会创建一个 mutation 来在现有数据上构建索引
        materialize_str = f"ALTER TABLE {cls.table_name} MATERIALIZE INDEX vector_index"
        sql(materialize_str)
        try:
            cls.command(materialize_str)
            step(f"MATERIALIZE INDEX triggered")
        except Exception as e:
            warn(f"failed to materialize vector index: {e}")
            return {}

        # 等待 MATERIALIZE INDEX mutation 完成
        # 通过检查 system.mutations 表来跟踪 mutation 状态
        check_mutation_sql = (
            f"SELECT mutation_id, command, is_done, latest_fail_reason "
            f"FROM system.mutations "
            f"WHERE database = 'default' AND table = '{cls.table_name}' AND is_done = 0 "
            f"ORDER BY create_time DESC LIMIT 1"
        )
        last_log = 0.0
        index_wait_begin = time.perf_counter()
        while True:
            time.sleep(5)
            try:
                rows = cls.query(check_mutation_sql)
                if not rows:
                    # 没有未完成的 mutation，说明构建完成
                    elapsed = time.perf_counter() - index_wait_begin
                    step(f"vector index build completed in {elapsed:.1f}s")
                    break
                else:
                    mutation_id = rows[0][0]
                    command = rows[0][1]
                    is_done = rows[0][2]
                    fail_reason = rows[0][3] if len(rows[0]) > 3 else ""
                    elapsed = time.perf_counter() - index_wait_begin
                    if fail_reason:
                        warn(f"vector index build failed: {fail_reason}")
                        break
                    if (elapsed - last_log) >= 30.0:
                        step(f"vector index building: mutation_id={mutation_id} "
                             f"is_done={is_done} elapsed={elapsed:.0f}s")
                        last_log = elapsed
            except Exception as e:
                step(f"mutation status check failed: {e}")
                time.sleep(3)
                continue
        vector_index_build_time = time.perf_counter() - vector_index_begin_time

        return {
            "vector_index_build_time": vector_index_build_time,
        }
