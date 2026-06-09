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

        # ClickHouse 26.6.1.1 支持的向量索引类型：annoy, usearch, flat
        # 将索引类型转换为小写
        clickhouse_index_type = index_type.lower()

        # 检查索引类型是否支持
        supported_index_types = ["annoy", "usearch", "flat"]
        if clickhouse_index_type not in supported_index_types:
            warn(f"unsupported index type for ClickHouse: {index_type}. Supported types: {supported_index_types}")
            return {}

        # 构建索引参数
        index_params = cls.upload_params.get("index_params") or {}
        param_items = [f"'metric_type={cls.distance}'"]
        for key, value in index_params.items():
            if str(key).lower() == "metric_type":
                continue
            param_items.append(f"'{key}={value}'")
        index_parameter_str = ",".join(param_items)

        # 创建向量索引
        index_create_str = (
            f"ALTER TABLE {cls.table_name} ADD INDEX vector_index vector "
            f"TYPE {clickhouse_index_type}({index_parameter_str})"
        )
        vector_index_begin_time = time.perf_counter()
        sql(index_create_str)
        try:
            cls.command(index_create_str)
        except Exception as e:
            warn(f"failed to add vector index: {e}")
            return {}

        # 等待向量索引构建完成
        # ClickHouse 使用 system.parts 表来检查索引状态
        check_index_status = f"""
        SELECT name, type, status
        FROM system.data_skipping_indices
        WHERE database = 'default' AND table = '{cls.table_name}' AND name = 'vector_index'
        """
        sql(check_index_status)
        last_status = None
        last_log = 0.0
        index_wait_begin = time.perf_counter()
        while True:
            time.sleep(5)
            try:
                rows = cls.query(check_index_status)
                if rows:
                    current_status = str(rows[0][2]) if len(rows[0]) > 2 else "unknown"
                else:
                    current_status = "not found"
                elapsed = time.perf_counter() - index_wait_begin
                if current_status != last_status or (elapsed - last_log) >= 60.0:
                    status("vector index", current_status, detail=f"elapsed {elapsed:.0f}s")
                    last_status = current_status
                    last_log = elapsed
                # ClickHouse 索引状态可能是 "Built" 或其他表示完成的状态
                if current_status in ("Built", "COMPLETE", "done"):
                    break
            except Exception as e:
                step(f"vector index status check failed: {e}")
                time.sleep(3)
                continue
        vector_index_build_time = time.perf_counter() - vector_index_begin_time

        return {
            "vector_index_build_time": vector_index_build_time,
        }
