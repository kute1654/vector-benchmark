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
from engine.clients.myscale.config import *


class MyScaleUploader(BaseUploader):
    client: Client = None
    upload_params = {}
    distance: str = None
    table_name: str = None
    protocol: str = "tcp"

    # def get_other_client(self):
    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        cls.protocol = connection_params.get("protocol", "tcp")
        if cls.protocol == "tcp":
            cls.client = DriverClient(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 9000),
                user=connection_params.get("user", MYSCALE_DEFAULT_USER),
                password=connection_params.get("password", MYSCALE_DEFAULT_PASSWD),
                database="default",
            )
        else:
            cls.client = clickhouse_connect.get_client(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 8123),
                username=connection_params.get("user", MYSCALE_DEFAULT_USER),
                password=connection_params.get("password", MYSCALE_DEFAULT_PASSWD),
                database="default",
            )
        cls.upload_params = upload_params
        cls.distance = DISTANCE_MAPPING[distance]
        cls.table_name = validate_table_name(connection_params.get("table", MYSCALE_DATABASE_NAME))

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
            raise RuntimeError("myscale batch upload unhealthy")

        col_list = ['id']
        only_text_search = cls.upload_params.get("_only_text_search", False)
        if not only_text_search:
            col_list.append('vector')

        # Getting the names of structured data columns based on the first meta information.
        if metadata[0] is not None:
            for col_name in list(metadata[0].keys()):
                col_list.append(str(col_name))

        res = []
        for i in range(0, len(ids)):
            temp_list = [ids[i]]
            if not only_text_search:
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
                step(f"myscale upload exception: {e}")
                time.sleep(3)

    @classmethod
    def post_upload(cls, distance):
        stage("POST UPLOAD")

        only_text_search = cls.upload_params.get("_only_text_search",)
        result_group = str(cls.upload_params.get("_result_group", "") or "")
        if only_text_search:
            if result_group == "hybrid_search":
                warn("hybrid_search dataset expects upload_params.index_type, but it is not set: falling back to text_search (no vector index will be built).")
                step(f"myscale post upload: table={cls.table_name} (hybrid dataset, text search only, no vector index)")
            else:
                step(f"myscale post upload: table={cls.table_name} ")
        else:
            step(f"myscale post upload: distance={distance} metric={cls.distance} table={cls.table_name}")

        index_type = cls.upload_params.get("_index_type", "")
        if index_type == "MSTG" or index_type == "MSRQ":
            mstg_disk_mode = cls.upload_params.get("_mstg_disk_mode", 0)
            mstg_mode_sql = f"ALTER TABLE {cls.table_name} MODIFY SETTING default_mstg_disk_mode = {mstg_disk_mode};"
            sql(mstg_mode_sql)
            cls.command(mstg_mode_sql)

        fts_idx_cols = cls.upload_params.get("fts_idx_cols", None) or []
        fts_fts_params = cls.upload_params.get("fts_idx_params", None) or {}
        fts_index_names = []
        fts_add_ddls = []

        if fts_idx_cols:
            for col in list(fts_idx_cols):
                col_name = str(col).strip()
                if not col_name:
                    continue
                idx_name = f"fts_{col_name}_{get_random_string(4)}"
                col_params = fts_fts_params.get(col_name, {})
                fts_json = json.dumps({col_name: col_params})
                add_fts_sql = (
                    f"ALTER TABLE {cls.table_name} ADD INDEX {idx_name} ({col_name}) "
                    f"TYPE fts('{fts_json}')"
                )
                fts_index_names.append(idx_name)
                fts_add_ddls.append(add_fts_sql)

        if cls.upload_params.get("optimize", True):
            optimize_begin_time = time.perf_counter()
            check_parts_str = f"select count(*) from system.parts where database='default' and table='{cls.table_name}' and active=1"
            check_merges_str = f"select count(*) from system.merges where database='default' and table='{cls.table_name}'"
            last_line_len = 0
            while True:
                time.sleep(3)
                try:
                    parts = int(cls.query(check_parts_str)[0][0])
                except Exception as e:
                    step(f"checking parts count failed: {e}")
                    continue

                try:
                    merges = int(cls.query(check_merges_str)[0][0])
                except Exception:
                    merges = -1

                elapsed = time.perf_counter() - optimize_begin_time
                if merges > 0:
                    line = " - waiting merge"
                else:
                    line = f" - optimize status: parts={parts} merges={merges} elapsed={elapsed:.0f}s"

                padding = " " * max(0, last_line_len - len(line))
                sys.stdout.write("\r" + line + padding)
                sys.stdout.flush()
                set_live_line(True)
                last_line_len = len(line)

                if parts == 1 and merges == 0:
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    set_live_line(False)
                    break

                if merges > 0:
                    continue

                try:
                    optimize_str = f"optimize table {cls.table_name} final"
                    sql(optimize_str)
                    cls.command(optimize_str)
                except Exception as e:
                    step(f"optimize failed: {e}")

            optimize_time = time.perf_counter() - optimize_begin_time
            step(f"optimize finished, time: {optimize_time:.3f}s")

        if fts_add_ddls:
            for ddl in list(fts_add_ddls):
                sql(ddl)
                cls.command(ddl)

        if fts_index_names:
            materialize_begin_time = time.perf_counter()
            for idx_name in list(fts_index_names):
                materialize_sql = f"ALTER TABLE {cls.table_name} MATERIALIZE INDEX {idx_name}"
                sql(materialize_sql)
                cls.command(materialize_sql)

            check_mutations_str = (
                f"select count(*) from system.mutations where database='default' and table='{cls.table_name}' and is_done=0"
            )
            last_log = 0.0
            while True:
                time.sleep(3)
                try:
                    pending = int(cls.query(check_mutations_str)[0][0])
                except Exception:
                    pending = -1
                elapsed = time.perf_counter() - materialize_begin_time
                if pending < 0 and elapsed >= 600.0:
                    step(f"fts materialize status: pending_mutations={pending} elapsed={elapsed:.0f}s, stop waiting due to query errors")
                    break
                if elapsed - last_log >= 30.0:
                    step(f"fts materialize status: pending_mutations={pending} elapsed={elapsed:.0f}s")
                    last_log = elapsed
                if pending == 0:
                    break

        if only_text_search:
            return {}

        # vector index
        index_params = cls.upload_params.get("index_params") or {}
        param_items = [f"'metric_type={cls.distance}'"]
        for key, value in index_params.items():
            if str(key).lower() == "metric_type":
                continue
            param_items.append(f"'{key}={value}'")
        index_parameter_str = ",".join(param_items)

        index_create_str = (
            f"alter table {cls.table_name} add vector index {cls.table_name}_{get_random_string(4)} "
            f"vector type {index_type}({index_parameter_str})"
        )
        vector_index_begin_time = time.perf_counter()
        sql(index_create_str)
        cls.command(index_create_str)

        # waiting for vector index create finished
        check_index_status = f"select status from system.vector_indices where database='default' and table='{cls.table_name}'"
        sql(check_index_status)
        last_status = None
        last_log = 0.0
        index_wait_begin = time.perf_counter()
        while True:
            time.sleep(5)
            try:
                rows = cls.query(check_index_status)
                current_status = str(rows[0][0]) if rows else "not found"
                elapsed = time.perf_counter() - index_wait_begin
                if current_status != last_status or (elapsed - last_log) >= 60.0:
                    status("vector index", current_status, detail=f"elapsed {elapsed:.0f}s")
                    last_status = current_status
                    last_log = elapsed
                if current_status == "Built":
                    break
            except Exception as e:
                step(f"vector index status check failed: {e}")
                time.sleep(3)
                continue
        vector_index_build_time = time.perf_counter() - vector_index_begin_time

        return {
            "vector_index_build_time": vector_index_build_time,
        }
