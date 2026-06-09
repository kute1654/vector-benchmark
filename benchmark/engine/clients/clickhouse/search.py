import threading
import string
import re
import json
from typing import List, Optional, Tuple
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_driver import Client as DriverClient

from dataset_reader.base_reader import Query
from engine.base_client import BaseSearcher
from benchmark.cli_output import warn, step
from engine.clients.clickhouse.config import *
from engine.clients.clickhouse.config import _to_int
from engine.clients.clickhouse.parser import ClickHouseConditionParser


def remove_punctuation(input_string):
    translator = str.maketrans('', '', string.punctuation)
    return input_string.translate(translator)

_BOOL_OP_RE = re.compile(r"\b(AND|OR|NOT)\b")
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]")


def sanitize_text_query(input_string: Optional[str]) -> str:
    if input_string is None:
        return ""
    text = _CONTROL_CHARS_RE.sub(" ", str(input_string))
    text = remove_punctuation(text)
    text = " ".join(text.split())
    text = _BOOL_OP_RE.sub(lambda m: m.group(1).lower(), text)
    return text


def escape_clickhouse_string_literal(input_string: str) -> str:
    return str(input_string).replace("\\", "\\\\").replace("'", "\\'")


def to_clickhouse_array_literal(values: List[float]) -> str:
    return json.dumps(values, separators=(",", ":"))


thread_local = threading.local()


class ClickHouseSearcher(BaseSearcher):
    search_params = {}
    client = None
    distance: str = None
    host: str = None
    parser = ClickHouseConditionParser()
    connection_params: dict = {}

    def setup_search(self, host, distance, connection_params: dict, search_params: dict, dataset_config):
        if dataset_config is not None and getattr(dataset_config, "result_group", None) == "text_search":
            params = search_params.get("params", None)
            if not isinstance(params, dict):
                params = {}
            params["only_text_search"] = True
            search_params["params"] = params

    def post_warmup(self, dataset_config):
        conn = self.connection_params or {}
        protocol = conn.get("protocol", "tcp")
        table_name = validate_table_name(conn.get("table", CLICKHOUSE_DATABASE_NAME))
        host_val = conn.get("host", "127.0.0.1")
        default_port = 9000 if protocol.lower() == "tcp" else 8123
        port_val = int(conn.get("port", default_port) or default_port)
        user_val = conn.get("user", CLICKHOUSE_DEFAULT_USER)
        password_val = conn.get("password", CLICKHOUSE_DEFAULT_PASSWD)
        timeout_raw = conn.get("timeout_s", None)
        if timeout_raw is None:
            timeout_raw = conn.get("timeout", None)
        base_timeout = _to_int(timeout_raw, 300)
        connect_timeout = _to_int(conn.get("connect_timeout", None), 10)
        send_receive_timeout = _to_int(conn.get("send_receive_timeout", base_timeout), base_timeout)
        sync_request_timeout = _to_int(conn.get("sync_request_timeout", base_timeout), base_timeout)
        # ClickHouse 26.6.1.1 使用 system.data_skipping_indices 来检查向量索引是否存在
        # 注意：该表没有 status 列，只有 data_compressed_bytes 等列
        # ADD INDEX 只是创建索引定义，实际索引数据在 MATERIALIZE INDEX 或 merge 时构建
        check_index_sql = (
            f"SELECT name, type, data_compressed_bytes FROM system.data_skipping_indices "
            f"WHERE database = 'default' AND table = '{table_name}' AND name = 'vector_index'"
        )
        # 同时检查是否有未完成的 mutation（MATERIALIZE INDEX 会创建 mutation）
        check_mutation_sql = (
            f"SELECT count() FROM system.mutations "
            f"WHERE database = 'default' AND table = '{table_name}' AND is_done = 0"
        )
        rows = None
        pending_mutations = 0
        if protocol.lower() == "tcp":
            client = DriverClient(
                host=host_val,
                port=port_val,
                user=user_val,
                password=password_val,
                database="default",
                connect_timeout=connect_timeout,
                send_receive_timeout=send_receive_timeout,
                sync_request_timeout=sync_request_timeout,
            )
            try:
                rows = client.execute(check_index_sql)
                mutation_rows = client.execute(check_mutation_sql)
                if mutation_rows:
                    pending_mutations = int(mutation_rows[0][0] or 0)
            except Exception:
                try:
                    client.disconnect()
                except Exception:
                    pass
                return
            try:
                client.disconnect()
            except Exception:
                pass
        else:
            client = clickhouse_connect.get_client(
                host=host_val,
                port=port_val,
                username=user_val,
                password=password_val,
                database="default",
                connect_timeout=connect_timeout,
                send_receive_timeout=send_receive_timeout,
            )
            try:
                rows = client.query(check_index_sql).result_rows
                mutation_rows = client.query(check_mutation_sql).result_rows
                if mutation_rows:
                    pending_mutations = int(mutation_rows[0][0] or 0)
            except Exception:
                try:
                    client.close()
                except Exception:
                    pass
                return
            try:
                client.close()
            except Exception:
                pass
        if not rows:
            warn(f"no vector_index found for table={table_name}, warmup skipped")
            return
        index_name = rows[0][0]
        index_type = rows[0][1]
        compressed_bytes = int(rows[0][2] or 0)
        if pending_mutations > 0:
            warn(f"vector index still building for table={table_name}: "
                 f"name={index_name} type={index_type} pending_mutations={pending_mutations}")
        elif compressed_bytes > 0:
            step(f"vector index ready for table={table_name}: "
                 f"name={index_name} type={index_type} size={compressed_bytes} bytes")
        else:
            warn(f"vector_index found but no data yet for table={table_name}: "
                 f"name={index_name} type={index_type} (may need MATERIALIZE INDEX or merge)")

    @classmethod
    def init_client(
            cls, host: str, distance, connection_params: dict, search_params: dict
    ):
        cls.connection_params = connection_params
        protocol = str((connection_params or {}).get("protocol", "tcp")).lower()
        if protocol == "tcp":
            timeout_raw = connection_params.get("timeout_s", None)
            if timeout_raw is None:
                timeout_raw = connection_params.get("timeout", None)
            base_timeout = _to_int(timeout_raw, 300)
            is_warmup = bool((search_params or {}).get("_warmup"))
            if is_warmup:
                warmup_timeout_raw = connection_params.get("warmup_timeout_s", None)
                timeout_s = _to_int(warmup_timeout_raw, max(base_timeout, 1800))
            else:
                timeout_s = base_timeout
            connect_timeout = _to_int(connection_params.get("connect_timeout", None), 10)
            send_receive_timeout = _to_int(connection_params.get("send_receive_timeout", timeout_s), timeout_s)
            sync_request_timeout = _to_int(connection_params.get("sync_request_timeout", timeout_s), timeout_s)
            thread_local.client = DriverClient(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 9000),
                user=connection_params.get("user", CLICKHOUSE_DEFAULT_USER),
                password=connection_params.get("password", CLICKHOUSE_DEFAULT_PASSWD),
                database="default",
                connect_timeout=connect_timeout,
                send_receive_timeout=send_receive_timeout,
                sync_request_timeout=sync_request_timeout,
            )
        else:
            timeout_raw = connection_params.get("timeout_s", None)
            if timeout_raw is None:
                timeout_raw = connection_params.get("timeout", None)
            base_timeout = _to_int(timeout_raw, 300)
            is_warmup = bool((search_params or {}).get("_warmup"))
            if is_warmup:
                warmup_timeout_raw = connection_params.get("warmup_timeout_s", None)
                timeout_s = _to_int(warmup_timeout_raw, max(base_timeout, 1800))
            else:
                timeout_s = base_timeout
            connect_timeout = _to_int(connection_params.get("connect_timeout", None), 10)
            send_receive_timeout = _to_int(connection_params.get("send_receive_timeout", timeout_s), timeout_s)
            thread_local.client = clickhouse_connect.get_client(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 8123),
                username=connection_params.get("user", CLICKHOUSE_DEFAULT_USER),
                password=connection_params.get("password", CLICKHOUSE_DEFAULT_PASSWD),
                database="default",
                connect_timeout=connect_timeout,
                send_receive_timeout=send_receive_timeout,
            )
        cls.host = host
        cls.distance = DISTANCE_MAPPING[distance]
        cls.search_params = search_params
        # cls.apply_query_plan_cache_settings(search_params, protocol)

    @classmethod
    def apply_query_plan_cache_settings(cls, search_params: dict, protocol: str):
        # ClickHouse 26.6.1.1 没有 MyScale 的 query plan cache 设置
        # 这些设置是 MyScale 特有的：use_query_plan_cache, query_plan_cache_enable_CAST 等
        # ClickHouse 使用 use_query_cache（查询缓存），但功能不同
        # 因此跳过这些设置，只记录日志
        cache_mode = _to_int((search_params or {}).get("use_query_plan_cache", 0), 0)
        CAST_mode = _to_int((search_params or {}).get("query_plan_cache_enable_CAST", 0), 0)
        only_vector = _to_int((search_params or {}).get("query_plan_cache_only_vector", 0), 0)
        use_number = _to_int((search_params or {}).get("query_plan_cache_use_number", 0), 0)

        if cache_mode != 0 or CAST_mode != 0 or only_vector != 0 or use_number != 0:
            warn(f"ClickHouse does not support MyScale query plan cache settings, "
                 f"skipping: use_query_plan_cache={cache_mode}, query_plan_cache_enable_CAST={CAST_mode}, "
                 f"query_plan_cache_only_vector={only_vector}, query_plan_cache_use_number={use_number}")
        else:
            step(f"query plan cache settings: all disabled (ClickHouse does not support these MyScale settings)")

    @classmethod
    def get_client(cls):
        return thread_local.client

    @classmethod
    def vector_search(cls, vector: List[float], meta_conditions, top: Optional[int]) -> List[Tuple[int, float]]:
        conn = cls.connection_params or {}
        protocol = str(conn.get("protocol", "tcp")).lower()
        table_name = validate_table_name(conn.get("table", CLICKHOUSE_DATABASE_NAME))
        search_params_dict = (cls.search_params or {}).get("params") or {}

        # ClickHouse 26.6.1.1 使用标准的距离函数
        # 从 DISTANCE_MAPPING 获取正确的距离函数名（L2Distance, cosineDistance, dotProduct）
        dist_func = cls.distance  # 已经通过 DISTANCE_MAPPING 转换为 ClickHouse 函数名
        dist_expr = f"{dist_func}(vector, {vector})"

        search_str = f"SELECT id, {dist_expr} as dis FROM {table_name}"

        if meta_conditions is not None:
            search_str += f" prewhere {cls.parser.parse(meta_conditions=meta_conditions)}"

        # ClickHouse 距离函数返回的是距离值，越小越相似（除了点积）
        # dotProduct 返回的是相似度分数，越大越相似
        if cls.distance == "dotProduct":
            search_str += f" order by dis DESC limit {top}"
        else:
            search_str += f" order by dis ASC limit {top}"

        # 添加 ClickHouse 特定的搜索参数
        # ClickHouse 26.6.1.1 使用 hnsw_candidate_list_size_for_search 设置（相当于 HNSW 的 ef_search）
        settings_parts = []
        ef_s = search_params_dict.get("ef_s", None)
        if ef_s is not None:
            settings_parts.append(f"hnsw_candidate_list_size_for_search = {ef_s}")

        if settings_parts:
            search_str += " SETTINGS " + ", ".join(settings_parts)

        res_list = []
        try:
            if protocol == "tcp":
                res = cls.get_client().execute(search_str)
            else:
                res = cls.get_client().query(search_str).result_rows
        except Exception as e:
            raise RuntimeError(e)

        for res_id_dis in res:
            res_list.append((res_id_dis[0], res_id_dis[1]))

        return res_list

    @classmethod
    def search_one(cls, vector: List[float], meta_conditions, top: Optional[int], schema, query: Query) -> List[
        Tuple[int, float]]:
        # ClickHouse 26.6.1.1 不支持 MyScale 的 HybridSearch 和 TextSearch
        # 只支持标准的向量搜索
        return cls.vector_search(vector, meta_conditions, top)

