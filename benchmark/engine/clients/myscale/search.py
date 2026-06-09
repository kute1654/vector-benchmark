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
from benchmark.cli_output import warn
from engine.clients.myscale.config import *
from engine.clients.myscale.config import _to_int
from engine.clients.myscale.parser import MyScaleConditionParser


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


class MyScaleSearcher(BaseSearcher):
    search_params = {}
    client = None
    distance: str = None
    host: str = None
    parser = MyScaleConditionParser()
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
        table_name = validate_table_name(conn.get("table", MYSCALE_DATABASE_NAME))
        host_val = conn.get("host", "127.0.0.1")
        default_port = 9000 if protocol.lower() == "tcp" else 8123
        port_val = int(conn.get("port", default_port) or default_port)
        user_val = conn.get("user", MYSCALE_DEFAULT_USER)
        password_val = conn.get("password", MYSCALE_DEFAULT_PASSWD)
        timeout_raw = conn.get("timeout_s", None)
        if timeout_raw is None:
            timeout_raw = conn.get("timeout", None)
        base_timeout = _to_int(timeout_raw, 300)
        connect_timeout = _to_int(conn.get("connect_timeout", None), 10)
        send_receive_timeout = _to_int(conn.get("send_receive_timeout", base_timeout), base_timeout)
        sync_request_timeout = _to_int(conn.get("sync_request_timeout", base_timeout), base_timeout)
        status_sql = (
            "SELECT count(), countIf(status = 'LOADED') "
            f"FROM system.vector_index_segments WHERE database = 'default' AND table = '{table_name}'"
        )
        rows = None
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
                rows = client.execute(status_sql)
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
                rows = client.query(status_sql).result_rows
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
            return
        total, loaded = rows[0]
        total = int(total or 0)
        loaded = int(loaded or 0)
        if total > 0 and loaded < total:
            warn(
                f"vector index segments not fully loaded into memory for table={table_name}: "
                f"loaded={loaded}/{total}. Recall may be lower than expected until loading finishes."
            )

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
                user=connection_params.get("user", MYSCALE_DEFAULT_USER),
                password=connection_params.get("password", MYSCALE_DEFAULT_PASSWD),
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
                username=connection_params.get("user", MYSCALE_DEFAULT_USER),
                password=connection_params.get("password", MYSCALE_DEFAULT_PASSWD),
                database="default",
                connect_timeout=connect_timeout,
                send_receive_timeout=send_receive_timeout,
            )
        cls.host = host
        cls.distance = DISTANCE_MAPPING[distance]
        cls.search_params = search_params
        cls.apply_query_plan_cache_settings(search_params, protocol)

    @classmethod
    def apply_query_plan_cache_settings(cls, search_params: dict, protocol: str):
        cache_mode = _to_int((search_params or {}).get("use_query_plan_cache", 0), 0)
        CAST_mode = _to_int((search_params or {}).get("query_plan_cache_enable_CAST", 0), 0)
        only_vector = _to_int((search_params or {}).get("query_plan_cache_only_vector", 0), 0)
        use_number = _to_int((search_params or {}).get("query_plan_cache_use_number", 0), 0)
        if cache_mode == 0:
            only_vector = 0
            use_number = 0
        set_cache_sql = f"SET use_query_plan_cache = {cache_mode}"
        set_replace_sql = f"SET query_plan_cache_enable_CAST = {CAST_mode}"
        set_only_vector_sql = f"SET query_plan_cache_only_vector = {only_vector}"
        set_use_number_sql = f"SET query_plan_cache_use_number = {use_number}"
        try:
            client = cls.get_client()
            if protocol == "tcp":
                client.execute(set_cache_sql)
                client.execute(set_replace_sql)
                client.execute(set_only_vector_sql)
                client.execute(set_use_number_sql)
            else:
                client.command(set_cache_sql)
                client.command(set_replace_sql)
                client.command(set_only_vector_sql)
                client.command(set_use_number_sql)
        except Exception as e:
            warn(f"failed to set query plan cache settings: {e}")

    @classmethod
    def get_client(cls):
        return thread_local.client

    @classmethod
    def vector_search(cls, vector: List[float], meta_conditions, top: Optional[int]) -> List[Tuple[int, float]]:
        conn = cls.connection_params or {}
        protocol = str(conn.get("protocol", "tcp")).lower()
        table_name = validate_table_name(conn.get("table", MYSCALE_DATABASE_NAME))
        search_params_dict = (cls.search_params or {}).get("params") or {}
        par = ""
        reserved_keys = {
            "only_text_search",
            "only_vector_search",
            "fusion_type",
            "fusion_weight",
            "fusion_k",
        }
        for key, value in search_params_dict.items():
            if key in reserved_keys or key.startswith("dense"):
                continue
            par += ", \'{}={}\'".format(key, value)
        if par != "":
            par = par[2:]
        if par != "":
            dist_expr = f"distance({par})(vector, {vector})"
        else:
            dist_expr = f"distance(vector, {vector})"
        search_str = f"SELECT id, {dist_expr} as dis FROM {table_name}"

        if meta_conditions is not None:
            search_str += f" prewhere {cls.parser.parse(meta_conditions=meta_conditions)}"

        if cls.distance == "IP":
            search_str += f" order by dis DESC limit {top}"
        else:
            search_str += f" order by dis limit {top}"

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
        if query.query_text is not None and cls.connection_params.get("fts_idx_cols", None) is not None:
            params = (cls.search_params or {}).get("params") or {}
            if params.get("only_vector_search", False):
                return cls.vector_search(vector, meta_conditions, top)
            if params.get("only_text_search", False):
                return cls.text_search(top, query)
            else:
                return cls.hybrid_search(top, query)
                # return cls.hybrid_search_with_ranx(top, query)
        else:
            return cls.vector_search(vector, meta_conditions, top)

    @classmethod
    def hybrid_search(cls, top: Optional[int], query: Query) -> List[Tuple[int, float]]:
        conn = cls.connection_params or {}
        protocol = str(conn.get("protocol", "tcp")).lower()
        table_name = validate_table_name(conn.get("table", MYSCALE_DATABASE_NAME))
        search_params_dict = (cls.search_params or {}).get("params") or {}
        dense_params = []
        # for key, value in search_params_dict.items():
        #     if key.startswith("dense"):
        #         dense_params.append(f"'%s=%s'" % (key, value))
        # if not dense_params:
        #     dense_params.append("'dense_alpha=1'")
        fusion_type = search_params_dict.get("fusion_type", "RRF")
        fusion_weight = search_params_dict.get("fusion_weight", 0.5)
        fusion_k = search_params_dict.get("fusion_k", 60)
        fusion_params = [
            f"'fusion_type={fusion_type}'",
            f"'fusion_weight={fusion_weight}'",
            f"'fusion_k={fusion_k}'",
        ]
        hybrid_params = ", ".join(dense_params + fusion_params)

        query_vec = to_clickhouse_array_literal(query.vector)
        query_text = escape_clickhouse_string_literal(sanitize_text_query(query.query_text))

        search_str = f"""
        SELECT
            id,
            HybridSearch({hybrid_params})(vector, {query.query_text_column}, {query_vec}, '{query_text}') AS dis
        FROM {table_name}
        ORDER BY dis DESC
        LIMIT {top}
        """
        res_list = []
        try:
            if protocol == "tcp":
                res = cls.get_client().execute(search_str)
            else:
                res = cls.get_client().query(search_str).result_rows
        except Exception as e:
            print(search_str)
            raise RuntimeError(e)
        for res_id_dis in res:
            res_list.append((res_id_dis[0], res_id_dis[1]))
        return res_list

    @classmethod
    def text_search(cls, top: Optional[int], query: Query) -> List[Tuple[int, float]]:
        conn = cls.connection_params or {}
        protocol = str(conn.get("protocol", "tcp")).lower()
        table_name = validate_table_name(conn.get("table", MYSCALE_DATABASE_NAME))
        search_params_dict = (cls.search_params or {}).get("params") or {}
        reserved_keys = {
            "only_text_search",
            "only_vector_search",
        }
        text_params = []
        for key, value in search_params_dict.items():
            if key in reserved_keys or key.startswith("dense") or key.startswith("fusion_"):
                continue
            text_params.append(f"'%s=%s'" % (key, value))
        if text_params:
            text_func = f"TextSearch({', '.join(text_params)})"
        else:
            text_func = "TextSearch"
        query_text = escape_clickhouse_string_literal(sanitize_text_query(query.query_text))
        search_str = f"""
        SELECT
            id,
            {text_func}({query.query_text_column}, '{query_text}') AS dis
        FROM {table_name}
        ORDER BY dis DESC
        LIMIT {top}
        """

        res_list = []
        try:
            if protocol == "tcp":
                res = cls.get_client().execute(search_str)
            else:
                res = cls.get_client().query(search_str).result_rows
        except Exception as e:
            print(search_str)
            raise RuntimeError(e)

        for res_id_dis in res:
            res_list.append((res_id_dis[0], res_id_dis[1]))
        return res_list
