import threading
import string
import re
import json
import struct
from typing import List, Optional, Tuple
import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_driver import Client as DriverClient

from benchmark.dataset_reader.base_reader import Query
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

def format_cast_array_literal(values: List[float]) -> str:
    """Render the inline-array literal used inside CAST([...] AS Array(Float32)).

    The h5 datasets (e.g. SIFT, which natively is uint8 descriptors) are stored
    as float32 on disk, so the sampled query vectors arrive here as Python
    floats and a plain ``str(vector)`` renders them as float literals
    (``[119.0, 0.0, ...]``).  Those literals parse as Float64 and value-based
    narrowing Float64 -> Float32 is always allowed, so the plan-cache
    cast_array type-mismatch path (Integer literal -> Array(UInt8) at dim
    <= 255 -> createColumnConst BAD_GET swallowed -> warm hits replay the first
    query's vector) is never exercised during recall measurement.

    Integral-valued vectors are therefore emitted as INTEGER literals
    (``[119,0,...]``); genuinely fractional vectors keep float rendering.
    """
    try:
        if values and all(float(v).is_integer() for v in values):
            return "[" + ",".join(str(int(float(v))) for v in values) + "]"
    except (TypeError, ValueError):
        pass
    return json.dumps(values, separators=(",", ":"))


CLICKHOUSE_SQL_TYPES = ["normal", "cast", "cast_array", "raw_bytes", "raw_bytes_x"]


def _vector_to_hex(values: List[float]) -> str:
    """Convert a float32 vector to uppercase hex string (no prefix).

    Each float is packed as little-endian Float32 (4 bytes), then the whole
    buffer is hex-encoded.  This matches the ClickHouse
    ``reinterpret(unhex('...'), 'Array(Float32)')`` and
    ``reinterpret(x'...', 'Array(Float32)')`` SQL forms.
    """
    buf = struct.pack(f"<{len(values)}f", *values)
    return buf.hex().upper()


def _build_query_literal(values: List[float], sql_type: str) -> str:
    """Build the query-vector literal expression for a given SQL type.

    Supported types (matching the bash benchmark naming convention):
      - normal:       [v1,v2,...]
      - cast:         cast('[v1,v2,...]','Array(Float32)')
      - cast_array:   CAST([v1,v2,...] AS Array(Float32))
      - raw_bytes:    reinterpret(unhex('HEX'), 'Array(Float32)')
      - raw_bytes_x:  reinterpret(x'HEX', 'Array(Float32)')
    """
    v_str = ",".join(str(v) for v in values)
    if sql_type == "normal":
        return f"[{v_str}]"
    elif sql_type == "cast":
        return f"cast('[{v_str}]','Array(Float32)')"
    elif sql_type == "cast_array":
        return f"CAST([{v_str}] AS Array(Float32))"
    elif sql_type == "raw_bytes":
        hex_str = _vector_to_hex(values)
        return f"reinterpret(unhex('{hex_str}'), 'Array(Float32)')"
    elif sql_type == "raw_bytes_x":
        hex_str = _vector_to_hex(values)
        return f"reinterpret(x'{hex_str}', 'Array(Float32)')"
    else:
        return f"CAST([{v_str}] AS Array(Float32))"


thread_local = threading.local()


class ClickHouseSearcher(BaseSearcher):
    search_params = {}
    client = None
    distance: str = None
    host: str = None
    parser = ClickHouseConditionParser()
    connection_params: dict = {}

    @classmethod
    def _apply_session_settings(cls, connection, session_settings: dict):
        if not session_settings:
            return
        protocol = str((cls.connection_params or {}).get("protocol", "tcp")).lower()
        for key, value in session_settings.items():
            try:
                sql = f"SET {key} = {value}"
                if protocol == "tcp":
                    connection.execute(sql)
                else:
                    connection.command(sql)
            except Exception as e:
                warn(f"failed to apply session setting {key}={value}: {e}")

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
        # Apply session-level SET commands from search_params
        session_settings = search_params.get("session_settings", {})
        if session_settings:
            cls._apply_session_settings(thread_local.client, session_settings)
        cls.apply_query_plan_cache_settings(search_params, protocol)

    @classmethod
    def apply_query_plan_cache_settings(cls, search_params: dict, protocol: str):
        """Apply query plan cache and query result cache settings to ClickHouse session.

        每个配置参数都是独立的一等参数，参照 MyScale 实现方式：
          - vector_query_plan_cache:              启用/关闭查询计划缓存 (0/1)
          - vector_use_cast:                      启用/关闭 CAST 向量 (0/1)
          - vector_query_plan_cache_only_vector:  仅缓存向量的查询计划 (0/1)
          - vector_only_cache_query_plan:         仅缓存 QueryPlan，不缓存完整执行计划 (0/1)
          - use_query_cache:                      启用/关闭查询结果缓存 (0/1)

        注意：与旧方案不同，这里不再从 vector_query_plan_cache 的编码值中
              推导 vector_only_cache_query_plan 和 use_query_cache，而是
              直接从 search_params 中读取独立值。
        """
        ef_s = search_params.get("ef_s", None)
        if ef_s is not None:
            set_ef_s_sql = f"SET hnsw_candidate_list_size_for_search = {ef_s}"

        # 以独立参数形式读取所有缓存配置，不再使用编码合并方案
        cache_mode = _to_int((search_params or {}).get("vector_query_plan_cache", 0), 0)
        CAST_mode = _to_int((search_params or {}).get("vector_use_cast", 0), 0)
        only_vector = _to_int((search_params or {}).get("vector_query_plan_cache_only_vector", 0), 0)
        # use_query_cache: 独立控制查询结果缓存，不从 cache_mode 推导
        query_cache = _to_int((search_params or {}).get("use_query_cache", 0), 0)
        # vector_only_cache_query_plan: 独立控制"仅缓存 QueryPlan"模式，不从 cache_mode 推导
        only_cache_query_plan = _to_int((search_params or {}).get("vector_only_cache_query_plan", 0), 0)

        # 如果查询计划缓存未启用，则 only_vector 必须为 0
        if cache_mode == 0:
            only_vector = 0

        set_cache_sql = f"SET vector_query_plan_cache = {cache_mode}"
        set_cast_sql = f"SET vector_use_cast = {CAST_mode}"
        set_only_vector_sql = f"SET vector_query_plan_cache_only_vector = {only_vector}"
        set_query_cache_sql = f"SET use_query_cache = {query_cache}"
        set_only_cache_query_plan_sql = f"SET vector_only_cache_query_plan = {only_cache_query_plan}"
        try:
            client = cls.get_client()
            # 先清除缓存，再设置新参数，确保每次实验从干净的缓存状态开始
            if protocol == "tcp":
                if ef_s is not None:
                    client.execute(set_ef_s_sql)
                client.execute("SYSTEM DROP VECTOR QUERY PLAN CACHE")
                client.execute(set_query_cache_sql)
                client.execute(set_cache_sql)
                client.execute(set_cast_sql)
                client.execute(set_only_vector_sql)
                client.execute(set_only_cache_query_plan_sql)
            else:
                if ef_s is not None:
                    client.command(set_ef_s_sql)
                client.command("SYSTEM DROP VECTOR QUERY PLAN CACHE")
                client.command(set_query_cache_sql)
                client.command(set_cache_sql)
                client.command(set_cast_sql)
                client.command(set_only_vector_sql)
                client.command(set_only_cache_query_plan_sql)
        except Exception as e:
            warn(f"failed to set query plan cache settings: {e}")

    @classmethod
    def get_client(cls):
        return thread_local.client

    @classmethod
    def vector_search(cls, vector: List[float], meta_conditions, top: Optional[int]) -> List[Tuple[int, float]]:
        conn = cls.connection_params or {}
        protocol = str(conn.get("protocol", "tcp")).lower()
        table_name = validate_table_name(conn.get("table", CLICKHOUSE_DATABASE_NAME))
        search_params_dict = (cls.search_params or {}).get("params") or {}

        dist_func = cls.distance
        sql_type = str((cls.search_params or {}).get("sql_type", "cast_array") or "cast_array")
        if vector is not None:
            query_literal = _build_query_literal(vector, sql_type)
        else:
            query_literal = ""
        dist_expr = f"{dist_func}(vector, {query_literal})"

        search_str = f"SELECT id, {dist_expr} as dis FROM {table_name}"

        if meta_conditions is not None:
            search_str += f" prewhere {cls.parser.parse(meta_conditions=meta_conditions)}"

        if cls.distance == "dotProduct":
            search_str += f" order by dis DESC limit {top}"
        else:
            search_str += f" order by dis ASC limit {top}"

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