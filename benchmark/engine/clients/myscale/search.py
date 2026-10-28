"""MyScale 检索阶段。

三种检索路径（由 ``search_one`` 根据数据集与 ``params`` 路由）：

* ``vector_search``：``distance(...)(vector, <literal>)`` 走向量索引的 ANN
  查询，可叠加 ``PREWHERE`` 标量过滤；
* ``text_search``：``TextSearch(...)(text_col, 'query')`` 走倒排全文索引；
* ``hybrid_search``：``HybridSearch(...)(vector, text_col, vec, 'query')``
  同时做向量与全文检索，并按 ``fusion_type``（默认 RRF）融合两路结果。

查询请求的时间计在 benchmark 框架侧，Python 路径拿到的时间单位为**秒**。

``sql_type`` 控制向量字面量的生成方式（见 ``_build_query_literal``），
这些变体用于对比不同参数化/类型推断路径对**查询计划缓存**
（``enable_query_plan_cache``）命中率的影响。
"""

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
from benchmark.cli_output import warn
from engine.clients.myscale.config import *
from engine.clients.myscale.config import _to_int
from engine.clients.myscale.parser import MyScaleConditionParser

# ---- 文本清洗与字面量工具 ----

def remove_punctuation(input_string):
    """删除所有 ASCII 标点符号。

    MyScale 全文检索的查询串把标点当作文法分隔符，原始 query_text 中的
    标点（尤其是引号、括号）可能被解析器当作语法而改变语义甚至报错，
    因此先整体去掉。
    """
    translator = str.maketrans('', '', string.punctuation)
    return input_string.translate(translator)

# 匹配独立的布尔操作符（单词边界），用于统一大小写。
_BOOL_OP_RE = re.compile(r"\b(AND|OR|NOT)\b")
# 控制字符（含 NUL、换行、DEL），直接替换为空格，避免污染 SQL 字符串字面量。
_CONTROL_CHARS_RE = re.compile(r"[\x00-\x1f\x7f]")


def sanitize_text_query(input_string: Optional[str]) -> str:
    """清洗全文检索查询串：控制字符 -> 空格、去标点、压缩空白、布尔词转小写。

    返回的字符串随后会经 ``escape_clickhouse_string_literal`` 转义并作为
    单引号字面量拼进 SQL。``None`` 视为空查询。
    """
    if input_string is None:
        return ""
    text = _CONTROL_CHARS_RE.sub(" ", str(input_string))
    text = remove_punctuation(text)
    text = " ".join(text.split())
    text = _BOOL_OP_RE.sub(lambda m: m.group(1).lower(), text)
    return text


def escape_clickhouse_string_literal(input_string: str) -> str:
    """转义 ClickHouse 单引号字符串：先转义反斜杠，再转义单引号。

    顺序很重要：若先处理单引号，随后替换 ``\\`` 会把刚加上的转义反斜杠
    再翻倍。这里最终得到可安全放进 ``'...'`` 的内容。
    """
    return str(input_string).replace("\\", "\\\\").replace("'", "\\'")


def to_clickhouse_array_literal(values: List[float]) -> str:
    """把 Python 浮点列表渲染成紧凑的 JSON/ClickHouse 数组字面量。

    ``separators`` 去掉多余空格，减小 SQL 文本长度；此处**不**做取整，
    用于 ``HybridSearch`` 的向量参数（该路径更关注语义正确而非计划缓存）。
    """
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


MYSCALE_SQL_TYPES = ["normal", "cast", "cast_array", "raw_bytes", "raw_bytes_x"]


def _vector_to_hex(values: List[float]) -> str:
    """把 float32 向量打包成大写十六进制串（每维 4 字节，小端）。

    对应 SQL 里的 ``reinterpret(unhex('...'), 'Array(Float32)')`` 与
    ``reinterpret(x'...', 'Array(Float32)')`` 两种写法，用于绕开文本解析。
    """
    buf = struct.pack(f"<{len(values)}f", *values)
    return buf.hex().upper()


def _build_query_literal(values: List[float], sql_type: str) -> str:
    """按 ``sql_type`` 生成查询向量字面量（与 ClickHouse searcher 的写法一致）。

    五种形态分别走服务端不同的解析/类型推断路径：

    * ``normal``：裸数组 ``[v1,v2,...]``；
    * ``cast``：先当字符串再 ``cast(...)``；
    * ``cast_array``：``CAST([...] AS Array(Float32))``；
    * ``raw_bytes`` / ``raw_bytes_x``：十六进制二进制重解释。

    这些差异会影响 ``enable_query_plan_cache`` 是否能复用同一份计划，是
    benchmark 对比的核心变量之一。未知取值回落到 ``cast_array``。
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


# 每个工作线程一份连接：ClickHouse 系列驱动不保证线程安全，而搜索阶段会
# 多线程并发查询，连接必须按线程隔离。
thread_local = threading.local()


class MyScaleSearcher(BaseSearcher):
    """MyScale 检索器；三种检索路径见模块 docstring。"""

    search_params = {}
    client = None
    distance: str = None
    host: str = None
    parser = MyScaleConditionParser()
    connection_params: dict = {}
    # None = not probed yet; see _supports_string_to_vector_parallel.
    _string_to_vector_supported: Optional[bool] = None

    @classmethod
    def _apply_session_settings(cls, connection, session_settings: dict):
        """逐条执行 ``SET key = value``（来自 search_params.session_settings）。

        设置是**连接级**的：ClickHouse 的 SET 只对当前 session 生效，所以每个
        工作线程在 ``init_client`` 建连后都要各自重放一遍，否则只有主线程的
        连接带上了这些参数。单条失败只告警不中断——不同版本的 setting 集合
        不完全一致，缺一条不影响其它设置与后续查询。
        """
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
        """按数据集分组做一次检索前的参数定调。

        ``text_search`` 组的数据集只测全文检索：这里把 ``only_text_search`` 写进
        ``search_params["params"]``，后续 ``vector_search`` / ``search_one`` 会据此
        跳过向量路径。写入前先确认 ``params`` 是 dict 并就地构造，避免配置里给
        的是 None 或列表时在别处炸掉。
        """
        if dataset_config is not None and getattr(dataset_config, "result_group", None) == "text_search":
            params = search_params.get("params", None)
            if not isinstance(params, dict):
                params = {}
            params["only_text_search"] = True
            search_params["params"] = params

    def post_warmup(self, dataset_config):
        """warmup 结束后检查向量索引段是否已全部 LOADED（仅告警，不阻塞）。

        为什么需要这一步：MyScale 的 ``ADD VECTOR INDEX`` 状态变成 ``Built``
        只代表索引**构建**完成，索引段还要异步加载进内存。加载未完成时查询
        会退化成部分扫描，recall 会莫名偏低，而这种偏差在没有提示的情况下
        极难归因。这里在正式测速前读一次 ``system.vector_index_segments``，
        有未加载段就打一条 warn 说明"recall 可能偏低，直到加载完成"。

        用临时连接（tcp 用 ``clickhouse_driver``，http 用
        ``clickhouse_connect``）而不是线程本地连接：本方法在 warmup 之后、
        多线程测速之前调用，不应把一条探测连接留给后续工作线程复用。
        """
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
        """在工作线程内建立连接、缓存度量名，并下发本连接的 SET 设置。

        超时策略分两档：warmup 阶段（``search_params["_warmup"]`` 为真）默认取
        ``max(base_timeout, 1800)`` 秒，因为冷启动后的首批查询要付出把索引段
        读进内存的代价，用常规超时会大量误报超时；正式测速阶段用
        ``base_timeout``（默认 300s），保证慢查询能如实暴露为超时而不是被
        无限放行。``connect_timeout`` 单独给 10s，避免网络不通时长时间挂住。

        连接只挂在 ``thread_local.client`` 上（每线程一份），最后应用
        session_settings 与计划缓存/查询缓存设置——这些 SET 都是连接级的，
        换线程即丢失，所以必须在建连后立刻执行。
        """
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
        # Apply session-level SET commands from search_params
        session_settings = search_params.get("session_settings", {})
        if session_settings:
            cls._apply_session_settings(thread_local.client, session_settings)
        cls.apply_query_plan_cache_settings(search_params, protocol)

    @classmethod
    def _string_to_vector_parallel_sql(cls, search_params: dict, protocol: str) -> Optional[str]:
        """SQL for the fork's parallel string->vector conversion settings.

        Returns None when the config does not pick a mode (leave the server
        default alone, i.e. the pre-existing serial path) or when the server
        has no such settings.

        中文说明：返回 ``None`` 有三种情况——配置没写 mode（保持服务端默认，
        即改造前的串行路径）、服务端没有这两个 setting（上游 ClickHouse 没有
        这个 fork 的改动）、或方言不支持。把两个 setting 拼成一条 ``SET``
        一起下发，避免分成两条时中途失败留下"只改了 mode 没改线程数"的半
        生效状态。
        """
        mode_raw = (search_params or {}).get("string_to_vector_parallel_mode", None)
        if mode_raw is None:
            return None
        if not cls._supports_string_to_vector_parallel(protocol):
            return None
        mode = _to_int(mode_raw, 0)
        threads = _to_int((search_params or {}).get("string_to_vector_parallel_threads", None), 0)
        return (f"SET string_to_vector_parallel_mode = {mode}, "
                f"string_to_vector_parallel_threads = {threads}")

    @classmethod
    def _supports_string_to_vector_parallel(cls, protocol: str) -> bool:
        """Probe system.settings once per process (upstream ClickHouse lacks these).

        中文说明：``string_to_vector_parallel_mode`` 只存在于本项目的 fork，上游
        ClickHouse 没有；直接 SET 会报未知 setting 并让整次搜索失败。因此先用
        一次探测查询确认两个 setting 都存在，结果缓存在
        ``_string_to_vector_supported`` 上，每个进程只探测一次。

        探测本身失败（连接抖动、权限）时**不缓存**结果：缓存成 False 会让本次
        运行剩余的全部查询静默降级成串行，性能数据就失去可比性；不缓存则下
        一次调用会重试。
        """
        if cls._string_to_vector_supported is None:
            probe = (
                "SELECT count() FROM system.settings WHERE name IN "
                "('string_to_vector_parallel_mode', 'string_to_vector_parallel_threads')"
            )
            found = None
            try:
                client = cls.get_client()
                if protocol == "tcp":
                    rows = client.execute(probe)
                    found = int(rows[0][0]) if rows else 0
                else:
                    found = int(client.command(probe))
            except Exception as e:
                warn(f"failed to probe string_to_vector_parallel_mode support: {e}")
            if found is None:
                # Transient failure (e.g. the connection just went away): do not
                # cache it, so the next searcher retries instead of silently
                # downgrading the rest of the run to the serial path.
                return False
            cls._string_to_vector_supported = found == 2
            if not cls._string_to_vector_supported:
                warn(
                    "server does not support string_to_vector_parallel_mode/threads: "
                    "string->vector conversion stays on the default (serial) path"
                )
        return cls._string_to_vector_supported

    @classmethod
    def apply_query_plan_cache_settings(cls, search_params: dict, protocol: str):
        """把配置里的缓存开关翻译成一组 ``SET``，并在本连接上生效。

        两类缓存分开控制：

        * **计划缓存**——``enable_query_plan_cache`` 用三值编码压缩了两个布尔
          量（见下方注释），外加 ``enable_cast_vector``（查询向量字面量是否
          在计划里保留 CAST）、``query_plan_cache_only_vector``（只缓存向量
          查询）与 ``only_cache_query_plan``（只缓存 QueryPlan 阶段）。
        * **结果缓存**——``use_query_cache`` 一个开关同时驱动四个
          ``enable_reads_from_query_cache`` / ``enable_writes_to_query_cache`` /
          ``query_cache_min_query_runs`` / ``use_query_cache``。四者必须一致，
          否则会出现"只读不写"（永不命中）或"只写不读"（白付写入开销）
          这种静默失真。

        顺序上先 DROP 两类缓存再 SET：DROP 本身也需要在开关打开前执行，且
        残留的缓存条目（上一次实验的）会让首轮命中率虚高，必须清干净。
        整段包在 try 里只告警：老版本服务端可能缺个别 setting，不应因此让
        搜索阶段起不来——缺哪个就会在日志里看到对应的失败项。
        """
        # enable_query_plan_cache 为三值编码(来自 config enable_query_plan_cache):
        # 0=缓存关, 1=启用计划缓存, 2=启用且仅缓存 QueryPlan(only_cache_query_plan=1)。
        # 因 only_cache_query_plan=1 必须 enable_query_plan_cache=1, 有效组合由 2*2
        # 压缩为 [0,1,2] 三种。
        cache_mode_enc = _to_int((search_params or {}).get("enable_query_plan_cache", search_params.get("use_query_plan_cache", 0)), 0)
        CAST_mode = _to_int((search_params or {}).get("query_plan_cache_enable_CAST", 0), 0)
        query_cache = _to_int((search_params or {}).get("use_query_cache", 0), 0)
        # 三值编码解码: cache_mode=1 iff enc>=1; only_cache_query_plan=1 iff enc>=2。
        # 兼容外部直接传入独立 vector_only_cache_query_plan 的场景。
        cache_mode = 1 if cache_mode_enc >= 1 else 0
        only_cache_query_plan = (1 if cache_mode_enc >= 2
                                 else _to_int((search_params or {}).get("vector_only_cache_query_plan", 0), 0))
        # if cache_mode > 1:
        #     query_cache = 1
        #     cache_mode = cache_mode - 2
        if cache_mode == 0:
            only_vector = 0
        else:
            only_vector = _to_int((search_params or {}).get("query_plan_cache_only_vector", 0), 0)
        set_cache_sql = f"SET enable_query_plan_cache = {cache_mode}"
        set_replace_sql = f"SET enable_cast_vector = {CAST_mode}"
        set_only_vector_sql = f"SET query_plan_cache_only_vector = {only_vector}"
        set_only_cache_query_plan_sql = f"SET only_cache_query_plan = {only_cache_query_plan}"
        drop_query_cache_sql = "SYSTEM DROP QUERY CACHE"
        drop_query_plan_cache_sql = "SYSTEM DROP QUERY PLAN CACHE"
        set_query_cache_sql = f"SET use_query_cache = {query_cache}"
        set_read_cache_sql = f"SET enable_reads_from_query_cache = {query_cache}"
        set_write_cache_sql = f"SET enable_writes_to_query_cache = {query_cache}"
        set_cache_min_query_sql = f"SET query_cache_min_query_runs = {query_cache}"
        set_sv_sql = cls._string_to_vector_parallel_sql(search_params, protocol)
        try:
            client = cls.get_client()
            if protocol == "tcp":
                client.execute(drop_query_cache_sql)
                client.execute(drop_query_plan_cache_sql)
                client.execute(set_query_cache_sql)
                client.execute(set_read_cache_sql)
                client.execute(set_write_cache_sql)
                client.execute(set_cache_min_query_sql)
                client.execute(set_cache_sql)
                client.execute(set_replace_sql)
                client.execute(set_only_vector_sql)
                client.execute(set_only_cache_query_plan_sql)
                if set_sv_sql:
                    client.execute(set_sv_sql)
            else:
                client.command(drop_query_cache_sql)
                client.command(drop_query_plan_cache_sql)
                client.command(set_query_cache_sql)
                client.command(set_read_cache_sql)
                client.command(set_write_cache_sql)
                client.command(set_cache_min_query_sql)
                client.command(set_cache_sql)
                client.command(set_replace_sql)
                client.command(set_only_vector_sql)
                client.command(set_only_cache_query_plan_sql)
                if set_sv_sql:
                    client.command(set_sv_sql)
        except Exception as e:
            warn(f"failed to set query plan cache settings: {e}")

    @classmethod
    def get_client(cls):
        """取本线程的连接（``init_client`` 里写入）。"""
        return thread_local.client

    @classmethod
    def vector_search(cls, vector: List[float], meta_conditions, top: Optional[int]) -> List[Tuple[int, float]]:
        """纯向量检索：``distance(<参数>)(vector, <字面量>)`` 排序取 top。

        SQL 形状（由下面几段拼接而成）::

            SELECT id, distance('alpha=...')(vector, <literal>) AS dis
            FROM <table>
            PREWHERE <标量条件>
            ORDER BY dis [DESC] LIMIT <top>

        几个容易踩的点：

        * 参数列表来自 ``search_params["params"]``，但 ``only_text_search`` /
          ``only_vector_search`` / ``fusion_*`` 是 benchmark 自己的调度开关，
          不是 MyScale 的距离参数，必须剔除，否则服务端会报未知参数；以
          ``dense`` 开头的是混合检索的稠密侧权重，同样不属于这里。
        * 标量过滤走 ``PREWHERE`` 而非 ``WHERE``：MyScale/ClickHouse 先按
          PREWHERE 裁剪 part 再读取向量列，能显著减少送入距离计算的行数。
        * 度量是内积（``IP``）时距离越大越相似，排序方向要反过来；L2/余弦
          都是越小越相似，默认升序。
        """
        conn = cls.connection_params or {}
        protocol = str(conn.get("protocol", "tcp")).lower()
        table_name = validate_table_name(conn.get("table", MYSCALE_DATABASE_NAME))
        search_params_dict = (cls.search_params or {}).get("params") or {}
        sql_type = str((cls.search_params or {}).get("sql_type", "cast_array") or "cast_array")
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
        if vector is not None:
            query_literal = _build_query_literal(vector, sql_type)
        else:
            query_literal = ""
        if par != "":
            dist_expr = f"distance({par})(vector, {query_literal})"
        else:
            dist_expr = f"distance(vector, {query_literal})"
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
        """单条查询的路径选择器（引擎无关接口的唯一入口）。

        只有"查询带文本"且"表上确实建了 fts 索引列"时才可能走文本路径，否则
        一律纯向量检索——没有 fts 索引还去调 ``TextSearch`` 会直接报错。
        进入文本分支后再看 ``params`` 里的开关：``only_vector_search`` 强制
        向量、``only_text_search`` 强制全文，都未设置时做混合检索（RRF）。

        ``schema`` 与 ``query`` 之外的参数保持与基类一致的签名，方便
        ``BaseSearcher`` 统一调用；本实现不需要 schema。
        """
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
        """稠密+稀疏融合检索：调用 MyScale 的 ``HybridSearch(...)`` 表函数。

        融合参数默认 RRF（Reciprocal Rank Fusion）：``fusion_weight=0.5`` 让
        两路权重相等，``fusion_k=60`` 是 RRF 里惯用的平滑常数（越大越削弱
        头部排名差异）。这些默认值只在配置没给的时候生效，配置写了就原样下传。

        查询向量用 ``to_clickhouse_array_literal`` 生成 ``[..]`` 数组字面量
        （不用 ``_build_query_literal`` 的四种变形）：混合检索这条路径在
        benchmark 里不参与计划缓存对比，保持最直白的写法即可。查询文本必须
        先清洗再转义——它会被包进单引号字面量，原始文本里的引号会破坏 SQL。

        失败时打印整条 SQL 再抛出：混合检索的参数组合最容易出错，没有 SQL
        就只能靠猜。
        """
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
        """纯全文检索：``TextSearch(<参数>)(<文本列>, '<清洗后的查询>')``。

        参数来自 ``search_params["params"]``，剔除 benchmark 调度开关
        （``only_text_search`` / ``only_vector_search``）和属于混合检索的
        ``dense*`` / ``fusion_*`` 键，剩下的才是 TextSearch 认识的分词/查询
        参数。一个参数都没有时退化成不带括号的 ``TextSearch``，让服务端用默认
        配置——注意这里不能再拼一对空括号 ``TextSearch()``，那是两种不同写法。

        和混合检索一样，清洗后的文本要再转义单引号后才能放进 SQL 字面量。
        """
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