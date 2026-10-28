"""pgvector 检索阶段（驱动式引擎的 searcher 实现）。

一次查询两种执行路径，由 ``use_query_plan_cache`` 档位决定：

* ``_direct_search``：把查询向量直接内联进 SQL 文本，走 PostgreSQL 的
  **simple 协议**。每次都要重新解析 + 规划，作为"无计划缓存"的基线。
* ``_prepared_search``：``PREPARE`` 命名语句 + ``EXECUTE``，走 **extended
  协议**（psycopg2 的 ``%s`` 占位绑定），``plan_cache_mode`` 决定服务端是
  复用 generic plan（force_generic_plan）还是每次重规划（force_custom_plan）。

``sql_type`` 决定查询向量在 SQL 里的**字面量形态**（``'[...]'::vector`` /
``CAST(...)`` / ``ARRAY[...]`` / ``halfvec`` / CTE ...）。不同形态会走不同的
类型推断路径，是计划缓存对比实验里被刻意保留的变量；``_render_prepare_sql``
与 ``_render_inline_sql`` 必须保持同一 ``sql_type`` 下语义等价，否则对比的
就不是"缓存与否"而是"查询写法不同"。

pgvector 的三个距离算子（``<->``、``<#>``、``<=>``）都满足"越小越相似"，
因此排序恒为 ``ASC``——这一点与 ClickHouse/MyScale 的内积（越大越好）不同。

时间由 benchmark 框架侧计量，Python 侧拿到的时间单位是**秒**。
"""

import math
import json
import threading
from typing import List, Optional, Tuple

import psycopg2

try:
    from pgvector.psycopg2 import register_vector as _pgv_register_vector
except Exception:  # pragma: no cover
    # 没装 pgvector-python 时静默降级：仍可用文本参数，只是少了二进制适配器。
    _pgv_register_vector = None

from benchmark.dataset_reader.base_reader import Query
from engine.base_client import BaseSearcher
from benchmark.cli_output import warn
from engine.clients.pgvector.config import *


# 预留的线程本地槽位。注意本类实际把连接存成**类属性** ``cls.connection``，
# 并未使用 thread_local——多进程模式下每个 worker 是独立进程，连接天然隔离。
thread_local = threading.local()


class PGVectorSearcher(BaseSearcher):
    """pgvector 检索器：按 ``sql_type`` 与计划缓存档位选择执行路径。"""

    search_params = {}
    connection = None
    distance_op: str = None
    distance_name: str = None
    host: str = None
    connection_params: dict = {}
    sql_type: str = "text_literal"
    use_query_plan_cache: int = 0
    use_result_cache: int = 0
    # 是否类型化绑定参数 (档1/档3, 对齐 polardb): PREPARE (vector) + $1 无 cast,
    # 文本在 Bind 阶段一次转成 vector Datum (与 sql_type 解耦)。
    typed_bind: bool = False
    # PREPARE 形式: 命名 PREPARE 语句名 + plan_cache_mode (查询计划缓存开关)
    prepared_statement_name: Optional[str] = None

    def __init__(self, host, connection_params, search_params):
        """补齐 pgvector 的连接默认值后交给基类。

        配置里只写关心的字段即可（例如只改 port/database），其余用默认值。
        先合并再 ``super().__init__``，是因为基类会立即用这份参数做初始化，
        合并晚了就等于默认值没生效。
        """
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

        super().__init__(host, merged_conn_params, search_params)

    @classmethod
    def _apply_session_settings(cls, connection, session_settings: dict):
        """逐条执行 ``SET key = value``。

        先按参数化写法 ``SET key = %s`` 执行（值走绑定，安全）；老版本
        PostgreSQL 拒绝 SET 的参数化形式，因此失败时回退成单引号字面量。
        单个 setting 失败会直接抛出，由调用方决定是否忽略。
        """
        if not session_settings:
            return
        with connection.cursor() as cursor:
            for key, value in session_settings.items():
                try:
                    cursor.execute(f"SET {key} = %s", (str(value),))
                except Exception:
                    cursor.execute(f"SET {key} = '{value}'")

    def setup_search(self, host, distance, connection_params: dict, search_params: dict, dataset_config):
        """pgvector 无需在检索前额外定调（基类接口占位）。"""
        pass

    @classmethod
    def _effective_ef_s(cls, search_params: dict) -> int:
        """取 ``hnsw.ef_search``，兼容配置里写成单元素列表的形式。

        ``params.ef_s`` 允许是标量或列表（方便一次扫多个取值）；这里只取
        第一个元素，缺省/空值都回落到 100（pgvector 的默认值）。
        """
        params = (search_params or {}).get("params", {})
        if not isinstance(params, dict):
            return 100
        ef_s = params.get("ef_s", 100)
        if isinstance(ef_s, (list, tuple)):
            ef_s = ef_s[0] if ef_s else 100
        return int(ef_s or 100)

    @classmethod
    def init_client(
            cls, host: str, distance, connection_params: dict, search_params: dict
    ):
        """在工作进程内建连，并把本实验的会话级设置全部下发。

        执行顺序是有意安排的：

        1. 建连 + ``autocommit``——``SET`` 在显式事务里只对当前事务有效，
           非 autocommit 下设置会随事务结束丢失。
        2. 注册 pgvector 二进制参数适配器（有包才做）：让 ``$1`` 以 vector
           二进制格式发送，避免 generic plan 每行重复走 ``vector_in``。
        3. session_settings（配置直给的 SET）→ 结果缓存 GUC → 文本解析器
           GUC → ``hnsw.ef_search``（召回率旋钮）。
        4. 计划缓存档位：0 不 PREPARE；1/2/3 分别对应 auto、
           ``force_custom_plan``、``force_generic_plan``，且都打开类型化绑定，
           使档位之间只差"计划是否复用"这一个变量。

        PREPARE 失败（例如 ``halfvec`` 形态拿不到维度）会把
        ``prepared_statement_name`` 置空，``vector_search`` 自动回退到内联
        SQL，因此不会让整轮实验失败。
        """
        cls.connection_params = connection_params
        cls.host = host
        cls.distance_op = DISTANCE_MAPPING[distance]
        cls.distance_name = str(getattr(distance, "value", distance)).lower()
        cls.search_params = search_params or {}
        cls.sql_type = str(cls.search_params.get("sql_type", "text_literal"))
        # sql_text_type: 0=默认形式(由 sql_type 决定的字面量写法),
        #                1=使用 vector 类型字面量 (为 '[...]'::vector)
        cls.sql_text_type = int(cls.search_params.get("sql_text_type", 0) or 0)
        cls.use_result_cache = int(cls.search_params.get("use_result_cache", 0))
        # 结果缓存不再用 python dict 模拟, 交由 PG 会话级 GUC 控制

        # Create connection per worker process
        cls.connection = psycopg2.connect(
            host=connection_params.get("host", "127.0.0.1"),
            port=connection_params.get("port", PGVECTOR_DEFAULT_PORT),
            user=connection_params.get("user", PGVECTOR_DEFAULT_USER),
            password=connection_params.get("password", PGVECTOR_DEFAULT_PASSWD),
            database=connection_params.get("database", PGVECTOR_DATABASE_NAME),
        )
        cls.connection.autocommit = True

        # 方案B/改造②: 注册 pgvector 二进制 vector 参数适配器。
        # register_vector 让 psycopg2 把 Python 列表以 vector_recv 二进制格式发送,
        # 避免 generic plan 下 "$1::vector" 每行重复走 vector_in 解析, 也不引入
        # real[]/数组 cast 的 per-query 开销。无 pgvector python 包时静默跳过,
        # 回退到文本参数路径 (behavior 与改造前一致)。
        if _pgv_register_vector is not None:
            try:
                _pgv_register_vector(cls.connection)
            except Exception as exc:
                warn(f"could not register pgvector vector adapter: {exc}")

        # Apply session-level SET commands from search_params
        session_settings = cls.search_params.get("session_settings", {})
        if session_settings:
            cls._apply_session_settings(cls.connection, session_settings)

        # 查询结果缓存: 对齐 sh 脚本, 用 PG 会话级 GUC (result_cache + result_cache_debug)
        # 统一开启/关闭, 而不是在 python 侧用 dict 模拟。
        cls._apply_result_cache_guc(on=bool(cls.use_result_cache))

        # 向量文本解析器: 按配置的 vector.in_parse_mode 设置会话级 GUC
        # (strtof / inline / fast_float)。未配置时跳过, 使用向量库默认值。
        cls._apply_in_parse_mode_guc()

        # Apply the HNSW search beam width (ef_search) -> controls recall.
        ef_s = cls._effective_ef_s(cls.search_params)
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute("SET hnsw.ef_search = %s", (ef_s,))
        except Exception as exc:
            warn(f"could not set hnsw.ef_search={ef_s}: {exc}")

        # 查询计划缓存 (enable_query_plan_cache, 配置键优先; 兼容旧 use_query_plan_cache):
        #   0 -> 不使用查询计划缓存: 不 PREPARE, prepared_statement_name 为空,
        #        走 _direct_search (simple 协议文本内联, 每次重新解析+规划) = 无计划缓存基线。
        #   1 -> 查询计划缓存 + auto 模式        (PREPARE + plan_cache_mode=auto)
        #   2 -> 查询计划缓存 + custom 模式      (PREPARE + plan_cache_mode=force_custom_plan)
        #   3 -> 查询计划缓存 + force_generic_plan 模式 (PREPARE + plan_cache_mode=force_generic_plan)
        # 计划缓存档均采用类型化绑定 (PREPARE (vector), $1 无 cast), 文本在 Bind 阶段
        # 一次转成 vector Datum; 档位差异只由 plan_cache_mode 承载 (auto 自动决策 /
        # custom 每次重规划 / generic 复用 generic plan), 从而干净隔离各模式收益。
        cls.use_query_plan_cache = int(
            cls.search_params.get(
                "enable_query_plan_cache",
                cls.search_params.get("use_query_plan_cache", 0),
            )
        )
        cls.typed_bind = False
        if cls.use_query_plan_cache == 0:
            cls.prepared_statement_name = None
        else:
            _mode = {
                1: "auto",
                2: "force_custom_plan",
                3: "force_generic_plan",
            }.get(cls.use_query_plan_cache, "auto")
            cls._set_plan_cache_mode(_mode)
            cls.typed_bind = True
            cls._prepare_statement(int(cls.search_params.get("top", 10)))

    @classmethod
    def _apply_result_cache_guc(cls, on: bool):
        """对齐 sh 脚本 session_set_line_for_result: 用 PG 会话级 GUC 控制查询结果缓存。

        on=True  → SET pgvector.result_cache = on; SET pgvector.result_cache_debug = on
        on=False → 两个开关都置 off
        (禁用 python 侧 dict 模拟, 避免与 PG 层缓存不一致、干扰召回率/QPS 对比。)
        """
        val = "on" if on else "off"
        for guc in (PGVECTOR_RESULT_CACHE_GUC, PGVECTOR_RESULT_CACHE_DEBUG_GUC):
            try:
                with cls.connection.cursor() as cursor:
                    cursor.execute(f"SET {guc} = {val}")
            except Exception as exc:
                warn(f"could not SET {guc}={val}: {exc}")

    @classmethod
    def _apply_in_parse_mode_guc(cls):
        """按配置的 vector.in_parse_mode 设置会话级 GUC。

        值取自 search_params["vector_in_parse_mode"] (由 config.json 的 upload_params
        经 client._run_pgvector_experiment 注入); 未配置时跳过, 使用向量库默认值。
        """
        mode = cls.search_params.get("vector_in_parse_mode")
        if not mode:
            return
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(f"SET {PGVECTOR_IN_PARSE_MODE_GUC} = %s", (str(mode),))
        except Exception as exc:
            warn(f"could not SET {PGVECTOR_IN_PARSE_MODE_GUC}={mode}: {exc}")

    @classmethod
    def _param_dim(cls) -> Optional[int]:
        """获取向量维度 (halfvec_literal / array_to_vector_fn 的准备语句需要 dim)。

        优先从配置读取, 否则回查表首行向量列实际维度; 失败返回 None。
        """
        dim = cls.search_params.get("dims") or cls.connection_params.get("dims")
        if dim:
            return int(dim)
        t = validate_table_name(cls.connection_params.get("table", "vec_items"))
        col = cls.connection_params.get("vector_col", "vector")
        try:
            with cls.get_connection().cursor() as cursor:
                # vector::real[] 拆开统计元素个数得到维度
                cursor.execute(
                    "SELECT count(*) FROM (SELECT unnest((SELECT {0} FROM {1} LIMIT 1)::real[]) AS x) AS s".format(
                        f'"{col}"', f'"{t}"'
                    )
                )
                row = cursor.fetchone()
                return int(row[0]) if row and row[0] is not None else None
        except Exception:
            return None

    @classmethod
    def _param_type(cls) -> str:
        """sql_type / 绑定档位 -> PREPARE 的参数类型。

        档1/档3 类型化绑定 (typed_bind=True, 对齐 polardb) 统一收窄为 vector 参数
        ($1 无 cast); 其余档位按 sql_type 保留各类型形态 (real[] 数组 / text 文本)。
        """
        st = cls.sql_type
        if cls.typed_bind:
            return "vector"
        if st in ("array_int_cast", "array_real_cast", "array_cast", "array_to_vector_fn"):
            return "real[]"
        return "text"

    @classmethod
    def _render_prepare_sql(cls, top: int, dim: Optional[int]) -> Optional[str]:
        """按 sql_type 生成 PREPARE 语句体, 保留该 SQL 类型各自的向量构造形态。

        查询向量以参数 $1 传入 (EXECUTE 时绑定), 避免每次直接内联字面量; 同时保留
        ARRAY[]/array_to_vector/CTE 等不同转换形态。返回 None 表示该类型需要 dim 却
        拿不到 (调用方回退 inline SQL)。TOP_K 常量内联以保证 HNSW 索引可被规划命中。
        """
        op = cls.distance_op
        t = validate_table_name(cls.connection_params.get("table", "vec_items"))
        col = cls.connection_params.get("vector_col", "vector")
        k = int(top)
        st = cls.sql_type
        if cls.sql_text_type == 1:
            st = "text_literal"

        # 距离右侧表达式 (以参数 $1 为源, 保留该 sql_type 的向量转换形态)
        if cls.typed_bind:
            # 档1/档3 类型化绑定 (对齐 polardb): $1 声明为 vector 参数, 直接用 $1
            # (无 cast)。文本在 Bind 阶段一次转成 vector Datum, generic plan 复用,
            # 消除逐行 vector_in 解析。
            expr = "$1"
        elif st == "text_literal":
            expr = "$1::vector"
        elif st == "text_cast":
            expr = "CAST($1 AS vector)"
        elif st == "vector_fn":
            expr = "vector($1)"
        elif st == "halfvec_literal":
            if dim is None:
                return None
            expr = f"$1::halfvec({dim})::vector"
        elif st == "array_int_cast":
            expr = "$1::vector"
        elif st == "array_real_cast":
            expr = "$1::vector"
        elif st == "array_cast":
            expr = "CAST($1 AS vector)"
        elif st == "array_to_vector_fn":
            if dim is None:
                return None
            expr = f"array_to_vector($1, {dim}, false)"
        elif st == "with_text_literal":
            expr = "$1::vector"
        elif st == "with_array_cast":
            expr = "CAST($1 AS vector)"
        elif st == "with_vector_fn":
            expr = "vector($1)"
        else:
            expr = "$1::vector"

        if st.startswith("with_"):
            return (f"WITH kv AS (SELECT {expr} AS v) "
                    f"SELECT t.id, (t.{col} {op} kv.v) AS dis FROM {t} t CROSS JOIN kv "
                    f"ORDER BY dis ASC LIMIT {k}")
        return (f"SELECT t.id, (t.{col} {op} {expr}) AS dis FROM {t} t "
                f"ORDER BY dis ASC LIMIT {k}")

    @classmethod
    def _set_plan_cache_mode(cls, mode: str):
        """SET plan_cache_mode = 指定值, 作为查询计划缓存的开关。

        force_generic_plan -> 命中并复用 generic plan (use_query_plan_cache=1)
        force_custom_plan  -> 每次都重新实例化 custom plan (use_query_plan_cache=0)
        """
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(f"SET plan_cache_mode = {mode}")
        except Exception as exc:
            warn(f"could not set plan_cache_mode={mode}: {exc}")

    @classmethod
    def _prepare_statement(cls, top: int):
        """PREPARE 形式: 对当前 sql_type 建立命名 PREPARE 语句并记录语句名。

        每个 sql_type 各自建立对应的参数化 PREPARE (保留该类型向量构造形态);
        失败时回退 inline SQL (use_query_plan_cache 置 0, vector_search 走 _direct_search)。
        """
        dim = cls._param_dim()
        body = cls._render_prepare_sql(top, dim)
        if body is None:
            cls.prepared_statement_name = None
            warn(f"sql_type {cls.sql_type} needs a dim; falling back to inline SQL")
            return
        pt = cls._param_type()
        cls.prepared_statement_name = f"pgv_ps_{cls.sql_type}_{get_random_string(6)}"
        prepare_sql = (
            f"PREPARE {cls.prepared_statement_name}({pt}) AS {body}"
        )
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(prepare_sql)
        except Exception as e:
            cls.prepared_statement_name = None
            warn(f"Failed to PREPARE statement for {cls.sql_type}, falling back to inline SQL: {e}")

    @classmethod
    def get_connection(cls):
        """取本进程的连接（``init_client`` 中创建）。"""
        return cls.connection

    @staticmethod
    def _vec_str(vector: List[float]) -> str:
        """把向量渲染成逗号分隔串，供 ``[...]`` / ``ARRAY[...]`` 包裹。

        NaN 替换成 ``0.0``：PostgreSQL 的 vector 类型不接受 NaN，数据集里若
        出现 NaN（通常是上游归一化除零产生的）会让整条查询报错，而 benchmark
        需要的是能跑完的对比数据，所以按"缺失即零"处理。
        """
        return ",".join('0.0' if math.isnan(x) else str(x) for x in vector)

    @classmethod
    def _render_inline_sql(cls, vector: List[float], top: int) -> str:
        """Render an inline-literal SQL form (vector baked into the text, simple protocol).

        pgvector's distance operators (<->, <#>, <=>) all return a value where LOWER is
        better, so the ORDER BY is always ASC.
        """
        op = cls.distance_op
        t = validate_table_name(cls.connection_params.get("table", "vec_items"))
        v = cls._vec_str(vector)          # bare comma-separated (for ARRAY[...] / '...' text)
        dim = len(vector)
        k = int(top)
        st = cls.sql_type
        if cls.sql_text_type == 1:
            # 使用 vector 类型字面量 (::vector), 覆盖 sql_type 的附加构造 (数组/CTE 等)
            st = "text_literal"

        if st == "text_literal":
            return f"SELECT id, (vector {op} '[{v}]'::vector) AS dis FROM {t} ORDER BY dis ASC LIMIT {k}"
        if st == "text_cast":
            return f"SELECT id, (vector {op} CAST('[{v}]' AS vector)) AS dis FROM {t} ORDER BY dis ASC LIMIT {k}"
        if st == "vector_fn":
            return f"SELECT id, (vector {op} vector('[{v}]')) AS dis FROM {t} ORDER BY dis ASC LIMIT {k}"
        if st == "array_int_cast":
            return f"SELECT id, (vector {op} ARRAY[{v}]::vector) AS dis FROM {t} ORDER BY dis ASC LIMIT {k}"
        if st == "array_real_cast":
            return f"SELECT id, (vector {op} ARRAY[{v}]::real[]::vector) AS dis FROM {t} ORDER BY dis ASC LIMIT {k}"
        if st == "array_cast":
            return f"SELECT id, (vector {op} CAST(ARRAY[{v}]::real[] AS vector)) AS dis FROM {t} ORDER BY dis ASC LIMIT {k}"
        if st == "array_to_vector_fn":
            return f"SELECT id, (vector {op} array_to_vector(ARRAY[{v}]::real[], {dim}, false)) AS dis FROM {t} ORDER BY dis ASC LIMIT {k}"
        if st == "halfvec_literal":
            return f"SELECT id, (vector {op} '[{v}]'::halfvec({dim})::vector) AS dis FROM {t} ORDER BY dis ASC LIMIT {k}"
        if st == "with_text_literal":
            return (f"WITH qv AS (SELECT '[{v}]'::vector AS v) "
                    f"SELECT t.id, (t.vector {op} qv.v) AS dis FROM {t} AS t CROSS JOIN qv "
                    f"ORDER BY dis ASC LIMIT {k}")
        if st == "with_array_cast":
            return (f"WITH qv AS (SELECT ARRAY[{v}]::real[]::vector AS v) "
                    f"SELECT t.id, (t.vector {op} qv.v) AS dis FROM {t} AS t CROSS JOIN qv "
                    f"ORDER BY dis ASC LIMIT {k}")
        if st == "with_vector_fn":
            return (f"WITH qv AS (SELECT vector('[{v}]') AS v) "
                    f"SELECT t.id, (t.vector {op} qv.v) AS dis FROM {t} AS t CROSS JOIN qv "
                    f"ORDER BY dis ASC LIMIT {k}")
        # Unknown form: fall back to the canonical text literal form
        return f"SELECT id, (vector {op} '[{v}]'::vector) AS dis FROM {t} ORDER BY dis ASC LIMIT {k}"

    @classmethod
    def _direct_search(cls, vector: List[float], meta_conditions, top: Optional[int]) -> List[Tuple[int, float]]:
        """Inline-literal search (simple protocol, plan cache is a structural no-op).

        If metadata conditions are present, fall back to a parameterized query with a
        WHERE clause (our vector datasets carry no metadata, so this is a safety path).
        """
        if meta_conditions is not None:
            where_clause = cls._build_where_clause(meta_conditions)
            table_name = validate_table_name(cls.connection_params.get("table", "vec_items"))
            vector_str = '[' + ','.join(str(x) for x in vector) + ']'
            search_str = f"SELECT id, vector {cls.distance_op} %s::vector AS distance FROM {table_name}"
            if where_clause:
                search_str += f" WHERE {where_clause}"
            search_str += f" ORDER BY distance LIMIT {int(top)}"
            try:
                with cls.get_connection().cursor() as cursor:
                    cursor.execute(search_str, (vector_str,))
                    return [(row[0], float(row[1])) for row in cursor.fetchall()]
            except Exception as e:
                raise RuntimeError(f"Search failed: {e}")

        # No metadata: render the configured inline form and execute via simple protocol.
        sql_text = cls._render_inline_sql(vector, top)
        try:
            with cls.get_connection().cursor() as cursor:
                cursor.execute(sql_text)
                return [(row[0], float(row[1])) for row in cursor.fetchall()]
        except Exception as e:
            raise RuntimeError(f"Search failed (sql_type={cls.sql_type}): {e}")

    @classmethod
    def _prepared_arg(cls, vector: List[float]):
        """按 PREPARE 参数类型生成 EXECUTE 绑定实参 (与 _param_type 对应)。

        返回裸值 (不拼 SQL 文本), 交由 psycopg2 的 %s 占位绑定 (extended protocol) 传输。
        - text:   向量的文本表示 "[1.1,2.2,...]"
        - int[]:  元素列表, psycopg2 序列化为数组参数
        - real[]: 元素列表, psycopg2 序列化为数组参数
        """
        pt = cls._param_type()
        if pt == "text":
            return "[" + ",".join('0.0' if math.isnan(x) else str(x) for x in vector) + "]"
        if pt == "int[]":
            return [int(x) if not math.isnan(x) else 0 for x in vector]
        if pt == "real[]":
            return [float(x) if not math.isnan(x) else 0.0 for x in vector]
        if pt == "vector":
            # 档1/档3 类型化参数绑定: 返回向量文本, 服务端在 Bind 期把它转换一次
            # 成 vector Datum。generic plan 的目标列表重估只取该 Datum, 不再触发
            # 逐行 vector_in —— 与 polardb 档3 的 pase 类型化绑定对齐。
            # 注: psycopg2 2.x 无法发送非 bytea 的真实二进制参数 (Binary 会被硬编码
            # 成 ::bytea 并在服务端被拒), pgvector-python 0.5 的适配器也因此走 to_text;
            # 类型化文本绑定是 psycopg2 协议下可达的最优形态, 已把 N+1 完全消除。
            return "[" + ",".join('0.0' if math.isnan(x) else str(x) for x in vector) + "]"
        return "[" + ",".join('0.0' if math.isnan(x) else str(x) for x in vector) + "]"

    @classmethod
    def _prepared_search(cls, vector: List[float], top: Optional[int]) -> List[Tuple[int, float]]:
        """PREPARE 形式: EXECUTE 预定义的命名 PREPARE 语句。

        每个 sql_type 各自的 PREPARE 模板 + 参数类型, 由 plan_cache_mode 决定是否复用
        generic plan (查询计划缓存开关): force_generic_plan 复用, force_custom_plan 重规划。
        实参通过 psycopg2 的 %s 占位绑定 (extended protocol) 传输 —— 这是服务器端真正
        的绑定, 让 generic plan 能被复用, 参数差计划与 custom plan 结构一致, 从而
        只把 analyze+plan 阶段的差异反映成 QPS 差异 (干净测量计划缓存收益)。
        """
        try:
            with cls.get_connection().cursor() as cursor:
                cursor.execute(
                    f"EXECUTE {cls.prepared_statement_name}(%s)",
                    (cls._prepared_arg(vector),),
                )
                return [(row[0], float(row[1])) for row in cursor.fetchall()]
        except Exception as e:
            warn(f"EXECUTE {cls.prepared_statement_name} failed, falling back to direct execution: {e}")
            return cls._direct_search(vector, None, top)

    @classmethod
    def vector_search(cls, vector: List[float], meta_conditions, top: Optional[int]) -> List[Tuple[int, float]]:
        """单条 kNN 查询：有 PREPARE 且无标量过滤时走 EXECUTE，否则内联 SQL。

        为什么带 ``meta_conditions`` 就必须走内联：标量条件由
        ``_build_where_clause`` 动态拼成 WHERE，而 PREPARE 语句体在
        ``init_client`` 时就固定了，无法再塞进过滤条件。本项目的向量数据集
        不带元数据，所以这条分支实际是安全兜底。

        返回 ``[(id, distance), ...]``，顺序即 ``ORDER BY dis ASC`` 的顺序。
        结果缓存不在这里处理——它由会话级 GUC 在服务端完成。
        """
        # 结果缓存由 init_client 中按 use_result_cache 设置的 PG 会话级 GUC
        # (result_cache / result_cache_debug) 控制 —— 与 sh 脚本一致, python 侧不再
        # 用 dict 模拟, 避免与 PG 层缓存不一致、干扰召回率/QPS 对比。
        # 查询计划缓存四档语义 (与 init_client 的映射保持一致):
        #   use_query_plan_cache=0 -> 不 PREPARE, prepared_statement_name 为空,
        #     走 _direct_search(简单协议文本内联, 每次重解析+规划) = 无计划缓存基线。
        #   use_query_plan_cache=1 -> PREPARE + plan_cache_mode=auto +
        #     类型化绑定 (PG 自动决定 custom / generic)。
        #   use_query_plan_cache=2 -> PREPARE + plan_cache_mode=force_custom_plan +
        #     类型化绑定 (每 EXECUTE 重新规划)。
        #   use_query_plan_cache=3 -> PREPARE + plan_cache_mode=force_generic_plan +
        #     类型化绑定 (复用 generic plan, Bind 期一次转 vector Datum)。
        # 1/2/3 档仅在 PREPARE 因缺维度等失败(prepared_statement_name 为空)时回退
        # simple 协议内联 SQL。
        if meta_conditions is None and cls.prepared_statement_name:
            result = cls._prepared_search(vector, top)
        else:
            result = cls._direct_search(vector, meta_conditions, top)
        return result

    @classmethod
    def _build_where_clause(cls, meta_conditions):
        """Build WHERE clause from metadata conditions (safety path; vector datasets have none).

        中文说明：把 metadata 条件翻译成 WHERE 片段，支持三种形状——区间
        （dict 带 ``gte``/``lte``）、集合（list → ``IN (...)``）、等值。注意值
        是直接拼进 SQL 的（没有参数化），因为这条路径只用于兜底且输入来自
        受控的数据集配置，不面向外部输入。
        """
        if not meta_conditions:
            return ""
        conditions = []
        for key, value in meta_conditions.items():
            if isinstance(value, dict):
                if 'gte' in value or 'lte' in value:
                    col_conditions = []
                    if 'gte' in value:
                        col_conditions.append(f"{key} >= {value['gte']}")
                    if 'lte' in value:
                        col_conditions.append(f"{key} <= {value['lte']}")
                    if col_conditions:
                        conditions.append(" AND ".join(col_conditions))
                else:
                    conditions.append(f"{key} = '{json.dumps(value)}'")
            elif isinstance(value, list):
                if value:
                    placeholders = ", ".join(f"'{v}'" for v in value)
                    conditions.append(f"{key} IN ({placeholders})")
            else:
                if isinstance(value, str):
                    conditions.append(f"{key} = '{value}'")
                else:
                    conditions.append(f"{key} = {value}")
        return " AND ".join(conditions) if conditions else ""

    @classmethod
    def search_one(cls, vector: List[float], meta_conditions, top: Optional[int], schema, query: Query) -> List[Tuple[int, float]]:
        """``BaseSearcher`` 的统一入口：pgvector 只有向量检索一条路径。

        ``query``（含原文与列名）在本引擎不使用，保留参数只为与接口一致。
        """
        return cls.vector_search(vector, meta_conditions, top)