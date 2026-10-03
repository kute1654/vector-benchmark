import math
import json
import threading
from typing import List, Optional, Tuple

import psycopg2

from benchmark.dataset_reader.base_reader import Query
from engine.base_client import BaseSearcher
from benchmark.cli_output import warn
from engine.clients.pgvector.config import *


thread_local = threading.local()


class PGVectorSearcher(BaseSearcher):
    search_params = {}
    connection = None
    distance_op: str = None
    distance_name: str = None
    host: str = None
    connection_params: dict = {}
    sql_type: str = "text_literal"
    use_query_plan_cache: int = 0
    use_result_cache: int = 0
    # PREPARE 形式: 命名 PREPARE 语句名 + plan_cache_mode (查询计划缓存开关)
    prepared_statement_name: Optional[str] = None

    def __init__(self, host, connection_params, search_params):
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
        if not session_settings:
            return
        with connection.cursor() as cursor:
            for key, value in session_settings.items():
                try:
                    cursor.execute(f"SET {key} = %s", (str(value),))
                except Exception:
                    cursor.execute(f"SET {key} = '{value}'")

    def setup_search(self, host, distance, connection_params: dict, search_params: dict, dataset_config):
        pass

    @classmethod
    def _effective_ef_s(cls, search_params: dict) -> int:
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
        cls.connection_params = connection_params
        cls.host = host
        cls.distance_op = DISTANCE_MAPPING[distance]
        cls.distance_name = str(getattr(distance, "value", distance)).lower()
        cls.search_params = search_params or {}
        cls.sql_type = str(cls.search_params.get("sql_type", "text_literal"))
        # 注意: use_query_plan_cache 在下方连接建立后统一读取并按 0/1/2 三档语义
        # (0=不PREPARE/直接文本, 1=PREPARE+USECUSTOM, 2=PREPARE+USEGENERIC) 处理。
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

        # 查询计划缓存: 仅当 use_query_plan_cache > 0 时才走 PREPARE/EXECUTE 形式。
        # cache=0 -> 完全不 PREPARE, 直接走 _direct_search (简单协议文本内联, 每次
        # 重新解析+规划), 作为不使用计划缓存的基线。
        # cache=1 -> 建立命名 PREPARE + EXECUTE, plan_cache_mode=force_custom_plan,
        #            每 EXECUTE 重新规划 (日志 ACTION=USE_CUSTOM)。
        # cache=2 -> 建立命名 PREPARE + EXECUTE, plan_cache_mode=force_generic_plan,
        #            复用 generic plan (日志 ACTION=USE_GENERIC)。
        # 因此 custom 与 generic 是"使用计划缓存"下的两个对照, 由日志 USE_CUSTOM /
        # USE_GENERIC 区分; 0 档则完全不 PREPARE 作基线。
        cls.use_query_plan_cache = int(cls.search_params.get("use_query_plan_cache", 0))
        if cls.use_query_plan_cache == 0:
            cls.prepared_statement_name = None
        else:
            if cls.use_query_plan_cache == 2:
                cls._set_plan_cache_mode("force_generic_plan")
            else:
                cls._set_plan_cache_mode("force_custom_plan")
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
        """sql_type -> PREPARE 的参数类型 (按 SQL 类型区分, 保留各类型形态)。"""
        st = cls.sql_type
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

        # 距离右侧表达式 (以参数 $1 为源, 保留该 sql_type 的向量转换形态)
        if st == "text_literal":
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
        return cls.connection

    @staticmethod
    def _vec_str(vector: List[float]) -> str:
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
        # 结果缓存由 init_client 中按 use_result_cache 设置的 PG 会话级 GUC
        # (result_cache / result_cache_debug) 控制 —— 与 sh 脚本一致, python 侧不再
        # 用 dict 模拟, 避免与 PG 层缓存不一致、干扰召回率/QPS 对比。
        # 查询计划缓存三档语义:
        #   use_query_plan_cache=0 -> 不 PREPARE, prepared_statement_name 为空,
        #     走 _direct_search(简单协议文本内联, 每次重解析+规划) = 无计划缓存基线。
        #   use_query_plan_cache=1 -> PREPARE + plan_cache_mode=force_custom_plan,
        #     每 EXECUTE 重新规划(USE_CUSTOM)。
        #   use_query_plan_cache=2 -> PREPARE + plan_cache_mode=force_generic_plan,
        #     复用 generic plan(USE_GENERIC)。
        # 1/2 档仅在 PREPARE 因缺维度等失败(prepared_statement_name 为空)时回退
        # simple 协议内联 SQL。
        if meta_conditions is None and cls.prepared_statement_name:
            result = cls._prepared_search(vector, top)
        else:
            result = cls._direct_search(vector, meta_conditions, top)
        return result

    @classmethod
    def _build_where_clause(cls, meta_conditions):
        """Build WHERE clause from metadata conditions (safety path; vector datasets have none)."""
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
        return cls.vector_search(vector, meta_conditions, top)