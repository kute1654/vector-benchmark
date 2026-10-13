import math
import json
import threading
from typing import List, Optional, Tuple

import psycopg2

from benchmark.dataset_reader.base_reader import Query
from engine.base_client import BaseSearcher
from benchmark.cli_output import warn
from engine.clients.polardb.config import (
    PASE_IN_PARSE_MODE_GUC,
    PASE_RESULT_CACHE_DEBUG_GUC, PASE_RESULT_CACHE_GUC,
    POLARDB_DATABASE_NAME, POLARDB_DEFAULT_PASSWD, POLARDB_DEFAULT_PORT,
    POLARDB_DEFAULT_USER, validate_table_name,
    distance_ds, sort_dir_for,
)

thread_local = threading.local()

# pase 距离算子: float4[] <?> pase 返回 L2(或内积)距离。
#   注意 <#> 在同一 operator class 里定义为 ivfflat 专用 (pase_ivfflat_float_ops),
#   而 <?> 是 hnsw 的 order-by 算子 (pase_hnsw_ops); hnsw 场景统一用 <?>。
DISTANCE_OP = "<?>"


class PolarDBSearcher(BaseSearcher):
    connection_params = {}
    search_params = {}
    # pase distance operator: float4[] <?> pase.
    distance_op = DISTANCE_OP
    distance = None
    sql_type: str = "text_pase_op"
    use_query_plan_cache = 0
    use_result_cache = 0
    # 当前会话应用的 pase_in 解析器档位 (strtof/inline/fast_float), None = 服务端默认。
    in_parse_mode = None
    # PREPARE 参数是否做 *pase 类型化绑定* (对齐 pgvector text_binary):
    #   True  → PREPARE (pase) + 正文 $1 (无 cast), 文本在 Bind 阶段一次 pase_in → Datum;
    #   False → PREPARE (text) + 正文 $1::pase (cast), generic plan 每行 cast/解析。
    # 由 use_query_plan_cache 档位在 init_client 里设置 (档2 text / 档3 pase)。
    pase_typed_bind = False
    prepared_statement_name = "polardb_vector_search_stmt"

    def __init__(self, host, connection_params, search_params):
        default_conn_params = {
            "host": "127.0.0.1",
            "port": POLARDB_DEFAULT_PORT,
            "user": POLARDB_DEFAULT_USER,
            "password": POLARDB_DEFAULT_PASSWD,
            "database": POLARDB_DATABASE_NAME,
            "table": "vec_items",
        }
        merged_conn_params = {**default_conn_params, **(connection_params or {})}
        super().__init__(host, merged_conn_params, search_params)

    @classmethod
    def _apply_session_settings(cls, connection, settings):
        for key, value in (settings or {}).items():
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f"SET {key} = %s", (str(value),))
            except Exception as exc:
                warn(f"failed to apply session setting {key}={value}: {exc}")

    @classmethod
    def _effective_ef_s(cls, search_params: dict) -> int:
        params = (search_params or {}).get("params", {})
        if not isinstance(params, dict):
            return 100
        ef_s = params.get("ef_s", 100)
        if isinstance(ef_s, (list, tuple)):
            ef_s = ef_s[0] if ef_s else 100
        return int(ef_s or 100)

    def setup_search(self, host, distance, connection_params: dict, search_params: dict, dataset_config):
        pass

    @classmethod
    def init_client(cls, host, distance, connection_params, search_params):
        cls.connection_params = connection_params or {}
        cls.search_params = search_params or {}
        cls.distance = distance
        cls.sql_type = str(cls.search_params.get("sql_type", "text_pase_op"))
        cls.connection = psycopg2.connect(
            host=cls.connection_params.get("host", host or "127.0.0.1"),
            port=cls.connection_params.get("port", POLARDB_DEFAULT_PORT),
            user=cls.connection_params.get("user", POLARDB_DEFAULT_USER),
            password=cls.connection_params.get("password", POLARDB_DEFAULT_PASSWD),
            database=cls.connection_params.get("database", POLARDB_DATABASE_NAME),
        )
        cls.connection.autocommit = True
        # 必须先 LOAD pase 扩展, pase_result_cache / pase_result_cache_debug /
        # pase_result_cache_capacity 等 pase GUC 才会在会话中注册; 否则后续
        # SET pase_result_cache=... 会 unrecognized configuration parameter,
        # 结果缓存档位将全部静默失效 (与 bash 脚本 L574-575 语义一致)。
        cls._load_pase_extension()
        # pase_in 文本解析器档位: 必须在 LOAD '$libdir/pase' 之后 (GUC 由
        # pase_handler.c _PG_init 注册), 与计划缓存/结果缓存档位正交。
        cls._apply_in_parse_mode_guc()
        cls._apply_session_settings(cls.connection, cls.search_params.get("session_settings", {}))
        # 计划缓存对比的可选控制: enable_seqscan=0 让 force_generic / force_custom
        # 的 generic 计划都保住 HNSW 索引 (generic 成本估计不是 pase 感知的, 默认
        # seqscan=on 时 generic 计划会退化为 Seq Scan, 把"索引 vs 全扫"混进
        # "计划缓存 vs 重规划" 的对比里, 详见 docs/polardb-pase-plan-cache-analysis.md)。
        if "enable_seqscan" in cls.search_params:
            try:
                with cls.connection.cursor() as cursor:
                    cursor.execute("SET enable_seqscan = %s", (int(bool(cls.search_params["enable_seqscan"])),))
            except Exception as exc:
                warn(f"could not set enable_seqscan: {exc}")
        cls.use_query_plan_cache = int(cls.search_params.get("use_query_plan_cache", 0) or 0)
        cls.use_result_cache = int(cls.search_params.get("use_result_cache", 0) or 0)

        # 查询结果缓存: 对齐 sh 脚本, 用 PolarDB pase 会话级 GUC (result_cache +
        # result_cache_debug) 统一开启/关闭, 而不是在 python 侧用 dict 模拟。
        cls._apply_result_cache_guc(on=bool(cls.use_result_cache))

        # 查询计划缓存 (对齐 pgvector 三档语义 + pase 类型化绑定第四档, 与 sql_type 解耦):
        #   use_query_plan_cache=0 -> 完全不 PREPARE, 直接走 _direct_search
        #      (simple 协议文本内联, 每次重新解析+规划) = 不使用计划缓存基线。
        #   use_query_plan_cache=1 -> PREPARE + plan_cache_mode=force_custom_plan,
        #      每条 EXECUTE 重新实例化 custom plan (每 EXECUTE 重规划)。
        #   use_query_plan_cache=2 -> PREPARE + plan_cache_mode=force_generic_plan +
        #      文本输入 (PREPARE (text), $1::pase) —— generic plan 每行 cast/重复解析,
        #      pase_in 次数多, 作为"generic 复用但逐行解析"的对比档。
        #   use_query_plan_cache=3 -> PREPARE + plan_cache_mode=force_generic_plan +
        #      pase 类型输入 (PREPARE (pase), $1) —— 文本在 Bind 阶段一次 pase_in 成 Datum,
        #      generic plan 复用该 Datum, 消除重复 vector_in (对齐 pgvector text_binary)。
        # 档 2 与档 3 同用 generic plan, 唯一差异是绑定方式, 便于直接对比性能/埋点。
        # 任意 sql_type 都能配各档: PREPARE 体按当前 sql_type 的向量构造形态渲染。
        # 仅在 PREPARE 失败 (prepared_statement_name 为空) 时回退 simple 协议内联 SQL。
        if cls.use_query_plan_cache > 0:
            if cls.use_query_plan_cache in (2, 3):
                # generic plan 档: 档2 文本输入($1::pase) / 档3 pase 类型输入($1)。
                cls._set_plan_cache_mode("force_generic_plan")
                cls.pase_typed_bind = (cls.use_query_plan_cache == 3)
            else:
                # 档1: custom plan (每 EXECUTE 重规划), 沿用 pase 类型绑定。
                cls._set_plan_cache_mode("auto")
                cls.pase_typed_bind = True
            cls._prepare_statement()
        else:
            cls.prepared_statement_name = None

    @classmethod
    def _load_pase_extension(cls):
        """LOAD '$libdir/pase' (会话级)。

        加载 pase 共享库, 让 pase_handler.c _PG_init 注册的 pase_result_cache /
        pase_result_cache_debug / pase_result_cache_capacity 等 GUC 在当前会话可用。
        每次 init_client 建立的新连接都必须重新 LOAD。LOAD 失败不影响主流程:
        后面 SET pase_result_cache 会因 GUC 未注册而告警, 但查询本身仍可执行。
        """
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute("LOAD '$libdir/pase'")
        except Exception as exc:
            warn(f"could not LOAD '$libdir/pase': {exc}")

    @classmethod
    def _apply_in_parse_mode_guc(cls):
        """按配置设置 pase_in() 的十进制->float 解析器 GUC。

        值取自 search_params["vector_in_parse_mode"] (与 pgvector 复用同一配置键;
        由 config.json 的 upload_params 经 client._run_pgvector_experiment 注入),
        映射到 pase 的 GUC "pase_in_parse_mode" (strtof / inline / fast_float)。
        未配置时跳过, 服务端保持默认 strtof (历史行为)。
        """
        mode = cls.search_params.get("vector_in_parse_mode")
        # 记录实际档位, 供日志/CSV 归因 (None 表示使用服务端默认)。
        cls.in_parse_mode = mode
        if not mode:
            return
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(f"SET {PASE_IN_PARSE_MODE_GUC} = %s", (str(mode),))
        except Exception as exc:
            warn(f"could not SET {PASE_IN_PARSE_MODE_GUC}={mode}: {exc}")

    @classmethod
    def _apply_result_cache_guc(cls, on: bool):
        """对齐 sh 脚本: 用 PolarDB pase 会话级 GUC 控制查询结果缓存。

        结果缓存是 pase 扩展源码层实现 (对齐 pgvector 的 veccache), 引擎侧不模拟。
        on=True  → SET pase_result_cache = on; SET pase_result_cache_debug = on
        on=False → 两个开关都置 off (保留 debug, 便于日志归零后作对比)。
        """
        val = "on" if on else "off"
        for guc in (PASE_RESULT_CACHE_GUC, PASE_RESULT_CACHE_DEBUG_GUC):
            try:
                with cls.connection.cursor() as cursor:
                    cursor.execute(f"SET {guc} = {val}")
            except Exception as exc:
                warn(f"could not SET {guc}={val}: {exc}")

    @classmethod
    def _set_plan_cache_mode(cls, mode: str):
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(f"SET plan_cache_mode = {mode}")
        except Exception as exc:
            warn(f"could not set plan_cache_mode={mode}: {exc}")

    @classmethod
    def _render_prepare_body(cls, top: int) -> str:
        """按当前 sql_type 渲染 PREPARE 语句体 (保留该 sql_type 的 SQL 结构形态)。

        向量以 *文本* 参数 $1 绑定 (`$1::pase`): 服务端在 Bind 期把它 cast 成 pase
        (pase_in 逗号解析)。ef_search / ds 需要编码进查询向量文本, 但 PREPARE 的 $1
        由 EXECUTE 时绑定 (见 _prepared_search, 会拼上 ':ef:ds' 后缀), 因此这里 $1::pase
        即表示"该运行时刻的 ef/ds"。不能用 real[] 数组绑定: pase 没有等价的数组
        参数化构造路径 (pase(ARRAY[]) 是函数, 其数组入参无法用参数表绑定)。TOP_K
        常量内联 ($2 改为字面量) 以保证 generic 计划能命中 HNSW 索引。
        """
        t = validate_table_name(cls.connection_params.get("table", "vec_items"))
        k = int(top)
        op = DISTANCE_OP
        sort_asc = sort_dir_for(cls.distance)
        # ds=1 (IP) 时内联形态用 DESC; 参数化 $1 文本里带 :ds, 排序方向由距离指标决定。
        st = cls.sql_type

        # pase 查询向量参数绑定方式由档位决定:
        #   pase_typed_bind=True  (档3) -> 类型化绑定 $1 (无 cast), Bind 阶段一次 pase_in;
        #   pase_typed_bind=False (档2) -> 文本绑定 $1::pase (cast), generic plan 逐行解析。
        # 结构差异 (order_by / with / plain select) 只在 SQL 外层形态。
        expr = "$1" if getattr(cls, "pase_typed_bind", False) else "$1::pase"

        if st == "order_by_op":
            return (f"SELECT id FROM {t} ORDER BY vector {op} {expr} {sort_asc} LIMIT {k}")
        if st.startswith("with_"):
            return (f"WITH qv AS (SELECT {expr} AS v) "
                    f"SELECT id, (vector {op} qv.v) AS dis FROM {t} CROSS JOIN qv "
                    f"ORDER BY dis {sort_asc} LIMIT {k}")
        # 默认: SELECT id, (vector <?> $1::pase) AS dis ... (text_pase_op / pase_fn_* /
        # hash_op_* / prepared)
        return (f"SELECT id, (vector {op} {expr}) AS dis FROM {t} "
                f"ORDER BY dis {sort_asc} LIMIT {k}")

    @classmethod
    def _prepare_statement(cls):
        """PREPARE 参数化 pase 查询 (按当前 sql_type + 绑定档位渲染语句体).

        参数类型随 pase_typed_bind 档位 (见 init_client 注释):
          * pase_typed_bind=True  (档3) -> `PREPARE ... (pase)`, 正文 $1 (无 cast)。
            EXECUTE 时 psycopg2 以文本发送 (逗号文本, 见 _prepared_search / _pase_text),
            服务端在 *Bind 阶段* 用 pase_in 一次性转成 pase Datum 存进 Param; generic
            plan 复用该 Datum, 不再逐行解析 —— 消除重复 vector_in, 对齐 pgvector
            的 text_binary 类型化绑定。
          * pase_typed_bind=False (档2) -> `PREPARE ... (text)`, 正文 $1::pase (cast),
            generic plan 每行 cast/解析 (pase_in 多次), 作对比档。
        ef/ds 由 EXECUTE 绑定的文本后缀 (pase_in 输入文本 ':ef:ds') 控制。不能用
        real[] 数组绑定: pase 类型没有等价于参数化构造数组的路径。PREPARE 失败时
        回退 direct (use_query_plan_cache 置 0)。
        """
        cls.prepared_statement_name = f"polardb_ps_{cls.sql_type}_{id(cls)}"
        body = cls._render_prepare_body(int(cls.search_params.get("top", 10)))
        # 参数类型随绑定方式: pase 类型化绑定 (档3) -> (pase); 文本绑定 (档2) -> (text)。
        # 详见 init_client 档位注释与 _render_prepare_body 的 expr 说明。
        param_t = "pase" if getattr(cls, "pase_typed_bind", False) else "text"
        statement = f"PREPARE {cls.prepared_statement_name} ({param_t}) AS {body}"
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(f"DEALLOCATE {cls.prepared_statement_name}")
        except Exception:
            pass
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(statement)
        except Exception as exc:
            cls.prepared_statement_name = None
            warn(f"PolarDB prepared statement unavailable for sql_type={cls.sql_type}, using direct SQL: {exc}")

    @staticmethod
    def _vec_str(vector: List[float]) -> str:
        return ",".join('0.0' if math.isnan(x) else str(x) for x in vector)

    @classmethod
    def _pase_text(cls, vector: List[float], extra: Optional[int] = None, ds: Optional[int] = None) -> str:
        """构造 pase 输入文本 '1.0,2.0,...[:extra[:ds]]' (逗号解析, 走 pase_in)。

        千万不要用 pase('...') 构造函数: 它绑定到 pase(text, ...) -> pase_text_i_i,
        把文本当 base64 解码, 维度会被解成垃圾值, 报 "query dimemsion(N) not equal to
        data dimemsion(D)"。文本进 pase 的唯一安全路径就是 'txt'::pase 输入函数。
        """
        text = cls._vec_str(vector)
        if ds is None:
            ds = distance_ds(cls.distance)
        if extra is None:
            extra = cls._effective_ef_s(cls.search_params)
        return f"{text}:{int(extra)}:{int(ds)}"

    @classmethod
    def _render_inline_sql(cls, vector: List[float], top: int) -> Tuple[str, bool]:
        """Render an inline-literal pase SQL form (simple protocol).

        Returns (sql, id_only). ef_search / ds are encoded in the query pase text
        (':extra:ds'), so recall is comparable across forms.
        """
        t = validate_table_name(cls.connection_params.get("table", "vec_items"))
        v = cls._vec_str(vector)
        k = int(top)
        ef = cls._effective_ef_s(cls.search_params)
        op = DISTANCE_OP
        st = cls.sql_type

        # ds / 排序方向来自数据集距离指标:
        #   ds=0 (L2, cosine 已归一化) -> ASC; ds=1 (IP) -> 返回原始内积, DESC。
        ds = distance_ds(cls.distance)
        sort_asc = sort_dir_for(cls.distance)
        sort_ds1 = "DESC"

        # pase 查询向量文本: '1.0,2.0,...[:extra[:ds]]'::pase (pase_in 逗号解析)。
        # 文本构造函数 pase('...') 走 base64 (pase_text_i_i), 必定解析出垃圾维度,
        # 这里一律用 cast 形式。
        text_extra = f"'{v}:{ef}:{ds}'::pase"
        text_ds1 = f"'{v}:{ef}:1'::pase"
        text_default = f"'{v}'::pase"
        array_default = f"pase(ARRAY[{v}]::float4[])"
        array_extra = f"pase(ARRAY[{v}]::float4[], {ef})"
        array_ds1 = f"pase(ARRAY[{v}]::float4[], {ef}, 1)"

        def select(expr: str, ds1: bool = False) -> str:
            return (f"SELECT id, (vector {op} {expr}) AS dis FROM {t} "
                    f"ORDER BY dis {(sort_ds1 if ds1 else sort_asc)} LIMIT {k}")

        render = {
            # config.json 简名 (run.py 默认 sql_type 列表)
            "text_pase_op":      select(text_extra),
            "order_by_op":       f"SELECT id FROM {t} ORDER BY vector {op} {text_extra} {sort_asc} LIMIT {k}",
            "pase_fn_text":      select(text_default),
            "pase_fn_array":     select(array_default),
            "pase_fn_array_extra": select(array_extra),
            "with_pase_fn_array": f"WITH qv AS (SELECT {array_default} AS v) SELECT id, (vector {op} qv.v) AS dis FROM {t} CROSS JOIN qv ORDER BY dis {sort_asc} LIMIT {k}",
            "with_pase_fn_text":  f"WITH qv AS (SELECT {text_default} AS v) SELECT id, (vector {op} qv.v) AS dis FROM {t} CROSS JOIN qv ORDER BY dis {sort_asc} LIMIT {k}",
            # bash-test 脚本 SQL_TYPES 全名 (13 种形式对齐)
            "text_pase_op_id":     f"SELECT id FROM {t} ORDER BY vector {op} {text_extra} {sort_asc} LIMIT {k}",
            "text_pase_op_extra":  select(text_extra),
            "text_pase_op_extra_ds": select(text_ds1, ds1=True),
            "pase_fn_text_default": select(text_default),
            "pase_fn_array_default": select(array_default),
            "pase_fn_array_ip":      select(array_ds1, ds1=True),
            "hash_op_default":       select(array_default),
            "hash_op_extra":         select(array_extra),
        }

        if st == "prepared":
            warn("sql_type=prepared reached inline renderer; using text_pase_op")
            st = "text_pase_op"
        if st in render:
            sql = render[st]
        else:
            warn(f"unknown sql_type {st!r}, falling back to text_pase_op")
            sql = render["text_pase_op"]
        id_only = st in ("order_by_op", "text_pase_op_id")
        return sql, id_only

    @classmethod
    def _direct_search(cls, vector: List[float], meta_conditions, top: Optional[int]) -> List[Tuple[int, float]]:
        """Inline-literal search via simple protocol (plan cache is a structural no-op)."""
        # Inline forms do not support a WHERE clause; our vector datasets carry no metadata.
        if meta_conditions is not None:
            # Safety fallback: parameterized query with a WHERE clause.
            where_clause = cls._build_where_clause(meta_conditions)
            table = validate_table_name(cls.connection_params.get("table", "vec_items"))
            pase_param = cls._pase_text(vector)
            search_str = f"SELECT id, (vector {DISTANCE_OP} %s::pase) AS distance FROM {table}"
            if where_clause:
                search_str += f" WHERE {where_clause}"
            search_str += f" ORDER BY distance {sort_dir_for(cls.distance)} LIMIT {int(top)}"
            with cls.connection.cursor() as cursor:
                cursor.execute(search_str, (pase_param,))
                return [(row[0], float(row[1])) for row in cursor.fetchall()]

        sql_text, id_only = cls._render_inline_sql(vector, top)
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(sql_text)
                if id_only:
                    return [(row[0], 0.0) for row in cursor.fetchall()]
                return [(row[0], float(row[1])) for row in cursor.fetchall()]
        except Exception as e:
            raise RuntimeError(f"Search failed (sql_type={cls.sql_type}): {e}")

    @classmethod
    def _prepared_search(cls, vector: List[float], top: Optional[int]) -> List[Tuple[int, float]]:
        """Parameterized plan-cache search via PREPARE/EXECUTE (plan_cache_mode-driven).

        查询向量以 pase 文本绑定 (带 :ef:ds 后缀), 每条 EXECUTE 走 pase_in 一次,
        这正好对应计划缓存救不了的"Bind 阶段向量解析"成本 —— 两个 plan_cache_mode
        档位的差异即"每次重规划 vs 复用缓存计划"。
        """
        pase_param = cls._pase_text(vector)
        # order_by_op 的 PREPARE 体是 `SELECT id FROM ... ORDER BY vector <?> $1 ...`
        # (单列 id, 与 _direct_search 的 id_only 形态一致); 其余 with_*/默认形态都带距离
        # 列。必须按 sql_type 区分, 否则对单列结果读 row[1] 会 "tuple index out of range"。
        id_only = cls.sql_type == "order_by_op"
        if cls.prepared_statement_name is None:
            warn("PolarDB prepared statement is None, falling back to direct SQL")
            return cls._direct_search(vector, None, top)
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(f"EXECUTE {cls.prepared_statement_name} (%s)", (pase_param,))
                if id_only:
                    return [(row[0], 0.0) for row in cursor.fetchall()]
                return [(row[0], float(row[1])) for row in cursor.fetchall()]
        except Exception as e:
            warn(f"PolarDB prepared statement execution failed, falling back to direct SQL: {e}")
            return cls._direct_search(vector, None, top)

    @classmethod
    def vector_search(cls, vector: List[float], meta_conditions, top: Optional[int]) -> List[Tuple[int, float]]:
        # 计划缓存 (对齐 pgvector 三档语义, 与 sql_type 解耦): use_query_plan_cache>0
        # 时命名 PREPARE + EXECUTE ($1::pase 文本绑定, PREPARE 体按 sql_type 渲染)。
        # 三档语义在 init_client 里映射:
        #   cache=0 -> 不 PREPARE, 走 _direct_search (simple 协议文本内联, 每次重解析)。
        #   cache=1 -> PREPARE + plan_cache_mode=force_custom_plan (每 EXECUTE 重规划)。
        #   cache=2 -> PREPARE + plan_cache_mode=force_generic_plan (复用 generic plan)。
        # 直接查询 SQL 与计划缓存查询SQL的重规划差异, 反映 Plan State Reuse 成本; 查询
        # 结果缓存由 init_client 里按 use_result_cache 设置的 pase 会话级 GUC
        # (pase_result_cache / pase_result_cache_debug) 控制 —— 与 sh 脚本一致。
        # _prepare_statement 不可用时 (prepared_statement_name 为 None / EXECUTE 抛错)
        # 自动落到 _direct_search。
        if cls.prepared_statement_name is not None and meta_conditions is None:
            result = cls._prepared_search(vector, top)
        else:
            result = cls._direct_search(vector, meta_conditions, top)
        return result

    @classmethod
    def _build_where_clause(cls, meta_conditions):
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