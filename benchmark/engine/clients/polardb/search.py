import math
import json
import threading
from typing import List, Optional, Tuple

import psycopg2

from benchmark.dataset_reader.base_reader import Query
from engine.base_client import BaseSearcher
from benchmark.cli_output import warn
from engine.clients.polardb.config import (
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
    result_cache = {}
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
        cls.result_cache = {}

        # Parameterized plan-cache form: plan_cache_mode ON (force_generic) vs OFF (force_custom)
        if cls.sql_type == "prepared":
            if cls.use_query_plan_cache == 1:
                cls._set_plan_cache_mode("force_generic_plan")
            else:
                cls._set_plan_cache_mode("force_custom_plan")
            cls._prepare_statement()

    @classmethod
    def _set_plan_cache_mode(cls, mode: str):
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(f"SET plan_cache_mode = {mode}")
        except Exception as exc:
            warn(f"could not set plan_cache_mode={mode}: {exc}")

    @classmethod
    def _prepare_statement(cls):
        """PREPARE 参数化 pase 查询.

        pase 距离表达式 (vector <?> $1::pase) 的查询向量以 *文本* 形式绑定：
        $1 类型为 text, 服务端把文本 cast 成 pase (pase_in, 逗号解析), 再叠加
        ':extra[:ds]' 后缀可以控制当次搜索的 ef_search / 距离空间。不能用
        real[] 数组绑定: pase 类型没有等价的参数化构造路径。
        """
        table = validate_table_name(cls.connection_params.get("table", "vec_items"))
        sort_dir = sort_dir_for(cls.distance)
        statement = (
            f"PREPARE {cls.prepared_statement_name} (text, integer) AS "
            f"SELECT id, (vector {DISTANCE_OP} $1::pase) AS distance FROM {table} "
            f"ORDER BY distance {sort_dir} LIMIT $2"
        )
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(f"DEALLOCATE {cls.prepared_statement_name}")
        except Exception:
            pass
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(statement)
        except Exception as exc:
            cls.use_query_plan_cache = 0
            warn(f"PolarDB prepared statement unavailable, using direct SQL: {exc}")

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
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(f"EXECUTE {cls.prepared_statement_name} (%s, %s)", (pase_param, int(top)))
                return [(row[0], float(row[1])) for row in cursor.fetchall()]
        except Exception as e:
            table = validate_table_name(cls.connection_params.get("table", "vec_items"))
            warn(f"PolarDB prepared statement execution failed, falling back to direct SQL: {e}")
            return cls._direct_search(vector, None, top)

    @classmethod
    def vector_search(cls, vector: List[float], meta_conditions, top: Optional[int]) -> List[Tuple[int, float]]:
        cache_key = (tuple(float(x) for x in vector), int(top or 0))
        if meta_conditions is None and cls.use_result_cache and cache_key in cls.result_cache:
            return cls.result_cache[cache_key]
        # prepared 形态 = 命名 PREPARE + EXECUTE ($1::pase 文本绑定)。plan-cache 开关
        # 在 init_client 里映射到 plan_cache_mode (cache=1 -> force_generic_plan,
        # cache=0 -> force_custom_plan), 两条腿都走 _prepared_search, 得到与
        # bash-test 脚本一致的 off(每次重规划) vs plan_cache(复用 generic) 对照。
        # 内置的 _prepare_statement 不可用抛错时自动落到 _direct_search。
        if cls.sql_type == "prepared" and meta_conditions is None:
            result = cls._prepared_search(vector, top)
        else:
            result = cls._direct_search(vector, meta_conditions, top)
        if meta_conditions is None and cls.use_result_cache:
            if len(cls.result_cache) >= 4096:
                cls.result_cache.pop(next(iter(cls.result_cache)))
            cls.result_cache[cache_key] = result
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