"""Pgvector engine implementation.

【职责】
本模块是"脚本式"的 pgvector 引擎封装: 直接继承 BaseEngine(engine/base.py), 用
psycopg2 同步接口完成建表 / 建索引 / 批量写入 / 近邻查询, 并额外提供 QPS 压测
所需的 SQL 形态生成与执行工具(对比 inline 字面量、CAST、ARRAY、WITH、PREPARE
等写法与计划缓存档位的耗时差异)。

【关键入口】
- 生命周期: connect / create_table / create_index / insert_vectors / query /
  server_version, 与 BaseEngine 抽象接口一一对应;
- 压测工具: gen_vector_text / gen_query_vectors / _make_inline_sql /
  _make_prepare_sql / query_with_type, 供上层压测脚本调用;
- 辅助探查: detect_vector_column / sample_vectors。

【与 BaseEngine 的调用关系】
BaseEngine.measure_qps 以 self.query(conn, table, vec, top_k, distance) 回调本类
query(); 但本类 query 的形参是 (conn_or_table, vector, top_k, distance), 位置参数
与 conn-first 形式并不对齐(详见 query 的 docstring), 因此压测实际走
query_with_type 等显式工具方法。

【副作用】
- 每个方法各自 connect()/close(), 不复用连接池;
- _execute() 在写路径上 commit; query/探查类方法只读、不 commit;
- create_table(drop=True) / create_index / insert_vectors 会修改数据库。

【方言】
仅适用于 PostgreSQL + pgvector: 距离算子 <-> / <#> / <=> 是 pgvector 扩展定义
的自定义运算符, 不是标准 SQL; vector 类型同理。
"""

import math
import os
import random
import threading
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from .base import BaseEngine

# pgvector 距离算子(distance -> SQL 运算符):
#   l2     <->  欧氏距离, 返回距离值, 越小越近      -> ASC
#   ip     <#>  pgvector 定义为 *负内积*, 越小越相似 -> 按语义应为 ASC
#   cosine <=>  余弦距离 (1 - cos), 越小越近        -> ASC
# 注意: 下表把 ip 固定写成 DESC, 与 pgvector 的 <#>(负内积)语义及同目录
# clients/pgvector/search.py "统一 ASC" 的写法不一致; 这是既有行为, 注释不改逻辑。
DIST_OPS = {"l2": "<->", "ip": "<#>", "cosine": "<=>"}
SORT_DIR = {"l2": "ASC", "ip": "DESC", "cosine": "ASC"}

# =========================================================================
# 查询计划缓存与向量检索测试工具
# =========================================================================

# 待测的 SQL 语句类型
SQL_TYPES = [
    "text_literal",      # SELECT ... WHERE vector <-> '[vec]'::vector (默认)
    "text_cast",         # SELECT ... WHERE vector <-> CAST('[vec]' AS vector)
    "array_cast",        # SELECT ... WHERE vector <-> ARRAY[...]::real[]::vector
    "with_text_literal", # WITH qv AS (...) SELECT ... CROSS JOIN qv
    "prepared",          # PREPARE/EXECUTE (受 plan_cache_mode 控制)
]

# 说明: 前 4 种是 inline 形态——向量字面量直接拼进 SQL, 走 simple 协议,
# 每次执行都会重新解析 + 重新规划; "prepared" 走 extended 协议(PREPARE/EXECUTE),
# 是否复用已缓存计划由下面的 plan_cache_mode 决定。

# 查询计划缓存模式映射: 配置侧语义 -> PostgreSQL 的 plan_cache_mode 取值。
#   off  -> force_custom_plan : 每次都按实际参数重新规划, 相当于不用计划缓存
#   on   -> force_generic_plan: 强制复用参数无关的通用计划, 即"命中缓存"
#   auto -> auto              : 交给 PG(默认前 5 次 custom, 之后按代价决定)
# 注意 off/on 是"基准对比"语义, 不是 PG 原生枚举值。
PLAN_CACHE_MODES = {
    "off":  "force_custom_plan",   # 每次重新规划（不使用缓存）
    "on":   "force_generic_plan",  # 强制通用计划（复用缓存）
    "auto": "auto",                # PostgreSQL 自动选择
}

# 预置的 5 个基准表。label 用于结果/日志标记; dim 用于生成对应维度的查询向量;
# distance 决定距离算子(l2/cosine)。表名直接来自历史实验, 调用方需保证真实存在。
BENCHMARK_TABLES = [
    {"label": "128dim",  "table": "benchmark_sift_128",   "dim": 128,  "distance": "l2"},
    {"label": "256dim",  "table": "benchmark_256_290k",   "dim": 256,  "distance": "cosine"},
    {"label": "768dim",  "table": "benchmark_768_1m",     "dim": 768,  "distance": "cosine"},
    {"label": "960dim",  "table": "benchmark_960_1m",     "dim": 960,  "distance": "l2"},
    {"label": "1536dim", "table": "benchmark_1536_1m",    "dim": 1536, "distance": "cosine"},
]


class PgvectorEngine(BaseEngine):
    """Pgvector engine for PostgreSQL with pgvector extension.

    面向脚本/压测的同步实现: 每个方法自行建立并关闭短连接, 不共享连接对象,
    也不维护连接池; DDL/写入由各方法内部 commit。
    ``ENGINE_NAME`` 供上层按名字选择引擎(见 benchmark/engine/__init__.py)。
    """

    ENGINE_NAME = "pgvector"

    def connect(self):
        """建立一条新的 psycopg2 连接(默认 autocommit=False)。

        返回 psycopg2 connection; 连接失败时抛 psycopg2.OperationalError。
        调用方负责 close(); 本方法不设置 statement_timeout 或任何会话 GUC。
        """
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
        )

    def _execute(self, conn, sql: str, params=None, fetch: bool = False):
        """执行单条 SQL。

        Args:
            conn: 已建立的 psycopg2 连接(不会在此关闭)。
            sql: SQL 文本, 可含 %s 占位符。
            params: 绑定参数(None 表示无)。
            fetch: True 且语句有结果集时返回 list[tuple]。

        Returns:
            fetch 命中结果集 -> list[tuple]; 其余情况 -> None。

        行为细节: 结果集分支在 with 块内直接 return, 因此 *不执行 commit*(查询无需
        提交); 非结果集分支(写操作/DDL/SET)在退出 with 后 commit。异常向上抛且此处
        不 rollback, 由调用方 close() 连接时隐式回滚。
        """
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetch and cur.description:
                return cur.fetchall()
        conn.commit()
        return None

    def server_version(self) -> str:
        """返回服务端 PostgreSQL 版本字符串(如 '16.3 (Debian ...)')。

        每次调用单独建连, 查 current_setting('server_version'), 在 finally 中关闭
        连接; 只读, 无副作用。注意这是 PG 版本, 不是 pgvector 扩展版本。
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_setting('server_version')")
                return cur.fetchone()[0]
        finally:
            conn.close()

    def create_table(
        self, table: str, dim: int, drop: bool = False
    ) -> None:
        """创建 (id serial, vector vector(dim)) 表; drop=True 时先删表。

        Args:
            table: 表名, 直接拼进 SQL, 调用方须保证是合法标识符。
            dim: 向量维度, 写入 vector({dim}) 类型; pgvector 会据此约束列长度。
            drop: True 时先执行 DROP TABLE IF EXISTS ... CASCADE(会删除依赖对象)。

        副作用: DDL 写入 schema; 由 _execute() 逐条 commit。若 DROP 与 CREATE 之间
        失败, DROP 已经生效(两条语句不在同一事务里)。
        """
        conn = self.connect()
        try:
            if drop:
                self._execute(conn, f"DROP TABLE IF EXISTS {table} CASCADE")
            # pgvector 的 vector(n) 带维度约束; id 为自增主键, 与读写基准对齐。
            self._execute(
                conn,
                f"CREATE TABLE IF NOT EXISTS {table} "
                f"(id serial PRIMARY KEY, vector vector({dim}))",
            )
        finally:
            conn.close()

    def create_index(
        self,
        table: str,
        index_type: str = "hnsw",
        distance: str = "l2",
        dim: int = 128,
        **kwargs,
    ) -> None:
        """在 table 上创建 pgvector 索引。

        Args:
            table: 目标表名(直接拼进 SQL)。
            index_type: 'hnsw' | 'ivfflat' | 'none'; 'none' 表示跳过建索引。
            distance: 'l2' | 'ip' | 'cosine', 决定 HNSW/IVFFlat 的 operator class。
            dim: 当前未使用, 保留以兼容 BaseEngine 签名。
            **kwargs: HNSW 读 m(默认 16)、ef_construction(默认 64);
                      IVFFlat 读 lists(默认 sqrt(行数)/10)。

        Raises:
            KeyError: distance 不在三种已知取值中。
            psycopg2 异常: 建索引失败(如维度/参数不合法)。

        副作用: 设置会话级并行参数; 若连接在事务中则建索引 DDL 会随 commit 生效。
        本方法不提交事务(create_index 的 SQL 通过 _execute 执行, 该函数会 commit)。
        """
        if index_type == "none":
            return

        # operator class 必须与查询算子/距离指标一致, 否则查询不会走该索引。
        op_class = {
            "l2": "vector_l2_ops",
            "ip": "vector_ip_ops",
            "cosine": "vector_cosine_ops",
        }[distance]
        idx_name = f"{table}_{index_type}_idx"

        conn = self.connect()
        try:
            # Configure high-concurrency parallel index build using 90% of the
            # available device threads, capped by the max_worker_processes limit.
            # Only the three parallel parameters that can be changed at session
            # level are set here; max_worker_processes is a PGC_POSTMASTER
            # parameter and must be set in postgresql.conf followed by a server
            # restart, so we read its current value and cap target_workers to it.
            desired_workers = max(1, int(len(os.sched_getaffinity(0)) * 0.9))
            max_worker_processes = int(
                self._execute(
                    conn,
                    "SHOW max_worker_processes",
                    fetch=True,
                )[0][0]
            )
            target_workers = min(desired_workers, max_worker_processes)
            self._execute(conn, f"SET max_parallel_workers = {target_workers}")
            self._execute(
                conn,
                f"SET max_parallel_maintenance_workers = {target_workers}",
            )
            self._execute(
                conn,
                f"SET max_parallel_workers_per_gather = {target_workers}",
            )

            if index_type == "hnsw":
                m = kwargs.get("m", 16)
                ef_construction = kwargs.get("ef_construction", 64)
                self._execute(
                    conn,
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} "
                    f"USING hnsw (vector {op_class}) "
                    f"WITH (m = {m}, ef_construction = {ef_construction})",
                )
            elif index_type == "ivfflat":
                total = self._execute(
                    conn,
                    f"SELECT count(*) FROM {table}",
                    fetch=True,
                )[0][0]
                import math
                lists = kwargs.get("lists", max(1, int(math.sqrt(total) / 10)))
                self._execute(
                    conn,
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} "
                    f"USING ivfflat (vector {op_class}) WITH (lists = {lists})",
                )
        finally:
            conn.close()

    def insert_vectors(
        self,
        table: str,
        vectors: List[List[float]],
        batch_size: int = 1000,
    ) -> None:
        """批量写入向量（脚本式路径，不带 id 与标量列）。

        每批用 ``execute_values`` 拼一条多行 INSERT 减少往返；向量以
        ``"[v1,v2,...]"`` 文本形式传入并在 SQL 里 ``CAST(%s AS vector)``，
        因为 psycopg2 没有 pgvector 类型的适配器。整批结束后统一 commit。
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                for start in range(0, len(vectors), batch_size):
                    end = min(start + batch_size, len(vectors))
                    rows = [
                        (f"[{','.join(str(v) for v in vec)}]",)
                        for vec in vectors[start:end]
                    ]
                    psycopg2.extras.execute_values(
                        cur,
                        f"INSERT INTO {table} (vector) VALUES %s",
                        rows,
                        template="(CAST(%s AS vector))",
                        page_size=1000,
                    )
                conn.commit()
        finally:
            conn.close()

    def query(
        self,
        conn_or_table,
        vector: Optional[List[float]] = None,
        top_k: int = 10,
        distance: str = "l2",
    ) -> List[Tuple[int, float]]:
        """Query nearest neighbors.

        Can be called as:
            engine.query(conn, table, vector, top_k, distance)
        or with a connection already established:
            engine.query(conn, table, vector, top_k, distance)
        """
        if vector is None:
            raise ValueError("vector is required")

        # Handle the case where conn_or_table is a connection or table name
        if isinstance(conn_or_table, str):
            table = conn_or_table
            conn = self.connect()
            own_conn = True
        else:
            conn = conn_or_table
            own_conn = False
            table = top_k  # This is a hack, let me fix the interface
            # Actually, the base class query signature is (table, vector, top_k, distance)
            # But measure_qps calls self.query(conn, table, vec, top_k, distance)
            # Let me handle both cases

        try:
            dist_op = DIST_OPS.get(distance, "<->")
            sort_dir = SORT_DIR.get(distance, "ASC")
            vec_str = f"[{','.join(str(v) for v in vector)}]"

            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, (vector {dist_op} '{vec_str}'::vector) AS ds "
                    f"FROM {table} ORDER BY ds {sort_dir} LIMIT {top_k}"
                )
                return [(row[0], row[1]) for row in cur.fetchall()]
        finally:
            if own_conn:
                conn.close()

    def query_sql(
        self,
        table: str,
        vector: List[float],
        top_k: int = 10,
        distance: str = "l2",
    ) -> str:
        """Generate the SQL for a query (used by shell scripts)."""
        dist_op = DIST_OPS.get(distance, "<->")
        sort_dir = SORT_DIR.get(distance, "ASC")
        vec_str = f"[{','.join(str(v) for v in vector)}]"
        return (
            f"SELECT id, (vector {dist_op} '{vec_str}'::vector) AS ds "
            f"FROM {table} ORDER BY ds {sort_dir} LIMIT {top_k}"
        )

    def detect_vector_column(self, table: str) -> Tuple[Optional[str], int, Optional[str]]:
        """Detect the vector column name, dimension, and type."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND data_type = 'USER-DEFINED' "
                    "AND udt_name = 'vector' LIMIT 1",
                    (table,),
                )
                row = cur.fetchone()
                if row:
                    col = row[0]
                    cur.execute(
                        f"SELECT vector_dims({col}) FROM {table} LIMIT 1"
                    )
                    dim = int(cur.fetchone()[0] or 0)
                    return col, dim, "vector"
                return None, 0, None
        finally:
            conn.close()

    def sample_vectors(
        self, table: str, col: str, n: int
    ) -> List[Tuple[int, List[float]]]:
        """Sample n random vectors from the table."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, {col}::text FROM {table} "
                    f"ORDER BY random() LIMIT {n}"
                )
                result = []
                for row_id, txt in cur.fetchall():
                    clean = txt.strip("{}[]\"'")
                    vals = [float(x.strip()) for x in clean.split(",") if x.strip()]
                    result.append((row_id, vals))
                return result
        finally:
            conn.close()

    # =====================================================================
    # QPS Benchmark 工具方法
    # =====================================================================

    @staticmethod
    def gen_vector_text(dim: int) -> str:
        """生成随机向量文本，如 '[0.1,-0.2,0.3,...]'"""
        v = [round(random.uniform(-1.0, 1.0), 6) for _ in range(dim)]
        return '[' + ','.join(str(x) for x in v) + ']'

    @staticmethod
    def gen_vector_list(dim: int) -> List[float]:
        """生成随机向量列表"""
        return [round(random.uniform(-1.0, 1.0), 6) for _ in range(dim)]

    def gen_query_vectors(self, dim: int, n: int) -> List[str]:
        """预生成 n 个随机查询向量（文本格式）"""
        return [self.gen_vector_text(dim) for _ in range(n)]

    def _make_inline_sql(self, table: str, dim: int, distance: str,
                         sql_type: str, top_k: int, vec_text: str) -> str:
        """生成 inline 类型 SQL（不涉及 PREPARE/EXECUTE）"""
        op = DIST_OPS.get(distance, "<->")
        sort = SORT_DIR.get(distance, "ASC")
        t = table

        if sql_type == "text_literal":
            return f"SELECT id, (vector {op} '{vec_text}'::vector) AS dis FROM {t} ORDER BY dis {sort} LIMIT {top_k}"
        elif sql_type == "text_cast":
            return f"SELECT id, (vector {op} CAST('{vec_text}' AS vector)) AS dis FROM {t} ORDER BY dis {sort} LIMIT {top_k}"
        elif sql_type == "array_cast":
            bare = vec_text.strip("[]")
            return f"SELECT id, (vector {op} CAST(ARRAY[{bare}]::real[] AS vector)) AS dis FROM {t} ORDER BY dis {sort} LIMIT {top_k}"
        elif sql_type == "with_text_literal":
            return (f"WITH qv AS (SELECT '{vec_text}'::vector AS v) "
                    f"SELECT t.id, (t.vector {op} qv.v) AS dis FROM {t} t CROSS JOIN qv "
                    f"ORDER BY dis {sort} LIMIT {top_k}")
        # fallback
        return f"SELECT id, (vector {op} '{vec_text}'::vector) AS dis FROM {t} ORDER BY dis {sort} LIMIT {top_k}"

    def _make_prepare_sql(self, table: str, dim: int, distance: str,
                          top_k: int) -> str:
        """生成 PREPARE 语句的 SQL 模板"""
        op = DIST_OPS.get(distance, "<->")
        sort = SORT_DIR.get(distance, "ASC")
        return (f"SELECT id, (vector {op} $1::vector) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k}")

    def _execute_query(self, conn, sql: str, params=None) -> list:
        """执行 SQL 查询并返回结果"""
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [(row[0], float(row[1])) for row in cur.fetchall()]

    def query_with_type(
        self,
        conn,
        table: str,
        dim: int,
        distance: str,
        sql_type: str,
        top_k: int,
        vec_text: str,
        stmt_name: str = "qps_stmt",
    ) -> list:
        """使用指定 SQL 类型执行一次向量查询

        Args:
            conn: 数据库连接
            table: 表名
            dim: 向量维度
            distance: 距离类型 (l2/cosine/ip)
            sql_type: SQL 语句类型
            top_k: 返回 top-k 结果
            vec_text: 查询向量文本 (如 '[0.1,0.2,...]')
            stmt_name: PREPARE 语句名（仅 prepared 类型使用）
        Returns:
            结果列表 [(id, distance), ...]
        """
        if sql_type == "prepared":
            return self._execute_query(conn, f"EXECUTE {stmt_name}(%s)", (vec_text,))
        else:
            sql = self._make_inline_sql(table, dim, distance, sql_type, top_k, vec_text)
            return self._execute_query(conn, sql)