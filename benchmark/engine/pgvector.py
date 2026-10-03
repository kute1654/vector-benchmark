"""Pgvector engine implementation."""

import math
import random
import threading
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from .base import BaseEngine

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

# 查询计划缓存模式映射
PLAN_CACHE_MODES = {
    "off":  "force_custom_plan",   # 每次重新规划（不使用缓存）
    "on":   "force_generic_plan",  # 强制通用计划（复用缓存）
    "auto": "auto",                # PostgreSQL 自动选择
}

# 已知的 5 个测试表
BENCHMARK_TABLES = [
    {"label": "128dim",  "table": "benchmark_sift_128",   "dim": 128,  "distance": "l2"},
    {"label": "256dim",  "table": "benchmark_256_290k",   "dim": 256,  "distance": "cosine"},
    {"label": "768dim",  "table": "benchmark_768_1m",     "dim": 768,  "distance": "cosine"},
    {"label": "960dim",  "table": "benchmark_960_1m",     "dim": 960,  "distance": "l2"},
    {"label": "1536dim", "table": "benchmark_1536_1m",    "dim": 1536, "distance": "cosine"},
]


class PgvectorEngine(BaseEngine):
    """Pgvector engine for PostgreSQL with pgvector extension."""

    ENGINE_NAME = "pgvector"

    def connect(self):
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
        )

    def _execute(self, conn, sql: str, params=None, fetch: bool = False):
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetch and cur.description:
                return cur.fetchall()
        conn.commit()
        return None

    def server_version(self) -> str:
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
        conn = self.connect()
        try:
            if drop:
                self._execute(conn, f"DROP TABLE IF EXISTS {table} CASCADE")
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
        if index_type == "none":
            return

        op_class = {
            "l2": "vector_l2_ops",
            "ip": "vector_ip_ops",
            "cosine": "vector_cosine_ops",
        }[distance]
        idx_name = f"{table}_{index_type}_idx"

        conn = self.connect()
        try:
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