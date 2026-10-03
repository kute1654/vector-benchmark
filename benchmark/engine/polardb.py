"""PolarDB-pg engine implementation with PASE vector extension."""

from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from .base import BaseEngine


def _int_or(value, default: int) -> int:
    return int(value) if isinstance(value, int) else default


def _pase_distance_sql(
    table: str,
    vector: List[float],
    top_k: int = 10,
    distance: str = "l2",
) -> str:
    """PASE 查询 SQL: float4[] <?> pase。

    `<?>` 右侧必须是 pase 类型; 用 '逗号文本[:extra[:ds]]'::pase 走输入函数
    pase_in (安全), 不能用 pase('...') 构造函数 (那是 base64 解码, 只有
    pase(float4[]) / pase(float4[], extra, ds) 数组构造函数是安全的)。
    ds: 0 = L2 (cosine 需先归一化), 1 = inner product (返回原始内积, 越大越好)。
    """
    dist_lower = str(distance or "l2").lower()
    ds = 1 if dist_lower == "ip" else 0
    sort_dir = "DESC" if ds == 1 else "ASC"
    vec_str = ",".join(str(v) for v in vector)
    return (
        f"SELECT id, (vector <?> '{vec_str}:{ds}'::pase) AS ds "
        f"FROM {table} ORDER BY ds {sort_dir} LIMIT {top_k}"
    )


class PolarDBEngine(BaseEngine):
    """PolarDB for PostgreSQL with PASE vector extension."""

    ENGINE_NAME = "polardb"

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
                f"(id serial PRIMARY KEY, vector float4[])",
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

        idx_name = f"{table}_{index_type}_idx"
        conn = self.connect()
        try:
            if index_type == "hnsw":
                base_nb_num = kwargs.get("base_nb_num", 16)
                ef_build = kwargs.get("ef_build", 40)
                ef_search = kwargs.get("ef_search", 100)
                self._execute(
                    conn,
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} "
                    f"USING pase_hnsw (vector) WITH ("
                    f"dim = {dim}, base_nb_num = {base_nb_num}, "
                    f"ef_build = {ef_build}, ef_search = {ef_search}, "
                    f"base64_encoded = 0)",
                )
            elif index_type == "ivfflat":
                dist_type = 0 if distance == "l2" else 1
                lists = kwargs.get("lists", 100)
                self._execute(
                    conn,
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} "
                    f"USING pase_ivfflat (vector) WITH ("
                    f"dimension = {dim}, distance_type = {dist_type}, "
                    f"clustering_type = 1, "
                    f"clustering_params = 'lists={lists}')",
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
                        ("{" + ",".join(str(v) for v in vec) + "}",)
                        for vec in vectors[start:end]
                    ]
                    psycopg2.extras.execute_values(
                        cur,
                        f"INSERT INTO {table} (vector) VALUES %s",
                        rows,
                        template="(CAST(%s AS float4[]))",
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
        """Query nearest neighbors using PASE operator.

        Callable as ``engine.query(table, vector, top_k, distance)`` (manages its
        own connection) or ``engine.query(conn, table, vector, top_k, distance)``
        (same conn-first shape as BaseEngine.measure_qps; mirrors
        ``engine/pgvector.py``).
        """
        if vector is None:
            raise ValueError("vector is required")
        if isinstance(conn_or_table, str):
            table = conn_or_table
            conn = self.connect()
            own_conn = True
        else:
            conn = conn_or_table
            own_conn = False
            table = top_k
            top_k = distance if isinstance(distance, int) else 10

        sql = _pase_distance_sql(table, vector, _int_or(top_k, 10), distance if not isinstance(distance, int) else "l2")
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
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
        """Generate the SQL for a query."""
        return _pase_distance_sql(table, vector, top_k, distance)

    def detect_vector_column(self, table: str) -> Tuple[Optional[str], int, Optional[str]]:
        """Detect the vector column."""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND data_type = 'ARRAY' "
                    "AND udt_name = '_float4' LIMIT 1",
                    (table,),
                )
                row = cur.fetchone()
                if row:
                    col = row[0]
                    cur.execute(
                        f"SELECT array_length({col}, 1) FROM {table} LIMIT 1"
                    )
                    dim = int(cur.fetchone()[0] or 0)
                    return col, dim, "float4[]"
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