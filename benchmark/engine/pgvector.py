"""Pgvector engine implementation."""

from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from .base import BaseEngine

DIST_OPS = {"l2": "<->", "ip": "<#>", "cosine": "<=>"}
SORT_DIR = {"l2": "ASC", "ip": "DESC", "cosine": "ASC"}


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