"""PolarDB-pg engine implementation with PASE vector extension."""

from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from .base import BaseEngine


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
        table: str,
        vector: List[float],
        top_k: int = 10,
        distance: str = "l2",
    ) -> List[Tuple[int, float]]:
        """Query nearest neighbors using PASE operator."""
        conn = self.connect()
        try:
            sort_dir = "ASC" if distance == "l2" else "DESC"
            vec_str = "{" + ",".join(str(v) for v in vector) + "}"

            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, (vector <?> CAST('{vec_str}' AS float4[])) AS ds "
                    f"FROM {table} ORDER BY ds {sort_dir} LIMIT {top_k}"
                )
                return [(row[0], row[1]) for row in cur.fetchall()]
        finally:
            conn.close()

    def query_sql(
        self,
        table: str,
        vector: List[float],
        top_k: int = 10,
        distance: str = "l2",
    ) -> str:
        """Generate the SQL for a query."""
        sort_dir = "ASC" if distance == "l2" else "DESC"
        vec_str = "{" + ",".join(str(v) for v in vector) + "}"
        return (
            f"SELECT id, (vector <?> CAST('{vec_str}' AS float4[])) AS ds "
            f"FROM {table} ORDER BY ds {sort_dir} LIMIT {top_k}"
        )

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