"""ClickHouse engine implementation."""

from typing import Any, Dict, List, Optional, Tuple

from .base import BaseEngine

try:
    from clickhouse_driver import Client
    HAS_CLICKHOUSE_DRIVER = True
except ImportError:
    HAS_CLICKHOUSE_DRIVER = False


DIST_FUNCS = {
    "l2": "L2Distance",
    "ip": "cosineDistance",
    "cosine": "cosineDistance",
}


class ClickHouseEngine(BaseEngine):
    """ClickHouse engine with vector search capabilities."""

    ENGINE_NAME = "clickhouse"

    def __init__(self, **kwargs):
        if not HAS_CLICKHOUSE_DRIVER:
            raise ImportError(
                "clickhouse-driver is required. Install with: "
                "pip install clickhouse-driver"
            )
        host = kwargs.pop("host", "127.0.0.1")
        port = kwargs.pop("port", 9000)
        user = kwargs.pop("user", "default")
        password = kwargs.pop("password", "")
        database = kwargs.pop("database", "default")
        super().__init__(host=host, port=port, user=user,
                         password=password, database=database, **kwargs)

    def connect(self):
        return Client(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def server_version(self) -> str:
        client = self.connect()
        try:
            return client.execute("SELECT version()")[0][0] or "?"
        finally:
            client.disconnect()

    def create_table(
        self, table: str, dim: int, drop: bool = False
    ) -> None:
        client = self.connect()
        try:
            if drop:
                client.execute(f"DROP TABLE IF EXISTS {table}")

            client.execute(f"""
                CREATE TABLE IF NOT EXISTS {table}
                (
                    id UInt32,
                    vector Array(Float32),
                    CONSTRAINT vector_dim CHECK length(vector) = {dim}
                )
                ENGINE = MergeTree()
                ORDER BY id
            """)
        finally:
            client.disconnect()

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

        client = self.connect()
        try:
            dist_func = DIST_FUNCS.get(distance, "L2Distance")
            if index_type == "hnsw":
                client.execute(f"""
                    ALTER TABLE {table}
                    ADD INDEX vec_idx vector TYPE {index_type}
                    GRANULARITY 1
                """)
            elif index_type == "annoy":
                client.execute(f"""
                    ALTER TABLE {table}
                    ADD INDEX vec_idx vector TYPE annoy({dist_func})
                    GRANULARITY 10000
                """)
        finally:
            client.disconnect()

    def insert_vectors(
        self,
        table: str,
        vectors: List[List[float]],
        batch_size: int = 10000,
    ) -> None:
        client = self.connect()
        try:
            rows = [
                (i + 1, vec)
                for i, vec in enumerate(vectors)
            ]
            client.execute(
                f"INSERT INTO {table} (id, vector) VALUES",
                rows,
            )
        finally:
            client.disconnect()

    def query(
        self,
        table: str,
        vector: List[float],
        top_k: int = 10,
        distance: str = "l2",
    ) -> List[Tuple[int, float]]:
        """Query nearest neighbors."""
        client = self.connect()
        try:
            dist_func = DIST_FUNCS.get(distance, "L2Distance")
            sort_dir = "ASC" if distance == "l2" else "DESC"

            results = client.execute(f"""
                SELECT id, {dist_func}(vector, %(vec)s) AS dis
                FROM {table}
                ORDER BY dis {sort_dir}
                LIMIT {top_k}
            """, {"vec": vector})
            return [(r[0], r[1]) for r in results]
        finally:
            client.disconnect()

    def query_sql(
        self,
        table: str,
        vector: List[float],
        top_k: int = 10,
        distance: str = "l2",
    ) -> str:
        """Generate the SQL for a query."""
        dist_func = DIST_FUNCS.get(distance, "L2Distance")
        sort_dir = "ASC" if distance == "l2" else "DESC"
        vec_str = "[" + ",".join(str(v) for v in vector) + "]"
        return (
            f"SELECT id, {dist_func}(vector, {vec_str}) AS dis "
            f"FROM {table} ORDER BY dis {sort_dir} LIMIT {top_k}"
        )

    def detect_vector_column(self, table: str) -> Tuple[Optional[str], int, Optional[str]]:
        """Detect the vector column."""
        client = self.connect()
        try:
            cols = client.execute(
                "SELECT name FROM system.columns "
                "WHERE database = %(db)s AND table = %(table)s "
                "AND type LIKE 'Array(Float%%)' LIMIT 1",
                {"db": self.database, "table": table},
            )
            if cols:
                col = cols[0][0]
                dim = client.execute(
                    f"SELECT length({col}) FROM {table} LIMIT 1"
                )[0][0]
                return col, int(dim or 0), "Array(Float32)"
            return None, 0, None
        finally:
            client.disconnect()

    def sample_vectors(
        self, table: str, col: str, n: int
    ) -> List[Tuple[int, List[float]]]:
        """Sample n random vectors from the table."""
        client = self.connect()
        try:
            rows = client.execute(
                f"SELECT id, {col} FROM {table} ORDER BY rand() LIMIT {n}"
            )
            return [(r[0], list(r[1])) for r in rows]
        finally:
            client.disconnect()