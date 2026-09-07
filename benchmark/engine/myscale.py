"""MyScale engine implementation (ClickHouse-compatible)."""

from typing import Any, Dict, List, Optional, Tuple

from .base import BaseEngine
from .clickhouse import ClickHouseEngine


class MyScaleEngine(ClickHouseEngine):
    """MyScale engine (ClickHouse-compatible with vector search extensions).

    MyScale uses the ClickHouse protocol with additional vector search
    capabilities. It inherits from ClickHouseEngine and adds MyScale-specific
    features.
    """

    ENGINE_NAME = "myscale"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def create_index(
        self,
        table: str,
        index_type: str = "MSTG",
        distance: str = "l2",
        dim: int = 128,
        **kwargs,
    ) -> None:
        if index_type == "none":
            return

        client = self.connect()
        try:
            if index_type == "MSTG":
                client.execute(f"""
                    ALTER TABLE {table}
                    ADD VECTOR INDEX vec_idx vector
                    TYPE MSTG
                    GRANULARITY 1
                """)
            elif index_type == "SCANN":
                client.execute(f"""
                    ALTER TABLE {table}
                    ADD VECTOR INDEX vec_idx vector
                    TYPE SCANN
                    GRANULARITY 1
                """)
        finally:
            client.disconnect()

    def query(
        self,
        table: str,
        vector: List[float],
        top_k: int = 10,
        distance: str = "l2",
    ) -> List[Tuple[int, float]]:
        """Query nearest neighbors using MyScale MSTG index."""
        client = self.connect()
        try:
            dist_func = "L2Distance" if distance == "l2" else "cosineDistance"
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
        dist_func = "L2Distance" if distance == "l2" else "cosineDistance"
        sort_dir = "ASC" if distance == "l2" else "DESC"
        vec_str = "[" + ",".join(str(v) for v in vector) + "]"
        return (
            f"SELECT id, {dist_func}(vector, {vec_str}) AS dis "
            f"FROM {table} ORDER BY dis {sort_dir} LIMIT {top_k}"
        )