"""Base engine class for vector database benchmarks."""

import time
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple


class BaseEngine(ABC):
    """Abstract base class for all vector database engines."""

    ENGINE_NAME = "base"

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 5432,
        user: str = "postgres",
        password: str = "123456",
        database: str = "postgres",
        **kwargs,
    ):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.extra = kwargs

    @abstractmethod
    def connect(self):
        """Return a database connection."""
        ...

    @abstractmethod
    def create_table(
        self, table: str, dim: int, drop: bool = False
    ) -> None:
        """Create a vector table with the given dimension."""
        ...

    @abstractmethod
    def create_index(
        self,
        table: str,
        index_type: str = "hnsw",
        distance: str = "l2",
        dim: int = 128,
        **kwargs,
    ) -> None:
        """Create a vector index on the table."""
        ...

    @abstractmethod
    def insert_vectors(
        self,
        table: str,
        vectors: List[List[float]],
        batch_size: int = 1000,
    ) -> None:
        """Insert vectors into the table."""
        ...

    @abstractmethod
    def query(
        self,
        table: str,
        vector: List[float],
        top_k: int = 10,
        distance: str = "l2",
    ) -> List[Tuple[int, float]]:
        """Query nearest neighbors. Returns list of (id, distance)."""
        ...

    @abstractmethod
    def server_version(self) -> str:
        """Return the server version string."""
        ...

    def measure_qps(
        self,
        table: str,
        vectors: List[List[float]],
        top_k: int = 10,
        distance: str = "l2",
        duration: float = 10.0,
        concurrency: int = 1,
        warmup: float = 2.0,
    ) -> Tuple[float, int]:
        """Measure queries per second by running queries concurrently.

        Returns:
            (qps, error_count)
        """
        if duration <= 0:
            return 0.0, 0

        n_vectors = len(vectors)
        if n_vectors == 0:
            return 0.0, 0

        stop = threading.Event()
        counts = {"count": 0, "errors": 0}
        lock = threading.Lock()

        def worker():
            conn = self.connect()
            try:
                idx = 0
                while not stop.is_set():
                    vec = vectors[idx % n_vectors]
                    try:
                        self.query(conn, table, vec, top_k, distance)
                        with lock:
                            counts["count"] += 1
                    except Exception:
                        with lock:
                            counts["errors"] += 1
                    idx += 1
            finally:
                conn.close()

        # Warmup
        warmup_end = time.time() + warmup
        wconn = self.connect()
        try:
            idx = 0
            while time.time() < warmup_end:
                vec = vectors[idx % n_vectors]
                self.query(wconn, table, vec, top_k, distance)
                idx += 1
        finally:
            wconn.close()

        # Benchmark
        threads = []
        t_start = time.time()
        for _ in range(concurrency):
            t = threading.Thread(target=worker, daemon=True)
            t.start()
            threads.append(t)

        time.sleep(duration)
        stop.set()
        for t in threads:
            t.join(timeout=5.0)

        elapsed = time.time() - t_start
        qps = counts["count"] / elapsed if elapsed > 0 else 0.0
        return qps, counts["errors"]

    def measure_recall(
        self,
        table: str,
        query_vectors: List[List[float]],
        ground_truth: List[List[int]],
        top_k: int = 10,
        distance: str = "l2",
    ) -> float:
        """Measure recall@k.

        Args:
            table: Table name.
            query_vectors: List of query vectors.
            ground_truth: ground_truth[i] is list of ground truth neighbor IDs
                          for query_vectors[i].
            top_k: Number of nearest neighbors to retrieve.
            distance: Distance metric.

        Returns:
            Average recall@k (0.0 to 1.0).
        """
        total_hits = 0
        total_gt = 0

        for qvec, gt_ids in zip(query_vectors, ground_truth):
            results = self.query(table, qvec, top_k, distance)
            result_ids = {r[0] for r in results}
            gt_set = set(gt_ids[:top_k])
            total_hits += len(result_ids & gt_set)
            total_gt += min(top_k, len(gt_ids))

        if total_gt == 0:
            return 0.0
        return total_hits / total_gt

    def __repr__(self):
        return (
            f"{self.ENGINE_NAME}(host={self.host}, port={self.port}, "
            f"db={self.database})"
        )