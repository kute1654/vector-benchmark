"""脚本式引擎基类：面向“快速自测”的轻量抽象。

本模块属于“脚本式”引擎层（``benchmark.engine.pgvector`` 等），与
``run.py`` 使用的“驱动式”客户端（``engine/base_client/*``）是两套独立抽象：

- 脚本式：一个类直接持有连接参数，提供 ``connect / create_table /
  create_index / insert_vectors / query`` 等最小接口，并在这里内置了
  多线程 QPS 与 recall 的测量循环，适合临时验证或小规模实验。
- 驱动式：Configurator/Uploader/Searcher 分离，由 ``ClientFactory`` 组装，
  支持配置矩阵展开、CSV/JSON 输出，是正式 benchmark 的路径。

子类只需实现抽象方法；``query`` 的签名在不同子类间并不完全一致
（pgvector/polardb 额外接受 conn-first 形式以配合 ``measure_qps``），
因此新增引擎时请同时检查 ``measure_qps`` 的调用约定。
"""

import time
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple


class BaseEngine(ABC):
    """所有脚本式引擎的抽象基类。

    子类通过类属性 ``ENGINE_NAME`` 声明引擎名，并实现
    ``connect/create_table/create_index/insert_vectors/query/server_version``。
    """

    # 用于 __repr__ 与日志展示；具体引擎覆盖为自己的名字。
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
        """只保存连接参数，不在构造时建连。

        连接参数全部在此保存，不在构造时建连：脚本式用法通常先构造、
        再按需 connect()，便于在同一个对象上反复开关连接（例如测量连接
        建立开销、或在 worker 线程里各自拿一条独占连接）。
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.extra = kwargs  # 各引擎特有的额外参数（如 http_port、secure 等）

    @abstractmethod
    def connect(self):
        """建立并返回一个新的数据库连接（调用方负责关闭）。"""
        ...

    @abstractmethod
    def create_table(
        self, table: str, dim: int, drop: bool = False
    ) -> None:
        """创建向量表。

        Args:
            table: 表名。
            dim: 向量维度。
            drop: 为 True 时先删除同名表（用于重复实验）。
        """
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
        """在 ``table`` 上创建向量索引。

        ``index_type`` / ``distance`` 是统一名称，各引擎自行映射：
        例如 ClickHouse 的 ``hnsw`` 需要写成 ``vector_similarity`` 索引，
        pgvector 需要映射成 ``vector_l2_ops`` 等 opclass。
        """
        ...

    @abstractmethod
    def insert_vectors(
        self,
        table: str,
        vectors: List[List[float]],
        batch_size: int = 1000,
    ) -> None:
        """批量写入向量；``batch_size`` 控制单次 INSERT 的行数。"""
        ...

    @abstractmethod
    def query(
        self,
        table: str,
        vector: List[float],
        top_k: int = 10,
        distance: str = "l2",
    ) -> List[Tuple[int, float]]:
        """查询 ``top_k`` 个最近邻，返回 ``[(id, distance), ...]``。

        注意：``measure_qps`` 内部以 ``self.query(conn, table, vec, ...)``
        的 conn-first 形式回调，pgvector/polardb 子类通过重载参数名兼容了
        两种调用；新引擎若只实现本签名，需同步调整 ``measure_qps``。
        """
        ...

    @abstractmethod
    def server_version(self) -> str:
        """返回服务端版本字符串，用于结果记录与兼容性判断。"""
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
        """多线程循环查询，测量 QPS。

        流程：串行预热 ``warmup`` 秒（填满计划缓存 / 页缓存，避免把冷启动
        开销算进结果）→ 启动 ``concurrency`` 个守护线程 → 运行 ``duration``
        秒 → 置位停止事件并等待线程退出。

        Args:
            table: 表名。
            vectors: 查询向量池；每个 worker 循环取用（``idx % n_vectors``）。
            top_k: 每次查询返回的邻居数。
            distance: 距离度量名称。
            duration: 正式计时时长（秒）。
            concurrency: 并发线程数。
            warmup: 预热时长（秒）。

        Returns:
            ``(qps, error_count)``。QPS 以“实际墙钟耗时”为分母（含线程创建
            与退出等待），单次查询异常只计入 errors、不终止测试。

        Notes:
            - ``vectors`` 为空或 ``duration <= 0`` 时直接返回 0，避免除零。
            - 每个线程独占一个连接；连接在 ``finally`` 中关闭。
            - worker 里调用的是 conn-first 的 ``self.query(conn, table, ...)``，
              子类需兼容该调用形态（参见抽象方法 ``query`` 的说明）。
        """
        if duration <= 0:
            return 0.0, 0

        n_vectors = len(vectors)
        if n_vectors == 0:
            return 0.0, 0

        # stop 用于让所有 worker 在看到置位后退出循环；counts 由 lock 保护，
        # 因为多线程同时自增在 Python 里不是原子的。
        stop = threading.Event()
        counts = {"count": 0, "errors": 0}
        lock = threading.Lock()

        def worker():
            """线程主体：循环发查询，成功/失败分别计数，直到 stop 置位。

            统计通过 ``counts`` + ``lock`` 汇总；单次异常只记错误数，避免
            一个超时把整轮压测打断。
            """
            # 每个线程一个连接：pgvector 等连接的 psycopg2 连接不是线程安全的。
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
                        # 单次查询失败（超时、连接断开等）只累计错误数，
                        # 不中断整轮测量；错误率由调用方结合 error_count 判断。
                        with lock:
                            counts["errors"] += 1
                    idx += 1
            finally:
                conn.close()

        # Warmup：单线程串行预热，目的是让服务端缓存、连接池、索引页都热起来；
        # 预热期间的异常不统计（预热本身不计入 QPS）。
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

        # Benchmark：daemon=True 保证即使主线程异常退出，也不会因为 worker
        # 仍在查询而挂住进程；stop.set() 后最多再等 5 秒。
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

        # 分母用实际墙钟时间而不是配置的 duration：线程启动/退出有开销，
        # 且 time.sleep 可能被信号打断。
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
        """测量 recall@k。

        对每条查询向量取 ``top_k`` 个结果，与 ground truth 的前 ``top_k`` 个
        id 求交集，最后用“命中数 / ground truth 总数”汇总（micro-average，
        而不是逐条 recall 再平均）。

        Args:
            table: 表名。
            query_vectors: 查询向量列表。
            ground_truth: 与 ``query_vectors`` 一一对应的真实邻居 id 列表。
            top_k: 检索邻居数，同时也是 ground truth 的截断长度。
            distance: 距离度量名称。

        Returns:
            平均 recall@k，范围 0.0～1.0；没有可比较的 ground truth 时返回 0.0。

        Notes:
            - 这里调用的是 ``self.query(table, ...)``（table-first），与
              ``measure_qps`` 的 conn-first 调用不同；pgvector/polardb 的
              重载签名同时支持两种形式。
            - 结果 id 去重后再与 ground truth 求交，避免重复 id 虚增命中数。
        """
        total_hits = 0
        total_gt = 0

        for qvec, gt_ids in zip(query_vectors, ground_truth):
            results = self.query(table, qvec, top_k, distance)
            result_ids = {r[0] for r in results}
            gt_set = set(gt_ids[:top_k])
            total_hits += len(result_ids & gt_set)
            # 分母按实际可比较的 ground truth 数量计（可能不足 top_k），
            # 这样数据末尾的短列表不会被当成召回失败。
            total_gt += min(top_k, len(gt_ids))

        if total_gt == 0:
            return 0.0
        return total_hits / total_gt

    def __repr__(self):
        """日志里用于定位实例；刻意不包含 user/password 等敏感字段。"""
        # 只展示定位连接所需的信息，不泄露密码。
        return (
            f"{self.ENGINE_NAME}(host={self.host}, port={self.port}, "
            f"db={self.database})"
        )