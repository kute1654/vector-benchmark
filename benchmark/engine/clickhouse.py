"""ClickHouse engine implementation.

中文说明
--------
脚本式（script-style）引擎实现：本类不经过 ``ClientFactory``，而是自己保存连接
参数、按需 ``connect()``，并复用基类 ``BaseEngine.measure_qps`` /
``measure_recall`` 内置的多线程循环来跑 QPS 与召回。正式 benchmark 走 ``run.py``
→ 驱动式 ``engine/base_client/*`` + ``engine/clients/<engine>/*``，两套抽象
互不依赖；本模块只属于脚本式那一套。

职责：ClickHouse 侧的建表、建索引、写入、查询与采样 SQL；``myscale.py`` 在本类
基础上覆盖索引创建与查询形态。
"""

from typing import Any, Dict, List, Optional, Tuple

from .base import BaseEngine

# clickhouse_driver 是可选依赖：缺失时模块仍可导入（便于只跑 PG 系引擎），
# 但构造 ClickHouseEngine 时会在 __init__ 中抛出带安装提示的 ImportError。
try:
    from clickhouse_driver import Client
    HAS_CLICKHOUSE_DRIVER = True
except ImportError:
    HAS_CLICKHOUSE_DRIVER = False


# 统一度量名 -> ClickHouse 标量距离函数。
# 注意 ``ip``（内积）也映射到 ``cosineDistance``：ClickHouse 没有可用的负内积
# 标量函数，这里沿用了与 generate_sql_files._ch_dist_func 相同的既有约定。
DIST_FUNCS = {
    "l2": "L2Distance",
    "ip": "cosineDistance",
    "cosine": "cosineDistance",
}


class ClickHouseEngine(BaseEngine):
    """ClickHouse engine with vector search capabilities.

    中文：ClickHouse 原生 MergeTree + ANN 跳数索引（``hnsw`` / ``annoy``）的
    最小封装。表结构固定为 ``(id UInt32, vector Array(Float32))``，向量列名
    写死为 ``vector``，以便与 generate_sql_files 生成的 SQL 形态对齐。
    """

    # 与配置文件 ``engine`` 字段、ENGINE_REGISTRY 的键保持一致。
    ENGINE_NAME = "clickhouse"

    def __init__(self, **kwargs):
        """保存连接参数；构造阶段不建立任何连接。

        先用 ``pop`` 取走通用参数，再把剩余 ``kwargs`` 透传给基类（存入
        ``self.extra``），这样调用方可以附带驱动特有选项而无需在此逐个声明。
        """
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

    # ---- 连接与元信息 ----

    def connect(self):
        """新建并返回一个 clickhouse-driver ``Client``。

        每次调用都创建独立客户端，调用方用 ``finally: client.disconnect()``
        关闭；多线程测量时每个 worker 各持一条连接，避免共享同一个 socket。
        """
        return Client(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

    def server_version(self) -> str:
        """返回 ``SELECT version()`` 的结果，用于记录服务端版本。"""
        client = self.connect()
        try:
            return client.execute("SELECT version()")[0][0] or "?"
        finally:
            client.disconnect()

    # ---- 建表与索引 ----

    def create_table(
        self, table: str, dim: int, drop: bool = False
    ) -> None:
        """建立 ``(id UInt32, vector Array(Float32))`` 表。

        ``drop=True`` 时先 ``DROP TABLE IF EXISTS``，便于重复实验。
        ``ENGINE = MergeTree() ORDER BY id`` 只是普通表引擎，不含向量索引；
        索引由 ``create_index`` 再以 ``ALTER TABLE ... ADD INDEX`` 单独添加。
        ``CONSTRAINT vector_dim CHECK length(vector) = dim`` 会在写入阶段拒绝
        维度不符的数据，属于数据层校验而非索引参数。
        """
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
        """为 ``table.vector`` 添加 ClickHouse ANN 跳数索引。

        ``index_type`` 取值：
          * ``none``：直接返回，不建索引（纯全表扫描基线）。
          * ``hnsw``：``ADD INDEX vec_idx vector TYPE hnsw GRANULARITY 1``；
            索引以向量列为键，具体距离函数由查询时的 ``L2Distance`` /
            ``cosineDistance`` 决定，因此 DDL 里不带距离参数。
          * ``annoy``：``TYPE annoy(<dist_func>) GRANULARITY 10000``；annoy 在
            索引定义中绑定距离函数，``GRANULARITY`` 表示每个索引块覆盖的
            granule 数，越大索引越粗、构建与查询开销越低。
        索引对之后写入的数据同样生效（ClickHouse 在 merge 时补建）；``distance``
        仅在 annoy 分支使用，``dim`` / ``kwargs`` 目前不参与 DDL。
        """
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

    # ---- 数据写入 ----

    def insert_vectors(
        self,
        table: str,
        vectors: List[List[float]],
        batch_size: int = 10000,
    ) -> None:
        """把 ``vectors`` 写入 ``table``，id 从 1 开始按顺序编号。

        注意：``batch_size`` 目前未被使用——所有行一次性交给 ``client.execute``，
        由驱动内部批量发送；保留该参数只为与 ``BaseEngine`` 的签名兼容。
        """
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

    # ---- 查询 ----

    def query(
        self,
        table: str,
        vector: List[float],
        top_k: int = 10,
        distance: str = "l2",
    ) -> List[Tuple[int, float]]:
        """Query nearest neighbors.

        中文：查询 ``top_k`` 个最近邻，返回 ``[(id, distance), ...]``。
        SQL 用 ``%(vec)s`` 绑定参数，避免把长向量字面量拼进 SQL。
        排序方向：``l2`` 用 ``ASC``；其余（含映射到 cosineDistance 的
        ``ip`` / ``cosine``）一律 ``DESC``——这是脚本式实现沿用的旧行为，
        与 generate_sql_files 中 cosine 使用 ``ASC`` 的约定不同，解读 QPS
        时需留意。
        """
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
        """Generate the SQL for a query.

        中文：返回可直接执行的 SQL 文本，但不绑定参数——向量被渲染成
        ``[v1,v2,...]`` 字面量内联进语句，便于交给其他执行器或写入日志。
        调用方需自行保证浮点字符串能被服务端解析；度量映射与排序方向同
        ``query``。
        """
        dist_func = DIST_FUNCS.get(distance, "L2Distance")
        sort_dir = "ASC" if distance == "l2" else "DESC"
        vec_str = "[" + ",".join(str(v) for v in vector) + "]"
        return (
            f"SELECT id, {dist_func}(vector, {vec_str}) AS dis "
            f"FROM {table} ORDER BY dis {sort_dir} LIMIT {top_k}"
        )

    # ---- 表结构探查与采样 ----

    def detect_vector_column(self, table: str) -> Tuple[Optional[str], int, Optional[str]]:
        """Detect the vector column.

        中文：在 ``system.columns`` 中查找第一个 ``Array(Float*)`` 列，返回
        ``(列名, 维度, 类型名)``。维度用 ``SELECT length(col) LIMIT 1`` 从首行
        实测；找不到向量列时返回 ``(None, 0, None)``，空表则维度记为 0。
        """
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
        """Sample n random vectors from the table.

        中文：用 ``ORDER BY rand() LIMIT n`` 随机取 n 行 ``(id, vector)``。
        该写法会扫描全表并按随机键排序，只适合脚本式的小规模采样；
        正式基准的采样在 generate_sql_files 中完成。
        """
        client = self.connect()
        try:
            rows = client.execute(
                f"SELECT id, {col} FROM {table} ORDER BY rand() LIMIT {n}"
            )
            return [(r[0], list(r[1])) for r in rows]
        finally:
            client.disconnect()