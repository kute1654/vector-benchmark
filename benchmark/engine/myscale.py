"""MyScale engine implementation (ClickHouse-compatible).

中文说明
--------
脚本式（script-style）MyScale 引擎：复用 ``ClickHouseEngine`` 的连接、建表、
写入与采样方法，只覆盖索引创建（``MSTG`` / ``SCANN``）和查询 SQL。
它与 ``run.py`` 使用的驱动式 ``engine/clients/myscale/*`` 是两套实现：驱动式
客户端会用 ``distance(...)(vector, q)`` 形态以命中 MSTG 索引，而本类的
``query`` 仍生成 ``L2Distance`` / ``cosineDistance`` 标量函数。
"""

from typing import Any, Dict, List, Optional, Tuple

from .base import BaseEngine
from .clickhouse import ClickHouseEngine


class MyScaleEngine(ClickHouseEngine):
    """MyScale engine (ClickHouse-compatible with vector search extensions).

    MyScale uses the ClickHouse protocol with additional vector search
    capabilities. It inherits from ClickHouseEngine and adds MyScale-specific
    features.

    中文：MyScale 使用 ClickHouse 协议，默认端口/用户等连接参数与父类一致
    （127.0.0.1:9000, default）。注意默认 ``index_type`` 从父类的 ``hnsw``
    改为 ``MSTG``——这是 MyScale 的原生向量索引。
    """

    # 与配置文件 ``engine`` 字段、ENGINE_REGISTRY 的键保持一致。
    ENGINE_NAME = "myscale"

    def __init__(self, **kwargs):
        """仅转调父类构造函数：连接参数、校验与可选驱动检查全部继承。"""
        super().__init__(**kwargs)

    # ---- 索引创建（MyScale 专有） ----

    def create_index(
        self,
        table: str,
        index_type: str = "MSTG",
        distance: str = "l2",
        dim: int = 128,
        **kwargs,
    ) -> None:
        """在 ``table.vector`` 上创建 MyScale 向量索引。

        ``index_type``：
          * ``none``：跳过，不建索引（全表扫描基线）。
          * ``MSTG``（默认）：Multi-Scale Tree Graph，MyScale 默认 ANN 索引。
          * ``SCANN``：另一种 ANN 索引实现。
        两者 DDL 均为 ``ADD VECTOR INDEX vec_idx vector TYPE <type>
        GRANULARITY 1``；这与 ClickHouse 的 ``ADD INDEX ... TYPE hnsw`` 语法
        不同，也是本类必须覆盖父类方法的原因。``distance`` / ``dim`` /
        ``kwargs`` 目前不参与 DDL。
        """
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

    # ---- 查询 ----

    def query(
        self,
        table: str,
        vector: List[float],
        top_k: int = 10,
        distance: str = "l2",
    ) -> List[Tuple[int, float]]:
        """Query nearest neighbors using MyScale MSTG index.

        中文：返回 ``[(id, distance), ...]``，查询向量通过 ``%(vec)s`` 绑定。
        距离函数显式二选一（``l2 -> L2Distance``，其余 -> ``cosineDistance``），
        不查父类的 ``DIST_FUNCS``；``l2`` 用 ``ASC``，其余用 ``DESC``。

        重要：MyScale 的 ANN 索引只对 ``distance(...)`` 家族函数生效，这里的
        标量 ``L2Distance`` 可能退化为全表扫描，属于脚本式的直接连接基线；
        需要索引加速的对比请使用 ``generate_clickhouse_sql_forms(engine=
        "myscale")`` 生成的 ``distance(...)(vector, q)`` 形态。
        """
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
        """Generate the SQL for a query.

        中文：与 ``query`` 语义相同，但把向量内联为 ``[v1,v2,...]`` 字面量
        而不做参数绑定，便于外部执行或记录；距离映射与排序方向同 ``query``。
        """
        dist_func = "L2Distance" if distance == "l2" else "cosineDistance"
        sort_dir = "ASC" if distance == "l2" else "DESC"
        vec_str = "[" + ",".join(str(v) for v in vector) + "]"
        return (
            f"SELECT id, {dist_func}(vector, {vec_str}) AS dis "
            f"FROM {table} ORDER BY dis {sort_dir} LIMIT {top_k}"
        )