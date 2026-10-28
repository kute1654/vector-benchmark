"""脚本式引擎层：直接暴露 connect/建表/查询/测 QPS 的轻量封装。

重要区分：本包（``benchmark.engine``）是“脚本式”实现，适合快速测量
QPS / 召回；正式实验走 ``run.py`` 使用的“驱动式”客户端
（``engine/base_client/*`` + ``engine/clients/<engine>/*``）。两套抽象
互不依赖，注释时不要混用。

本模块在导入时会把四个引擎类全部导入，因此任一引擎的模块级导入错误都会
让整个包不可用；实际连接数据库仍发生在实例方法中，导入本身无网络副作用。
"""

from .base import BaseEngine
from .pgvector import PgvectorEngine
from .polardb import PolarDBEngine
from .clickhouse import ClickHouseEngine
from .myscale import MyScaleEngine

__all__ = [
    "BaseEngine",
    "PgvectorEngine",
    "PolarDBEngine",
    "ClickHouseEngine",
    "MyScaleEngine",
]

# 引擎名 -> 实现类。键与配置文件中 ``engine`` 字段的取值一致。
ENGINE_REGISTRY = {
    "pgvector": PgvectorEngine,
    "polardb": PolarDBEngine,
    "clickhouse": ClickHouseEngine,
    "myscale": MyScaleEngine,
}


def get_engine(name, **kwargs):
    """按名称实例化脚本式引擎。

    ``kwargs`` 原样传给引擎构造函数（host/port/user/password/database 等）。
    未知名称立即抛 ``ValueError`` 并列出可用引擎，避免拼写错误被静默忽略。
    构造过程不建连接，``connect()`` 由调用方按需触发。
    """
    cls = ENGINE_REGISTRY.get(name)
    if cls is None:
        raise ValueError(f"Unknown engine: {name}. Available: {list(ENGINE_REGISTRY)}")
    return cls(**kwargs)