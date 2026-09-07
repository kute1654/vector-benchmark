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

ENGINE_REGISTRY = {
    "pgvector": PgvectorEngine,
    "polardb": PolarDBEngine,
    "clickhouse": ClickHouseEngine,
    "myscale": MyScaleEngine,
}


def get_engine(name, **kwargs):
    cls = ENGINE_REGISTRY.get(name)
    if cls is None:
        raise ValueError(f"Unknown engine: {name}. Available: {list(ENGINE_REGISTRY)}")
    return cls(**kwargs)