"""驱动式客户端的“建表 / 建索引”阶段基类。

一次实验里 ``ClientFactory`` 为每个引擎实例化一个具体的 ``*Configurator``
（位于 ``engine/clients/<engine>/configure.py``），并调用 ``configure()``：
先 ``clean()`` 清掉上一次的表，再 ``recreate()`` 按数据集维度、距离度量和
``collection_params``（索引参数）重建表与索引。

子类必须实现 ``clean`` 与 ``recreate``；``DISTANCE_MAPPING`` 用于把统一的
``Distance`` 名称翻译成该引擎自己的距离标识（如 pgvector 的 ``vector_l2_ops``）。
"""

from typing import Optional


class BaseConfigurator:
    """建表/建索引阶段的抽象接口。

    子类通常还会在 ``__init__`` 里建立数据库连接或保存连接参数。
    """

    # 统一距离名 -> 引擎内部距离标识。子类覆盖，例如
    # {"l2": "vector_l2_ops", "cosine": "vector_cosine_ops"}。
    DISTANCE_MAPPING = {}

    def __init__(self, host, collection_params: dict, connection_params: dict):
        """只保存参数，不建立连接（子类在 ``init_client``/``__init__`` 里再建）。

        之所以不在基类建连：不同引擎的连接对象类型完全不同（psycopg2 连接、
        clickhouse_driver Client、clickhouse_connect Client），基类无法统一。

        host 是 "host:port" 形式的字符串（由 ClientFactory 拼接）；
        ``collection_params`` 是配置里的建索引参数（m、ef_construction 等）；
        ``connection_params`` 是用户名、密码、库名等连接级参数。
        """
        self.host = host
        self.collection_params = collection_params
        self.connection_params = connection_params

    def clean(self):
        """删除旧表/旧索引，保证 ``recreate`` 从干净状态开始。子类实现。"""
        raise NotImplementedError()

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name, extra_columns_type):
        """按参数重建表与索引。子类实现。

        ``extra_columns_name`` / ``extra_columns_type`` 是数据集附带的标量列
        （如标题、时间戳），建表时需要一并声明。
        """
        raise NotImplementedError()

    def configure(self, distance, vector_size, extra_columns_name, extra_columns_type) -> Optional[dict]:
        """对外入口：清理旧对象后重建表/索引，返回本次实验的元数据。

        子类 ``recreate`` 返回 ``None`` 时统一归一成空字典，调用方无需判空。
        """
        self.clean()
        return self.recreate(distance, vector_size, self.collection_params, self.connection_params, extra_columns_name, extra_columns_type) or {}

    def execution_params(self, distance, vector_size) -> dict:
        """返回与执行阶段相关的参数（如查询时的 ef 搜索参数）。

        默认无额外参数；需要按距离/维度调整查询参数的引擎覆盖此方法。
        """
        return {}
