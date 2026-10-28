"""PolarDB-pg（PASE 扩展）的建表配置器。

与 pgvector 的 Configurator 结构相同，但 DDL 完全不同：
* 先 ``CREATE EXTENSION IF NOT EXISTS pase``，向量类型由扩展提供；
* 向量列声明为 ``float4[]``，不带维度（维度只体现在索引 reloption）；
* DROP 带 ``CASCADE``，因为表上可能挂着 pase 索引/统计对象。

``recreate`` 结束后会关闭连接（``finally``），避免建表连接被继承到后续
fork 出来的搜索/上传进程里。
"""

import psycopg2

from benchmark.cli_output import compact_kv, sql as sql_log, step
from engine.base_client.configure import BaseConfigurator
from engine.base_client.distances import Distance
from engine.clients.polardb.config import (
    DISTANCE_MAPPING, POLARDB_DATABASE_NAME, POLARDB_DEFAULT_PASSWD,
    POLARDB_DEFAULT_PORT, POLARDB_DEFAULT_USER, convert_H52PostgreSQLType,
    validate_table_name,
)


class PolarDBConfigurator(BaseConfigurator):
    """PolarDB 建表配置器；连接状态挂在类属性上。"""

    connection = None
    table_name = None

    def __init__(self, host, collection_params, connection_params):
        """补齐 PolarDB-PASE 连接默认值后交给基类。

        默认端口取 5433：同机通常还跑着 pgvector（5432），把两者分开可以避免
        "连错库但表结构恰好也能建"的静默错误。用户显式配置优先。

        补齐连接默认值（端口 5433 与同机 pgvector 的 5432 区分开）。
        """
        defaults = {
            "host": "127.0.0.1", "port": POLARDB_DEFAULT_PORT,
            "user": POLARDB_DEFAULT_USER, "password": POLARDB_DEFAULT_PASSWD,
            "database": POLARDB_DATABASE_NAME, "table": "vec_items",
        }
        super().__init__(host, collection_params, {**defaults, **(connection_params or {})})

    @classmethod
    def init_client(cls, connection_params):
        """建立 psycopg2 连接并校验表名（表名会被拼进 DDL）。"""
        cls.connection = psycopg2.connect(
            host=connection_params.get("host", "127.0.0.1"),
            port=connection_params.get("port", POLARDB_DEFAULT_PORT),
            user=connection_params.get("user", POLARDB_DEFAULT_USER),
            password=connection_params.get("password", POLARDB_DEFAULT_PASSWD),
            database=connection_params.get("database", POLARDB_DATABASE_NAME),
        )
        cls.table_name = validate_table_name(connection_params.get("table", "vec_items"))

    @classmethod
    def command(cls, statement):
        """执行 SQL：无结果集时提交事务并返回 None，有结果集时返回所有行。

        执行完成后统一 ``commit()``，因此调用方不需要自己管理事务。
        """
        with cls.connection.cursor() as cursor:
            cursor.execute(statement)
            result = cursor.fetchall() if cursor.description else None
        cls.connection.commit()
        return result

    def clean(self):
        """空实现：清理由 ``sub_recreate`` 中的 DROP TABLE 完成。"""
        return None

    @classmethod
    def sub_recreate(cls, distance, vector_size, collection_params, extra_columns_name, extra_columns_type):
        """建扩展、删表、建表。

        ``vector_size <= 0`` 时不建向量列（纯标量/文本实验）。标量列类型走
        公共的 :func:`convert_H52PostgreSQLType`。
        """
        # PASE 扩展 (PolarDB 版 pgvector 对应物); DDL 与 pgvector 完全不同:
        #   * 向量列是 float4[] (pase 无 "带维度的类型", 维度只在索引 reloption 里)
        #   * 距离指标不写在列/类型上, 而在查询向量文本的 ds 段 + 索引 reloption 里
        create_ext = "CREATE EXTENSION IF NOT EXISTS pase;"
        cls.command(create_ext)

        columns = ["id INTEGER PRIMARY KEY"]
        if vector_size > 0:
            columns.append("vector float4[]")
        for name, typ in zip(extra_columns_name, extra_columns_type):
            columns.append(f"{name} {convert_H52PostgreSQLType(typ)}")
        # CASCADE：表上可能已挂着 pase 索引等依赖对象，普通 DROP 会失败。
        drop = f"DROP TABLE IF EXISTS {cls.table_name} CASCADE"
        create = f"CREATE TABLE {cls.table_name} ({', '.join(columns)})"
        sql_log(drop)
        cls.command(drop)
        sql_log(create)
        cls.command(create)
        step("recreate finished")

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name, extra_columns_type):
        """对外入口：打印配置摘要 → 建连 → 重建表 → 关闭连接。

        ``distance`` 在这里只用于日志（建表 DDL 不需要它，度量体现在索引
        与查询文本里）。连接在 ``finally`` 中关闭，避免资源泄漏。
        """
        compact_kv("configure", distance=distance, vector_size=vector_size, index_type=collection_params.get("index_type"))
        # 注意：这里用的是构造时缓存的 self.connection_params，而不是入参
        # connection_params；两者在正常调用路径上内容一致。
        self.__class__.init_client(self.connection_params)
        try:
            self.sub_recreate(distance, vector_size, collection_params, extra_columns_name, extra_columns_type)
        finally:
            self.connection.close()

    def execution_params(self, distance, vector_size):
        """余弦距离用"归一化 + L2"模拟，因此需要写入/查询两侧都归一化。

        PASE 没有原生 cosine 度量（只有 ds=0 L2、ds=1 内积），归一化之后
        L2 与余弦在排序上等价；若不归一化，结果是错的但不会报错，属于最难
        发现的一类失真。

        pase 不原生支持 cosine; 沿用 pgvector 套路: 上传+查询都归一化向量,
        之后 ds=0 的 L2 距离即余弦距离。
        """
        return {"normalize": distance == Distance.COSINE}
