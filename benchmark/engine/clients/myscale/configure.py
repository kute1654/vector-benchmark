"""MyScale 建表/清理阶段。

本模块负责 benchmark 生命周期中的 ``configure`` 步骤：

1. 根据连接参数创建客户端（原生 TCP 9000 或 HTTP 8123 两种协议）；
2. ``DROP TABLE IF EXISTS ... SYNC`` 后重建一张 ``MergeTree`` 表，
   写入 ``id``、``vector Array(Float32)`` 以及数据集附带的结构化列；
3. 注意此处**只建表，不建向量索引**。向量索引由 upload 阶段的
   ``post_upload`` 在数据落盘后通过 ``ALTER TABLE ... ADD VECTOR INDEX``
   创建（MyScale 需要先有数据才能训练 MSTG 等索引）。

所有 DDL 均通过 Python 字符串拼接后执行，表名/列名在进入 SQL 前需校验。
"""

import clickhouse_connect

from multiprocessing import get_context
from clickhouse_connect.driver.client import Client
from clickhouse_driver import Client as DriverClient
from benchmark.cli_output import compact_kv, sql, step
from engine.base_client.configure import BaseConfigurator
from engine.clients.myscale.config import *


class MyScaleConfigurator(BaseConfigurator):
    """MyScale 配置器：管理建表所需的类级连接与表名。

    类属性在 ``init_client`` 中赋值；``protocol`` 决定使用
    ``clickhouse_driver``（tcp）还是 ``clickhouse_connect``（http）。
    """

    client: Client = None
    table_name: str = None
    protocol: str = "tcp"

    def __init__(self, host, collection_params: dict, connection_params: dict):
        """只保存参数，不在父进程中建连。

        建连推迟到 ``recreate`` 子进程的 ``init_client``：fork 之前创建的
        socket 会被子进程继承，多个 worker 共用同一条连接会导致协议错乱。

        基类只保存 host/参数，真正的连接在 recreate 的子进程里通过
        ``init_client`` 建立，避免 fork 前创建连接。
        """
        super().__init__(host, collection_params, connection_params)

    @classmethod
    def init_client(cls, connection_params):
        """按 ``protocol`` 建立类级客户端并解析校验表名。

        * tcp：``clickhouse_driver.Client``，默认端口 9000，走原生协议；
        * 其它（http）：``clickhouse_connect.get_client``，默认端口 8123。
        两者都连接到 ``default`` 库，后续 SQL 中显式写 ``default.<table>``。
        """
        cls.protocol = connection_params.get("protocol", "tcp")
        if cls.protocol == "tcp":
            cls.client = DriverClient(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 9000),
                user=connection_params.get("user", MYSCALE_DEFAULT_USER),
                password=connection_params.get("password", MYSCALE_DEFAULT_PASSWD),
                database="default",
            )
        else:
            cls.client = clickhouse_connect.get_client(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 8123),
                username=connection_params.get("user", MYSCALE_DEFAULT_USER),
                password=connection_params.get("password", MYSCALE_DEFAULT_PASSWD),
                database="default",
            )
        cls.table_name = validate_table_name(connection_params.get("table", MYSCALE_DATABASE_NAME))

    @classmethod
    def command(cls, sql: str):
        """执行一条不取回结果集的 SQL（DDL/SET 等）。

        两种客户端 API 名称不同：原生驱动用 ``execute``，HTTP 用 ``command``。
        """
        if cls.protocol == "tcp":
            return cls.client.execute(sql)
        return cls.client.command(sql)

    def clean(self):
        """空实现：``recreate`` 里的 ``DROP TABLE`` 已经覆盖清理职责。

        清理动作实际由 recreate 中的 DROP TABLE 完成，这里保持空实现以
        满足 BaseConfigurator 接口。
        """
        pass

    @classmethod
    def sub_recreate(cls, distance, vector_size, collection_params, extra_columns_name, extra_columns_type):
        """在已初始化的客户端上执行删表 + 建表（在子进程中运行）。

        建表 DDL 形态：
        ``CREATE TABLE default.<table>(id UInt32, vector Array(Float32),
        <extra cols>, CONSTRAINT check_length CHECK length(vector) = N)
        ENGINE MergeTree ORDER BY id``。

        * ``vector_size == 0`` 表示纯文本检索数据集，不建 vector 列；
        * ``MergeTree`` 是 MyScale 的底层表引擎，向量索引（MSTG/MSRQ/
          MSTG_DISK）是建在 ``vector`` 列上的二级索引，由上传阶段添加；
        * ``ORDER BY id`` 让主键排序与写入顺序一致，方便按 id 读取/合并；
        * ``CONSTRAINT ... CHECK`` 用于在写入时就拒绝维度不符的向量。
        """
        columns = ["id UInt32"]
        constraint_clause = ""
        if vector_size > 0:
            columns.append("vector Array(Float32)")
            constraint_clause = f", CONSTRAINT check_length CHECK length(vector) = {vector_size}"

        # 把 H5 schema 的附加列逐个翻译成 MyScale 列定义。
        for col_index in range(0, len(extra_columns_name)):
            columns.append(
                f"{extra_columns_name[col_index]} {convert_H52ClickHouseType(extra_columns_type[col_index])}"
            )
        columns_str = ", ".join(columns)

        # 先同步删表：``sync`` 保证 DROP 返回时本地/副本上的数据已清理，
        # 否则紧接着的同名 CREATE 可能因表仍存在而失败。
        drop_table = f"DROP TABLE IF EXISTS default.{cls.table_name} sync"
        sql(drop_table)
        cls.command(drop_table)

        create_table = (
            f"create table default.{cls.table_name}("
            f"{columns_str}"
            f"{constraint_clause}"
            f") engine MergeTree order by id"
        )
        sql(create_table)
        cls.command(create_table)
        step("recreate finished")

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):
        """benchmark 的 ``configure`` 入口：打印配置并在子进程中重建表。

        通过 ``multiprocessing.Pool(processes=1, initializer=init_client)``
        把客户端初始化放在子进程里，避免父进程的连接对象在 fork 后被共享，
        也隔离了驱动可能持有的事件循环/线程状态。``pool.apply`` 是同步调用，
        因此 recreate 返回时建表已经完成。
        """
        if vector_size and vector_size > 0:
            compact_kv(
                "configure",
                distance=distance,
                vector_size=vector_size,
                index_type=collection_params.get("index_type"),
            )
        ctx = get_context(None)
        with ctx.Pool(
                processes=1,
                initializer=self.__class__.init_client,
                initargs=(connection_params,),
        ) as pool:
            pool.apply(func=self.sub_recreate,
                       args=(DISTANCE_MAPPING[distance], vector_size, collection_params, extra_columns_name, extra_columns_type,))

    def execution_params(self, distance, vector_size) -> dict:
        """返回执行阶段参数；``normalize`` 表示是否在客户端归一化查询向量。

        注意：``DISTANCE_MAPPING`` 把 COSINE 映射成 ``"Cosine"``，此处却与
        ``'COSINE'`` 比较，二者大小写不同，因此当前恒为 ``False``。
        """
        return {"normalize": DISTANCE_MAPPING[distance] == 'COSINE'}
