"""ClickHouse 建表阶段（configure）实现。

职责：在开始上传前把目标表“重建成干净状态”，即先 ``DROP TABLE`` 再 ``CREATE TABLE``。
建表语句只包含 ``id``、向量列与数据集附加列，向量索引留到 upload 阶段（``post_upload``）
再通过 ``ALTER TABLE ... ADD INDEX`` + ``MATERIALIZE INDEX`` 构建 —— 先写数、后建索引
比边写边维护索引快得多。

连接有两条路径（``protocol`` 参数选择）：
  - ``"tcp"``：原生 TCP 协议，默认端口 9000，驱动 ``clickhouse_driver.Client``；
  - 其它（如 ``"http"``）：HTTP 协议，默认端口 8123，库 ``clickhouse_connect``。
数据库固定为 ``default``，表名可由 ``connection_params["table"]`` 覆盖。
"""

import clickhouse_connect

from multiprocessing import get_context
from clickhouse_connect.driver.client import Client
from clickhouse_driver import Client as DriverClient
from benchmark.cli_output import compact_kv, sql, step
from engine.base_client.configure import BaseConfigurator
from engine.clients.clickhouse.config import *


class ClickHouseConfigurator(BaseConfigurator):
    """建表器；用类属性保存连接，便于在子进程 initializer 中重新初始化。"""

    client: Client = None
    table_name: str = None
    # "tcp" => 原生 9000 端口；其它值 => HTTP 8123 端口
    protocol: str = "tcp"

    def __init__(self, host, collection_params: dict, connection_params: dict):
        """仅转存参数，真正的连接建立推迟到 ``init_client`` / ``recreate`` 时进行。

        原因同其它引擎：进程池 fork 之后每个 worker 需要自己的连接，父进程
        提前建连会变成共享 socket。
        """
        super().__init__(host, collection_params, connection_params)

    @classmethod
    def init_client(cls, connection_params):
        """按 ``protocol`` 建立连接并解析目标表名。

        这里显式写死 ``database="default"``：benchmark 的 Table 语义由表名承载，
        不额外创建 database，简化清理与实验隔离。
        """
        cls.protocol = connection_params.get("protocol", "tcp")
        if cls.protocol == "tcp":
            # 原生协议：默认 9000 端口，驱动参数名为 user/password
            cls.client = DriverClient(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 9000),
                user=connection_params.get("user", CLICKHOUSE_DEFAULT_USER),
                password=connection_params.get("password", CLICKHOUSE_DEFAULT_PASSWD),
                database="default",
            )
        else:
            cls.client = clickhouse_connect.get_client(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 8123),
                username=connection_params.get("user", CLICKHOUSE_DEFAULT_USER),
                password=connection_params.get("password", CLICKHOUSE_DEFAULT_PASSWD),
                database="default",
            )
        cls.table_name = validate_table_name(connection_params.get("table", CLICKHOUSE_DATABASE_NAME))

    @classmethod
    def command(cls, sql: str):
        """执行 DDL/无结果集的语句；两种协议的调用方法名不同。"""
        if cls.protocol == "tcp":
            # clickhouse_driver：execute 同时可用于 DDL 与 SELECT
            return cls.client.execute(sql)
        # clickhouse_connect：command 专用于无结果集语句（DDL/SET 等）
        return cls.client.command(sql)

    def clean(self):
        """空实现：``recreate`` 的 ``DROP TABLE IF EXISTS`` 已承担清理职责。

        生命周期钩子：重建表已经承担清理职责（DROP + CREATE），这里无需额外动作。
        """
        pass

    @classmethod
    def sub_recreate(cls, distance, vector_size, collection_params, extra_columns_name, extra_columns_type):
        """重建表结构：``DROP TABLE IF EXISTS`` 后按数据集 schema 建新表。

        列顺序固定为 ``id`` -> ``vector`` -> 数据集附加列，与 upload 阶段
        ``INSERT ... (列名列表) VALUES`` 的列序保持一致（显式列名可避免位置错位）。
        ``distance`` / ``collection_params`` 目前未参与 DDL：向量索引类型在 upload 阶段
        才确定并单独创建。
        """
        # 主键 id 用 UInt32：与 benchmark 的整型 id 对应，也让 MergeTree 的排序键足够紧凑
        columns = ["id UInt32"]
        constraint_clause = ""
        if vector_size > 0:
            # 向量列统一 Array(Float32)；用 CHECK 约束在插入时校验维度，防止脏数据进入索引
            columns.append("vector Array(Float32)")
            constraint_clause = f", CONSTRAINT check_length CHECK length(vector) = {vector_size}"

        # 数据集附加列（用于标量过滤），类型由 h5 逻辑类型映射得到
        for col_index in range(0, len(extra_columns_name)):
            columns.append(
                f"{extra_columns_name[col_index]} {convert_H52ClickHouseType(extra_columns_type[col_index])}"
            )
        columns_str = ", ".join(columns)

        # ``sync`` 让 DROP 等待副本同步完成；对普通 MergeTree 无副作用，
        # 但对 Replicated 表能保证后续 CREATE 不会撞名。
        drop_table = f"DROP TABLE IF EXISTS default.{cls.table_name} sync"
        sql(drop_table)
        cls.command(drop_table)

        # 使用 MergeTree，排序键取 id。此处不建向量索引：先批量灌数，再统一
        # OPTIMIZE + ADD INDEX + MATERIALIZE INDEX，构建速度与查询性能都更好。
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
        """对外入口：打印配置摘要，并在独立子进程中执行实际建表。

        使用单进程 ``Pool`` + ``initializer=init_client`` 的目的：连接必须由子进程
        自己创建。父进程若先建连接再 fork，驱动内部的 socket/线程状态会被继承，
        容易在重连或析构时出错；initializer 在子进程启动后重新连库即可规避。
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
        """告知上层发送前是否需要对向量做归一化。

        注意：这里比较的是**映射后的函数名**（``"cosineDistance"``）与 ``"COSINE"``，
        两者不会相等，因此当前实现下该标志恒为 ``False``；余弦距离所需归一化
        实际依赖数据集本身已归一化。此处只说明现状，未改动逻辑。
        """
        return {"normalize": DISTANCE_MAPPING[distance] == 'COSINE'}
