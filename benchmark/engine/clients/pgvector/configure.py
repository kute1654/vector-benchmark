"""pgvector 的建表/清表配置器（driver 式抽象）。

职责：建立到 PostgreSQL 的连接，重建 benchmark 用表
（``id INTEGER PRIMARY KEY`` + 可选 ``vector`` 列 + 若干标量列）。
索引在 upload 阶段的 ``post_upload`` 里创建，不在这里，因为建索引需要
先有数据（HNSW/IVFFlat 的构建成本与数据量相关）。

这里所有 DDL 都通过字符串拼接表名列名（PostgreSQL 不支持用绑定参数传
标识符），所以表名必须先过 :func:`validate_table_name` 校验。
"""

import psycopg2
from psycopg2 import sql
import signal
import time

from benchmark.cli_output import compact_kv, sql as sql_log, step
from engine.base_client.configure import BaseConfigurator
from engine.clients.pgvector.config import *


class PGVectorConfigurator(BaseConfigurator):
    """pgvector 建表配置器；``connection`` 是类属性，便于子进程共享同一套逻辑。"""

    connection = None
    table_name: str = None

    def __init__(self, host, collection_params: dict, connection_params: dict):
        """补齐 pgvector 连接默认值后再交给基类。

        默认值只填补缺失键，用户配置优先，因此最小配置（只写 port 之类）也可以
        直接跑通。

        ``connection_params`` 来自配置文件，这里补齐缺省值（端口、账号、库名、
        表名），使最小配置也能直接跑通。
        """
        # Set default connection parameters for PGVector
        default_conn_params = {
            "host": "127.0.0.1",
            "port": PGVECTOR_DEFAULT_PORT,
            "user": PGVECTOR_DEFAULT_USER,
            "password": PGVECTOR_DEFAULT_PASSWD,
            "database": PGVECTOR_DATABASE_NAME,
            "table": "vec_items"
        }

        # Merge provided connection_params with defaults
        # 用户显式提供的值优先覆盖默认值。
        merged_conn_params = {**default_conn_params, **(connection_params or {})}

        super().__init__(host, collection_params, merged_conn_params)

    @classmethod
    def init_client(cls, connection_params):
        """建立连接并设置会话级超时；同时校验表名。

        连接挂在类属性上，方便同一进程内复用；``statement_timeout`` 设为 30s
        是为了避免 DDL/DROP 因为锁等待无限挂起。
        """
        cls.connection = psycopg2.connect(
            host=connection_params.get("host", "127.0.0.1"),
            port=connection_params.get("port", PGVECTOR_DEFAULT_PORT),
            user=connection_params.get("user", PGVECTOR_DEFAULT_USER),
            password=connection_params.get("password", PGVECTOR_DEFAULT_PASSWD),
            database=connection_params.get("database", PGVECTOR_DATABASE_NAME),
        )
        # Set statement timeout to 30 seconds to prevent hanging
        with cls.connection.cursor() as cursor:
            cursor.execute("SET statement_timeout = '30s'")
        cls.table_name = validate_table_name(connection_params.get("table", "vec_items"))

    @classmethod
    def command(cls, sql_query: str, timeout_seconds: int = 30):
        """Execute SQL command with timeout protection

        执行一条 SQL：有结果集则返回 ``fetchall()``，否则提交事务并返回 None。
        超时/事务已失败/其他异常都先 ``rollback()`` 再抛 ``RuntimeError``，
        避免连接停留在 aborted 事务里导致后续命令全部报
        ``InFailedSqlTransaction``。

        ``timeout_seconds`` 目前只用于错误信息展示，实际超时由会话级
        ``statement_timeout`` 控制。
        """
        try:
            with cls.connection.cursor() as cursor:
                cursor.execute(sql_query)
                if cursor.description:
                    return cursor.fetchall()
                cls.connection.commit()
                return None
        except psycopg2.errors.QueryCanceled as e:
            if "canceling statement due to statement timeout" in str(e):
                # Rollback the aborted transaction before raising the error
                cls.connection.rollback()
                raise RuntimeError(f"SQL command timed out after {timeout_seconds} seconds: {sql_query[:100]}...")
            else:
                # Rollback for other query canceled errors
                cls.connection.rollback()
                raise
        except psycopg2.errors.InFailedSqlTransaction as e:
            # This can happen if we try to execute after a previous failure
            # Rollback and retry the command
            # 中文：连接处于 aborted 状态时，回滚后重试一次；仍失败则包装成
            # RuntimeError 抛出（附 SQL 前 100 字符便于定位）。
            cls.connection.rollback()
            try:
                with cls.connection.cursor() as cursor:
                    cursor.execute(sql_query)
                    if cursor.description:
                        return cursor.fetchall()
                    cls.connection.commit()
                    return None
            except Exception as retry_e:
                raise RuntimeError(f"Failed to execute SQL command after transaction rollback: {sql_query[:100]}... Error: {retry_e}")
        except Exception as e:
            # For any other exception, rollback the transaction
            cls.connection.rollback()
            raise RuntimeError(f"Failed to execute SQL command: {sql_query[:100]}... Error: {e}")

    def clean(self):
        """空实现：清理工作由 ``sub_recreate`` 里的 DROP TABLE 完成。"""
        pass

    @classmethod
    def sub_recreate(cls, distance, vector_size, collection_params, extra_columns_name, extra_columns_type):
        """删表并重建：``id`` 主键 + 可选 ``vector`` 列 + 标量列。

        ``vector_size <= 0`` 表示本次实验没有向量列（纯文本/纯标量场景）。
        标量列类型由 :func:`convert_H52PostgreSQLType` 决定。

        DROP 使用 60s 超时并允许失败继续：表可能不存在，或被其他会话持有
        锁；此时 CREATE 会因 "already exists" 失败，但先删后建的正常路径不受
        影响。用户在 DROP 期间按 Ctrl+C 会回滚并向上抛出。
        """
        # Create table with id, vector, and extra columns
        columns = ["id INTEGER PRIMARY KEY"]

        if vector_size > 0:
            columns.append(f"vector vector({vector_size})")

        for col_index in range(0, len(extra_columns_name)):
            columns.append(
                f"{extra_columns_name[col_index]} {convert_H52PostgreSQLType(extra_columns_type[col_index])}"
            )

        columns_str = ", ".join(columns)

        # Drop table if exists
        drop_table = f"DROP TABLE IF EXISTS {cls.table_name}"
        sql_log(drop_table)
        step("Dropping existing table (if exists)...")
        step("⚠️  If this takes too long, press Ctrl+C to cancel")

        try:
            # Set a shorter timeout for DDL operations (default 60s)
            # 中文：SET LOCAL 只在当前事务内有效；这里单独开一个隐式事务，
            # 目的是让 DROP 有独立的 60s 上限，而不是沿用会话的 30s。
            with cls.connection.cursor() as cursor:
                cursor.execute("SET LOCAL statement_timeout = '60s'")

            cls.command(drop_table, timeout_seconds=60)
            step(f"✓ Table dropped successfully")
        except KeyboardInterrupt:
            step("\n✗ DROP TABLE cancelled by user")
            cls.connection.rollback()
            raise
        except RuntimeError as e:
            # 表不存在或删除超时都继续执行：后面的 CREATE 会给出明确结果。
            step(f"Warning: Failed to drop table {cls.table_name}: {e}")
            step("   Continuing anyway (table may not exist or is locked)")
            # The command method already handled the transaction rollback, so we can proceed

        # Create table
        create_table = f"CREATE TABLE {cls.table_name} ({columns_str})"
        sql_log(create_table)
        cls.command(create_table)
        step("recreate finished")

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name,
                 extra_columns_type):
        """对外入口：初始化连接后重建表。

        ``distance`` 会先映射成 SQL 运算符再传下去；虽然建表阶段用不到运算符，
        保留该参数是为了与 pgvector/polardb 的调用签名统一（PolarDB 建表时要
        用 PASE 专属类型）。
        """
        if vector_size and vector_size > 0:
            compact_kv(
                "configure",
                distance=distance,
                vector_size=vector_size,
                index_type=collection_params.get("index_type"),
            )

        # Initialize client and recreate table
        self.__class__.init_client(connection_params)
        self.sub_recreate(DISTANCE_MAPPING[distance], vector_size, collection_params, extra_columns_name, extra_columns_type)

    def execution_params(self, distance, vector_size) -> dict:
        """告诉写入/查询阶段是否需要先做 L2 归一化。

        pgvector 的余弦算子自己会归一，但 benchmark 统一在做归一化后再写入，
        这样 ground truth（按余弦相似度算的邻居）与数据库内计算出的距离可比。

        For cosine distance, vectors should be normalized
        中文：pgvector 的 ``<=>`` 计算的是 ``1 - cos`` 相似度，并不要求欧氏
        归一；但 benchmark 统一在写入前做 L2 归一化以保证与 ground truth 一致。
        """
        return {"normalize": distance == Distance.COSINE}
