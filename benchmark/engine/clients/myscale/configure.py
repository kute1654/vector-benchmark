import clickhouse_connect

from multiprocessing import get_context
from clickhouse_connect.driver.client import Client
from clickhouse_driver import Client as DriverClient
from benchmark.cli_output import compact_kv, sql, step
from engine.base_client.configure import BaseConfigurator
from engine.clients.myscale.config import *


class MyScaleConfigurator(BaseConfigurator):
    client: Client = None
    table_name: str = None
    protocol: str = "tcp"

    def __init__(self, host, collection_params: dict, connection_params: dict):
        super().__init__(host, collection_params, connection_params)

    @classmethod
    def init_client(cls, connection_params):
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
        if cls.protocol == "tcp":
            return cls.client.execute(sql)
        return cls.client.command(sql)

    def clean(self):
        pass

    @classmethod
    def sub_recreate(cls, distance, vector_size, collection_params, extra_columns_name, extra_columns_type):
        columns = ["id UInt32"]
        constraint_clause = ""
        if vector_size > 0:
            columns.append("vector Array(Float32)")
            constraint_clause = f", CONSTRAINT check_length CHECK length(vector) = {vector_size}"

        for col_index in range(0, len(extra_columns_name)):
            columns.append(
                f"{extra_columns_name[col_index]} {convert_H52ClickHouseType(extra_columns_type[col_index])}"
            )
        columns_str = ", ".join(columns)

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
        return {"normalize": DISTANCE_MAPPING[distance] == 'COSINE'}
