import psycopg2

from benchmark.cli_output import compact_kv, sql as sql_log, step
from engine.base_client.configure import BaseConfigurator
from engine.clients.polardb.config import (
    DISTANCE_MAPPING, POLARDB_DATABASE_NAME, POLARDB_DEFAULT_PASSWD,
    POLARDB_DEFAULT_PORT, POLARDB_DEFAULT_USER, convert_H52PostgreSQLType,
    validate_table_name,
)


class PolarDBConfigurator(BaseConfigurator):
    connection = None
    table_name = None

    def __init__(self, host, collection_params, connection_params):
        defaults = {
            "host": "127.0.0.1", "port": POLARDB_DEFAULT_PORT,
            "user": POLARDB_DEFAULT_USER, "password": POLARDB_DEFAULT_PASSWD,
            "database": POLARDB_DATABASE_NAME, "table": "vec_items",
        }
        super().__init__(host, collection_params, {**defaults, **(connection_params or {})})

    @classmethod
    def init_client(cls, connection_params):
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
        with cls.connection.cursor() as cursor:
            cursor.execute(statement)
            result = cursor.fetchall() if cursor.description else None
        cls.connection.commit()
        return result

    def clean(self):
        return None

    @classmethod
    def sub_recreate(cls, distance, vector_size, collection_params, extra_columns_name, extra_columns_type):
        columns = ["id INTEGER PRIMARY KEY"]
        if vector_size > 0:
            columns.append("vector REAL[]")
        for name, typ in zip(extra_columns_name, extra_columns_type):
            columns.append(f"{name} {convert_H52PostgreSQLType(typ)}")
        drop = f"DROP TABLE IF EXISTS {cls.table_name} CASCADE"
        create = f"CREATE TABLE {cls.table_name} ({', '.join(columns)})"
        sql_log(drop)
        cls.command(drop)
        sql_log(create)
        cls.command(create)
        step("recreate finished")

    def recreate(self, distance, vector_size, collection_params, connection_params, extra_columns_name, extra_columns_type):
        compact_kv("configure", distance=distance, vector_size=vector_size, index_type=collection_params.get("index_type"))
        self.__class__.init_client(self.connection_params)
        try:
            self.sub_recreate(DISTANCE_MAPPING[distance], vector_size, collection_params, extra_columns_name, extra_columns_type)
        finally:
            self.connection.close()

    def execution_params(self, distance, vector_size):
        return {"normalize": False}
