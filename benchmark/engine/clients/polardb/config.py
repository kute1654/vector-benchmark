import re

from engine.base_client.distances import Distance

POLARDB_DEFAULT_PORT = 5433
POLARDB_DEFAULT_USER = "postgres"
POLARDB_DEFAULT_PASSWD = "123456"
POLARDB_DATABASE_NAME = "postgres"

DISTANCE_MAPPING = {
    Distance.L2: "<?>",
    Distance.DOT: "<?>",
    Distance.COSINE: "<?>",
}

H5_COLUMN_TYPES_MAPPING = {
    "float64": "DOUBLE PRECISION",
    "float32": "REAL",
    "float": "DOUBLE PRECISION",
    "int32": "INTEGER",
    "int": "INTEGER",
    "integer": "INTEGER",
    "text": "TEXT",
    "string": "TEXT",
    "blob": "BYTEA",
    "geo": "POINT",
    "keyword": "TEXT",
    "boolean": "BOOLEAN",
}

_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_table_name(table_name: str) -> str:
    if not isinstance(table_name, str) or not _TABLE_NAME_RE.fullmatch(table_name):
        raise RuntimeError(f"invalid table name: {table_name}")
    return table_name


def convert_H52PostgreSQLType(h5_column_type: str) -> str:
    value = H5_COLUMN_TYPES_MAPPING.get(str(h5_column_type).lower())
    if value is None:
        raise RuntimeError(f"polardb doesn't support h5 column type: {h5_column_type}")
    return value
