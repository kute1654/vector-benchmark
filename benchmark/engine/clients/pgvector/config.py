import re
import random
import string

from engine.base_client.distances import Distance

PGVECTOR_DEFAULT_HOST = "127.0.0.1"
PGVECTOR_DEFAULT_PORT = 5432
PGVECTOR_DEFAULT_USER = "postgres"
PGVECTOR_DEFAULT_PASSWD = "123456"
PGVECTOR_DATABASE_NAME = "postgres"

# PGvector uses different distance operators:
# - L2 (Euclidean): <-> 
# - Inner Product: <#> 
# - Cosine: <=>
DISTANCE_MAPPING = {
    Distance.L2: "<->",
    Distance.DOT: "<#>",
    Distance.COSINE: "<=>"
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
    "geo": "POINT",  # PostgreSQL POINT type for geo coordinates
    "keyword": "TEXT",
    "boolean": "BOOLEAN",
}


def convert_H52PostgreSQLType(h5_column_type: str):
    pg_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if pg_type is None:
        raise RuntimeError(f"pgvector doesn't support h5 column type: {h5_column_type}")
    return pg_type


def get_random_string(length: int):
    random_list = []
    for i in range(length):
        random_list.append(random.choice(string.ascii_lowercase + string.digits))
    return ''.join(random_list)


_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_table_name(table_name: str) -> str:
    if not isinstance(table_name, str) or not _TABLE_NAME_RE.fullmatch(table_name):
        raise RuntimeError(f"invalid table name: {table_name}")
    return table_name


def _to_int(value, default: int) -> int:
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default