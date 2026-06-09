import re
import random
import string

from engine.base_client.distances import Distance

CLICKHOUSE_DATABASE_NAME = "Benchmark"
CLICKHOUSE_DEFAULT_PORT = "8123"
CLICKHOUSE_DEFAULT_USER = "default"
CLICKHOUSE_DEFAULT_PASSWD = ""

# ClickHouse 26.6.1.1 向量距离函数映射
DISTANCE_MAPPING = {
    Distance.L2: "L2",
    Distance.DOT: "dotProduct",
    Distance.COSINE: "cosine"  # cosine problem, when normalized, IP=COSINE
}

H5_COLUMN_TYPES_MAPPING = {
    "float64": "Float64",
    "float32": "Float32",
    "float": "Float64",
    "int32": "Int32",
    "int": "Int32",
    "integer": "Int32",
    "text": "Nullable(String)",  # some text can be null
    "string": "String",
    "blob": "String",
    "geo": "Tuple(Float64, Float64)",  # geo use Point to store, Point == Tuple(Float64, Float64)
    "keyword": "LowCardinality(String)",  # TODO handle ann-filter payload is null
    "boolean": "Boolean",
}


def convert_H52ClickHouseType(h5_column_type: str):
    clickhouse_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if clickhouse_type is None:
        raise RuntimeError(f"clickhouse doesn't support h5 column type: {h5_column_type}")
    return clickhouse_type


def get_random_string(length: int):
    random_list = []
    for i in range(length):
        random_list.append(random.choice(string.ascii_uppercase + string.digits))
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
