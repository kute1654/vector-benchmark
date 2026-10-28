"""pgvector 客户端的常量、类型映射与配置校验小工具。

集中放置连接默认值、距离运算符映射、以及若干 GUC 名称。这些 GUC 由
pgvector 服务端（src/vector.c 等）定义，客户端只负责按下发名字 SET；
名称一旦与服务端不一致会被当成 custom GUC 静默接受而不生效，因此这里
集中定义，供 configure/search/upload 三处共用。
"""

import re
import random
import string

from engine.base_client.distances import Distance

# 连接默认值；仅在 connection_params 未提供对应字段时兜底。
PGVECTOR_DEFAULT_HOST = "127.0.0.1"
PGVECTOR_DEFAULT_PORT = 5432
PGVECTOR_DEFAULT_USER = "postgres"
PGVECTOR_DEFAULT_PASSWD = "123456"
PGVECTOR_DATABASE_NAME = "postgres"

# PGvector uses different distance operators:
# - L2 (Euclidean): <->
# - Inner Product: <#>
# - Cosine: <=>
# 中文：距离度量 → SQL 运算符。建索引/查询必须与索引 opclass 匹配，否则
# 查询会退化为顺序扫描。
DISTANCE_MAPPING = {
    Distance.L2: "<->",
    Distance.DOT: "<#>",
    Distance.COSINE: "<=>"
}

# 查询结果缓存 GUC (与 bash-test/pgvector-benchmark.sh 对齐:
#   SET pgvector.result_cache_debug = on/off, 并同步主开关 pgvector.result_cache)
# 中文：result_cache 是结果级缓存（命中直接复用查询结果）；debug 开关用于在
# 服务端日志里输出命/未命中的细节，便于核对缓存是否真的生效。
PGVECTOR_RESULT_CACHE_GUC = "pgvector.result_cache"
PGVECTOR_RESULT_CACHE_DEBUG_GUC = "pgvector.result_cache_debug"

# 向量文本输入的十进制->float 解析器 GUC (运行时切换, 与 src/vector.c 的
# DefineCustomEnumVariable("vector.in_parse_mode") 对齐):
#   strtof       -> libc strtof
#   inline       -> 手写 inline 解析器
#   fast_float   -> fast_float_parse() (Eisel-Lemire)
# 中文：该参数只影响"[1,2,3]"文本形式参数解析向量时的 CPU 开销，不影响二进制
# 参数（typedbinary）路径。
PGVECTOR_IN_PARSE_MODE_GUC = "vector.in_parse_mode"

# HDF5 / json 中声明的列类型 → PostgreSQL 列类型。
# 注意 float64 与 float 都映射到 DOUBLE PRECISION，而 pgvector 的 vector 列
# 固定使用 float4 存储，二者不要混淆。
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
    """把 h5/数据集里的列类型名转换成 PostgreSQL 类型名。

    类型名大小写不敏感；未知类型直接抛错，而不是默默建一个 TEXT 列——那样
    会让标量过滤的值语义出错却难以发现。
    """
    pg_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if pg_type is None:
        raise RuntimeError(f"pgvector doesn't support h5 column type: {h5_column_type}")
    return pg_type


def get_random_string(length: int):
    """生成指定长度的随机小写字母+数字串（用于临时表名/后缀）。

    这里用 random 而非 secrets：仅用于避免命名冲突，不涉及安全语义。
    """
    random_list = []
    for i in range(length):
        random_list.append(random.choice(string.ascii_lowercase + string.digits))
    return ''.join(random_list)


# 表名白名单：字母/下划线开头，后续只允许字母数字下划线。表名会直接拼进
# DDL 字符串（无法用占位符），因此必须在入口处挡住引号/分号等注入字符。
_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_table_name(table_name: str) -> str:
    """校验表名合法性；不合法直接抛 ``RuntimeError``，合法则原样返回。"""
    if not isinstance(table_name, str) or not _TABLE_NAME_RE.fullmatch(table_name):
        raise RuntimeError(f"invalid table name: {table_name}")
    return table_name


def _to_int(value, default: int) -> int:
    """尽力把配置值转成 int，失败（None/非数字）时返回 ``default``。"""
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default
