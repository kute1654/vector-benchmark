"""MyScale 客户端共享配置。

该模块只存放常量与纯函数，被 configure/upload/search 三个子模块通过
``from engine.clients.myscale.config import *`` 复用，内容分为四类：

* 连接默认值（数据库名、默认端口、默认用户/密码）；
* 距离度量映射：benchmark 的 :class:`Distance` -> MyScale SQL 中的度量名；
* H5 数据集列类型 -> ClickHouse/MyScale 列类型映射；
* 少量字符串/表名/整数解析辅助函数。

注意：所有客户端计时（上传、索引构建等）均由调用方用
``time.perf_counter()`` 计算，单位是**秒**，本模块不参与计时。
"""

import re
import random
import string

from engine.base_client.distances import Distance

# 默认建库/建表时使用的库名，同时也是未显式配置 table 参数时的回退表名。
MYSCALE_DATABASE_NAME = "Benchmark"
# HTTP 协议(clickhouse_connect)默认端口；TCP 原生协议端口(9000)在调用处硬编码。
MYSCALE_DEFAULT_PORT = "8123"
MYSCALE_DEFAULT_USER = "default"
MYSCALE_DEFAULT_PASSWD = ""

# 距离度量映射：key 是 benchmark 内部枚举，value 是 MyScale 索引/函数接受的度量名。
# 注意 COSINE 映射为 "Cosine"（首字母大写），而 execution_params 中按 "COSINE"
# 比较，二者字符串并不相等，阅读相关逻辑时需留意这一大小写差异。
DISTANCE_MAPPING = {
    Distance.L2: "L2",
    Distance.DOT: "IP",
    Distance.COSINE: "Cosine"  # cosine problem, when normalized, IP=COSINE
}

# H5 数据集 schema 中的逻辑类型 -> MyScale 实际列类型。
# 其中：
# * ``text`` 使用 ``Nullable(String)``，因为部分数据集的文本字段允许缺失；
# * ``keyword`` 使用 ``LowCardinality(String)``，低基数字典编码能显著压缩过滤列；
# * ``geo`` 存成 ``Tuple(Float64, Float64)``，MyScale 的 Point 即该元组类型，
#   下游 ``geoDistance`` 通过 ``.1/.2`` 访问经度/纬度分量；
# * ``blob`` 直接映射为 ``String``（地址无关、按原始字节写入）。
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
    """把 H5 schema 类型名转换成 MyScale 列类型。

    查找前统一转小写，因此 ``Float32``/``float32`` 等价；未知类型直接抛
    ``RuntimeError``，避免建表时悄悄产生错误的列类型。
    """
    myscale_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if myscale_type is None:
        raise RuntimeError(f"myscale doesn't support h5 column type: {h5_column_type}")
    return myscale_type


def get_random_string(length: int):
    """生成 ``length`` 位大写字母+数字的随机串。

    用于给 FTS/向量索引生成唯一后缀（索引名形如 ``fts_col_AB12``），
    避免同一张表上重复建索引时名称冲突。
    """
    random_list = []
    for i in range(length):
        random_list.append(random.choice(string.ascii_uppercase + string.digits))
    return ''.join(random_list)


# 表名白名单：字母/下划线开头，后续只允许字母、数字、下划线。
# 表名会被直接拼进 SQL（DDL/查询都未使用参数化），因此必须先做严格校验，
# 防止配置中的表名注入额外 SQL 语句。
_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_table_name(table_name: str) -> str:
    """校验表名合法性，不合法时抛 ``RuntimeError``，合法则原样返回。"""
    if not isinstance(table_name, str) or not _TABLE_NAME_RE.fullmatch(table_name):
        raise RuntimeError(f"invalid table name: {table_name}")
    return table_name


def _to_int(value, default: int) -> int:
    """宽松的整数解析：``None`` 或无法转换时返回 ``default``。

    连接参数来自 JSON/CLI，可能是字符串、空值或非法值；这里统一兜底，
    避免超时/线程数等配置写错时直接让 client 初始化崩溃。
    """
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default
