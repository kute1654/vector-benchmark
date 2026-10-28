"""ClickHouse 客户端的公共配置与类型映射。

本模块只放“纯数据 + 无状态工具函数”，被 configure / upload / search 三个模块共享，
刻意不建立任何连接，也不依赖 clickhouse 驱动，方便在子进程（fork/池）里安全导入。

连接协议有两套（由各模块的 ``protocol`` 参数选择）：
  - 原生 TCP 协议：默认端口 9000，使用 ``clickhouse_driver.Client``，吞吐高，适合批量写入；
  - HTTP 协议：默认端口 8123，使用 ``clickhouse_connect.get_client``。
两条路径返回结果的形态不同（行元组 vs ResultRows），所以调用方都要按协议分支处理。
"""

import re
import random
import string

from engine.base_client.distances import Distance

# 未显式指定表名时的默认表名。注意名字虽叫 DATABASE_NAME，实际用作 table 的缺省值
# （真实数据库固定为 ``default``，见各模块 client 的 database 参数）。
CLICKHOUSE_DATABASE_NAME = "Benchmark"
# HTTP 接口的默认端口。仅作为文档性常量保留；各模块实际按协议硬编码 9000/8123 缺省值。
CLICKHOUSE_DEFAULT_PORT = "8123"
CLICKHOUSE_DEFAULT_USER = "default"
CLICKHOUSE_DEFAULT_PASSWD = ""

# ClickHouse 26.6.1.1 向量距离函数映射
# 用于 vector_similarity 索引创建和搜索函数
# benchmark 侧统一使用 Distance 枚举（L2/DOT/COSINE），这里翻译成 ClickHouse 的标量函数名：
#   - L2Distance      ：欧氏距离，越小越相似；
#   - cosineDistance  ：余弦距离（1 - cos），越小越相似；
#   - dotProduct      ：点积，越大越相似（search 侧因此要对结果做 DESC 排序）。
DISTANCE_MAPPING = {
    Distance.L2: "L2Distance",
    Distance.DOT: "dotProduct",
    Distance.COSINE: "cosineDistance"
}

# H5 数据集 schema 的列类型 -> ClickHouse 列类型。
# H5 文件里只保存逻辑类型（float32/int/text/geo/...），建表时必须显式翻译成
# ClickHouse 的物理类型，否则 CREATE TABLE 会因为不认识 h5 类型名而失败。
H5_COLUMN_TYPES_MAPPING = {
    "float64": "Float64",
    "float32": "Float32",      # 向量列固定用 Float32，匹配 Array(Float32) DDL
    "float": "Float64",
    "int32": "Int32",
    "int": "Int32",
    "integer": "Int32",
    "text": "Nullable(String)",  # some text can be null
    # 部分数据集 text 列存在空值，用 Nullable 承接，避免 INSERT 空值报错
    "string": "String",
    "blob": "String",          # 二进制/长字符串统一按 String 存
    "geo": "Tuple(Float64, Float64)",  # geo use Point to store, Point == Tuple(Float64, Float64)
    # 地理坐标以 Tuple(lon, lat) 存储，供 geoDistance() 过滤使用
    "keyword": "LowCardinality(String)",  # TODO handle ann-filter payload is null
    # 低基数列用 LowCardinality 压缩字典编码，等值/IN 过滤更快
    "boolean": "Boolean",
}


def convert_H52ClickHouseType(h5_column_type: str):
    """把 h5 逻辑列类型翻译成 ClickHouse 列类型；大小写不敏感。

    未知类型直接抛错而不是回退成 String，避免建表成功但数据被静默截断/错存。
    """
    clickhouse_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if clickhouse_type is None:
        raise RuntimeError(f"clickhouse doesn't support h5 column type: {h5_column_type}")
    return clickhouse_type


def get_random_string(length: int):
    """生成 ``length`` 位的大写字母+数字随机串（可用作表名/临时对象后缀）。"""
    random_list = []
    for i in range(length):
        random_list.append(random.choice(string.ascii_uppercase + string.digits))
    return ''.join(random_list)


# 表名会以字符串插值方式拼进 DDL / SELECT，所以必须先做白名单校验，防止 SQL 注入
# 或因为表名带反引号/空格导致语法错误。
_TABLE_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def validate_table_name(table_name: str) -> str:
    """校验表名只含字母/数字/下划线且不以数字开头，否则抛 ``RuntimeError``。"""
    if not isinstance(table_name, str) or not _TABLE_NAME_RE.fullmatch(table_name):
        raise RuntimeError(f"invalid table name: {table_name}")
    return table_name


def _to_int(value, default: int) -> int:
    """尽力把配置值转成 int；``None`` 或转换失败时回退到 ``default``。

    benchmark 的 connection/search 参数来自 JSON，数字可能是字符串；同时老配置里
    可能残留非法值。这里永不抛异常，保证超时类参数总能拿到一个可用值。
    """
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default
