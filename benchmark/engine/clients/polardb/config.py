import re

from engine.base_client.distances import Distance

POLARDB_DEFAULT_PORT = 5433
POLARDB_DEFAULT_USER = "postgres"
POLARDB_DEFAULT_PASSWD = "123456"
POLARDB_DATABASE_NAME = "postgres"

# pase 在查询向量文本里编码的 "distance space" 参数 (ds):
#   ds=0 -> L2/欧氏, ds=1 -> 内积 (inner product, 返回原始内积值, 越大越相似)
# cosine 数据集沿用 pgvector 的套路: 上传/查询前把向量归一化, 再以 L2 (ds=0) 排序,
#   L2(单位向量) = 余弦距离, 这样 pase 的 hnsw/ivfflat 索引在两套指标下都可用。
# PASE_MAX_DIM=512 (type/pase_data.h), 更大的维度建不了 pase 索引, 由 upload/post_upload 拦截。
DISTANCE_MAPPING = {
    Distance.L2: 0,
    Distance.DOT: 1,
    Distance.COSINE: 0,
}

PASE_MAX_DIM = 512

# 查询结果缓存 GUC (与 bash-test/polardb-pase-query-forms-benchmark.sh 对齐:
#   SET pase_result_cache = on/off, 并按需 SET pase_result_cache_debug = on/off)
# 结果缓存是 PolarDB pase 扩展在源码层实现的功能 (对齐 pgvector 的 veccache),
# 引擎侧只负责开关会话级 GUC, 不在 python 侧用 dict 模拟缓存。
# 注: 这些 GUC 不再使用 "pase." 限定前缀, 而注册为普通 GUC 名。
PASE_RESULT_CACHE_GUC = "pase_result_cache"
PASE_RESULT_CACHE_DEBUG_GUC = "pase_result_cache_debug"

# pase_in() 文本向量的十进制->float 解析器 GUC (运行时切换, 对齐 pgvector 的
# vector.in_parse_mode, 源码见 external/pase/type/pase_type.c pase_parse_float):
#   strtof       -> libc strtof
#   inline       -> 手写 inline 解析器
#   fast_float   -> fast_float_parse() (Eisel-Lemire, 与 pgvector 同源移植)
# 由 pase_handler.c _PG_init 注册; LOAD '$libdir/pase' 后会话内可 SET。
PASE_IN_PARSE_MODE_GUC = "pase_in_parse_mode"

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


def distance_ds(distance: Distance) -> int:
    """pase ds (distance space) for a dataset metric."""
    return DISTANCE_MAPPING.get(distance, Distance.L2)


def sort_dir_for(distance: Distance) -> str:
    """ORDER BY direction for `<?>` output.

    ds=0 (L2 / cosine-via-normalized-L2) returns a real distance: smaller is
    better -> ASC.  ds=1 returns the raw inner product: larger is better -> DESC.
    """
    return "DESC" if distance == Distance.DOT else "ASC"