#!/usr/bin/env python3
"""
Generate SQL benchmark files for pgvector/polardb/clickhouse bash-based QPS testing.

Supports all query literal forms for each engine, enabling apples-to-apples
comparison of which SQL form yields the highest QPS.

Usage (PGVector):
    python -m benchmark generate_sql_files --engine pgvector \
        --table benchmark_sift_128_1k --query-count 1000 --top-k 10

Usage (PolarDB):
    python -m benchmark generate_sql_files --engine polardb \
        --table benchmark_sift_1m --query-count 1000 --top-k 10

Usage (ClickHouse):
    python -m benchmark generate_sql_files --engine clickhouse \
        --table Benchmark_768_1m --query-count 1000 --top-k 10

Config-driven:
    python -m benchmark generate_sql_files \
        --config ../configurations/pgvector_sift128.json

中文说明
--------
职责：从目标表随机采样若干条查询向量，把该引擎已注册的每一种 SQL 形态渲染成"每行一条 SQL"
的文本文件，用于比较不同向量字面量写法在相同查询池下的 QPS。

关键入口：``main()``（命令行）。同时对外暴露采样器 ``sample_vectors_pg`` /
``sample_vectors_clickhouse`` / ``get_vector_dimension`` 与形态生成器
``generate_pgvector_sql_forms`` / ``generate_polardb_sql_forms`` /
``generate_clickhouse_sql_forms``，由 ``benchmark.query_forms`` 复用，从而保证
Python QPS 对比与这里写出的 .sql 文件使用完全相同的 SQL 形态和采样方式。

命令行用法::

    python -m benchmark generate_sql_files --engine pgvector --table T \\
        --query-count 1000 --top-k 10
    python -m benchmark generate_sql_files --config ../configurations/xxx.json

与 shell 脚本 / run.py 的分工：
  * ``run.py`` 负责建库、建表、建索引、导入数据与召回率；本模块不建表、不测 QPS。
  * ``bash-test/*.sh``（pgbench / clickhouse-benchmark）是独立的交叉校验路径，按各自的
    SQL_TYPES 注册表在运行时采样；本模块产出的文件与它们共享形态语义，默认写入
    ``bash-test/sql-bench/<引擎子目录>/``，可通过 shell 的 ``SQL_DIR`` 指向该目录复用。

输出与副作用：
  * 连接目标数据库执行只读采样查询（PG: ``ORDER BY random()``；CH: ``ORDER BY rand()``）。
  * 创建输出目录；同名文件会被覆盖；每个形态一个文件，命名见 ``main()``。
  * 文件名编码了引擎、表名、形态名与查询条数，例如
    ``pgvector_benchmark_sift_128_1k_text_literal_1000.sql``。
"""

import argparse
import json
import sys
import random
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Supported query forms per engine
# ---------------------------------------------------------------------------

# pgvector SQL 形态矩阵（每个形态都会为查询池中的每条向量生成一行 SQL）：
#   字面量: text_literal('[...]'::vector) / text_cast(CAST('[...]' AS vector))
#           / vector_fn(vector('[...]'))
#   ARRAY : array_int_cast(ARRAY[...]::vector) / array_real_cast(ARRAY[...]::real[]::vector)
#           / array_cast(CAST(ARRAY[...]::real[] AS vector))
#           / array_to_vector_fn(array_to_vector(ARRAY[...]::real[], dim, false))
#   半精度: halfvec_literal('[...]'::halfvec(dim)::vector)
#   WITH  : with_text_literal / with_array_cast / with_vector_fn（查询向量先放进 CTE，
#           再 CROSS JOIN 到全表，避免逐行重复构造）
#   ID    : subquery_id / with_subquery_id（查询向量直接取自表内某一行，绕过文本→vector 解析）
PGVECTOR_SQL_TYPES = [
    "text_literal",
    "text_cast",
    "vector_fn",
    "array_int_cast",
    "array_real_cast",
    "array_cast",
    "array_to_vector_fn",
    "halfvec_literal",
    "with_text_literal",
    "with_array_cast",
    "with_vector_fn",
    "subquery_id",
    "with_subquery_id",
]

# PolarDB PASE SQL 形态矩阵（PASE 使用 ``<?>`` 距离运算符和 pase 类型）：
#   text_pase_op_id       : ORDER BY vector <?> '...'::pase，只 SELECT id（无距离列）
#   text_pase_op_extra    : 文本后追加 ":ef"，即 '...:5'::pase
#   text_pase_op_extra_ds : 再追加 ":ds"（0=L2, 1=IP），即 '...:ef:ds'::pase
#   pase_fn_text_default  : 同样是 '...'::pase；pase(text) 构造是 base64 陷阱，见生成器注释
#   pase_fn_array_*       : pase(ARRAY[...]::float4[], [ef[, ds]])
#   hash_op_*             : 以 <?> 直接对 pase(...) 表达式排序的短形态
#   with_pase_fn_*        : 查询 pase 值放进 CTE 后再 CROSS JOIN
#   subquery_id / with_subquery_id : 查询向量取自表内某行并转换成 pase
POLARDB_SQL_TYPES = [
    "text_pase_op_id",
    "text_pase_op_extra",
    "text_pase_op_extra_ds",
    "pase_fn_text_default",
    "pase_fn_array_default",
    "pase_fn_array_extra",
    "pase_fn_array_ip",
    "hash_op_default",
    "hash_op_extra",
    "with_pase_fn_array",
    "with_pase_fn_text",
    "subquery_id",
    "with_subquery_id",
]

# Keep the query-form registry in one place.  The shell benchmarks historically
# used a larger list than this module, which meant that a generated benchmark
# did not cover the same SQL shapes as the shell benchmark.
# 中文：注册表集中在此，shell 脚本的 SQL_TYPES 也按同一组形态名对齐。
# 形态语义（详见 generate_clickhouse_sql_forms）：
#   normal        : L2Distance/cosineDistance(vector, [a,b,...])
#   cast          : CAST([...] AS Array(Float32))
#   cast_array    : cast('[...]', 'Array(Float32)')（字符串解析路径）
#   type_hint     : arrayMap(x -> toFloat32(x), [...])
#   raw_bytes     : [...]::Array(Float32)
#   raw_bytes_x   : 与 normal 文本相同，保留独立形态名以兼容 shell 注册表
#   with_*        : 上述写法放进 WITH ... AS query_vector 的 CTE 变体
CLICKHOUSE_SQL_TYPES = [
    "normal",
    "cast",
    "cast_array",
    "type_hint",
    "raw_bytes",
    "raw_bytes_x",
    "with",
    "with_cast",
    "with_cast_array",
    "with_type_hint",
    "with_raw_bytes",
    "with_raw_bytes_x",
]


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args():
    """解析命令行参数并返回 ``argparse.Namespace``。

    关键参数与单位：
      * ``--engine``：目标引擎（pgvector / polardb / clickhouse / myscale）。
      * ``--table``：必需，待采样的目标表名。
      * ``--query-count``：每个形态文件包含的查询条数（即从表中采样的向量个数）。
      * ``--top-k``：SQL 中 ``LIMIT`` 的 k。
      * ``--distance``：l2/ip/cosine，决定距离运算符或距离函数以及排序方向。
      * ``--dimension``：向量维度；0 表示自动探测（halfvec / type_hint 会用到）。
      * ``--sql-types``：只生成指定形态；缺省为对应引擎的完整注册表。
      * ``--output-dir``：输出根目录，实际文件写入其下的引擎子目录（见 ``main``）。
      * PG 连接参数（host/port/user/password/database）与 ClickHouse 连接参数
        （clickhouse-host/clickhouse-port）分别用于两类采样器。
      * PolarDB 专有：``--pase-extra``（ef_search）与 ``--pase-ds``（0=L2, 1=IP）。
    ``--config`` 指向 JSON 文件时，其字段会在 ``main`` 中覆盖同名参数。
    """
    p = argparse.ArgumentParser(
        description="Generate SQL benchmark files for bash-based QPS testing"
    )
    p.add_argument(
        "--engine",
        choices=["pgvector", "polardb", "clickhouse", "myscale"],
        default="pgvector",
        help="Target database engine",
    )
    p.add_argument("--table", help="Table name (required)")
    p.add_argument("--query-count", type=int, default=1000, help="Number of query vectors")
    p.add_argument("--top-k", type=int, default=10, help="LIMIT clause")
    p.add_argument("--distance", choices=["l2", "ip", "cosine"], default="l2",
                   help="Distance metric (pgvector/clickhouse only)")
    p.add_argument("--dimension", type=int, default=0,
                   help="Vector dimension (pgvector: halfvec; clickhouse: type_hint)")
    p.add_argument("--sql-types", nargs="*",
                   help="Specific SQL forms to generate (default: all)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=5432)
    p.add_argument("--user", default="postgres")
    p.add_argument("--password", default="123456")
    p.add_argument("--database", default="postgres")
    p.add_argument("--output-dir", default="../bash-test/sql-bench",
                   help="Output directory for SQL files")
    p.add_argument("--config", help="JSON config file path")
    # PolarDB-specific
    p.add_argument("--pase-extra", type=int, default=5,
                   help="PolarDB HNSW ef_search parameter")
    p.add_argument("--pase-ds", type=int, default=0,
                   help="PolarDB distance type (0=L2, 1=IP)")
    # ClickHouse-specific
    p.add_argument("--clickhouse-host", default="127.0.0.1")
    p.add_argument("--clickhouse-port", type=int, default=9000)
    return p.parse_args()


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def sample_vectors_pg(
    host: str, port: int, user: str, password: str, database: str,
    table: str, n: int, vector_col: str = "vector",
) -> List[Tuple[int, List[float]]]:
    """从 PostgreSQL 表随机采样查询向量。

    参数：host/port/user/password/database 为连接信息；table 为目标表；
    n 为采样条数；vector_col 为向量列名（默认 ``vector``）。
    返回：``[(row_id, [float, ...]), ...]``，长度不超过 n。
    实现要点：``ORDER BY random() LIMIT n`` 做全表随机排序；向量以 ``::text`` 读出，
    兼容 vector/halfvec 的文本表示；随后剥掉 ``{}[]"'`` 并按逗号切分为浮点数。
    副作用：建立一次数据库连接（``with`` 退出时自动关闭）。
    """
    import psycopg2
    with psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=database
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT id, {vector_col}::text FROM {table} "
                f"ORDER BY random() LIMIT {n}"
            )
            result = []
            for row_id, txt in cur.fetchall():
                clean = txt.strip("{}[]\"'")
                vals = [float(x.strip()) for x in clean.split(",") if x.strip()]
                result.append((row_id, vals))
            return result


def get_vector_dimension(
    host: str, port: int, user: str, password: str, database: str,
    table: str, vector_col: str = "vector",
) -> int:
    """读取向量列的维度（``vector_dims``）。

    参数与 :func:`sample_vectors_pg` 相同；返回表中第一行的向量维度。
    用于 ``--dimension 0`` 时自动确定 halfvec / array_to_vector 所需的维度。
    副作用：建立一次数据库连接；表为空或调用失败时由调用方捕获并回退到
    ``_detect_dim_from_vectors``（从已采样向量推断）。
    """
    import psycopg2
    with psycopg2.connect(
        host=host, port=port, user=user, password=password, dbname=database
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT vector_dims({vector_col}) FROM {table} LIMIT 1"
            )
            return cur.fetchone()[0]


def sample_vectors_clickhouse(
    host: str, port: int, table: str, n: int, vector_col: str = "vector",
) -> List[Tuple[int, List[float]]]:
    """从 ClickHouse 表随机采样查询向量。

    Uses clickhouse-connect library (project dependency) instead of shell command.
    Falls back to clickhouse-driver if connect is unavailable.

    参数：host、port（**native 协议端口**，默认 9000）、table、n、vector_col。
    返回 ``[(row_id, [float, ...]), ...]``。
    实现要点：
      1) 首选 clickhouse-connect（HTTP 接口）：把 9000/9440 映射到 8123/8443 后查询，
         直接拿到 ``Array(Float32)`` 的 Python 列表，避免文本往返；
      2) 该库缺失或查询抛异常时，回退 clickhouse-driver（native 协议），用
         ``arrayStringConcat(arrayMap(x -> toString(x), col), ',')`` 把数组拼成文本再解析；
      3) 两条路径都带 ``SETTINGS use_query_cache=0``，防止采样结果被服务端结果缓存污染。
    异常：两个驱动都不可用时抛 ImportError。
    """
    try:
        import clickhouse_connect as cc
    except ImportError:
        cc = None

    # clickhouse-connect speaks the HTTP interface, while ``port`` is the native
    # protocol port (9000 by default, same one clickhouse-driver uses below).
    # Map the well-known native ports to their HTTP peers so the first attempt works.
    http_port = {9000: 8123, 9440: 8443}.get(port, port)

    if cc is not None:
        try:
            client = cc.get_client(host=host, port=http_port)
            rows = client.query(
                f"SELECT id, {vector_col} FROM {table} "
                f"ORDER BY rand() LIMIT {n} "
                f"SETTINGS use_query_cache=0"
            ).result_rows
            vectors = []
            for row in rows:
                row_id = int(row[0])
                vec = list(row[1]) if hasattr(row[1], '__iter__') else [float(x) for x in str(row[1]).strip('[]').split(',')]
                vectors.append((row_id, vec))
            return vectors
        except Exception:
            pass

    try:
        from clickhouse_driver import Client as CHClient
    except ImportError:
        raise ImportError(
            "No ClickHouse driver available. Install one of:\n"
            "  pip install clickhouse-connect\n"
            "  pip install clickhouse-driver"
        )

    ch_client = CHClient(host=host, port=port)
    # The vector column is Array(Float32); arrayStringConcat needs Array(String).
    result = ch_client.execute(
        f"SELECT id, arrayStringConcat(arrayMap(x -> toString(x), {vector_col}), ',') "
        f"FROM {table} ORDER BY rand() LIMIT {n} SETTINGS use_query_cache=0"
    )
    vectors = []
    for row_id, vec_str in result:
        vals = [float(x.strip()) for x in str(vec_str).split(",") if x.strip()]
        vectors.append((int(row_id), vals))
    return vectors


# ---------------------------------------------------------------------------
# PGVector SQL generators
# ---------------------------------------------------------------------------

def _vec_str(vals: List[float]) -> str:
    """把浮点列表渲染成逗号分隔文本（不带方括号/花括号），供各形态内联进 SQL。"""
    return ",".join(str(v) for v in vals)


def _dist_op(distance: str) -> str:
    """pgvector 距离运算符：l2→``<->``、ip→``<#>``、cosine→``<=>``。"""
    return {"l2": "<->", "ip": "<#>", "cosine": "<=>"}[distance]


def _sort_dir(distance: str) -> str:
    """排序方向：L2/余弦距离越小越近用 ASC；内积越大越近用 DESC。"""
    return "ASC" if distance in ("l2", "cosine") else "DESC"


def generate_pgvector_sql_forms(
    table: str,
    vectors: List[Tuple[int, List[float]]],
    top_k: int,
    distance: str,
    dimension: int = 0,
    sql_types: Optional[List[str]] = None,
) -> Dict[str, str]:
    """为 pgvector 生成全部 SQL 形态。

    参数：
      * table：目标表名（向量列固定叫 ``vector``，id 列固定叫 ``id``）。
      * vectors：``sample_vectors_pg`` 的返回值，作为查询池；每条向量生成一行 SQL。
      * top_k：``ORDER BY dis ... LIMIT top_k`` 的 k。
      * distance：l2/ip/cosine，决定运算符与排序方向（见 ``_dist_op`` / ``_sort_dir``）。
      * dimension：halfvec / array_to_vector 需要的维度；0 时从采样向量推断，最后兜底 128。
      * sql_types：要生成的形态名列表；None 表示 ``PGVECTOR_SQL_TYPES`` 全量。
    返回：``{形态名: 以 "\\n" 连接的 SQL 文本 + 末尾换行}``。未注册/未请求的形态不出现。
    副作用：无（纯字符串拼接）。
    """
    dist_op = _dist_op(distance)
    sort = _sort_dir(distance)
    dim = dimension or _detect_dim_from_vectors(vectors) or 128

    if sql_types is None:
        sql_types = PGVECTOR_SQL_TYPES

    # Collectors
    collectors: Dict[str, List[str]] = {t: [] for t in sql_types}

    # 下面的 ``if ... in collectors`` 既是"请求过滤"也是"追加到该形态的行列表"：
    # collectors 只为 sql_types 中的形态建立键，未请求的形态不会浪费时间渲染。
    for rid, vec in vectors:
        v_str = _vec_str(vec)
        v_arr = "{" + v_str + "}"

        # ---- 文本字面量路径：整个向量以字符串发送，服务端由 vector_in 解析 ----
        if "text_literal" in collectors:
            collectors["text_literal"].append(
                f"SELECT id, (vector {dist_op} '[{v_str}]'::vector) "
                f"AS dis FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "text_cast" in collectors:
            collectors["text_cast"].append(
                f"SELECT id, (vector {dist_op} "
                f"CAST('[{v_str}]' AS vector)) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "vector_fn" in collectors:
            collectors["vector_fn"].append(
                f"SELECT id, (vector {dist_op} "
                f"vector('[{v_str}]')) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        # ---- ARRAY 路径：先构造 PostgreSQL 数组再转 vector，用于对比元素类型/解析开销 ----
        if "array_int_cast" in collectors:
            collectors["array_int_cast"].append(
                f"SELECT id, (vector {dist_op} ARRAY[{v_str}]::vector) "
                f"AS dis FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "array_real_cast" in collectors:
            collectors["array_real_cast"].append(
                f"SELECT id, (vector {dist_op} "
                f"ARRAY[{v_str}]::real[]::vector) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "array_cast" in collectors:
            collectors["array_cast"].append(
                f"SELECT id, (vector {dist_op} "
                f"CAST(ARRAY[{v_str}]::real[] AS vector)) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "array_to_vector_fn" in collectors:
            collectors["array_to_vector_fn"].append(
                f"SELECT id, (vector {dist_op} "
                f"array_to_vector(ARRAY[{v_str}]::real[], {dim}, false)) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        # ---- 半精度路径：先解析成 halfvec(dim) 再转回 vector，衡量 fp16 解码代价 ----
        if "halfvec_literal" in collectors:
            collectors["halfvec_literal"].append(
                f"SELECT id, (vector {dist_op} "
                f"'[{v_str}]'::halfvec({dim})::vector) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        # ---- WITH 路径：查询向量在 CTE 中只构造一次，再 CROSS JOIN 到全表 ----
        if "with_text_literal" in collectors:
            collectors["with_text_literal"].append(
                f"WITH query_vector AS (SELECT '[{v_str}]'::vector AS v) "
                f"SELECT t.id, (t.vector {dist_op} query_vector.v) AS dis "
                f"FROM {table} AS t CROSS JOIN query_vector "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "with_array_cast" in collectors:
            collectors["with_array_cast"].append(
                f"WITH query_vector AS "
                f"(SELECT ARRAY[{v_str}]::real[]::vector AS v) "
                f"SELECT t.id, (t.vector {dist_op} query_vector.v) AS dis "
                f"FROM {table} AS t CROSS JOIN query_vector "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "with_vector_fn" in collectors:
            collectors["with_vector_fn"].append(
                f"WITH query_vector AS "
                f"(SELECT vector('[{v_str}]') AS v) "
                f"SELECT t.id, (t.vector {dist_op} query_vector.v) AS dis "
                f"FROM {table} AS t CROSS JOIN query_vector "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

        # ---- 子查询路径：查询向量直接取自表内 id=rid 的行，完全绕过文本→vector 解析 ----
        if "subquery_id" in collectors:
            collectors["subquery_id"].append(
                f"SELECT id, (vector {dist_op} "
                f"(SELECT vector FROM {table} WHERE id = {rid})::vector) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "with_subquery_id" in collectors:
            collectors["with_subquery_id"].append(
                f"WITH query_vector AS "
                f"(SELECT vector FROM {table} WHERE id = {rid}) "
                f"SELECT t.id, (t.vector {dist_op} query_vector.vector) AS dis "
                f"FROM {table} AS t CROSS JOIN query_vector "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

    return {t: "\n".join(collectors[t]) + "\n" for t in sql_types}


# ---------------------------------------------------------------------------
# PolarDB SQL generators
# ---------------------------------------------------------------------------

def generate_polardb_sql_forms(
    table: str,
    vectors: List[Tuple[int, List[float]]],
    top_k: int,
    distance: str,
    pase_extra: int = 5,
    pase_ds: int = 0,
    sql_types: Optional[List[str]] = None,
) -> Dict[str, str]:
    """为 PolarDB PASE 生成全部 SQL 形态。

    参数：
      * table/vectors/top_k/distance：与 pgvector 生成器相同，distance 只影响排序方向。
      * pase_extra：PASE 的 HNSW ``ef_search``，以追加在字面量冒号后或函数第二参数的形式出现。
      * pase_ds：距离类型（0=L2, 1=IP），仅 ``text_pase_op_extra_ds`` 形态使用。
      * sql_types：要生成的形态名；None 表示 ``POLARDB_SQL_TYPES`` 全量。
    返回：``{形态名: 以 "\\n" 连接的 SQL 文本 + 末尾换行}``。
    说明：PASE 的查询向量类型是 ``pase``，距离用 ``<?>`` 运算符；``text_pase_op_id``
    只返回 id（无 ``dis`` 列），其余形态返回 ``id, dis``。副作用：无。
    """
    sort = _sort_dir(distance)

    if sql_types is None:
        sql_types = POLARDB_SQL_TYPES

    collectors: Dict[str, List[str]] = {t: [] for t in sql_types}

    for rid, vec in vectors:
        v_str = _vec_str(vec)

        # ---- 文本路径：'...'::pase 走 pase 输入函数；可附加 ":ef"/":ef:ds" 后缀 ----
        if "text_pase_op_id" in collectors:
            collectors["text_pase_op_id"].append(
                f"SELECT id FROM {table} ORDER BY "
                f"vector <?> '{v_str}'::pase {sort} LIMIT {top_k};"
            )

        if "text_pase_op_extra" in collectors:
            extra_str = f"{v_str}:{pase_extra}"
            collectors["text_pase_op_extra"].append(
                f"SELECT id, (vector <?> '{extra_str}'::pase) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "text_pase_op_extra_ds" in collectors:
            extra_ds_str = f"{v_str}:{pase_extra}:{pase_ds}"
            collectors["text_pase_op_extra_ds"].append(
                f"SELECT id, (vector <?> '{extra_ds_str}'::pase) AS ds "
                f"FROM {table} ORDER BY ds {sort} LIMIT {top_k};"
            )

        if "pase_fn_text_default" in collectors:
            # 陷阱: pase(text) 构造函数绑定 pase_text_i_i (base64 解码), 逗号文本会被
            # 解出垃圾维度; 文本进 pase 唯一安全路径是 'txt'::pase 输入函数。
            collectors["pase_fn_text_default"].append(
                f"SELECT id, (vector <?> '{v_str}'::pase) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        # ---- 数组函数路径：pase(ARRAY[...]::float4[], [ef[, ds]])，绕过文本解析 ----
        if "pase_fn_array_default" in collectors:
            collectors["pase_fn_array_default"].append(
                f"SELECT id, (vector <?> "
                f"pase(ARRAY[{v_str}]::float4[])) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "pase_fn_array_extra" in collectors:
            collectors["pase_fn_array_extra"].append(
                f"SELECT id, (vector <?> "
                f"pase(ARRAY[{v_str}]::float4[], {pase_extra})) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "pase_fn_array_ip" in collectors:
            collectors["pase_fn_array_ip"].append(
                f"SELECT id, (vector <?> "
                f"pase(ARRAY[{v_str}]::float4[], {pase_extra}, 1)) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        # ---- hash_op 路径：SELECT 列表里直接写 <?> 与 pase(...) 的紧凑形态 ----
        if "hash_op_default" in collectors:
            collectors["hash_op_default"].append(
                f"SELECT id, vector <?> "
                f"pase(ARRAY[{v_str}]::float4[]) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "hash_op_extra" in collectors:
            collectors["hash_op_extra"].append(
                f"SELECT id, vector <?> "
                f"pase(ARRAY[{v_str}]::float4[], {pase_extra}) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        # ---- WITH 路径：查询 pase 值放进 CTE，再 CROSS JOIN 到全表 ----
        if "with_pase_fn_array" in collectors:
            collectors["with_pase_fn_array"].append(
                f"WITH query_pase AS "
                f"(SELECT pase(ARRAY[{v_str}]::float4[]) AS p) "
                f"SELECT id, (vector <?> query_pase.p) AS dis "
                f"FROM {table} CROSS JOIN query_pase "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "with_pase_fn_text" in collectors:
            # 同上: pase('...') 是 base64 陷阱, 用 cast 走 pase_in
            collectors["with_pase_fn_text"].append(
                f"WITH query_pase AS "
                f"(SELECT '{v_str}'::pase AS p) "
                f"SELECT id, (vector <?> query_pase.p) AS dis "
                f"FROM {table} CROSS JOIN query_pase "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

        # ---- 子查询路径：查询向量取自表内某行并转成 pase，无文本解析 ----
        if "subquery_id" in collectors:
            collectors["subquery_id"].append(
                f"SELECT id, (vector <?> "
                f"(SELECT pase(vector) FROM {table} WHERE id = {rid})) AS dis "
                f"FROM {table} ORDER BY dis {sort} LIMIT {top_k};"
            )

        if "with_subquery_id" in collectors:
            collectors["with_subquery_id"].append(
                f"WITH query_pase AS "
                f"(SELECT (SELECT pase(vector) FROM {table} WHERE id = {rid}) AS v) "
                f"SELECT id, (vector <?> query_pase.v) AS dis "
                f"FROM {table} CROSS JOIN query_pase "
                f"ORDER BY dis {sort} LIMIT {top_k};"
            )

    return {t: "\n".join(collectors[t]) + "\n" for t in sql_types}


# ---------------------------------------------------------------------------
# ClickHouse SQL generators
# ---------------------------------------------------------------------------

def _ch_dist_func(distance: str) -> str:
    """ClickHouse 距离函数名：l2→L2Distance；ip 与 cosine 都映射到 cosineDistance。

    注意 ip 也走 cosineDistance 是本模块的既有约定（ClickHouse 侧没有独立的负内积
    标量函数）；``benchmark/engine/clickhouse.py`` 的距离映射同样是 ``ip -> cosineDistance``。
    """
    return {"l2": "L2Distance", "ip": "cosineDistance", "cosine": "cosineDistance"}[distance]


def generate_clickhouse_sql_forms(
    table: str,
    vectors: List[Tuple[int, List[float]]],
    top_k: int,
    distance: str,
    dimension: int = 0,
    sql_types: Optional[List[str]] = None,
    engine: str = "clickhouse",
    search_params: Optional[dict] = None,
) -> Dict[str, str]:
    """Generate all ClickHouse SQL query forms.

    ``engine`` distinguishes the two ClickHouse-family servers, which index
    different function families:

    * ``clickhouse`` (the ``xxb-vectorqueryplancache`` branch) accelerates the
      plain scalar helpers ``L2Distance`` / ``cosineDistance`` in an
      ORDER BY ... LIMIT query, so the forms keep those names.
    * ``myscale`` (the ``myscaledb-oss-queryplancache`` branch) only routes a
      query through its ANN vector index when the top-level function belongs to
      the ``distance(...)`` family (the planner prefix-matches the function
      name; ``L2Distance`` etc. only produce a full-table scan).  For that
      engine the same query-vector literal spellings are therefore wrapped in
      ``distance('key=value', ...)(vector, query)``, mirroring how
      ``engine/clients/myscale/search.py::vector_search`` issues the query.
      ``search_params`` supplies the tuple arguments (e.g. ``ef_s``) and is
      ignored for any other engine.

    中文补充：
      参数 table/vectors/top_k/distance/dimension 与其它生成器相同；dimension 仅在
      type_hint 等形态需要时参与，0 时从采样向量推断（兜底 768）。
      sql_types 为 None 时使用 ``CLICKHOUSE_SQL_TYPES`` 全量。
      ``search_params``（MyScale）会被渲染成 ``distance('ef_s=100', 'k=20')(...)``
      的柯里化参数元组：list/tuple 取第一个元素、None 跳过、bool 转 0/1。
      返回 ``{形态名: 以 "\\n" 连接的 SQL 文本 + 末尾换行}``；无副作用。
    """
    dist_func = _ch_dist_func(distance)
    is_myscale = engine.lower() == "myscale"
    sort = _sort_dir(distance)
    dim = dimension or _detect_dim_from_vectors(vectors) or 768

    # distance() search parameters rendered as its curried tuple, e.g.
    # "('ef_s=100', 'k=20')" -- the exact form run.py sends via the MyScale
    # client.  A bare `distance(vector, q)` is used when none are configured.
    dist_params = ""
    if is_myscale and search_params:
        rendered = []
        for key, value in dict(search_params).items():
            if isinstance(value, (list, tuple)):
                value = value[0] if value else None
            if value is None:
                continue
            if isinstance(value, bool):
                value = int(value)
            rendered.append(f"'{key}={value}'")
        if rendered:
            dist_params = "(" + ", ".join(rendered) + ")"

    if sql_types is None:
        sql_types = CLICKHOUSE_SQL_TYPES

    collectors: Dict[str, List[str]] = {t: [] for t in sql_types}

    def distance_expr(query_expr: str) -> str:
        """按引擎选择距离函数：MyScale 用 ``distance<params>``，CK 用 ``dist_func``。

        MyScale 的距离度量与参数都写在函数名后缀里（如
        ``distanceL2(vector, q)``），ClickHouse 则是 ``L2Distance(vector, q)``。
        """
        if is_myscale:
            return f"distance{dist_params}(vector, {query_expr})"
        return f"{dist_func}(vector, {query_expr})"

    def select_sql(query_expr: str, order: str = sort) -> str:
        """生成"查询向量直接内联在 SELECT 中"的 SQL 形态。

        ``order`` 默认沿用全局排序方向（距离越小越好为 ASC，内积为 DESC）。
        """
        return (
            f"SELECT id, {distance_expr(query_expr)} AS dis FROM {table} "
            f"ORDER BY dis {order} LIMIT {top_k};"
        )

    def with_sql(query_expr: str) -> str:
        """生成 ``WITH <expr> AS query_vector`` 的形态，查询向量只写一次。

        该形态用于对比"表达式复用"是否影响计划缓存/常量折叠。
        """
        return (
            f"WITH {query_expr} AS query_vector SELECT id, "
            f"{distance_expr('query_vector')} AS dis FROM {table} "
            f"ORDER BY dis {sort} LIMIT {top_k};"
        )

    for rid, vec in vectors:
        v_str = _vec_str(vec)

        if "normal" in collectors:
            collectors["normal"].append(select_sql(f"[{v_str}]"))

        if "cast" in collectors:
            collectors["cast"].append(select_sql(f"CAST([{v_str}] AS Array(Float32))"))

        if "cast_array" in collectors:
            collectors["cast_array"].append(select_sql(f"cast('[{v_str}]','Array(Float32)')"))

        if "raw_bytes" in collectors:
            collectors["raw_bytes"].append(select_sql(f"[{v_str}]::Array(Float32)"))

        if "raw_bytes_x" in collectors:
            collectors["raw_bytes_x"].append(select_sql(f"[{v_str}]"))

        if "type_hint" in collectors:
            collectors["type_hint"].append(
                select_sql(f"arrayMap(x -> toFloat32(x), [{v_str}])")
            )

        if "with" in collectors:
            collectors["with"].append(with_sql(f"[{v_str}]"))
        if "with_cast" in collectors:
            collectors["with_cast"].append(with_sql(f"CAST([{v_str}] AS Array(Float32))"))
        if "with_cast_array" in collectors:
            collectors["with_cast_array"].append(with_sql(f"cast('[{v_str}]','Array(Float32)')"))
        if "with_type_hint" in collectors:
            collectors["with_type_hint"].append(
                with_sql(f"arrayMap(x -> toFloat32(x), [{v_str}])")
            )
        if "with_raw_bytes" in collectors:
            collectors["with_raw_bytes"].append(with_sql(f"[{v_str}]::Array(Float32)"))
        if "with_raw_bytes_x" in collectors:
            collectors["with_raw_bytes_x"].append(with_sql(f"[{v_str}]"))

    return {t: "\n".join(collectors[t]) + "\n" for t in sql_types}


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _detect_dim_from_vectors(vectors: List[Tuple[int, List[float]]]) -> int:
    """用第一条向量的长度推断维度；空列表返回 0。
    调用方需自行兜底或报错。"""
    if vectors:
        return len(vectors[0][1])
    return 0


def _engine_to_sql_dir(engine: str) -> str:
    """把引擎名映射到生成 SQL 的子目录名；未知引擎原样作为目录名。"""
    mapping = {
        "pgvector": "pgvector-query-forms",
        "polardb": "polardb-pase-query-forms",
        "clickhouse": "vector-query-forms",
        "myscale": "vector-query-forms",
    }
    return mapping.get(engine, engine)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """命令行入口：解析参数/配置文件 → 读取向量 → 按引擎生成各形态 SQL。"""
    args = parse_args()

    if args.config:
        config_path = Path(args.config)
        if not config_path.exists():
            print(f"Error: config not found: {config_path}", file=sys.stderr)
            sys.exit(1)
        with open(config_path) as f:
            config = json.load(f)
        for key, value in config.items():
            key_clean = key.replace("-", "_")
            if hasattr(args, key_clean) and value is not None and not isinstance(value, list):
                setattr(args, key_clean, value)

    if not args.table:
        print("Error: --table is required", file=sys.stderr)
        sys.exit(1)

    engine = args.engine
    sql_dir = _engine_to_sql_dir(engine)
    output_dir = Path(args.output_dir) / sql_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine which SQL types to generate
    if args.sql_types:
        sql_types = args.sql_types
    else:
        type_map = {
            "pgvector": PGVECTOR_SQL_TYPES,
            "polardb": POLARDB_SQL_TYPES,
            "clickhouse": CLICKHOUSE_SQL_TYPES,
            "myscale": CLICKHOUSE_SQL_TYPES,
        }
        sql_types = type_map.get(engine, [])

    # Sample vectors
    if engine in ("pgvector", "polardb"):
        print(f"Connecting to PostgreSQL at {args.host}:{args.port}...")
        print(f"Sampling {args.query_count} vectors from {args.table}...")
        vectors = sample_vectors_pg(
            args.host, args.port, args.user, args.password,
            args.database, args.table, args.query_count,
        )
        # Auto-detect dimension if not provided
        if args.dimension <= 0:
            try:
                args.dimension = get_vector_dimension(
                    args.host, args.port, args.user, args.password,
                    args.database, args.table,
                )
                print(f"Auto-detected vector dimension: {args.dimension}")
            except Exception:
                args.dimension = _detect_dim_from_vectors(vectors)
                print(f"Fallback dimension from data: {args.dimension}")
    elif engine in ("clickhouse", "myscale"):
        print(f"Connecting to ClickHouse at {args.clickhouse_host}:{args.clickhouse_port}...")
        print(f"Sampling {args.query_count} vectors from {args.table}...")
        vectors = sample_vectors_clickhouse(
            args.clickhouse_host, args.clickhouse_port, args.table, args.query_count,
        )
        if args.dimension <= 0:
            args.dimension = _detect_dim_from_vectors(vectors)
    else:
        print(f"Error: unsupported engine {engine}", file=sys.stderr)
        sys.exit(1)

    print(f"Sampled: {len(vectors)} vectors")

    # Generate SQL forms
    if engine == "pgvector":
        forms = generate_pgvector_sql_forms(
            args.table, vectors, args.top_k, args.distance,
            args.dimension, sql_types,
        )
    elif engine == "polardb":
        forms = generate_polardb_sql_forms(
            args.table, vectors, args.top_k, args.distance,
            args.pase_extra, args.pase_ds, sql_types,
        )
    elif engine in ("clickhouse", "myscale"):
        forms = generate_clickhouse_sql_forms(
            args.table, vectors, args.top_k, args.distance,
            args.dimension, sql_types, engine=engine,
        )
    else:
        print(f"Error: unknown engine {engine}", file=sys.stderr)
        sys.exit(1)

    # Write SQL files
    for form_name, sql_content in forms.items():
        filename = f"{engine}_{args.table}_{form_name}_{args.query_count}.sql"
        filepath = output_dir / filename
        with open(filepath, "w") as f:
            f.write(sql_content)
        line_count = sql_content.strip().count("\n") + (1 if sql_content.strip() else 0)
        print(f"  Generated: {filepath} ({line_count} queries)")

    print(f"\nDone! SQL files in: {output_dir}")


if __name__ == "__main__":
    main()