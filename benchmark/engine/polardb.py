"""PolarDB-pg engine implementation with PASE vector extension.

模块职责:
  * 提供 PolarDB-for-PostgreSQL (PASE 向量扩展) 的 BaseEngine 实现: 连接管理、
    建表、建索引、批量写入与近邻查询;
  * 供 engine/base.py 的 measure_qps 等通用压测入口复用 (建表/写数/查询);
  * 给命令行工具 (benchmark/__main__.py, generate_sql_files.py) 生成 PASE SQL。

关键入口:
  PolarDBEngine.connect / create_table / create_index / insert_vectors /
  query / query_sql / detect_vector_column / sample_vectors。

副作用:
  * create_index 会在当前会话 SET 并行 worker 参数, 并对目标库执行 DDL (耗时);
  * insert_vectors 会 commit 事务;
  * 除 query 的 conn-first 形态外, 每个方法都自建连接并在 finally 中关闭 (无连接池)。

说明: 本文件是较薄的引擎封装; benchmark 正式压测路径实际走
engine/clients/polardb/search.py (PolarDBSearcher), 两者 SQL 形态并不完全一致。
"""

import os
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras

from .base import BaseEngine


def _int_or(value, default: int) -> int:
    """仅当 value 是 int 时原样转换, 否则回退 default (bool 也是 int 子类)。

    用于给 top_k 之类的可选参数兜底; 注意它不做字符串到整数的解析。
    """
    return int(value) if isinstance(value, int) else default


def _pase_distance_sql(
    table: str,
    vector: List[float],
    top_k: int = 10,
    distance: str = "l2",
) -> str:
    """PASE 查询 SQL: float4[] <?> pase。

    `<?>` 右侧必须是 pase 类型; 用 '逗号文本[:extra[:ds]]'::pase 走输入函数
    pase_in (安全), 不能用 pase('...') 构造函数 (那是 base64 解码, 只有
    pase(float4[]) / pase(float4[], extra, ds) 数组构造函数是安全的)。
    ds: 0 = L2 (cosine 需先归一化), 1 = inner product (返回原始内积, 越大越好)。

    参数:
      table: 表名, 直接拼进 SQL (本函数不做标识符校验, 调用方负责);
      vector: 查询向量 (float 列表), 以逗号文本内联;
      top_k: LIMIT 条数;
      distance: 仅识别 "ip" (大小写不敏感); 其余 (含 "cosine") 都按 ds=0/L2 处理。

    返回: 完整 SELECT 语句字符串。无副作用 (不连库)。
    拼出的 ds 列 alias 与 ds 参数同名, 但 SQL 中 ORDER BY 引用的是结果列 alias。
    """
    dist_lower = str(distance or "l2").lower()
    ds = 1 if dist_lower == "ip" else 0
    # ds=1 返回内积 (越大越近) -> DESC; ds=0 返回距离 (越小越近) -> ASC。
    sort_dir = "DESC" if ds == 1 else "ASC"
    # pase 输入文本: 'v1,v2,...:ds'。缺省 extra (ef_search) 时用服务端默认。
    vec_str = ",".join(str(v) for v in vector)
    return (
        f"SELECT id, (vector <?> '{vec_str}:{ds}'::pase) AS ds "
        f"FROM {table} ORDER BY ds {sort_dir} LIMIT {top_k}"
    )


class PolarDBEngine(BaseEngine):
    """PolarDB for PostgreSQL + PASE 向量扩展的引擎实现。

    所有方法都是实例方法; 连接参数 (host/port/user/password/database) 由基类
    在构造时注入。除 query 的 conn-first 形态外, 每个方法自行开/关连接。
    """

    ENGINE_NAME = "polardb"

    def connect(self):
        """新建一条 psycopg2 连接 (非 autocommit, 默认事务语义)。

        Returns:
            psycopg2.extensions.connection: 调用方负责 close。
        """
        return psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            dbname=self.database,
        )

    def _execute(self, conn, sql: str, params=None, fetch: bool = False):
        """执行一条 SQL; fetch=True 且有结果集时返回 fetchall(), 否则返回 None。

        注意: fetch 分支在 `with` 块内直接 return, 因此不会执行末尾的
        conn.commit() —— 只读查询无需提交; 非 fetch 分支 (DDL/DML/SET) 才提交。
        """
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if fetch and cur.description:
                return cur.fetchall()
        conn.commit()
        return None

    def server_version(self) -> str:
        """返回服务器版本字符串 (SELECT current_setting('server_version'))。"""
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT current_setting('server_version')")
                return cur.fetchone()[0]
        finally:
            conn.close()

    def create_table(
        self, table: str, dim: int, drop: bool = False
    ) -> None:
        """创建向量表 (可先 DROP)。

        表结构固定为 `id serial PRIMARY KEY, vector float4[]`; dim 参数当前未使用
        (PASE 的维度在索引上声明, 数组列本身不约束长度)。
        drop=True 时先执行 DROP TABLE IF EXISTS ... CASCADE (连带删除索引/依赖)。
        """
        conn = self.connect()
        try:
            if drop:
                self._execute(conn, f"DROP TABLE IF EXISTS {table} CASCADE")
            self._execute(
                conn,
                f"CREATE TABLE IF NOT EXISTS {table} "
                f"(id serial PRIMARY KEY, vector float4[])",
            )
        finally:
            conn.close()

    def create_index(
        self,
        table: str,
        index_type: str = "hnsw",
        distance: str = "l2",
        dim: int = 128,
        **kwargs,
    ) -> None:
        """创建 PASE 向量索引。

        参数:
          index_type: "hnsw" | "ivfflat" | "none"; "none" 直接返回 (不建索引)。
          distance: 仅 ivfflat 使用; "l2" -> distance_type=0, 其他 -> 1。
          dim: 向量维度, 写入索引 WITH 子句。
          kwargs (hnsw): base_nb_num=16, ef_build=40, ef_search=100。
          kwargs (ivfflat): lists=100。

        副作用: 会话级 SET 并行参数 (仅 hnsw); 执行 CREATE INDEX (耗时)。
        索引名固定为 `{table}_{index_type}_idx`。
        """
        if index_type == "none":
            return

        idx_name = f"{table}_{index_type}_idx"
        conn = self.connect()
        try:
            if index_type == "hnsw":
                base_nb_num = kwargs.get("base_nb_num", 16)
                ef_build = kwargs.get("ef_build", 40)
                ef_search = kwargs.get("ef_search", 100)
                # Configure high-concurrency parallel index build using 90% of
                # the available device threads, capped by max_worker_processes.
                # 用 sched_getaffinity 取当前进程可用 CPU 数 (尊重 cgroup/绑核),
                # 取 90% 作为期望并行度, 再用 SHOW max_worker_processes 封顶。
                desired_workers = max(1, int(len(os.sched_getaffinity(0)) * 0.9))
                max_worker_processes = int(
                    self._execute(
                        conn,
                        "SHOW max_worker_processes",
                        fetch=True,
                    )[0][0]
                )
                target_workers = min(desired_workers, max_worker_processes)
                # 三个 GUC 均是会话级 (仅影响本连接上的建索引), 不写回全局配置。
                self._execute(conn, f"SET max_parallel_workers = {target_workers}")
                self._execute(
                    conn,
                    f"SET max_parallel_maintenance_workers = {target_workers}",
                )
                self._execute(
                    conn,
                    f"SET max_parallel_workers_per_gather = {target_workers}",
                )
                # base64_encoded=0: 输入/输出向量用普通数组文本, 不用 base64 编码。
                self._execute(
                    conn,
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} "
                    f"USING pase_hnsw (vector) WITH ("
                    f"dim = {dim}, base_nb_num = {base_nb_num}, "
                    f"ef_build = {ef_build}, ef_search = {ef_search}, "
                    f"base64_encoded = 0)",
                )
            elif index_type == "ivfflat":
                # distance_type: 0=L2, 1=内积。注意这里只判断字面量 "l2",
                # 因此 "cosine" 也会落到 1 (IP); 调用方需按需传入 "l2"。
                dist_type = 0 if distance == "l2" else 1
                lists = kwargs.get("lists", 100)
                # clustering_type=1 (PASE 的 k-means 聚类), lists 通过
                # clustering_params 以字符串 k=v 传入。
                self._execute(
                    conn,
                    f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} "
                    f"USING pase_ivfflat (vector) WITH ("
                    f"dimension = {dim}, distance_type = {dist_type}, "
                    f"clustering_type = 1, "
                    f"clustering_params = 'lists={lists}')",
                )
        finally:
            conn.close()

    def insert_vectors(
        self,
        table: str,
        vectors: List[List[float]],
        batch_size: int = 1000,
    ) -> None:
        """分批写入向量 (只写 vector 列, id 由 serial 自增生成)。

        vectors 每行被渲染成 PostgreSQL 数组文本 `'{v1,v2,...}'`, 再由
        `CAST(%s AS float4[])` 转成 float4[]。batch_size 控制每批行数;
        全部批次成功后统一 commit。异常时事务不提交 (连接 close 时回滚)。
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                for start in range(0, len(vectors), batch_size):
                    end = min(start + batch_size, len(vectors))
                    # 行内是数组字面量文本, 由 server 端 CAST 解析成 float4[]。
                    rows = [
                        ("{" + ",".join(str(v) for v in vec) + "}",)
                        for vec in vectors[start:end]
                    ]
                    # page_size=1000: psycopg2 execute_values 单次发送的页大小,
                    # 与 batch_size 独立; 大批量时会再按页切分。
                    psycopg2.extras.execute_values(
                        cur,
                        f"INSERT INTO {table} (vector) VALUES %s",
                        rows,
                        template="(CAST(%s AS float4[]))",
                        page_size=1000,
                    )
                conn.commit()
        finally:
            conn.close()

    def query(
        self,
        conn_or_table,
        vector: Optional[List[float]] = None,
        top_k: int = 10,
        distance: str = "l2",
    ) -> List[Tuple[int, float]]:
        """Query nearest neighbors using PASE operator.

        Callable as ``engine.query(table, vector, top_k, distance)`` (manages its
        own connection) or ``engine.query(conn, table, vector, top_k, distance)``
        (same conn-first shape as BaseEngine.measure_qps; mirrors
        ``engine/pgvector.py``).

        注意 (按实际签名): 本方法只有 conn_or_table/vector/top_k/distance 4 个
        位置参数, 因此 base.measure_qps 的 5 参 conn-first 调用
        `query(conn, table, vec, top_k, distance)` 会先抛 TypeError;
        下面的 else 分支仅在 3 参 `query(conn, table, vec)` 形态下可达, 且其
        `table = top_k` 会把列表赋给 table, 属于历史兼容 hack, 不要依赖。
        """
        if vector is None:
            raise ValueError("vector is required")
        if isinstance(conn_or_table, str):
            # 常规形态: 第一个参数是表名, 自建连接并在 finally 关闭。
            table = conn_or_table
            conn = self.connect()
            own_conn = True
        else:
            # conn-first 形态: 参数位置整体错位一级 (见 docstring 说明)。
            conn = conn_or_table
            own_conn = False
            table = top_k
            top_k = distance if isinstance(distance, int) else 10

        # conn-first 形态下 distance 实际是 top_k (int), 此时强制按 l2 生成 SQL;
        # 正常形态则原样透传 distance。
        sql = _pase_distance_sql(table, vector, _int_or(top_k, 10), distance if not isinstance(distance, int) else "l2")
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return [(row[0], row[1]) for row in cur.fetchall()]
        finally:
            if own_conn:
                conn.close()

    def query_sql(
        self,
        table: str,
        vector: List[float],
        top_k: int = 10,
        distance: str = "l2",
    ) -> str:
        """Generate the SQL for a query (不连库, 供 sh 脚本/日志复用)。"""
        return _pase_distance_sql(table, vector, top_k, distance)

    def detect_vector_column(self, table: str) -> Tuple[Optional[str], int, Optional[str]]:
        """探测向量列名、维度与类型。

        Returns:
            (column_name, dim, udt_type); 未找到返回 (None, 0, None)。

        通过 information_schema 找第一个 ARRAY/_float4 列, 再用 array_length(col, 1)
        取首行维度。dim 取不到时为 0。表名/列名直接拼 SQL (调用方需可信)。
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = %s AND data_type = 'ARRAY' "
                    "AND udt_name = '_float4' LIMIT 1",
                    (table,),
                )
                row = cur.fetchone()
                if row:
                    col = row[0]
                    # PG 没有 Array 列的固定维度, 只能从一条实际数据里量。
                    cur.execute(
                        f"SELECT array_length({col}, 1) FROM {table} LIMIT 1"
                    )
                    dim = int(cur.fetchone()[0] or 0)
                    return col, dim, "float4[]"
                return None, 0, None
        finally:
            conn.close()

    def sample_vectors(
        self, table: str, col: str, n: int
    ) -> List[Tuple[int, List[float]]]:
        """随机采样 n 条 (id, vector) 供 avg_distance 等统计使用。

        用 ORDER BY random() LIMIT n (全表随机排序, 大表代价高)。
        数组先 ::text 再手工剥掉 `{}[]"'` 并按逗号切分转 float; 空 token 被忽略。
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT id, {col}::text FROM {table} "
                    f"ORDER BY random() LIMIT {n}"
                )
                result = []
                for row_id, txt in cur.fetchall():
                    clean = txt.strip("{}[]\"'")
                    vals = [float(x.strip()) for x in clean.split(",") if x.strip()]
                    result.append((row_id, vals))
                return result
        finally:
            conn.close()