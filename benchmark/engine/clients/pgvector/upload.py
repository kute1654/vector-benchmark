"""pgvector 写入阶段：批量 INSERT + 写入后建索引。

上传被拆成两段：
* ``upload_batch`` 由 ``parallel`` 个 worker 并发调用，只做 INSERT；
* ``post_upload`` 在单独的进程里执行，负责创建 HNSW/IVFFlat 索引
  （建索引是重 CPU/IO 操作，必须等数据全部写完）以及可选的 ANALYZE。

索引创建期间会启动一个后台线程每 30s 打印一次 "elapsed"，让用户知道进程
没有卡死；同时捕获 SIGINT 以便在 Ctrl+C 时清理半成品索引。
"""

import math
import os
import time
import sys
from typing import List, Optional

import psycopg2
from psycopg2.extras import execute_values

from benchmark.cli_output import sql as sql_log, stage, status, step, set_live_line, warn
from engine.base_client import BaseUploader
from engine.base_client.utils import format_bytes
from engine.clients.pgvector.config import *


# Map distance types to PGvector operator classes
# 中文：索引 opclass 必须与查询使用的运算符一致，否则规划器不会选择该索引。
DISTANCE_TO_OPERATOR_CLASS = {
    "<->": "vector_l2_ops",      # L2 distance
    "<#>": "vector_ip_ops",      # Inner product
    "<=>": "vector_cosine_ops"   # Cosine distance
}


class PGVectorUploader(BaseUploader):
    """pgvector 写入器；状态挂在类属性上供子进程内的 ``init_client`` 初始化。"""

    connection = None
    upload_params = {}
    distance_op: str = None
    table_name: str = None
    vector_size: int = 0

    def __init__(self, host, connection_params, upload_params):
        """补齐 pgvector 默认连接参数后交给基类保存（不在此建连）。

        与 Configurator 保持一致：缺省键才用默认值，用户显式配置优先；
        真正的连接在每个 worker 进程的 ``init_client`` 里建立。
        """
        # Set default connection parameters for PGVector
        default_conn_params = {
            "host": "127.0.0.1",
            "port": PGVECTOR_DEFAULT_PORT,
            "user": PGVECTOR_DEFAULT_USER,
            "password": PGVECTOR_DEFAULT_PASSWD,
            "database": PGVECTOR_DATABASE_NAME,
            "table": "vec_items"
        }

        # Merge provided connection_params with defaults
        merged_conn_params = {**default_conn_params, **(connection_params or {})}

        super().__init__(host, merged_conn_params, upload_params)

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        """在每个 worker 进程内建立连接，并缓存本批写入所需的状态。

        ``distance`` 决定索引 opclass；``upload_params`` 里含 ``_index_type``
        等由 ClientFactory 归一化后的内部字段。
        """
        cls.connection = psycopg2.connect(
            host=connection_params.get("host", "127.0.0.1"),
            port=connection_params.get("port", PGVECTOR_DEFAULT_PORT),
            user=connection_params.get("user", PGVECTOR_DEFAULT_USER),
            password=connection_params.get("password", PGVECTOR_DEFAULT_PASSWD),
            database=connection_params.get("database", PGVECTOR_DATABASE_NAME),
        )
        cls.upload_params = upload_params
        cls.distance_op = DISTANCE_MAPPING[distance]
        cls.table_name = validate_table_name(connection_params.get("table", "vec_items"))
        # 注意：这里赋的是"总条数"而非向量维度，名字有误导性。它只在
        # upload_batch 里被当作"本次实验是否有向量列"的布尔标志（> 0 即有），
        # 真正的维度在建表时已经写进列定义，写入阶段用不到。
        cls.vector_size = vector_count  # Use vector_count as vector_size

    @classmethod
    def command(cls, sql_query: str):
        """执行一条 SQL：有结果集返回行，否则提交并返回 None。

        与 Configurator 的 ``command`` 不同，这里不做异常包装/重试，错误直接
        冒泡给进程池。
        """
        with cls.connection.cursor() as cursor:
            cursor.execute(sql_query)
            if cursor.description:
                return cursor.fetchall()
            cls.connection.commit()
            return None

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]):
        """把一批记录写进表里。

        列顺序固定为 ``id[, vector][, 标量列...]``；标量列名取自本批第一条
        非 None 的 metadata，因此同一批内所有记录的字段集合必须一致（数据集
        reader 保证了这一点）。

        两个数据清洗点：
        * 向量转成 ``'[1.0,2.0,...]'`` 文本；pgvector 不支持 NaN，NaN 一律
          写成 0.0（与 L2 距离语义上等价于"该维无信息"）。
        * 经纬度 dict 转成 PostgreSQL ``POINT(lon, lat)`` 字面量。

        失败处理：任意异常都会打印后 sleep 3 秒并重试同一批，直到成功。
        这样能扛住数据库短暂不可用，但也意味着持续的错误会让进程无限循环，
        需要外部干预。
        """
        if len(ids) != len(vectors):
            raise RuntimeError("pgvector batch upload unhealthy")

        # Build column list
        col_list = ['id']
        col_list_str = 'id'

        has_vector = cls.vector_size > 0
        if has_vector:
            col_list.append('vector')
            col_list_str = 'id, vector'

        # Add metadata columns if present
        # 以一个"代表记录"的字段顺序作为整批的列顺序；None 表示该批无标量列。
        meta_columns = []
        if metadata and metadata[0] is not None:
            meta_columns = list(metadata[0].keys())
            for col_name in meta_columns:
                col_list.append(str(col_name))
                col_list_str += f", {col_name}"

        # Prepare data rows
        rows = []
        for i in range(len(ids)):
            row = [ids[i]]

            if has_vector:
                # Convert vector to string format: '[1.0,2.0,3.0]'
                # Replace NaN values with 0.0 since pgvector does not support NaN
                vector_str = '[' + ','.join('0.0' if math.isnan(x) else str(x) for x in vectors[i]) + ']'
                row.append(vector_str)

            if metadata[i] is not None:
                for col_name in meta_columns:
                    value = metadata[i][col_name]
                    # Handle geo coordinates as PostgreSQL POINT
                    if isinstance(value, dict) and ('lon' in value and 'lat' in value):
                        row.append(f"POINT({value['lon']}, {value['lat']})")
                    else:
                        row.append(value)

            rows.append(tuple(row))

        # Insert data using execute_values for better performance
        # execute_values 把多行合并成一条 INSERT ... VALUES (...),(...)，比逐行
        # execute 快一个数量级。
        while True:
            try:
                with cls.connection.cursor() as cursor:
                    placeholders = '(' + ','.join(['%s'] * len(col_list)) + ')'
                    execute_values(
                        cursor,
                        f"INSERT INTO {cls.table_name} ({col_list_str}) VALUES %s",
                        rows,
                        template=placeholders
                    )
                    cls.connection.commit()
                break
            except Exception as e:
                step(f"pgvector upload exception: {e}")
                time.sleep(3)

    @classmethod
    def post_upload(cls, distance):
        """写入后收尾：并行度设置 + 建索引 + 可选 ANALYZE。

        返回 ``{"vector_index_build_time": 秒, ...}``；该字段会被
        ``BaseUploader.upload`` 合并进结果字典，但**不计入** ``upload_time``。
        """
        stage("POST UPLOAD")

        # 索引类型可能来自内部字段 `_index_type`（ClientFactory 归一化过），
        # 也可能直接来自用户配置；统一转小写并兼容 HNSWFLAT 别名。
        index_type = str(
            cls.upload_params.get("_index_type")
            or cls.upload_params.get("index_type", "hnsw")
            or "hnsw"
        ).lower()
        if index_type == "hnswflat":
            index_type = "hnsw"
        index_params = cls.upload_params.get("index_params", {}) or {}

        step(f"pgvector post upload: distance={distance} metric={cls.distance_op} table={cls.table_name} index_type={index_type}")

        # High-concurrency index build: use 90% of available device threads,
        # capped by max_worker_processes. Only the three session-settable
        # parallel parameters are set; max_worker_processes is a PGC_POSTMASTER
        # parameter controlled in postgresql.conf + restart, so we cap to it.
        # 中文：留 10% 的核给后台/客户端进程；max_worker_processes 是
        # postmaster 级参数，只能在 postgresql.conf 改并重启，所以这里只读取
        # 当前值作为上限，不尝试修改它。
        desired_workers = max(1, int(len(os.sched_getaffinity(0)) * 0.9))
        max_worker_processes = int(
            cls.command("SHOW max_worker_processes")[0][0]
        )
        target_workers = min(desired_workers, max_worker_processes)
        step(
            f"Setting parallel workers to 90% of available CPU threads "
            f"({desired_workers}), capped by max_worker_processes "
            f"({max_worker_processes}) -> {target_workers}..."
        )
        # 三个会话级参数分别控制：并行 worker 总数、维护类命令（建索引）
        # 的并行度、以及单个 Gather 节点的并行度。建 HNSW 索引走的是
        # maintenance 路径，因此第二个最关键。
        cls.command(f"SET max_parallel_workers = {target_workers}")
        cls.command(f"SET max_parallel_maintenance_workers = {target_workers}")
        cls.command(f"SET max_parallel_workers_per_gather = {target_workers}")

        # Create HNSW index
        if index_type.lower() == "hnsw":
            # Default HNSW parameters. config.json uses the ck/myscale-style key
            # `ef_c`; `ef_construction` is kept as a fallback.
            # 中文：优先读 ef_construction，没有则读 ClickHouse 风格的 ef_c，
            # 最后默认 64。
            m = index_params.get("m", 16)
            ef_construction = index_params.get("ef_construction", index_params.get("ef_c", 64))

            # Get the correct operator class for the distance type
            operator_class = DISTANCE_TO_OPERATOR_CLASS.get(cls.distance_op, "vector_l2_ops")

            # Build index creation SQL with proper operator class
            index_name = f"{cls.table_name}_idx"
            index_sql = (
                f"CREATE INDEX {index_name} ON {cls.table_name} "
                f"USING hnsw (vector {operator_class}) "
                f"WITH (m = {m}, ef_construction = {ef_construction})"
            )

            sql_log(index_sql)
            step(f"Creating HNSW index on {cls.table_name} (m={m}, ef_construction={ef_construction})...")
            step("⚠️  Press Ctrl+C to cancel (may wait for current transaction to finish)...")

            # ---- 进度提示与中断处理 ----
            import threading
            import signal

            stop_spinner = [False]   # 用 list 代替 nonlocal，便于线程内可见
            cancelled = [False]      # SIGINT 只会置位，真正的中断由数据库超时完成

            def show_progress():
                """后台心跳：每 30 秒打印一次已耗时，直到停止标志置位。

                每 30 秒输出一次已耗时，避免长时间建索引时用户误以为卡死。
                用 daemon 线程 + 轮询 ``stop_spinner`` 而非 Event，是因为
                这里只需要"尽快退出"，对精度没有要求。
                """
                start_time = time.time()
                while not stop_spinner[0]:
                    if not stop_spinner[0]:
                        time.sleep(30)
                    if not stop_spinner[0]:
                        elapsed = int(time.time() - start_time)
                        step(f"⏳ Building HNSW index... elapsed {elapsed}s (in progress, not hung)")
                print("", flush=True)

            def signal_handler(signum, frame):
                """SIGINT 处理器：只置位取消标记并停掉心跳，不直接杀连接。

                不能直接 terminate 服务端会话，只能停止本地等待并让
                statement_timeout / 中断路径收尾。在主线程执行的 CREATE
                INDEX 会随后收到服务端的取消响应。
                """
                cancelled[0] = True
                stop_spinner[0] = True
                print("\n\n⚠️  Cancel requested! Waiting for PostgreSQL to abort...")
                print("   (This may take a few seconds)")

            # Register signal handler for Ctrl+C
            original_handler = signal.signal(signal.SIGINT, signal_handler)

            spinner_thread = threading.Thread(target=show_progress, daemon=True)
            spinner_thread.start()

            try:
                vector_index_begin_time = time.perf_counter()

                # Set a statement timeout to ensure we can eventually cancel
                # Default: 1 hour max, can be overridden by upload_params.index_timeout.
                # Must be session-level SET: `cls.command` commits, and a `SET LOCAL`
                # would be discarded by that commit (the timeout would never apply).
                # 中文：默认 1 小时；注意这里必须用会话级 SET，`command()` 里的
                # commit 会把 `SET LOCAL` 的生效范围结束掉。
                index_timeout = cls.upload_params.get("index_timeout", 3600)
                cls.command(f"SET statement_timeout = '{index_timeout}s'")

                cls.command(index_sql)
                vector_index_build_time = time.perf_counter() - vector_index_begin_time

                if cancelled[0]:
                    step("✗ Index creation cancelled")
                    return {"cancelled": True}

            except KeyboardInterrupt:
                cancelled[0] = True
                step("✗ Index creation interrupted by user")
                # Try to rollback and cleanup
                try:
                    cls.connection.rollback()
                    # Try to drop the partial index if it exists
                    cls.command(f"DROP INDEX IF EXISTS {index_name}")
                except Exception as e:
                    warn(f"Cleanup after cancellation failed: {e}")
                return {"cancelled": True, "error": "interrupted"}

            except Exception as e:
                # 只有用户已请求取消时才吞掉异常；否则交给上层终止实验，
                # 避免"没有索引却继续测搜索"导致结果不可解释。
                if cancelled[0]:
                    step(f"✗ Index creation failed: {e}")
                else:
                    raise

            finally:
                stop_spinner[0] = True
                spinner_thread.join(timeout=2)
                # Restore original signal handler
                signal.signal(signal.SIGINT, original_handler)
                # Restore the connection default (no statement timeout). Roll back
                # first: after a statement timeout the transaction is aborted and
                # SET would raise InFailedSqlTransaction.
                # 中文：先 ROLLBACK 清除 aborted 状态，否则 SET 本身也会失败。
                try:
                    cls.connection.rollback()
                    cls.command("SET statement_timeout = 0")
                except Exception:
                    pass

            step(f"✓ Vector index built successfully in {vector_index_build_time:.3f}s")

            # `optimize: true` (ck/myscale convention) -> ANALYZE so the planner has
            # fresh statistics for both custom and generic plans
            # 中文：不更新统计信息时，custom/generic plan 可能选错路径，
            # 进而让"计划缓存"对比失真。
            if cls.upload_params.get("optimize"):
                analyze_begin = time.perf_counter()
                sql_log(f"ANALYZE {cls.table_name}")
                cls.command(f"ANALYZE {cls.table_name}")
                step(f"ANALYZE finished, time: {time.perf_counter() - analyze_begin:.3f}s")

            return {
                "vector_index_build_time": vector_index_build_time,
            }

        if index_type.lower() == "ivfflat":
            # IVFFlat 的 lists 缺省取 sqrt(n)/10（随数据量增长，避免固定值在
            # 大规模数据上过小），用户可用 index_params.lists 覆盖。
            operator_class = DISTANCE_TO_OPERATOR_CLASS.get(cls.distance_op, "vector_l2_ops")
            index_name = f"{cls.table_name}_ivfflat_idx"
            total = cls.command(f"SELECT count(*) FROM {cls.table_name}")[0][0]
            import math
            lists = int(index_params.get("lists", max(1, int(math.sqrt(max(1, total)) / 10))))
            index_sql = (
                f"CREATE INDEX {index_name} ON {cls.table_name} "
                f"USING ivfflat (vector {operator_class}) WITH (lists = {lists})"
            )
            sql_log(index_sql)
            begin = time.perf_counter()
            cls.command(index_sql)
            return {
                "vector_index_build_time": time.perf_counter() - begin,
                "index_type": "ivfflat",
                "lists": lists,
            }

        # index_type == "none"：不建索引，搜索走顺序扫描（用于基线对比）。
        return {}
