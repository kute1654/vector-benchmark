"""PolarDB-pg（PASE 扩展）的写入阶段。

流程与 pgvector 的 uploader 相同（批量 INSERT → post_upload 建索引），
差异在于：

* 向量以 PostgreSQL 数组字面量 ``{0.1,0.2,...}`` 写入 ``float4[]`` 列，
  并用 ``CAST(%s AS float4[])`` 显式转换；
* 建索引语句是 PASE 的 ``pase_hnsw`` / ``pase_ivfflat``，维度写在 reloption
  而不是列类型上；
* cosine 数据集在建索引前会删除全零向量行，否则它们会污染 HNSW 图
  （原因见 :meth:`PolarDBUploader._remove_degenerate_rows`）。
"""

import math
import os
import threading
import time
from typing import Callable, List, Optional

import psycopg2
from psycopg2.extras import execute_values

from benchmark.cli_output import sql as sql_log, stage, step
from engine.base_client import BaseUploader
from engine.base_client.distances import Distance
from engine.clients.polardb.config import (
    PASE_MAX_DIM, POLARDB_DATABASE_NAME, POLARDB_DEFAULT_PASSWD,
    POLARDB_DEFAULT_PORT, POLARDB_DEFAULT_USER, distance_ds,
    validate_table_name,
)


class PolarDBUploader(BaseUploader):
    """PolarDB 写入器；连接与配置挂在类属性上，供每个 worker 进程独立初始化。"""

    connection = None
    upload_params = {}
    table_name = None
    vector_size = 0

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params, upload_params, extra_columns_name, extra_columns_type):
        """在 worker 进程内建立连接，并缓存表名与向量维度。

        这里与 pgvector 不同：``vector_size`` 取的是 ``upload_params`` 里的
        ``_vector_size``（真正的维度，由 ClientFactory 注入），而不是
        ``vector_count``；因为 PASE 索引的 ``dim`` reloption 需要准确维度。
        """
        cls.connection = psycopg2.connect(
            host=connection_params.get("host", "127.0.0.1"),
            port=connection_params.get("port", POLARDB_DEFAULT_PORT),
            user=connection_params.get("user", POLARDB_DEFAULT_USER),
            password=connection_params.get("password", POLARDB_DEFAULT_PASSWD),
            database=connection_params.get("database", POLARDB_DATABASE_NAME),
        )
        cls.upload_params = upload_params or {}
        cls.table_name = validate_table_name(connection_params.get("table", "vec_items"))
        cls.vector_size = int(cls.upload_params.get("_vector_size", 0) or 0)

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]):
        """写入一批记录：一次 ``execute_values`` 拼多行 INSERT。

        列顺序为 ``id, vector, 标量列...``；标量列名取自本批第一条 metadata
        （为 None 时该批不带标量列）。向量转成 PostgreSQL 数组字面量
        ``{0.1,0.2,...}``，NaN 改写成 0.0（PASE/pgvector 都不接受 NaN）；
        插入模板里对第二列显式 ``CAST(%s AS float4[])``，避免被当成 text[]。

        最多重试 3 次（每次间隔 1s），失败先 rollback；仍失败则抛给进程池。
        """
        columns = ["id", "vector"]
        meta_columns = list((metadata[0] or {}).keys()) if metadata else []
        columns.extend(meta_columns)
        rows = []
        for row_id, vector, meta in zip(ids, vectors, metadata):
            values = [row_id, "{" + ",".join('0.0' if math.isnan(x) else str(x) for x in vector) + "}"]
            values.extend((meta or {}).get(k) for k in meta_columns)
            rows.append(tuple(values))
        for attempt in range(3):
            try:
                with cls.connection.cursor() as cursor:
                    execute_values(
                        cursor,
                        f"INSERT INTO {cls.table_name} ({', '.join(columns)}) VALUES %s",
                        rows, template="(" + ",".join(
                            # pase 向量列是 float4[], 文本 '{0.1,0.2,...}' 直接 cast
                            ["%s", "CAST(%s AS float4[])"] + ["%s"] * len(meta_columns)
                        ) + ")",
                    )
                cls.connection.commit()
                return
            except Exception:
                cls.connection.rollback()
                if attempt == 2:
                    raise
                time.sleep(1)

    @classmethod
    def _remove_degenerate_rows(cls, distance):
        """Drop rows whose (normalised) vector is all-zero.

        cosine 场景下 uploader 拿到的已经是 reader 归一化过的向量; 零范数的行
        归一化时算出 0/0 = NaN, 再被 upload_batch 的 math.isnan 兜底改写成 0.0,
        于是这些行在库里变成全 0 向量。

        pase 的 HNSW 与 `<?>` 算子只实现了平方 L2 (见 hnsw/hnsw_utils.c 的
        Distance() = fvec_L2sqr), 所以 cosine 只能用 "归一化 + L2" 近似。对一条
        单位查询向量来说, 全 0 行的平方 L2 距离恒为 1.0; 而真正的近邻若余弦
        相似度 < 0.5, 其平方 L2 = 2 - 2cos 就 > 1.0 —— 也就是说全 0 行反而比
        "相似度不到 0.5" 的真实近邻更近。nytimes-256-angular 这类低余弦数据集
        (典型 cos ≈ 0.4~0.7) 上, 239 个全 0 行会成片涌入 top-k, 召回率因此掉到
        0.02 左右。pgvector 用的是真余弦 (`<=>`), 全 0 行距离为 1.0/NaN 而沉底,
        所以不受影响。

        这些行没有任何方向信息, 在 cosine 语义下无意义, 且数据集的 ground truth
        也从不引用它们 (该数据集 10000 条查询 top-100 中 0 次命中全 0 行), 因此在
        建索引前直接删除。删除放在建索引之前, 保证 HNSW 图里不会残留这些
        "聚集在距离 1.0" 的伪近邻。
        """
        if distance != Distance.COSINE or cls.vector_size <= 0:
            return
        zero = f"array_fill(0.0::float4, ARRAY[{int(cls.vector_size)}])::float4[]"
        with cls.connection.cursor() as cursor:
            cursor.execute(f"DELETE FROM {cls.table_name} WHERE vector = {zero}")
            removed = cursor.rowcount
        cls.connection.commit()
        if removed:
            step(f"removed {removed} degenerate all-zero vector row(s) before index build "
                 f"(zero-norm input normalised to NaN, coerced to 0.0)")

    @classmethod
    def post_upload(cls, distance):
        """写入后收尾：清理退化行 → 建 PASE 索引 →（可选）ANALYZE。

        返回 ``{"vector_index_build_time": 秒, "index_type": ...}``；建索引耗时
        由独立的 ``post_upload`` 阶段统计，不混入 ``upload_time``。

        ``index_type == "none"`` 时直接返回空字典（搜索走顺序扫描）。
        PASE 索引要求维度 <= ``PASE_MAX_DIM``（512），超限直接报错而不是
        静默退化为顺序扫描，避免"以为在用 ANN 索引"的误判。
        """
        cls._remove_degenerate_rows(distance)
        index_type = str(cls.upload_params.get("_index_type", cls.upload_params.get("index_type", "hnsw")) or "").lower()
        if not index_type or index_type == "none":
            return {}
        params = cls.upload_params.get("index_params") or {}
        index_name = f"{cls.table_name}_{index_type}_idx"
        dim = int(cls.vector_size or 0)

        if dim > PASE_MAX_DIM:
            raise RuntimeError(
                f"PolarDB pase index requires dim <= {PASE_MAX_DIM} (PASE_MAX_DIM), got {dim}; "
                f"this dataset cannot build a pase index"
            )

        # 索引名带表名，重跑实验时 DROP TABLE ... CASCADE 会一并清掉。
        if index_type == "hnsw":
            # config.json 用 pgvector/ck 风格参数 ef_c / m; 映射到 pase reloptions:
            #   m  -> base_nb_num (HNSW 建图时每个节点的邻居数)
            #   ef_c / ef_construction -> ef_build (建图时候选集大小)
            #   ef_search          -> 构建期默认搜索 ef (查询向量文本里的 :extra 可覆盖)
            base_nb_num = int(params.get('base_nb_num', params.get('m', 16)) or 16)
            ef_build = int(params.get('ef_build', params.get('ef_c', params.get('ef_construction', 40))) or 40)
            ef_search = int(params.get('ef_search', 100) or 100)
            sql = (
                f"CREATE INDEX {index_name} ON {cls.table_name} "
                f"USING pase_hnsw (vector) WITH ("
                f"dim = {dim}, base_nb_num = {base_nb_num}, ef_build = {ef_build}, "
                f"ef_search = {ef_search}, base64_encoded = 0)"
            )
        elif index_type == "ivfflat":
            # IVFFlat 走聚类中心；distance_type 必须与查询时的 ds 一致，
            # 否则索引结构按错误的度量建立，召回会明显异常。
            lists = int(params.get("lists", 100))
            dist_type = distance_ds(distance)   # ds: 0 = L2, 1 = inner product
            sql = (
                f"CREATE INDEX {index_name} ON {cls.table_name} "
                f"USING pase_ivfflat (vector) WITH ("
                f"dimension = {dim}, distance_type = {dist_type}, "
                f"clustering_type = 1, clustering_params = 'lists={lists}')"
            )
        else:
            raise RuntimeError(f"PolarDB does not support index_type={index_type}")
        stage("POST UPLOAD")
        sql_log(sql)
        build_begin = time.perf_counter()
        # pase HNSW 建索引是一个长时间阻塞的 DDL。把它放到后台线程执行，
        # 主线程每隔 heartbeat_every 秒输出一次提示（与其它数据库索引阶段的
        # 定期进度提示保持一致），让用户能实时看到建索引仍在进行而不是"卡死"。
        _build_errors: List[Exception] = []

        def _run_ddl() -> None:
            """后台线程执行的建索引 DDL；异常收集到 ``_build_errors`` 而非直接抛出。

            线程里抛出的异常不会传回主线程，因此先存起来，join 之后由主线程
            统一判断是否失败（这样才能保留心跳输出并给出完整错误上下文）。
            """
            try:
                # High-concurrency index build: use 90% of available device
                # threads, capped by max_worker_processes. Only the three
                # session-settable parallel parameters are set here.
                with cls.connection.cursor() as cursor:
                    cursor.execute("SHOW max_worker_processes")
                    max_worker_processes = int(cursor.fetchone()[0])
                    desired_workers = max(1, int(len(os.sched_getaffinity(0)) * 0.9))
                    target_workers = min(desired_workers, max_worker_processes)
                    step(
                        f"Setting parallel workers to 90% of available threads "
                        f"({desired_workers}), capped by max_worker_processes "
                        f"({max_worker_processes}) -> {target_workers}..."
                    )
                    cursor.execute(f"SET max_parallel_workers = {target_workers}")
                    cursor.execute(f"SET max_parallel_maintenance_workers = {target_workers}")
                    cursor.execute(f"SET max_parallel_workers_per_gather = {target_workers}")
                with cls.connection.cursor() as cursor:
                    cursor.execute(sql)
                cls.connection.commit()
            except BaseException as exc:  # noqa: BLE001
                _build_errors.append(exc)

        build_thread = threading.Thread(target=_run_ddl, daemon=True)
        build_thread.start()
        while build_thread.is_alive():
            build_thread.join(timeout=30)
            if build_thread.is_alive():
                step(f"⏳ index building in progress... elapsed {time.perf_counter() - build_begin:.1f}s")
        # 后台线程的异常不能直接抛出（会在子线程里丢失），统一收集后由主线程
        # 重新抛出，让进程池能感知建索引失败并终止实验。
        if _build_errors:
            raise _build_errors[0]
        build_time = time.perf_counter() - build_begin
        step(f"✓ Vector index built successfully in {build_time:.3f}s")
        # `optimize: true` (ck/myscale convention) -> ANALYZE so the planner has
        # fresh statistics for both custom and generic plans
        # 中文：统计信息缺失会让 custom/generic plan 的选择失真，影响计划缓存对比。
        if cls.upload_params.get("optimize"):
            analyze_begin = time.perf_counter()
            sql_log(f"ANALYZE {cls.table_name}")
            with cls.connection.cursor() as cursor:
                cursor.execute(f"ANALYZE {cls.table_name}")
            cls.connection.commit()
            step(f"ANALYZE finished, time: {time.perf_counter() - analyze_begin:.3f}s")
        return {"vector_index_build_time": build_time, "index_type": index_type}