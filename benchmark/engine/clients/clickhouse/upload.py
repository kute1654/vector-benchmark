"""ClickHouse 写入阶段：批量 INSERT → OPTIMIZE → 向量索引。

与 MyScale 的 uploader 结构几乎相同（两者都兼容 ClickHouse 协议），差别在
建索引语法：ClickHouse 26.x 用 ``vector_similarity`` 跳过索引，且必须
``ADD INDEX`` + ``MATERIALIZE INDEX`` 两步——前者只写元数据，后者才真正在
历史数据上构建（以异步 mutation 形式执行，需要轮询 ``system.mutations``）。

同样支持两种协议：tcp（9000，clickhouse_driver）与 http（8123，
clickhouse_connect）。
"""

import time
import sys
from typing import List, Optional

import clickhouse_connect
from clickhouse_connect.driver.client import Client
from clickhouse_driver import Client as DriverClient
import json

from benchmark.cli_output import sql, stage, status, step, set_live_line, warn
from engine.base_client import BaseUploader
from engine.base_client.utils import format_bytes
from engine.clients.clickhouse.config import *


class ClickHouseUploader(BaseUploader):
    """ClickHouse 写入器；连接与参数挂在类属性上，供 worker 进程初始化。"""

    client: Client = None
    upload_params = {}
    distance: str = None
    table_name: str = None
    protocol: str = "tcp"

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        """在 worker 进程内建立连接，缓存表名与度量函数名。

        ``distance`` 经 ``DISTANCE_MAPPING`` 转成 ClickHouse 的距离函数名
        （``L2Distance`` / ``cosineDistance`` / ``dotProduct``），建索引与查询
        两处必须使用同一个函数，否则索引不会被查询使用。
        """
        cls.protocol = connection_params.get("protocol", "tcp")
        if cls.protocol == "tcp":
            cls.client = DriverClient(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 9000),
                user=connection_params.get("user", CLICKHOUSE_DEFAULT_USER),
                password=connection_params.get("password", CLICKHOUSE_DEFAULT_PASSWD),
                database="default",
            )
        else:
            cls.client = clickhouse_connect.get_client(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 8123),
                username=connection_params.get("user", CLICKHOUSE_DEFAULT_USER),
                password=connection_params.get("password", CLICKHOUSE_DEFAULT_PASSWD),
                database="default",
            )
        cls.upload_params = upload_params
        cls.distance = DISTANCE_MAPPING[distance]
        cls.table_name = validate_table_name(connection_params.get("table", CLICKHOUSE_DATABASE_NAME))

    @classmethod
    def command(cls, sql: str):
        """执行 DDL/DML；两个客户端方法名不同，这里统一（同 MyScale uploader）。"""
        if cls.protocol == "tcp":
            return cls.client.execute(sql)
        return cls.client.command(sql)

    @classmethod
    def query(cls, sql: str):
        """执行查询并统一返回行列表（tcp 的 execute / http 的 result_rows）。"""
        if cls.protocol == "tcp":
            return cls.client.execute(sql)
        return cls.client.query(sql).result_rows

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]):
        """写入一批记录：列顺序 ``id, vector[, 标量列...]``。

        与 MyScale 不同，ClickHouse 的表**总是**有 vector 列（没有纯文本检索
        模式），所以 ``vector`` 无条件写入。标量列名取自本批第一条 metadata；
        经纬度 dict 转成 ``(lon, lat)`` 元组以匹配 ``Point`` 列。

        失败处理：打印后 sleep 3 秒无限重试（扛短暂故障，持续失败需人工介入）。
        """
        if len(ids) != len(vectors):
            raise RuntimeError("clickhouse batch upload unhealthy")

        col_list = ['id']
        col_list.append('vector')

        # Getting the names of structured data columns based on the first meta information.
        # 用第一条记录代表整批的字段顺序（reader 保证同批字段集合一致）。
        if metadata[0] is not None:
            for col_name in list(metadata[0].keys()):
                col_list.append(str(col_name))

        res = []
        for i in range(0, len(ids)):
            temp_list = [ids[i]]
            temp_list.append(vectors[i])

            if metadata[i] is not None:
                for col_name in list(metadata[i].keys()):
                    value = metadata[i][col_name]
                    # Determining if the data is a dictionary type of latitude and longitude.
                    # 注意：``('lon' and 'lat')`` 实际只等价于 ``'lat'``，所以这里
                    # 只校验是否含 'lat'，缺 'lon' 时会取到 None（保持现状）。
                    if isinstance(value, dict) and ('lon' and 'lat') in list(value.keys()):
                        # Keep the correct order of longitude and latitude.
                        temp_list.append(tuple([value.get('lon'), value.get('lat')]))
                    else:
                        temp_list.append(value)
            res.append(temp_list)

        # 无限重试：tcp 用带参数行的 INSERT（SQL 只写 "VALUES"），http 用
        # insert() 显式给列名。
        while True:
            try:
                if cls.protocol == "tcp":
                    columns = ", ".join(col_list)
                    insert_sql = f"INSERT INTO {cls.table_name} ({columns}) VALUES"
                    cls.client.execute(insert_sql, res)
                else:
                    cls.client.insert(cls.table_name, res, column_names=col_list)
                break
            except Exception as e:
                step(f"clickhouse upload exception: {e}")
                time.sleep(3)

    @classmethod
    def post_upload(cls, distance):
        """写入后收尾：OPTIMIZE → ADD INDEX → MATERIALIZE INDEX。

        返回 ``{"vector_index_build_time": 秒}``（从提交 ADD INDEX 算到
        mutation 完成，包含 materialize 的等待）。``_index_type`` 为空或不受
        支持时返回空字典，表示不建索引（搜索走顺序扫描）。

        OPTIMIZE 放在建索引之前：向量索引是按 part 构建的，碎片化的小 part
        会让索引本身碎片化，进而拖慢查询；先合并成单 part 再建索引，索引也
        只有一份。OPTIMIZE 失败不致命（只是索引质量差一点），所以仅告警。
        """
        stage("POST UPLOAD")

        step(f"clickhouse post upload: distance={distance} metric={cls.distance} table={cls.table_name}")

        # 没指定索引类型就不建索引：用于"顺序扫描"基线对比。
        index_type = cls.upload_params.get("_index_type", "")
        if not index_type:
            warn("no index_type specified, skipping vector index creation")
            return {}

        # ClickHouse 26.6.1.1 向量索引语法（从源码确认）：
        # INDEX name vector TYPE vector_similarity('hnsw', 'distance_function', dimensions,
        #     [quantization, hnsw_max_connections_per_layer, hnsw_candidate_list_size_for_construction])
        #
        # 参数说明：
        # 1. method: 'hnsw'（目前只支持 hnsw）
        # 2. distance_function: 'L2Distance', 'cosineDistance', 'dotProduct'
        # 3. dimensions: 向量维度
        # 4. quantization（可选）: 'f64', 'f32', 'f16', 'bf16', 'i8', 'b1'，默认 'bf16'
        # 5. hnsw_max_connections_per_layer（可选）: 类似 HNSW 的 m 参数
        # 6. hnsw_candidate_list_size_for_construction（可选）: 类似 HNSW 的 ef_construction 参数

        # 将索引类型映射到 ClickHouse 的 vector_similarity
        supported_index_types = ["hnswflat", "hnsw", "annoy", "usearch", "flat", "vector_similarity"]
        if index_type.lower() not in supported_index_types:
            warn(f"unsupported index type for ClickHouse: {index_type}. Supported types: {supported_index_types}")
            return {}

        # 距离函数：cls.distance 已经通过 DISTANCE_MAPPING 转换为 ClickHouse 函数名
        # （L2Distance, cosineDistance, dotProduct）
        distance_func = cls.distance
        # 调试输出：把本轮 upload_params 打到 stdout，便于事后核对实际生效的
        # 索引参数（这一行是既有的调试打印，保留以备排查）。
        print(cls.upload_params)
        # 构建索引参数（位置参数，不是键值对）
        index_params = cls.upload_params.get("index_params") or {}
        # 获取向量维度（从 configure 阶段设置的 vector_size）
        vector_size = cls.upload_params.get("_vector_size", 0)

        # 获取 HNSW 参数
        m = index_params.get("m", 32)  # hnsw_max_connections_per_layer，默认 32
        ef_c = index_params.get("ef_c", 200)  # hnsw_candidate_list_size_for_construction，默认 200

        # 量化类型（默认 bf16)
        quantization = index_params.get("quantization", "bf16")

        if vector_size > 0:
            # 有向量维度信息，使用完整参数
            index_args = f"'hnsw', '{distance_func}', {vector_size}, '{quantization}', {m}, {ef_c}"
        else:
            # 没有向量维度信息，使用简化参数（只支持 3 个参数的情况）
            warn(f"vector_size not available, using simplified index args (3 parameters)")
            index_args = f"'hnsw', '{distance_func}'"

        # 先合并所有小 parts 为 1 个 part，避免碎片化的向量索引影响查询性能
        # 参考: vector_search_stress_tests_copy.py 的 optimize_table()
        optimize_start = time.perf_counter()
        step("Optimizing table to merge parts before building vector index...")
        try:
            cls.command(f"OPTIMIZE TABLE {cls.table_name} FINAL")
            # 等待 OPTIMIZE 完成：轮询 system.merges，直到本表没有进行中的
            # merge。按 table 名过滤（没有按 database 过滤），同名表不会并存。
            while True:
                rows = cls.query(
                    f"SELECT COUNT(*) FROM system.merges WHERE table = '{cls.table_name}'"
                )
                if rows and rows[0][0] == 0:
                    break
                step("Waiting for OPTIMIZE TABLE to complete...")
                time.sleep(5)
            optimize_elapsed = time.perf_counter() - optimize_start
            step(f"OPTIMIZE TABLE completed in {optimize_elapsed:.1f}s")
        except Exception as e:
            warn(f"OPTIMIZE TABLE failed (non-fatal): {e}")
            
        # 创建向量索引定义
        # ClickHouse 26.6.1.1 使用 vector_similarity 索引类型
        # ADD INDEX 只是创建索引定义，不会立即构建索引数据
        index_create_str = (
            f"ALTER TABLE {cls.table_name} ADD INDEX vector_index "
            f"vector TYPE vector_similarity({index_args})"
        )
        # 计时从"提交索引定义"开始，包含后续 MATERIALIZE 与轮询等待。
        vector_index_begin_time = time.perf_counter()
        sql(index_create_str)
        try:
            cls.command(index_create_str)
            step(f"vector index definition added successfully")
        except Exception as e:
            warn(f"failed to add vector index: {e}")
            return {}

        # 使用 MATERIALIZE INDEX 触发实际的索引构建
        # 这会创建一个 mutation 来在现有数据上构建索引
        materialize_str = f"ALTER TABLE {cls.table_name} MATERIALIZE INDEX vector_index"
        sql(materialize_str)
        try:
            cls.command(materialize_str)
            step(f"MATERIALIZE INDEX triggered")
        except Exception as e:
            warn(f"failed to materialize vector index: {e}")
            return {}

        # 等待 MATERIALIZE INDEX mutation 完成
        # 通过检查 system.mutations 表来跟踪 mutation 状态
        check_mutation_sql = (
            f"SELECT mutation_id, command, is_done, latest_fail_reason "
            f"FROM system.mutations "
            f"WHERE database = 'default' AND table = '{cls.table_name}' AND is_done = 0 "
            f"ORDER BY create_time DESC LIMIT 1"
        )
        # 每 5s 轮询一次：没有未完成 mutation 即视为建成；出现 fail_reason
        # 则告警并提前退出（继续等下去也不会成功）；每 30s 打一行进度。
        last_log = 0.0
        index_wait_begin = time.perf_counter()
        while True:
            time.sleep(5)
            try:
                rows = cls.query(check_mutation_sql)
                if not rows:
                    # 没有未完成的 mutation，说明构建完成
                    elapsed = time.perf_counter() - index_wait_begin
                    step(f"vector index build completed in {elapsed:.1f}s")
                    break
                else:
                    mutation_id = rows[0][0]
                    command = rows[0][1]
                    is_done = rows[0][2]
                    fail_reason = rows[0][3] if len(rows[0]) > 3 else ""
                    elapsed = time.perf_counter() - index_wait_begin
                    if fail_reason:
                        warn(f"vector index build failed: {fail_reason}")
                        break
                    if (elapsed - last_log) >= 30.0:
                        step(f"vector index building: mutation_id={mutation_id} "
                             f"is_done={is_done} elapsed={elapsed:.0f}s")
                        last_log = elapsed
            except Exception as e:
                # 查询失败（元数据未可见、连接抖动）不终止等待，稍后重试。
                step(f"mutation status check failed: {e}")
                time.sleep(3)
                continue
        # 注意：这里包含了等待 mutation 的时间，建索引耗时与 upload_time 分开统计。
        vector_index_build_time = time.perf_counter() - vector_index_begin_time

        return {
            "vector_index_build_time": vector_index_build_time,
        }
