"""MyScale 写入阶段：批量 INSERT →（可选）optimize / 全文索引 → 向量索引。

MyScale 兼容 ClickHouse 协议，因此这里有两套客户端：

* ``protocol == "tcp"``：``clickhouse_driver.Client``，走 9000 端口，插入用
  ``INSERT ... VALUES`` + 嵌套列表；
* 其它（http）：``clickhouse_connect``，走 8123 端口，插入用它的
  ``insert(table, rows, column_names=...)`` 接口。

两套客户端的 ``execute`` / ``command`` / ``query`` 返回结构不同，统一封装在
:meth:`MyScaleUploader.command` 与 :meth:`MyScaleUploader.query` 里。

``post_upload`` 与 pgvector/PolarDB 不同：MyScale 的向量索引是表的
"ALTER TABLE ... ADD VECTOR INDEX"，建索引是后台异步任务，必须轮询
``system.vector_indices.status`` 直到 ``Built``；同时可选地对表做
``optimize final`` 与 fts 索引的 materialize。
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
from engine.clients.myscale.config import *


class MyScaleUploader(BaseUploader):
    """MyScale 写入器；连接与参数挂在类属性上，供 worker 进程内的 ``init_client`` 初始化。"""

    client: Client = None
    upload_params = {}
    distance: str = None
    table_name: str = None
    protocol: str = "tcp"

    # def get_other_client(self):
    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params, upload_params,
                    extra_columns_name: list, extra_columns_type: list):
        """在 worker 进程内建立连接并缓存表名/度量。

        ``protocol`` 决定用哪个客户端：默认 ``tcp``（9000，clickhouse_driver），
        其余值走 http（8123，clickhouse_connect）。``distance`` 映射成 MyScale
        索引的 ``metric_type``（如 ``Cosine`` / ``L2`` / ``IP``）。
        """
        cls.protocol = connection_params.get("protocol", "tcp")
        if cls.protocol == "tcp":
            cls.client = DriverClient(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 9000),
                user=connection_params.get("user", MYSCALE_DEFAULT_USER),
                password=connection_params.get("password", MYSCALE_DEFAULT_PASSWD),
                database="default",
            )
        else:
            cls.client = clickhouse_connect.get_client(
                host=connection_params.get("host", "127.0.0.1"),
                port=connection_params.get("port", 8123),
                username=connection_params.get("user", MYSCALE_DEFAULT_USER),
                password=connection_params.get("password", MYSCALE_DEFAULT_PASSWD),
                database="default",
            )
        cls.upload_params = upload_params
        cls.distance = DISTANCE_MAPPING[distance]
        cls.table_name = validate_table_name(connection_params.get("table", MYSCALE_DATABASE_NAME))

    @classmethod
    def command(cls, sql: str):
        """执行 DDL/DML 类语句；两个客户端的方法名不同，这里做一层归一。

        ``clickhouse_driver`` 只有一个 ``execute``；``clickhouse_connect`` 区分
        ``command``（不返回结果集）与 ``query``（返回结果集）。
        """
        if cls.protocol == "tcp":
            return cls.client.execute(sql)
        return cls.client.command(sql)

    @classmethod
    def query(cls, sql: str):
        """执行查询并统一返回"行列表"。

        http 客户端返回的 ``QueryResult`` 带 ``result_rows`` 属性，tcp 客户端
        的 ``execute`` 本身就返回行列表，因此统一成 ``rows[i][j]`` 的访问方式。
        """
        if cls.protocol == "tcp":
            return cls.client.execute(sql)
        return cls.client.query(sql).result_rows

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]):
        """写入一批记录：列顺序 ``id[, vector][, 标量列...]``。

        ``_only_text_search`` 为真时不写向量列（纯文本检索场景的表根本没有
        vector 列，写进去会报列不存在）。标量列名取自本批第一条 metadata，
        因此同一批内所有记录的字段集合必须一致。

        经纬度 dict 转成 ``(lon, lat)`` 元组：MyScale 的 Point 列是元组类型，
        顺序必须是经度在前。

        tcp 协议下 ``execute(insert_sql, res)`` 的第二参数就是数据行（驱动会做
        参数替换），http 协议下用 ``insert`` 接口显式给出列名。

        失败处理：打印后 sleep 3 秒无限重试，用于扛住数据库短暂不可用；持续
        错误会让进程一直循环，需要外部干预。
        """
        if len(ids) != len(vectors):
            raise RuntimeError("myscale batch upload unhealthy")

        col_list = ['id']
        only_text_search = cls.upload_params.get("_only_text_search", False)
        if not only_text_search:
            col_list.append('vector')

        # Getting the names of structured data columns based on the first meta information.
        # 用第一条记录的字段顺序代表整批（reader 保证同批字段集合一致）；
        # metadata 与 ids 等长且批次非空，所以这里可以直接取 [0]。
        if metadata[0] is not None:
            for col_name in list(metadata[0].keys()):
                col_list.append(str(col_name))

        res = []
        for i in range(0, len(ids)):
            temp_list = [ids[i]]
            if not only_text_search:
                temp_list.append(vectors[i])

            if metadata[i] is not None:
                for col_name in list(metadata[i].keys()):
                    value = metadata[i][col_name]
                    # Determining if the data is a dictionary type of latitude and longitude.
                    # 注意：``('lon' and 'lat')`` 先求值成 ``'lat'``，所以实际只校验了
                    # 是否含 'lat'；含 'lat' 但缺 'lon' 时会取到 None。保持现状以免
                    # 改变现有行为，这里仅作说明。
                    if isinstance(value, dict) and ('lon' and 'lat') in list(value.keys()):
                        # Keep the correct order of longitude and latitude.
                        temp_list.append(tuple([value.get('lon'), value.get('lat')]))
                    else:
                        temp_list.append(value)
            res.append(temp_list)

        # 无限重试直到成功：写入阶段的短暂失败（连接抖动、表仍在建）不应让
        # 整个实验失败，但持续失败会在此处死循环，需要人工介入。
        while True:
            try:
                if cls.protocol == "tcp":
                    # tcp 驱动支持带参数的批量 VALUES：SQL 里只写 "VALUES"，
                    # 行数据作为第二个参数交给驱动按列类型序列化。
                    columns = ", ".join(col_list)
                    insert_sql = f"INSERT INTO {cls.table_name} ({columns}) VALUES"
                    cls.client.execute(insert_sql, res)
                else:
                    cls.client.insert(cls.table_name, res, column_names=col_list)
                break
            except Exception as e:
                step(f"myscale upload exception: {e}")
                time.sleep(3)

    @classmethod
    def post_upload(cls, distance):
        """写入后收尾：optimize → fts 索引 → 向量索引（异步轮询到 Built）。

        返回 ``{"vector_index_build_time": 秒}``；纯文本检索（``only_text_search``）
        场景不建向量索引，返回空字典。

        顺序有讲究：fts 索引的 DDL 先拼好，但要等 ``optimize final`` 之后再
        执行并 materialize —— optimize 会重写 part，先建索引再 optimize 等于
        让索引白建立一次。
        """
        stage("POST UPLOAD")

        # 注意这里 get 的默认值是隐式 None（多写了一个逗号），等价于
        # upload_params.get("_only_text_search")，即缺省视为 False。
        only_text_search = cls.upload_params.get("_only_text_search",)
        result_group = str(cls.upload_params.get("_result_group", "") or "")
        # hybrid_search 数据集本应带索引配置；如果 ClientFactory 判定为纯文本，
        # 说明配置里没给 index_type，明确告警而不是静默少建一个索引。
        if only_text_search:
            if result_group == "hybrid_search":
                warn("hybrid_search dataset expects upload_params.index_type, but it is not set: falling back to text_search (no vector index will be built).")
                step(f"myscale post upload: table={cls.table_name} (hybrid dataset, text search only, no vector index)")
            else:
                step(f"myscale post upload: table={cls.table_name} ")
        else:
            step(f"myscale post upload: distance={distance} metric={cls.distance} table={cls.table_name}")

        # MSTG/MSRQ 索引需要先设置表的默认存储模式：0 = 内存，1 = 磁盘。
        # 该设置必须落在索引创建之前，否则索引会按旧模式建立。
        index_type = cls.upload_params.get("_index_type", "")
        if index_type == "MSTG" or index_type == "MSRQ":
            mstg_disk_mode = cls.upload_params.get("_mstg_disk_mode", 0)
            mstg_mode_sql = f"ALTER TABLE {cls.table_name} MODIFY SETTING default_mstg_disk_mode = {mstg_disk_mode};"
            sql(mstg_mode_sql)
            cls.command(mstg_mode_sql)

        # full-text search 索引：fts_idx_cols 列出要建索引的文本列，
        # fts_idx_params 是"列名 -> 分词/构建参数"的字典。这里只拼 DDL，
        # 真正执行放到 optimize 之后。索引名带 4 位随机后缀，避免重跑实验时
        # 与已存在（或未清理）的索引重名。
        fts_idx_cols = cls.upload_params.get("fts_idx_cols", None) or []
        fts_fts_params = cls.upload_params.get("fts_idx_params", None) or {}
        fts_index_names = []
        fts_add_ddls = []

        if fts_idx_cols:
            for col in list(fts_idx_cols):
                col_name = str(col).strip()
                if not col_name:
                    continue
                idx_name = f"fts_{col_name}_{get_random_string(4)}"
                col_params = fts_fts_params.get(col_name, {})
                fts_json = json.dumps({col_name: col_params})
                add_fts_sql = (
                    f"ALTER TABLE {cls.table_name} ADD INDEX {idx_name} ({col_name}) "
                    f"TYPE fts('{fts_json}')"
                )
                fts_index_names.append(idx_name)
                fts_add_ddls.append(add_fts_sql)

        # ---- optimize：等后台 merge 收敛到单个 part ----
        # 写入会产生大量小 part，查询时元数据开销和索引效率都会变差。默认
        # 执行（可用 upload_params.optimize=false 关闭）。流程是轮询
        # system.parts / system.merges：还有 merge 在跑就只等待，否则发一次
        # ``optimize table ... final``；直到 active part 数为 1 且无 merge。
        if cls.upload_params.get("optimize", True):
            optimize_begin_time = time.perf_counter()
            check_parts_str = f"select count(*) from system.parts where database='default' and table='{cls.table_name}' and active=1"
            check_merges_str = f"select count(*) from system.merges where database='default' and table='{cls.table_name}'"
            last_line_len = 0
            # 每 3 秒采样一次；状态用 \r 覆盖同一行输出（set_live_line 让日志
            # 系统知道当前行是"活"的，结束时用换行收尾）。
            while True:
                time.sleep(3)
                try:
                    parts = int(cls.query(check_parts_str)[0][0])
                except Exception as e:
                    # 查询失败通常是表元数据尚未可见，重试即可。
                    step(f"checking parts count failed: {e}")
                    continue

                # merges 查询失败记成 -1（未知），不影响主循环退出条件。
                try:
                    merges = int(cls.query(check_merges_str)[0][0])
                except Exception:
                    merges = -1

                elapsed = time.perf_counter() - optimize_begin_time
                if merges > 0:
                    line = " - waiting merge"
                else:
                    line = f" - optimize status: parts={parts} merges={merges} elapsed={elapsed:.0f}s"

                padding = " " * max(0, last_line_len - len(line))
                sys.stdout.write("\r" + line + padding)
                sys.stdout.flush()
                set_live_line(True)
                last_line_len = len(line)

                if parts == 1 and merges == 0:
                    sys.stdout.write("\n")
                    sys.stdout.flush()
                    set_live_line(False)
                    break

                # 有 merge 在跑时不发 optimize，避免与新 merge 抢 IO；
                # 等 merge 自己收敛后再判断。
                if merges > 0:
                    continue

                # 没有 merge 但 part 仍多于 1 个：主动触发一次 final optimize。
                # 该语句本身可能同步等待较久，失败（超时/被取消）不致命，
                # 下一轮循环会重试。
                try:
                    optimize_str = f"optimize table {cls.table_name} final"
                    sql(optimize_str)
                    cls.command(optimize_str)
                except Exception as e:
                    step(f"optimize failed: {e}")

            optimize_time = time.perf_counter() - optimize_begin_time
            step(f"optimize finished, time: {optimize_time:.3f}s")

        # ---- fts 索引：先 ADD INDEX，再 MATERIALIZE ----
        # ADD INDEX 只改表定义、对已有 part 不生效；MATERIALIZE 才会真正为
        # 历史数据构建索引，并在后台以 mutation 形式执行。
        if fts_add_ddls:
            for ddl in list(fts_add_ddls):
                sql(ddl)
                cls.command(ddl)

        if fts_index_names:
            materialize_begin_time = time.perf_counter()
            for idx_name in list(fts_index_names):
                materialize_sql = f"ALTER TABLE {cls.table_name} MATERIALIZE INDEX {idx_name}"
                sql(materialize_sql)
                cls.command(materialize_sql)

            # 轮询 system.mutations 直到没有未完成的 mutation（is_done=0）。
            # 这里只统计本表的全部 mutation，所以若同时有别的 mutation 在跑
            # 也会一起等待——对 benchmark 场景（单表独占）足够。
            check_mutations_str = (
                f"select count(*) from system.mutations where database='default' and table='{cls.table_name}' and is_done=0"
            )
            last_log = 0.0
            while True:
                time.sleep(3)
                # 查询失败记成 -1；连续失败超过 600s 就放弃等待（避免因为
                # 权限/版本差异永远卡住），否则每 30s 打一行进度。
                try:
                    pending = int(cls.query(check_mutations_str)[0][0])
                except Exception:
                    pending = -1
                elapsed = time.perf_counter() - materialize_begin_time
                if pending < 0 and elapsed >= 600.0:
                    step(f"fts materialize status: pending_mutations={pending} elapsed={elapsed:.0f}s, stop waiting due to query errors")
                    break
                if elapsed - last_log >= 30.0:
                    step(f"fts materialize status: pending_mutations={pending} elapsed={elapsed:.0f}s")
                    last_log = elapsed
                if pending == 0:
                    break

        # 纯文本检索没有向量列，到此为止（没有索引可建）。
        if only_text_search:
            return {}

        # ---- 向量索引 ----
        # MyScale 的建索引语法是
        #   ALTER TABLE t ADD VECTOR INDEX idx vector type MSTG('metric_type=Cosine', ...)
        # 参数是逗号分隔的 ``'key=value'`` 字符串；metric_type 由 distance 映射
        # 得到并强制放在第一位，用户 index_params 里若重复指定则忽略。
        index_params = cls.upload_params.get("index_params") or {}
        param_items = [f"'metric_type={cls.distance}'"]
        for key, value in index_params.items():
            if str(key).lower() == "metric_type":
                continue
            param_items.append(f"'{key}={value}'")
        index_parameter_str = ",".join(param_items)

        # 索引名带随机后缀，避免重跑实验时与残留索引冲突。
        index_create_str = (
            f"alter table {cls.table_name} add vector index {cls.table_name}_{get_random_string(4)} "
            f"vector type {index_type}({index_parameter_str})"
        )
        vector_index_begin_time = time.perf_counter()
        sql(index_create_str)
        cls.command(index_create_str)

        # waiting for vector index create finished
        # ADD VECTOR INDEX 是异步的：语句返回只代表任务已提交，索引仍在后台
        # 构建。必须轮询 system.vector_indices.status，只有 "Built" 才可查询，
        # 否则搜索阶段会退化为全表扫描或直接报错。
        check_index_status = f"select status from system.vector_indices where database='default' and table='{cls.table_name}'"
        sql(check_index_status)
        last_status = None
        last_log = 0.0
        index_wait_begin = time.perf_counter()
        while True:
            # 5s 一次轮询；状态变化或每满 60s 打一行，避免刷屏。
            time.sleep(5)
            try:
                rows = cls.query(check_index_status)
                current_status = str(rows[0][0]) if rows else "not found"
                elapsed = time.perf_counter() - index_wait_begin
                if current_status != last_status or (elapsed - last_log) >= 60.0:
                    status("vector index", current_status, detail=f"elapsed {elapsed:.0f}s")
                    last_status = current_status
                    last_log = elapsed
                if current_status == "Built":
                    break
            except Exception as e:
                # 查询失败（索引刚提交、元数据还没可见等）不终止等待，
                # 稍后重试。
                step(f"vector index status check failed: {e}")
                time.sleep(3)
                continue
        # 建索引耗时从"提交 DDL"算到"状态变为 Built"，单独统计，
        # 不混入 upload_time。
        vector_index_build_time = time.perf_counter() - vector_index_begin_time

        return {
            "vector_index_build_time": vector_index_build_time,
        }
