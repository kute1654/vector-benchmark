"""驱动式客户端的“数据写入”阶段基类。

流程： ``Pool(initializer=init_client)`` 起 ``upload_params.parallel`` 个
进程 → 主进程用 :func:`iter_batches` 按 ``batch_size`` 流式切批 →
``imap`` 分发给 worker 调 ``upload_batch`` → 写完后单独起一个单进程池调用
``post_upload``（用于等待索引构建完成等收尾工作）。

写入进度按"已完成条数 / 总条数"刷新同一行；``post_upload`` 的耗时单独
计入 ``post_upload`` 字段，不混进 ``upload_time``。
"""

import time
import os
import sys
from multiprocessing import get_context
from typing import Iterable, List, Optional, Tuple

import tqdm

from benchmark.dataset_reader.base_reader import Record
from benchmark.cli_output import compact_kv, step, warn, set_live_line
from engine.base_client.utils import iter_batches, get_mem_available_bytes, format_bytes


class BaseUploader:
    """写入阶段基类；子类实现 ``init_client`` 与 ``upload_batch``。

    由于 worker 是独立进程，连接等状态挂在类属性 ``client`` 上，由
    ``init_client`` 在子进程内初始化。
    """

    client = None

    @staticmethod
    def _warn_memory(label: str, estimated_bytes: int, ratio: float = 0.8):
        """可用内存不足 ``ratio`` 阈值时提示调小并发/批大小（不中断写入）。"""
        available = get_mem_available_bytes()
        if available is None:
            return
        if estimated_bytes >= int(available * ratio):
            warn(
                f"{label} memory warning: estimated {format_bytes(estimated_bytes)} "
                f">= {int(ratio * 100)}% of available {format_bytes(available)}"
            )
            warn("Notice: the process may be killed.")
            warn(
                "If that happens, reduce upload_params.parallel and upload_params.batch_size in the test config (see README.md)."
            )

    def __init__(self, host, connection_params, upload_params):
        """保存写入阶段的连接与批参数；本身不建连（连接在 worker 里建）。

        分三份参数是有意为之：连接信息随部署环境变化，批参数随实验配置变化，
        两者生命周期不同，混成一个 dict 会让"只改 batch_size 却重启连接"这类
        行为变得难以追踪。

        host 为 "host:port"；connection_params 含账号/库名；upload_params
        含 parallel / batch_size 等写入参数。
        """
        self.host = host
        self.connection_params = connection_params
        self.upload_params = upload_params

    @classmethod
    def get_mp_start_method(cls):
        """multiprocessing 启动方式；``None`` 表示使用平台默认值。"""
        return None

    @classmethod
    def init_client(cls, host, distance, vector_count, connection_params: dict, upload_params: dict,
                    extra_columns_name: list, extra_columns_type: list):
        """在每个 worker 进程内初始化客户端连接（必须由子类实现）。

        额外拿到 ``vector_count`` 与标量列定义，便于按总量做分片或建表校验。
        """
        raise NotImplementedError()

    def upload(
            self,
            distance,
            vector_count,
            records: Iterable[Record],
            extra_columns_name: list,
            extra_columns_type: list
    ) -> dict:
        """执行写入阶段，返回各阶段耗时与引擎自报的统计。

        Args:
            distance: 距离度量（建表/写入时用于校验或类型选择）。
            vector_count: 预期写入总量；``<= 0`` 时只跳过进度显示，不影响写入。
            records: 记录流（由数据集 reader 提供），边读边切批，不在内存囤积。
            extra_columns_name / extra_columns_type: 标量列的列名与类型。

        Returns:
            ``{"post_upload": 秒, "upload_time": 秒, "total_time": 秒, ...}``，
            其中 ``...`` 由 ``post_upload`` 的返回值展开合并。
        """
        start = time.perf_counter()
        parallel = self.upload_params.get("parallel", 16)
        batch_size = self.upload_params.get("batch_size", 256)

        compact_kv(
            "upload",
            parallel=parallel,
            batch_size=batch_size,
        )

        ctx = get_context(self.get_mp_start_method())
        with ctx.Pool(
                processes=int(parallel),
                initializer=self.__class__.init_client,
                initargs=(
                        self.host,
                        distance,
                        vector_count,
                        self.connection_params,
                        self.upload_params,
                        extra_columns_name,
                        extra_columns_type,
                ),
        ) as pool:
            uploaded = 0
            last_logged_percent = -1
            last_line_len = 0
            # imap（而非 imap_unordered）保证批次顺序，且任务生成与消费背压
            # 同步：主进程只在 worker 有空位时才读下一批 records，内存可控。
            for batch_count in pool.imap(
                    self.__class__._upload_batch,
                    iter_batches(
                        records,
                        batch_size,
                    ),
            ):
                # worker 返回 None 视为该批被跳过（子类可能过滤无效记录）。
                if batch_count is None:
                    continue
                uploaded += int(batch_count)
                if vector_count > 0:
                    percent = int(uploaded * 100 / vector_count)
                    # 只在百分比变化时重绘，避免刷屏；用空格补齐上一次更长的
                    # 行，防止残留字符。
                    if percent > last_logged_percent:
                        line = f" - upload progress: {uploaded}/{vector_count} ({percent}%)"
                        padding = " " * max(0, last_line_len - len(line))
                        sys.stdout.write("\r" + line + padding)
                        sys.stdout.flush()
                        set_live_line(True)
                        last_line_len = len(line)
                        last_logged_percent = percent
            if vector_count > 0 and last_logged_percent >= 0:
                sys.stdout.write("\n")
                sys.stdout.flush()
                set_live_line(False)
        upload_time = time.perf_counter() - start

        step(f"upload time: {upload_time:.3f}s")

        # 收尾阶段（例如等待索引构建完成）单独计时：它可能远长于网络写入，
        # 混在一起会掩盖真实写入吞吐。
        wait_index_begin = time.perf_counter()
        # 默认用 forkserver 而不是 fork：主进程此时已持有大量连接/线程，
        # fork 可能因锁状态被复制而卡住。Nuitka 打包或 frozen 环境不支持
        # forkserver，退回 fork。
        mp_method = "forkserver"
        if os.environ.get("NUITKA_ONEFILE_PARENT") or getattr(sys, "frozen", False):
            mp_method = "fork"

        try:
            ctx = get_context(mp_method)  # When use None, sometimes it will be blocked.
        except ValueError:
            ctx = get_context()
        with ctx.Pool(
                processes=1,
                initializer=self.__class__.init_client,
                initargs=(self.host,
                          distance,
                          vector_count,
                          self.connection_params,
                          self.upload_params,
                          extra_columns_name,
                          extra_columns_type,),
        ) as pool:
            post_upload_stats = pool.apply(func=self.post_upload, args=(distance,)) or {}

        total_time = time.perf_counter() - start
        post_time = time.perf_counter() - wait_index_begin

        return {
            "post_upload": post_time,
            "upload_time": upload_time,
            "total_time": total_time,
            **post_upload_stats,
        }

    # Upload data[ids, vectors, metadata] and return time consume
    @classmethod
    def _upload_batch(
            cls, batch: Tuple[List[int], List[list], List[Optional[dict]]]
    ) -> int:
        """worker 侧入口：写入一批并返回成功条数（供主进程累加进度）。

        返回 ``len(ids)`` 而非实际写入数，写入失败的批次应由子类直接抛异常，
        让进程池把错误冒泡到主进程。
        """
        ids, vectors, metadata = batch
        cls.upload_batch(ids, vectors, metadata)
        return len(ids)

    @classmethod
    def post_upload(cls, distance):
        """写入后收尾（默认无操作），如等待异步索引构建、刷新表等。"""
        return {}

    @classmethod
    def upload_batch(cls, ids: List[int], vectors: List[list], metadata: List[Optional[dict]]):
        """把一批记录写入引擎（必须由子类实现）。

        ``ids`` / ``vectors`` / ``metadata`` 是等长的三个平行列表；
        ``metadata`` 的元素可为 None（该条记录无标量字段）。
        """
        raise NotImplementedError()