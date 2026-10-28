"""驱动式客户端的“检索”阶段基类。

``search_all()`` 支持三种测量模式（由 ``search_params`` 决定）：

1. ``recall_only=True``：不测 QPS，遍历数据集全部（或查询池指定的）查询，
   逐条计算 recall/MRR。用进程池并行，但每条查询只执行一次。
2. ``test_duration > 0``：固定时长的吞吐测试。每个 worker 随机取查询、
   循环执行，直到主进程置位 ``stop_event``；记录每次查询的延迟。
3. 其余情况：固定条数测试（默认至少 ``1000 * parallel`` 条），把查询切成
   每批 100 条一次性下发。

三种模式共用 ``_search_one()`` 做单次查询与精度计算，最终由
``search_all()`` 汇总成 ``rps / mean_time / p95_time / p99_time / recall``。
注意本层输出的 ``mean_time`` / ``p95_time`` / ``p99_time`` 单位是**秒**
（与 shell/pgbench 路径的 ``*_ms`` 毫秒列不同），写 CSV 时由调用方负责。

并发模型是 ``multiprocessing`` 进程池而非线程池：psycopg2 连接、ClickHouse
客户端在进程内更容易隔离，也避免 GIL 限制。查询池存放在类属性
``shared_queries`` 上，供 fork 出的子进程通过继承直接复用。
"""

import functools
import time
import random
import multiprocessing
import gc
from multiprocessing import get_context
from typing import List, Optional, Tuple
from pathlib import Path
import gzip
import json
import sys

import numpy as np
import tqdm

from benchmark.dataset_reader.base_reader import Query
from benchmark.dataset_reader.utils import _to_uint32_id
from benchmark.cli_output import compact_kv, step, warn
from engine.base_client.utils import mrr, intersect_precision, get_mem_available_bytes, format_bytes


# 未显式配置 top 时的默认邻居数。与各引擎 search 实现里的默认值保持一致。
DEFAULT_TOP = 100


def init_worker(stop_event, cls, init_args):
    """进程池 initializer（固定时长模式）。

    fork 出的子进程没有父进程的实例状态，因此通过类属性把停止信号与客户端
    初始化参数传进去：``cls.stop_event`` 是共享内存里的 Boolean，
    ``cls.init_client(...)`` 在子进程内建立自己的数据库连接。
    """
    cls.stop_event = stop_event
    cls.init_client(*init_args)


class BaseSearcher:
    """检索阶段基类；子类实现 ``init_client`` / ``search_one``。

    类属性而非实例属性是刻意为之：进程池 worker 通过继承拿到同一份查询池，
    避免把上万条查询 pickle 给每个子进程。
    """

    # 查询池：父进程加载一次，fork 后子进程只读共享。
    shared_queries = []
    # 查询池的来源描述（pool_size/query_meta/dataset），用于判断能否复用。
    shared_queries_info = None
    # 固定时长模式下由主进程置位的停止信号（multiprocessing.Value）。
    stop_event = None

    def __init__(self, host, connection_params, search_params):
        """保存连接/检索参数；``search_params`` 浅拷贝一份。

        拷贝是为了隔离调用方：同一份配置字典会被多个 searcher 复用，若直接
        持有引用，任何一方写入（例如 ``setup_search`` 补 ``only_text_search``）
        都会串到别人身上。连接本身不在这里建——由子类的 ``init_client`` 在
        工作进程内建立。

        host 形如 "host:port"；search_params 会做浅拷贝，避免调用方后续修改
        影响本对象的配置。
        """
        self.host = host
        self.connection_params = connection_params
        self.search_params = dict(search_params) if search_params is not None else {}

    @classmethod
    def _apply_session_settings(cls, connection, session_settings: dict):
        """在 ``connection`` 上装配会话级 SETTINGS（默认空实现）。

        ClickHouse/MyScale 子类用它把 ``vector_query_plan_cache`` 等开关以
        会话级 ``SET`` 下发；PostgreSQL 系引擎通常用不到，保持空实现。
        """
        pass

    def post_warmup(self, dataset_config):
        """预热结束后的钩子（默认无操作），子类可在此清缓存或记日志。"""
        return

    @staticmethod
    def _estimate_query_bytes(query: Query) -> int:
        """粗估单条查询对象占用的内存（字节）。

        只用于内存告警，不要求精确：向量按 32 字节/维（Python float 对象
        或 numpy 标量的保守估计），ground truth 按 28 字节/id，其余字段用
        固定开销近似。返回 0 表示没有可估算的数据。
        """
        if query is None:
            return 0
        vector = query.vector or []
        # 64 为列表/对象头开销；32 字节是 Python float 的保守大小
        # （numpy float32 实际更小，这里宁可高估以提前告警）。
        vector_bytes = 64 + len(vector) * 32
        expected_result = query.expected_result or []
        expected_scores = query.expected_scores or []
        expected_bytes = 64 + len(expected_result) * 28
        scores_bytes = 64 + len(expected_scores) * 32
        meta = query.meta_conditions or {}
        meta_bytes = 0
        if meta:
            meta_bytes = 128 + len(meta) * 64
        text_bytes = 0
        if query.query_text:
            text_bytes = 64 + len(query.query_text.encode("utf-8"))
        return vector_bytes + expected_bytes + scores_bytes + meta_bytes + text_bytes + 128

    @staticmethod
    def _warn_memory(label: str, estimated_bytes: int, ratio: float = 0.8):
        """估算内存接近可用内存的 ``ratio`` 时打印告警。

        进程池是 fork 的，查询池在父子进程间是写时复制；但如果查询对象里
        含 numpy 数组等可变结构，实际占用可能翻倍。这里只提示风险与调参
        方向（``search_params.parallel``、``queries_pool_size``），不阻止运行。
        """
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
                "If that happens, reduce search_params.parallel in the config and the dataset queries_pool_size field in datasets.json (see README.md)."
            )

    @staticmethod
    def _compute_total_queries_for_recall_only(dataset_config, query_files, query_meta, default_pool_size: int) -> int:
        """仅测召回时，计算需要遍历的查询总数（不入库、不测 QPS）。

        这类模式要跑完全部带 ground truth 的查询，而不是固定的查询池，因此
        需要按数据集格式分别计数：

        - ``h5``：直接读 HDF5 里 ``test`` 数据集的 shape[0]；读不到就退回
          ``default_pool_size``。
        - ``gz_tsv``：qrels 与 queries 分别记录相关性标注与查询原文，两边
          的 id 都要过 :func:`_to_uint32_id` 归一（qrels 里可能是带前缀的
          字符串 id，queries 里可能是 "_id"），只有同时满足“有正相关标注”
          和“查询记录存在”的 id 才算数。

        任何异常（缺 h5py、文件缺失、格式不符）都静默回退到
        ``default_pool_size``，保证实验不会因为统计口径失败而中断。
        """
        dataset_type = getattr(dataset_config, "type", "")
        if dataset_type == "h5":
            try:
                import h5py  # type: ignore
            except Exception:
                return int(default_pool_size or 0)
            files = list(query_files or [])
            selected_files = []
            # 指定了 query_meta 时只统计匹配的那个文件；找不到匹配项则用全部文件。
            if query_meta is not None:
                for qf in files:
                    if qf.get("meta") == query_meta:
                        selected_files.append(qf)
                        break
            if not selected_files:
                selected_files = files
            total = 0
            for qf in selected_files:
                path = qf.get("path")
                if not path:
                    continue
                # H5 约定：查询向量放在 "test" 组/数据集中，行数即查询条数。
                try:
                    with h5py.File(path, "r") as f:
                        if "test" in f:
                            total += int(f["test"].shape[0])
                except Exception:
                    continue
            if total > 0:
                return total
        if dataset_type == "gz_tsv":
            files = list(query_files or [])
            if not files:
                return int(default_pool_size or 0)
            selected = None
            if query_meta is not None:
                for qf in files:
                    if qf.get("meta") == query_meta:
                        selected = qf
                        break
            if selected is None:
                selected = files[0]
            queries_path = Path(selected.get("path"))
            qrels_name = getattr(dataset_config, "qrels_file", None)
            if not qrels_name:
                return int(default_pool_size or 0)
            qrels_path = queries_path.parent / str(qrels_name)
            if not qrels_path.exists() or not queries_path.exists():
                return int(default_pool_size or 0)

            # 第一遍扫 qrels：收集“至少有一个正相关文档”的 query id。
            # 列约定为 "qid<TAB>docid[<TAB>score]"；score <= 0 视为不相关。
            qids_with_rel = set()
            try:
                with open(qrels_path, "r", encoding="utf-8") as fp:
                    for line in fp:
                        line = line.strip()
                        if not line:
                            continue
                        parts = line.split("\t")
                        if len(parts) < 2:
                            continue
                        if not parts[0].isdigit() or not parts[1].isdigit():
                            continue
                        qid_raw = parts[0]
                        score = 1
                        if len(parts) >= 3:
                            try:
                                score = int(parts[2])
                            except Exception:
                                score = 1
                        if score <= 0:
                            continue
                        qid = _to_uint32_id(qid_raw)
                        qids_with_rel.add(qid)
            except Exception:
                return int(default_pool_size or 0)

            # 第二遍扫 queries（gzip 存放的 JSONL）：只保留在 qrels 中有正相关
            # 标注的 id，因为无标注的查询无法评估召回。
            query_ids = set()
            try:
                with gzip.open(queries_path, "rt", encoding="utf-8") as fp:
                    for line in fp:
                        line = line.strip()
                        if not line:
                            continue
                        row = json.loads(line)
                        raw_qid = row.get("_id", row.get("id", None))
                        if raw_qid is None:
                            continue
                        qid = _to_uint32_id(raw_qid)
                        if qid in qids_with_rel:
                            query_ids.add(qid)
            except Exception:
                return int(default_pool_size or 0)
            if query_ids:
                return len(query_ids)
        return int(default_pool_size or 0)

    @classmethod
    def init_client(cls, host: str, distance, connection_params: dict, search_params: dict):
        """在 worker 进程内建立客户端连接（必须由子类实现）。

        作为 ``Pool(initializer=...)`` 调用，每个 worker 进程执行一次；
        ``self`` 在这里不可用，因此只能依赖类属性和传入参数。
        """
        raise NotImplementedError()

    @classmethod
    def get_mp_start_method(cls):
        """返回 multiprocessing 启动方式；``None`` 表示用平台默认值。

        只有依赖 fork 语义共享类属性的引擎才需要覆盖（默认的 fork 下
        ``shared_queries`` 等类属性在子进程里天然可见）。
        """
        return None

    @classmethod
    def search_one(
        cls,
        vector: List[float],
        meta_conditions,
        top: Optional[int],
        schema: Optional[dict],
        query: Query,
    ) -> List[Tuple[int, float]]:
        """执行一条查询，返回 ``[(id, distance), ...]``（必须由子类实现）。

        部分引擎会返回 ``(results, debug_info)`` 二元组，``_search_one``
        会识别这种形态并把 debug 信息透传出去。
        """
        raise NotImplementedError()

    @classmethod
    def _search_one(cls, query: Query, top: Optional[int] = None, schema: Optional[dict] = None):
        """执行单条查询并计算指标，返回 ``(precision, latency, results, debug)``。

        - 计时用 ``perf_counter``，单位是秒，且只包住 ``search_one`` 本身，
          不含客户端侧的结果整理。
        - 指标按 ``query.score_type`` 选择：``mrr`` 走 MRR，其余（默认）
          走交集精度（即 recall@k）。没有 ``expected_result`` 时精度记 0，
          但仍会记录延迟，所以纯 QPS 场景不受影响。
        - 子类若返回 ``(list, dict)`` 形态，第二个元素作为 debug 信息透传。
        """
        if top is None:
            top = DEFAULT_TOP

        start = time.perf_counter()
        search_res = cls.search_one(query.vector, query.meta_conditions, top, schema, query)
        end = time.perf_counter()

        precision = 0.0
        search_results = search_res
        debug_info = None
        # 兼容两种返回形态：纯结果列表，或 (结果列表, debug 字典)。
        if isinstance(search_res, tuple) and len(search_res) == 2 and isinstance(search_res[1], dict):
            search_results = search_res[0]
            debug_info = search_res[1]
        actual_ids = [x[0] for x in search_results]
        if query.expected_result is not None:
            if query.score_type == "mrr":
                precision = mrr(actual_ids, query.expected_result, top)
            else:
                precision = intersect_precision(actual_ids, query.expected_result, top)
        return precision, end - start, search_results, debug_info

    @classmethod
    def _run_worker_duration(cls, top, schema):
        """固定时长模式下的 worker 主循环：在 ``stop_event`` 置位前反复查询。

        每个 worker 从共享查询池里随机取样本（模拟真实负载不重复的访问
        分布），直到主进程把 ``stop_event.value`` 设为 True。返回值是
        ``(完成数, 精度列表, 延迟列表)``，由主进程汇总。

        注意精度可能包含大量 0 值（查询池未必都带 ground truth），因此固定
        时长模式下的精度只作参考，QPS 才是主指标。
        """
        count = 0
        precisions = []
        latencies = []

        queries = cls.shared_queries
        num_queries = len(queries)
        if num_queries == 0:
            return 0, [], []

        while not cls.stop_event.value:
            idx = random.randint(0, num_queries - 1)
            query = queries[idx]
            query.dispatch_time = time.time()
            p, l, search_results, debug_info = cls._search_one(query, top, schema)
            precisions.append(p)
            latencies.append(l)
            count += 1
        return count, precisions, latencies

    @classmethod
    def _search_batch(cls, item, top: Optional[int] = None, schema: Optional[dict] = None):
        """按索引批量执行（固定条数模式的 worker 入口）。

        ``item`` 是 ``(indices, dispatch_time)``：下标的切片和统一的派发时间
        由主进程预先算好，worker 只负责按序查询。同批次共享同一个
        ``dispatch_time``，便于后续按派发到完成的间隔统计延迟。
        """
        indices, dispatch_time = item
        batch_precisions = []
        batch_latencies = []

        for index in indices:
            query = cls.shared_queries[index]
            query.dispatch_time = dispatch_time
            p, l, search_results, debug_info = cls._search_one(query, top, schema)
            batch_precisions.append(p)
            batch_latencies.append(l)
        return batch_precisions, batch_latencies

    @classmethod
    def _search_batch_queries(cls, queries, top: Optional[int] = None, schema: Optional[dict] = None):
        """直接按查询对象批量执行（``recall_only`` 模式的 worker 入口）。

        与 ``_search_batch`` 的区别：这里拿到的是查询对象本身而不是共享池
        下标，因为 recall 模式要求逐条遍历、不能走 ``shared_queries``。
        """
        dispatch_time = time.time()
        batch_precisions = []
        batch_latencies = []
        for query in queries:
            query.dispatch_time = dispatch_time
            p, l, search_results, debug_info = cls._search_one(query, top, schema)
            batch_precisions.append(p)
            batch_latencies.append(l)
        return batch_precisions, batch_latencies

    def search_all(
        self,
        distance,
        get_queries,
        query_files,
        queries_pool_size,
        schema,
        dataset_config,
        warn_memory: bool = True,
        recall_only: bool = False,
    ):
        """执行搜索阶段并返回指标字典。

        三种模式由参数决定，优先级从高到低：

        1. ``recall_only=True``：遍历全部带 ground truth 的查询，只算精度，
           QPS 字段保持 0（用于正确性验证，不用于性能对比）。
        2. ``test_duration > 0``：固定时长压测。先把查询池加载到类属性
           ``shared_queries``，再起 ``parallel`` 个 worker 循环随机查询，
           主进程睡满 ``test_duration`` 后置位 ``stop_event``。
        3. 否则为固定条数模式：取 ``max(1000 * parallel, queries_pool_size)``
           条查询，按 100 条一批用进程池跑完。

        返回字段中 ``mean_time`` / ``p95_time`` / ``p99_time`` 的单位是
        **秒**，``rps`` 是吞吐（次/秒）；精度字段名取决于数据集的
        ``result_group``——文本/混合检索记 ``mrr``，其余记
        ``mean_precisions``（即 recall@k）。

        Args:
            distance: 距离度量（``Distance`` 枚举或名称）。
            get_queries: 查询生成器工厂 ``(times, query_meta) -> iterable``。
            query_files: 数据集声明的查询文件列表，用于解析池大小与元信息。
            queries_pool_size: 默认查询池大小，可被 ``query_meta`` 命中的文件覆盖。
            schema: 传给引擎的标量列 schema。
            dataset_config: 数据集配置，提供 ``type`` / ``name`` /
                ``result_group`` 等属性。
            warn_memory: 是否在估算内存过高时告警。
            recall_only: 是否进入仅召回模式。
        """
        parallel = self.search_params.get("parallel", 1)
        top = self.search_params.get("top", None)

        self.setup_search(self.host, distance, self.connection_params, self.search_params, dataset_config)

        # 固定条数模式复用同一个 partial：worker 只关心下标，参数在此绑定。
        search_batch = functools.partial(self.__class__._search_batch, top=top, schema=schema)

        query_meta = self.search_params.get("query_meta", None)

        # 配置里指定了查询子集时，用该子集自己的池大小覆盖全局值。
        if query_meta is not None:
            for query_file in query_files:
                if query_meta == query_file["meta"]:
                    queries_pool_size = query_file["queries_pool_size"]
                    break
        test_duration = self.search_params.get("test_duration", 0)
        # ---- 模式 1：仅召回（遍历全量查询，不测吞吐）----
        if recall_only:
            test_duration = 0
            queries_pool_size = self._compute_total_queries_for_recall_only(
                dataset_config, query_files, query_meta, queries_pool_size
            )
            total_queries = int(queries_pool_size or 0)
            # 统计不到任何可评估的查询时返回全 0，避免下游除零。
            if total_queries <= 0:
                return {
                    "rps": 0.0,
                    "mean_time": 0.0,
                    "p95_time": 0.0,
                    "p99_time": 0.0,
                    "mean_precisions": 0.0,
                }
            BATCH_SIZE = 100
            # 分块只是为了避免一次性把全部查询物化到内存：块大小为
            # parallel * 100 的整数倍，最多 12000 条。块在生成器里逐个产出，
            # 因此实际驻留内存只与块大小有关。
            base_unit = max(1, int(parallel)) * BATCH_SIZE
            max_chunk = min(12000, total_queries)
            if max_chunk >= base_unit:
                chunk_size = (max_chunk // base_unit) * base_unit
            else:
                chunk_size = max_chunk
            compact_kv("search", mode="count", queries=total_queries, top=top)
            ctx = get_context(self.get_mp_start_method())
            pool = ctx.Pool(
                processes=parallel,
                initializer=self.__class__.init_client,
                initargs=(
                    self.host,
                    distance,
                    self.connection_params,
                    self.search_params,
                ),
            )
            start = time.perf_counter()
            precisions = []
            latencies = []
            completed = 0
            last_report = 0

            # query_iterator 包装查询生成器（可被复用/替换），chunk_generator
            # 在其上再切块；两层分开是为了让"取查询"和"分批派发"互不干扰。
            def query_iterator():
                """按需产出查询，共 ``total_queries`` 条（可重复采样时更长）。

                包一层而不是直接用 ``get_queries`` 的生成器，是为了让
                ``chunk_generator`` 只依赖"一个迭代器"这个契约，方便替换
                查询来源而不动分块逻辑。
                """
                generator = get_queries(times=total_queries, query_meta=query_meta)
                for query in generator:
                    yield query

            def chunk_generator():
                """把查询流切成每块 ``chunk_size`` 条，供并行池一次派发。

                终止条件有两条：查询迭代器耗尽，或累计 ``completed`` 已达
                ``total_queries``。两者都要保留——固定时长模式下查询流可能
                比预期长，而计数模式下流可能因采样提前结束。
                """
                it = query_iterator()
                while True:
                    chunk = []
                    while len(chunk) < chunk_size and len(chunk) + completed < total_queries:
                        try:
                            chunk.append(next(it))
                        except StopIteration:
                            break
                    if not chunk:
                        break
                    yield chunk

            with pool:
                for chunk in chunk_generator():
                    if warn_memory and chunk:
                        per_query_bytes = self._estimate_query_bytes(chunk[0])
                        estimated = int(per_query_bytes * len(chunk) * max(1, int(parallel)))
                        self._warn_memory(
                            label=f"search parallel={parallel} pool_size={len(chunk)}",
                            estimated_bytes=estimated,
                        )
                    # 块内再按 BATCH_SIZE 切成派发单元；imap_unordered 让
                    # worker 完成即返回，不要求按提交顺序，避免长尾阻塞。
                    batch_items = []
                    for i in range(0, len(chunk), BATCH_SIZE):
                        end = min(i + BATCH_SIZE, len(chunk))
                        batch_items.append(chunk[i:end])
                    for batch_res in pool.imap_unordered(
                        functools.partial(self.__class__._search_batch_queries, top=top, schema=schema),
                        iterable=batch_items,
                        chunksize=1,
                    ):
                        completed += len(batch_res[0])
                        precisions.extend(batch_res[0])
                        latencies.extend(batch_res[1])
                        if total_queries > 0:
                            current = int(completed * 100 / total_queries)
                            if current != last_report:
                                last_report = current
                                sys.stdout.write(f"\rsearch progress: {current}% ({completed}/{total_queries})")
                                sys.stdout.flush()
                if total_queries > 0:
                    sys.stdout.write("\rsearch progress: 100% ({}/{})\n".format(total_queries, total_queries))
                    sys.stdout.flush()
        # ---- 模式 2：固定时长压测（QPS 主场景）----
        elif test_duration > 0:
            pool_size = queries_pool_size
            # pool_meta 是查询池的身份标签：同一进程里连续跑多个 target 时，
            # 只要数据集与池大小一致就复用已加载的查询池，省掉重复读盘/解码。
            pool_meta = {"pool_size": pool_size, "query_meta": query_meta, "dataset": dataset_config.name}
            if self.__class__.shared_queries and self.__class__.shared_queries_info == pool_meta:
                candidates_queries = self.__class__.shared_queries
                step(f"reuse query pool: {len(candidates_queries)}")
            else:
                multi_queries = get_queries(times=pool_size, query_meta=query_meta)
                candidates_queries = [query for query in multi_queries]
                # 挂在类属性上：fork 出的 worker 直接可读，无需再序列化整池。
                self.__class__.shared_queries = candidates_queries
                self.__class__.shared_queries_info = pool_meta
                step(f"loaded query pool: {len(candidates_queries)}")
            if warn_memory and candidates_queries:
                per_query_bytes = self._estimate_query_bytes(candidates_queries[0])
                estimated = int(per_query_bytes * len(candidates_queries) * max(1, int(parallel)))
                self._warn_memory(
                    label=f"search parallel={parallel} pool_size={len(candidates_queries)}",
                    estimated_bytes=estimated,
                )

            # "b" = 有符号字节，跨进程共享的停止标志；主进程置 True 后所有
            # worker 在下一次循环检查时退出。
            stop_event = multiprocessing.Value("b", False)
            init_args = (self.host, distance, self.connection_params, self.search_params)

            ctx = get_context(self.get_mp_start_method())
            pool = ctx.Pool(
                processes=parallel,
                initializer=init_worker,
                initargs=(stop_event, self.__class__, init_args),
            )

            start = time.perf_counter()
            with pool:
                # 每个 worker 只提交一个长任务，任务内部自己循环取查询，
                # 这样进程池的调度开销不会计入吞吐。
                async_results = []
                for _ in range(parallel):
                    async_results.append(
                        pool.apply_async(
                            self.__class__._run_worker_duration,
                            args=(top, schema),
                        )
                    )

                step(f"workers: {parallel} running for {test_duration}s")
                time.sleep(test_duration)
                # 置位后 worker 需要跑完当前这条查询才会返回，因此下面的
                # get() 会稍晚于 test_duration 完成。
                stop_event.value = True

                results = [res.get() for res in async_results]

            precisions = []
            latencies = []
            ops = 0
            for res in results:
                ops += int(res[0])
                precisions.extend(res[1])
                latencies.extend(res[2])
            step(f"completed requests: {ops}")
            gc.collect()

        # ---- 模式 3：固定条数（无时长限制，跑完即止）----
        else:
            # 每个并发至少 1000 条查询，池更大时用池大小，保证样本足够。
            queries_need = 1000 * parallel
            count = queries_pool_size if queries_need <= queries_pool_size else queries_need
            multi_queries = get_queries(times=count, query_meta=self.search_params.get("query_meta", None))
            candidates_queries = [query for query in multi_queries]
            # worker 通过类属性按下标取查询，避免每条都跨进程序列化。
            self.__class__.shared_queries = candidates_queries
            if warn_memory and candidates_queries:
                per_query_bytes = self._estimate_query_bytes(candidates_queries[0])
                estimated = int(per_query_bytes * len(candidates_queries) * max(1, int(parallel)))
                self._warn_memory(
                    label=f"search parallel={parallel} pool_size={len(candidates_queries)}",
                    estimated_bytes=estimated,
                )

            compact_kv("search", mode="count", queries=count, top=top)
            start = time.perf_counter()

            BATCH_SIZE = 100
            total_queries = len(candidates_queries)
            total_batches = (total_queries + BATCH_SIZE - 1) // BATCH_SIZE

            # 批次以 "下标区间 + 派发时间" 的形式提交；派发时间在生成时取，
            # 随后的排队时间也算进该批次的观测窗口。
            def batch_index_generator(total, batch_size):
                """按批产出 ``(下标区间, 派发时刻)``，交给 imap_unordered 执行。

                派发时刻在这里取（而不是 worker 里取），这样延迟统计包含进程池
                排队时间，反映的是客户端真实观察到的耗时。
                """
                for i in range(0, total, batch_size):
                    end = min(i + batch_size, total)
                    yield (range(i, end), time.time())

            ctx = get_context(self.get_mp_start_method())
            pool = ctx.Pool(
                processes=parallel,
                initializer=self.__class__.init_client,
                initargs=(
                    self.host,
                    distance,
                    self.connection_params,
                    self.search_params,
                ),
            )

            with pool:
                results = list(
                    pool.imap_unordered(
                        search_batch,
                        iterable=tqdm.tqdm(
                            batch_index_generator(total_queries, BATCH_SIZE),
                            total=total_batches,
                            desc="search",
                            mininterval=0.5,
                            maxinterval=1.0,
                        ),
                        chunksize=1,
                    )
                )

                precisions = []
                latencies = []
                for batch_res in results:
                    precisions.extend(batch_res[0])
                    latencies.extend(batch_res[1])

        # 总耗时覆盖整个搜索阶段（含进程池创建/退出），因此 rps 略低于
        # 纯查询吞吐；延迟分位数只基于单次查询计时。
        total_time = time.perf_counter() - start

        # 精度逐条平均；文本/混合检索场景语义是"首个相关结果的排名"，用 mrr
        # 作为字段名，其余（向量检索）沿用 mean_precisions（即 recall@k）。
        metric_value = float(np.mean(precisions)) if precisions else 0.0
        result_group = getattr(dataset_config, "result_group", "")
        if result_group in ("text_search", "hybrid_search"):
            result_dict = {
                "rps": len(latencies) / total_time,
                "mean_time": float(np.mean(latencies)) if latencies else 0.0,
                "p95_time": float(np.percentile(latencies, 95)) if latencies else 0.0,
                "p99_time": float(np.percentile(latencies, 99)) if latencies else 0.0,
                "mrr": metric_value,
            }
        else:
            result_dict = {
                "rps": len(latencies) / total_time,
                "mean_time": float(np.mean(latencies)) if latencies else 0.0,
                "p95_time": float(np.percentile(latencies, 95)) if latencies else 0.0,
                "p99_time": float(np.percentile(latencies, 99)) if latencies else 0.0,
                "mean_precisions": metric_value,
            }

        return result_dict

    def setup_search(self, host, distance, connection_params: dict, search_params: dict, dataset_config):
        """搜索阶段开始前的钩子（默认无操作）。

        在主进程调用，可在此做客户端预热、下发会话级 SETTINGS 或打印版本
        信息；子类按需覆盖，未覆盖时等同于空实现。
        """
        pass

    def post_search(self):
        """搜索阶段结束后的钩子（默认无操作），可用于清理连接或输出统计。"""
        pass