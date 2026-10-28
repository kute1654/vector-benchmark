"""驱动式客户端的组装核心：一次实验 = 建表 → 写入 → 搜索 → 落盘。

``BaseClient`` 持有三个阶段的组件（Configurator / Uploader / Searcher），
并按 ``engine`` 字段走不同的执行分支：

- ``_run_myscale_experiment`` / ``_run_clickhouse_experiment``：
  ClickHouse 家族，配置矩阵会额外展开 ``string_to_vector`` 维度。
- ``_run_pgvector_experiment``：PostgreSQL 家族（pgvector 与 PolarDB-PASE
  共用），矩阵维度是计划缓存等级、``CAST`` 形态与结果缓存。

各引擎写各自的 CSV（列结构不同，不能混写），JSON 明细统一落在
``results/json/``。时间字段口径：``mean_time`` / ``p95_time`` /
``p99_time`` 均为**秒**，``rps`` 为每秒请求数。
"""

import functools
import itertools
import json
import csv
import os
from datetime import datetime
from typing import List

from benchmark import ROOT_DIR
from benchmark.cli_output import compact_kv, stage, step, warn
from benchmark.dataset import Dataset
from engine.base_client.configure import BaseConfigurator
from engine.base_client.search import BaseSearcher
from engine.base_client.upload import BaseUploader
from engine.base_client.utils import get_mem_available_bytes, format_bytes

RESULTS_DIR = ROOT_DIR / "results"
RESULTS_DIR.mkdir(exist_ok=True)
JSON_DIR = RESULTS_DIR / "json"
JSON_DIR.mkdir(exist_ok=True)
CSV_DIR = RESULTS_DIR / "csv"
CSV_DIR.mkdir(exist_ok=True)
CSV_RESULTS_FILE = CSV_DIR / "benchmark_results.csv"
# ClickHouse writes to its own CSV (different column schema than MyScale)
CLICKHOUSE_CSV_FILE = CSV_DIR / "clickhouse_benchmark_results.csv"
# PGvector / PolarDB write to their own CSV (different column schema than MyScale/ClickHouse)
PGVECTOR_CSV_FILE = CSV_DIR / "pgvector_benchmark_results.csv"

# Column order for PGVECTOR_CSV_FILE.  vector_in_parse_mode (pgvector:
# vector.in_parse_mode / polardb: pase_in_parse_mode) is a swept dimension of
# the text->vector parser comparison, so it must be recorded per row.
PGVECTOR_CSV_FIELDS = [
    'timestamp', 'experiment_name', 'dataset', 'vector_size', 'rows', 'distance',
    'sql_type', 'vector_in_parse_mode', 'use_cache', 'use_query_cache', 'parallel',
    'top', 'test_duration', 'ef_s', 'rps', 'recall', 'mean_precisions', 'mrr',
    'mean_time', 'p95_time', 'p99_time',
]

# Column order for CSV_RESULTS_FILE (MyScale).  string_to_vector_mode /
# string_to_vector_threads (setting string_to_vector_parallel_mode: 0=serial,
# 1=blocked, 2=cyclic; threads fixed) are the swept dimension of the MyScale
# fork's parallel string->vector conversion, so they sit next to sql_type.
# Same column names as bash-test/clickhouse-benchmark.sh writes, so both CSVs
# can be analysed together.
MYSCALE_CSV_FIELDS = [
    'timestamp', 'experiment_name', 'dataset', 'vector_size', 'distance',
    'use_query_plan_cache', 'query_plan_cache_enable_CAST',
    'query_plan_cache_only_vector', 'vector_only_cache_query_plan',
    'use_query_cache', 'string_to_vector_mode', 'string_to_vector_threads',
    'sql_type', 'parallel', 'top', 'test_duration', 'ef_s',
    'rps', 'recall', 'mean_precisions', 'mrr',
]

# Column order for CLICKHOUSE_CSV_FILE.  The vector_*_query_plan_cache names
# differ from MyScale's (ToT engine variant), so ClickHouse keeps its own schema
# and its own output file (never merged with the MyScale benchmark_results.csv).
CLICKHOUSE_CSV_FIELDS = [
    'timestamp', 'experiment_name', 'dataset', 'vector_size', 'distance',
    'vector_query_plan_cache', 'vector_use_cast', 'vector_query_plan_cache_only_vector',
    'vector_only_cache_query_plan',
    'use_query_cache',
    'sql_type', 'parallel', 'top', 'test_duration', 'ef_s',
    'rps', 'recall', 'mean_precisions', 'mrr', 'mean_time', 'p95_time', 'p99_time',
]

# string_to_vector_parallel_threads used when a config turns on the parallel
# string->vector dimension but does not name a thread count.  Same default as
# STRING_TO_VECTOR_THREADS in bash-test/clickhouse-benchmark.sh.
SV_DEFAULT_THREADS = 8


def string_to_vector_combos(upload_params: dict):
    """Expand upload_params into the (mode, threads) pairs to benchmark.

    Returns (configured, combos).  configured=False means the config does not
    mention the parallel string->vector dimension at all, in which case we leave
    the server setting alone (and record 0/0 in the CSV).
    """
    upload_params = upload_params or {}
    modes = upload_params.get("string_to_vector_parallel_mode", None)
    threads = upload_params.get("string_to_vector_parallel_threads", None)
    if modes is None and threads is None:
        return False, [(0, 0)]
    if modes is None:
        modes = [0]
    elif not isinstance(modes, list):
        modes = [modes]
    if threads is None:
        threads = [SV_DEFAULT_THREADS]
    elif not isinstance(threads, list):
        threads = [threads]
    return True, [(m, t) for m in modes for t in threads]


def string_to_vector_combos_for_row(cache_mode, configured: bool, combos):
    """Narrow sv combos to what can actually take effect for one plan-cache mode.

    The conversion only happens when a query plan (or its AST) is restored from
    the plan cache, so with enable_query_plan_cache=0 every parallel mode is
    unreachable: run one mode=0 row instead of N identical ones.  Mirrors
    normalize_string_to_vector_modes in bash-test/clickhouse-benchmark.sh.
    """
    if configured and int(cache_mode) != 0:
        return combos
    return [(0, combos[0][1] if configured else 0)]


def _ensure_csv_schema(path, fieldnames):
    """Migrate an existing results CSV to the current header, if needed.

    Older runs wrote the CSV without the newest columns.  Appending a wider row
    to a narrower file would silently shift every value one column left, so
    rewrite the file in place with the new header and pad the old rows with
    empty values.
    """
    path = str(path)
    if not os.path.exists(path):
        return
    with open(path, newline='') as f:
        reader = csv.reader(f)
        old_header = next(reader, None)
        if old_header == fieldnames:
            return
        rows = list(reader)
    tmp = path + ".migrating"
    with open(tmp, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        for row in rows:
            record = dict(zip(old_header or [], row))
            writer.writerow([record.get(name, '') for name in fieldnames])
    os.replace(tmp, path)
    warn(f"migrated {path} header: added missing columns {set(fieldnames) - set(old_header or [])}")


def _ensure_pgvector_csv_schema(fieldnames):
    """Migration for PGVECTOR_CSV_FILE (see _ensure_csv_schema)."""
    _ensure_csv_schema(PGVECTOR_CSV_FILE, fieldnames)


class BaseClient:
    """实验客户端的基类：由 ``ClientFactory`` 组装，``run.py`` 驱动。

    真正的引擎差异不在子类，而在 ``engine`` 字段选择的三条执行分支；因此
    新增引擎时通常只需提供 Configurator/Uploader/Searcher 三件套，并在这里
    增加一条 ``_run_<engine>_experiment`` 分支。
    """

    def __init__(
            self,
            name: str,   # name of the experiment, for example: myscale-m-16-ef-128...
            meta: dict,  # information of your engine version
            configurator: BaseConfigurator,
            uploader: BaseUploader,
            searchers: List[BaseSearcher],
            engine: str = "myscale",  # engine type from configuration: "pgvector" or "myscale"
    ):
        """组装一个实验：配置器 + 写入器 + 一组检索器，并抽出索引参数快照。

        ``engine`` 统一转小写：后续 ``run_experiment`` 用它选实验分支与 CSV
        列结构，大小写不一致会让分支落空而不报错，因此在这里归一。

        索引参数在这里从 ``upload_params`` 复制一份快照（而不是每次去读
        uploader）：写盘时要把"这轮实验用的索引参数"记进结果，若写入器之后
        被修改，快照能保证记录与实际不符时至少可追溯。
        """
        # Fixme do not reference a dict, please deep copy
        self.name = name
        self.meta = meta
        self.configurator = configurator
        self.uploader = uploader
        self.searchers = searchers
        self.engine = engine.lower()
        # 索引创建参数从 upload_params 里抽取（配置里索引参数与写入参数同处
        # 一节）：index_type/index_params 是通用项，mstg_disk_mode 仅 MyScale
        # 使用，fts_* 是全文检索索引，未配置时为 None（落盘时会被过滤）。
        upload_params = self.uploader.upload_params or {}
        self.index_create_parameter = {
            "index_type": upload_params.get("index_type"),
            "index_params": upload_params.get("index_params") or {},
        }
        if upload_params.get("mstg_disk_mode", None) is not None:
            self.index_create_parameter["mstg_disk_mode"] = upload_params.get("mstg_disk_mode")

        self.index_create_parameter["fts_idx_cols"] = upload_params.get("fts_idx_cols")
        self.index_create_parameter["fts_idx_params"] = upload_params.get("fts_idx_params")

        compact_kv(
            "client",
            name=name,
            searchers=len(self.searchers),
            uploader=1,
        )

    def _warn_upload_memory(self, dataset: Dataset):
        """写入前估算内存峰值，接近可用内存 80% 时给出调参建议。

        估算口径：单条记录按 ``64 + 维数*32 + 256`` 字节（对象头 + 向量 +
        元数据），乘以 ``parallel * batch_size``，即同时在途的批次上限。
        """
        available = get_mem_available_bytes()
        if available is None:
            return
        upload_params = self.uploader.upload_params or {}
        parallel = int(upload_params.get("parallel", 16) or 16)
        batch_size = int(upload_params.get("batch_size", 256) or 256)
        vector_size = int(dataset.config.vector_size or 0)
        if parallel <= 0 or batch_size <= 0 or vector_size <= 0:
            return
        per_record_bytes = 64 + vector_size * 32 + 256
        estimated = int(per_record_bytes * parallel * batch_size)
        if estimated >= int(available * 0.8):
            warn(
                f"upload parallel={parallel} batch_size={batch_size} memory warning: "
                f"estimated {format_bytes(estimated)} >= 80% of available {format_bytes(available)}"
            )
            warn("Notice: the process may be killed.")
            warn(
                "If that happens, reduce upload_params.parallel and upload_params.batch_size in the test config (see README.md)."
            )

    def save_search_and_upload_results(
            self, search_results: dict, search_id: int, search_params: dict,
            upload_params: dict, upload_results: dict, result_group: str, cache_mode: int, CAST_mode: int, only_vector: int, only_cache_query_plan: int, result_cache: int = 0
    ):
        """把一次搜索（连同写入统计）写成 JSON 明细。

        文件名把所有实验维度都编进去（cache/CAST/only_vector/...），便于
        事后按维度筛选；写入内容做"去默认值"处理——与默认值相同的参数不
        落盘，保持 JSON 简洁。``result_cache`` 是该行是否启用结果缓存的标记。
        """
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
        parallel = (search_params or {}).get("parallel", 1)
        top = (search_params or {}).get("top", None)
        top_label = top if top is not None else "default"
        sv_mode = (search_params or {}).get("string_to_vector_parallel_mode")
        sv_label = "default" if sv_mode is None else str(sv_mode)
        experiments_file = (f"{self.name}-search-{search_id}-cache-{cache_mode}-CAST-{CAST_mode}-only_vector-{only_vector}-only_cache_query_plan-{only_cache_query_plan}-result_cache-{result_cache}-sv_mode-{sv_label}-parallel-{parallel}"
                            f"-top-{top_label}-{timestamp}.json")
        step(f"saved search results: results/json/{experiments_file}")
        with open(JSON_DIR / experiments_file, "w") as out:
            run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            meta = {**self.meta, "run_date": run_date}
            index_create_parameter_output = {}
            for key, value in (self.index_create_parameter or {}).items():
                if value is None:
                    continue
                if isinstance(value, dict) and not value:
                    continue
                index_create_parameter_output[key] = value
            search_params_output = dict(search_params) if search_params is not None else {}
            params_output = search_params_output.get("params", None)
            # text_search 时的 only_text_search 是内部开关而非可复现参数，
            # 写入前剔除；params 被剔空则整键删掉。
            if isinstance(params_output, dict):
                if result_group == "text_search":
                    params_output = {k: v for k, v in params_output.items() if k != "only_text_search"}
                if params_output:
                    search_params_output["params"] = params_output
                else:
                    search_params_output.pop("params", None)
            # 与默认值相同的参数不落盘：保持 JSON 只记录"这次实验特殊在哪"。
            defaults = {"parallel": 1, "top": None, "test_duration": 0}
            for k, dv in list(defaults.items()):
                if search_params_output.get(k, dv) == dv and k in search_params_output:
                    search_params_output.pop(k, None)
            search_results["cache_mode"] = cache_mode
            search_results["CAST_mode"] = CAST_mode
            search_results["only_vector"] = only_vector
            search_results["only_cache_query_plan"] = only_cache_query_plan
            search_results["use_result_cache"] = int(result_cache)
            if sv_mode is not None:
                search_results["string_to_vector_mode"] = int(sv_mode)
                search_results["string_to_vector_threads"] = int(
                    (search_params or {}).get("string_to_vector_parallel_threads", 0) or 0
                )
            payload = {
                "result_group": result_group,  # single search or hybrid search
                "meta": meta,
                "search_results": search_results,
            }
            if upload_results:
                payload["upload_results"] = upload_results
            if index_create_parameter_output:
                payload["index_create_parameter"] = index_create_parameter_output
            if search_params_output:
                payload["index_search_parameter"] = search_params_output
            parallel_val = upload_params.get("parallel", 16)
            batch_size_val = upload_params.get("batch_size", 256)
            if not (parallel_val == 16 and batch_size_val == 256):
                payload["data_upload_parameter"] = {
                    "parallel": parallel_val,
                    "batch_size": batch_size_val
                }
            out.write(
                json.dumps(payload, indent=2)
            )

    def save_upload_results(self, results: dict, upload_params: dict, result_group: str):
        """单独保存写入阶段的统计（跳过搜索的实验，或排障时使用）。"""
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
        experiments_file = f"{self.name}-upload-{timestamp}.json"
        step(f"saved upload results: results/json/{experiments_file}")
        with open(JSON_DIR / experiments_file, "w") as out:
            run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            meta = {**self.meta, "run_date": run_date}
            index_create_parameter_output = {}
            for key, value in (self.index_create_parameter or {}).items():
                if value is None:
                    continue
                if isinstance(value, dict) and not value:
                    continue
                index_create_parameter_output[key] = value
            parallel_val = upload_params.get("parallel", 16)
            batch_size_val = upload_params.get("batch_size", 256)
            upload_stats = {
                "result_group": result_group,  # single search or hybrid search
                "meta": meta,
                "results": results,
            }
            if index_create_parameter_output:
                upload_stats["index_create_parameter"] = index_create_parameter_output
            if not (parallel_val == 16 and batch_size_val == 256):
                upload_stats["data_upload_parameter"] = {
                    "parallel": parallel_val,
                    "batch_size": batch_size_val
                }
            out.write(json.dumps(upload_stats, indent=2))

    def save_recall_only_results(self, results: list[dict]):
        """保存仅召回模式的逐条结果（列表而非单条汇总，便于定位漏召回样本）。"""
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
        experiments_file = f"{self.name}-recall-only-{timestamp}.json"
        step(f"saved recall-only results: results/json/{experiments_file}")
        with open(JSON_DIR / experiments_file, "w") as out:
            out.write(json.dumps(results, indent=2))

    def _estimate_h5_query_bytes(self, path: str) -> int:
        """估算 H5 查询文件加载到内存后的字节数。

        累加 ``test``（查询向量）、``filter``（过滤条件）、``neighbors`` /
        ``distances``（ground truth）以及 ``query_columns_in_hdf5`` 声明的
        标量列。任何一步失败（缺 h5py、组不存在、格式异常）都返回 0，
        表示"无法估算"，调用方据此跳过这部分开销。
        """
        try:
            import h5py
        except Exception:
            return 0
        try:
            total = 0
            with h5py.File(path, "r") as fp:
                if "test" in fp:
                    d = fp["test"]
                    total += int(d.size) * int(d.dtype.itemsize)
                if "filter" in fp:
                    d = fp["filter"]
                    total += int(d.size) * int(d.dtype.itemsize)
                if "neighbors" in fp:
                    d = fp["neighbors"]
                    total += int(d.size) * int(d.dtype.itemsize)
                if "distances" in fp:
                    d = fp["distances"]
                    total += int(d.size) * int(d.dtype.itemsize)
                query_columns_in_hdf5 = fp.attrs.get("query_columns_in_hdf5", [])
                if len(query_columns_in_hdf5) != 0 and query_columns_in_hdf5[0] in fp:
                    d = fp[query_columns_in_hdf5[0]]
                    total += int(d.size) * int(d.dtype.itemsize)
            return int(total)
        except Exception:
            return 0

    def _warn_search_memory(self, reader, dataset: Dataset):
        """对所有 searcher 估算查询池+查询文件内存，取最坏情况告警。

        固定时长模式下池大小就是全部查询；固定条数模式下每条并发至少
        1000 条。``text_search`` 额外按 1KB/条计文本开销。只告警不阻止。
        """
        available = get_mem_available_bytes()
        if available is None:
            return
        query_files = reader.get_query_files() or []
        if not query_files or not self.searchers:
            return
        max_estimated = 0
        max_label = None
        for searcher in self.searchers:
            search_params = searcher.search_params or {}
            parallel = int(search_params.get("parallel", 1) or 1)
            top = search_params.get("top", None)
            top_val = int(top) if top else 100
            test_duration = int(search_params.get("test_duration", 0) or 0)
            query_meta = search_params.get("query_meta", None)
            query_file = None
            if query_meta is not None:
                for qf in query_files:
                    if qf.get("meta") == query_meta:
                        query_file = qf
                        break
            if query_file is None:
                query_file = query_files[0]
            queries_count = int((query_file or {}).get("queries_pool_size") or dataset.config.queries_pool_size or 0)
            pool_size = queries_count if test_duration > 0 else max(queries_count, 1000 * parallel)
            vector_size = int(dataset.config.vector_size or 0)
            per_query_bytes = 128 + vector_size * 32 + top_val * 28
            if getattr(dataset.config, "result_group", "") == "text_search":
                per_query_bytes += 1024
            query_pool_bytes = int(per_query_bytes * pool_size * max(1, parallel))
            query_load_bytes = 0
            if getattr(dataset.config, "type", "") == "h5" and query_file and query_file.get("path"):
                query_load_bytes = self._estimate_h5_query_bytes(str(query_file.get("path")))
            estimated = int(query_load_bytes + query_pool_bytes)
            label = f"search parallel={parallel} pool_size={pool_size}"
            if estimated > max_estimated:
                max_estimated = estimated
                max_label = label
        if max_label and max_estimated >= int(available * 0.8):
            warn(
                f"{max_label} memory warning: estimated {format_bytes(max_estimated)} "
                f">= 80% of available {format_bytes(available)}"
            )
            warn("Notice: the process may be killed.")
            warn(
                "If that happens, reduce search_params.parallel in the config and the dataset queries_pool_size field in datasets.json (see README.md)."
            )

    def save_to_csv(self, search_results, search_params, dataset_config, cache_mode, CAST_mode, only_vector, only_cache_query_plan, query_cache, sql_type="cast_array", sv_mode=0, sv_threads=0):
        """
        Save benchmark results to CSV file with all required parameters for comparison.

        sv_mode / sv_threads: the string->vector parallel conversion settings this
        row ran with (MyScale fork); 0/0 when the dimension is not configured.

        追加一行到 ``results/csv/benchmark_results.csv``（MyScale 专用表结构）。
        写入前先做表头迁移，避免旧文件列数不足导致数据整体错位；任何异常
        只打印告警，不中断实验。
        """
        try:
            parallel = search_params.get("parallel", 0)
            test_duration = search_params.get("test_duration", 0)
            ef_s = search_params.get("params", {}).get("ef_s", 0) if isinstance(search_params.get("params"), dict) else 0
            
            # 历史字段名沿用 rps；时间字段（mean/p95/p99）的单位是秒。
            qps = search_results.get("rps", 0)

            csv_row = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'experiment_name': self.name,
                'dataset': getattr(dataset_config, 'name', ''),
                'vector_size': getattr(dataset_config, 'vector_size', 0),
                'distance': getattr(dataset_config, 'distance', ''),
                'use_query_plan_cache': int(cache_mode),
                'query_plan_cache_enable_CAST': int(CAST_mode),
                'query_plan_cache_only_vector': int(only_vector),
                'vector_only_cache_query_plan': int(only_cache_query_plan),
                'use_query_cache': int(query_cache),
                'string_to_vector_mode': int(sv_mode),
                'string_to_vector_threads': int(sv_threads),
                'sql_type': sql_type,
                'parallel': int(parallel),
                'top': int(search_params.get("top") or 0),
                'test_duration': int(test_duration),
                'ef_s': int(ef_s),
                'rps': float(qps),
                'recall': search_results.get("recall", 0),
                'mean_precisions': search_results.get("mean_precisions", 0),
                'mrr': search_results.get("mrr", 0),
            }
            
            file_exists = os.path.exists(CSV_RESULTS_FILE)

            _ensure_csv_schema(CSV_RESULTS_FILE, MYSCALE_CSV_FIELDS)

            with open(CSV_RESULTS_FILE, 'a', newline='') as csvfile:
                fieldnames = MYSCALE_CSV_FIELDS

                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow(csv_row)
                
        except Exception as e:
            warn(f"Failed to save CSV results: {e}")
    
    def _run_myscale_experiment(self, dataset: Dataset, skip_upload: bool, recall_only: bool, upload_stats, reader):
        """Complete MyScale-specific experiment implementation with full query plan cache parameters

        MyScale 的配置矩阵维度：计划缓存开关 × 是否对 CAST 生效 × 仅缓存向量
        部分 × 仅缓存 QueryPlan × 结果缓存 × SQL 形态 × string->vector 并行
        模式。每个组合都会：

        1. 首次进入时先做一次表级预热（按时长压测的参数，至少 60 秒）；
        2. 对每个 searcher 重复 ``search_number`` 轮搜索；
        3. 多轮结果做聚合（>2 轮时去掉最大/最小值取截尾均值）；
        4. 写 JSON 明细 + 追加一行 MyScale CSV。
        """
        import functools

        # search_number：同一配置重复测几轮，用于平滑 QPS 抖动。
        search_number = self.uploader.upload_params.get("search_number", 1)
        search_number = int(search_number or 1)
        use_query_plan_cache = self.uploader.upload_params.get("use_query_plan_cache", self.uploader.upload_params.get("enable_query_plan_cache", [0]))
        query_plan_cache_enable_CAST = self.uploader.upload_params.get("query_plan_cache_enable_CAST", [0])
        query_plan_cache_only_vector = self.uploader.upload_params.get("query_plan_cache_only_vector", [0])
        vector_only_cache_query_plan = self.uploader.upload_params.get("vector_only_cache_query_plan", [0])
        use_query_cache = self.uploader.upload_params.get("use_query_cache", [0])
        sql_type_values = self.uploader.upload_params.get("sql_type", ["cast_array"])
        # Parallel string->vector conversion (MyScale fork).  mode is the swept
        # dimension; threads is a fixed value (like STRING_TO_VECTOR_THREADS in
        # bash-test/clickhouse-benchmark.sh), but a list is accepted as an axis too.
        sv_configured, sv_combos = string_to_vector_combos(self.uploader.upload_params)
        if not isinstance(use_query_plan_cache, list):
            use_query_plan_cache = [use_query_plan_cache]
        if not isinstance(query_plan_cache_enable_CAST, list):
            query_plan_cache_enable_CAST = [query_plan_cache_enable_CAST]
        if not isinstance(query_plan_cache_only_vector, list):
            query_plan_cache_only_vector = [query_plan_cache_only_vector]
        if not isinstance(vector_only_cache_query_plan, list):
            vector_only_cache_query_plan = [vector_only_cache_query_plan]
        if not isinstance(use_query_cache, list):
            use_query_cache = [use_query_cache]
        if not isinstance(sql_type_values, list):
            sql_type_values = [sql_type_values]
        for cache_mode in use_query_plan_cache:
            row_sv_combos = string_to_vector_combos_for_row(cache_mode, sv_configured, sv_combos)
            for CAST_mode in query_plan_cache_enable_CAST:
                for only_vector in query_plan_cache_only_vector:
                    for only_cache_query_plan in vector_only_cache_query_plan:
                        for query_cache in use_query_cache:
                            for sql_type in sql_type_values:
                              for sv_mode, sv_threads in row_sv_combos:
                                # 把当前迭代的缓存维度写进 search_params 的副本；
                                # 每次返回新 dict，避免各组合之间互相污染。
                                def with_cache_modes(search_params):
                                    """复制一份 search_params，写入本轮 MyScale 组合的参数。

                                    每次调用返回新 dict：同一组基准参数会被多维
                                    网格（cache/CAST/only_vector/query_cache/
                                    sql_type/string_to_vector）反复复用，就地修改
                                    会让后一轮继承前一轮的设置。
                                    """
                                    params = dict(search_params or {})
                                    params["use_query_plan_cache"] = int(cache_mode)
                                    params["query_plan_cache_enable_CAST"] = int(CAST_mode)
                                    params["query_plan_cache_only_vector"] = int(only_vector)
                                    params["vector_only_cache_query_plan"] = int(only_cache_query_plan)
                                    params["use_query_cache"] = int(query_cache)
                                    params["sql_type"] = sql_type
                                    if sv_configured:
                                        params["string_to_vector_parallel_mode"] = int(sv_mode)
                                        params["string_to_vector_parallel_threads"] = int(sv_threads)
                                    return params

                                if self.searchers and not getattr(self, "_table_warmup_done", False):
                                    duration_searchers = [
                                        s for s in self.searchers if int((s.search_params or {}).get("test_duration", 0) or 0) > 0
                                    ]
                                    if duration_searchers:
                                        last_searcher = duration_searchers[-1]
                                        last_test_duration = int((last_searcher.search_params or {}).get("test_duration", 0) or 0)
                                        stage("WARMUP")
                                        # 预热时长取配置 test_duration，但至少 60 秒
                                        warmup_seconds = max(60, last_test_duration)
                                        warmup_search_params = with_cache_modes(last_searcher.search_params)
                                        warmup_search_params["test_duration"] = warmup_seconds
                                        warmup_search_params["_warmup"] = True
                                        warmup_searcher = last_searcher.__class__(
                                            last_searcher.host,
                                            connection_params={**(last_searcher.connection_params or {})},
                                            search_params=warmup_search_params,
                                        )
                                        warmup_params = (warmup_search_params.get("params") or {})
                                        if not isinstance(warmup_params, dict):
                                            warmup_params = {}
                                        compact_kv(
                                            "warmup params",
                                            parallel=warmup_search_params.get("parallel"),
                                            top=warmup_search_params.get("top"),
                                            test_duration=warmup_search_params.get("test_duration"),
                                            use_query_plan_cache=warmup_search_params.get("use_query_plan_cache"),
                                            query_plan_cache_enable_CAST=warmup_search_params.get("query_plan_cache_enable_CAST"),
                                            query_plan_cache_only_vector=warmup_search_params.get("query_plan_cache_only_vector"),
                                            vector_only_cache_query_plan=warmup_search_params.get("vector_only_cache_query_plan"),
                                            use_query_cache=warmup_search_params.get("use_query_cache"),
                                            sql_type=warmup_search_params.get("sql_type"),
                                            **warmup_params,
                                        )
                                        get_queries = functools.partial(reader.read_queries)
                                        warmup_searcher.search_all(
                                            dataset.config.distance,
                                            get_queries,
                                            reader.get_query_files(),
                                            dataset.config.queries_pool_size,
                                            dataset.config.schema,
                                            dataset.config,
                                            warn_memory=False,
                                            recall_only=False,
                                        )
                                        warmup_searcher.post_warmup(dataset.config)
                                        self._table_warmup_done = True

                                stage("SEARCH")
                                if not recall_only:
                                    self._warn_search_memory(reader, dataset)
                                recall_only_results: list[dict] = [] if recall_only else []
                                for search_id, searcher in enumerate(self.searchers):
                                    if recall_only:
                                        stage(f"SEARCHER {search_id + 1}/{len(self.searchers)} (recall-only, sql_type={sql_type})")
                                    else:
                                        stage(f"SEARCHER {search_id + 1}/{len(self.searchers)} (sql_type={sql_type})")
                                    original_search_params = {**searcher.search_params}
                                    effective_search_params = with_cache_modes(original_search_params)
                                    if recall_only:
                                        effective_search_params["test_duration"] = 0

                                    all_search_stats = []
                                    for i in range(search_number):
                                        current_searcher = searcher.__class__(
                                            searcher.host,
                                            connection_params={**(searcher.connection_params or {})},
                                            search_params=effective_search_params,
                                        )

                                        params = (effective_search_params.get("params") or {})
                                        if not isinstance(params, dict):
                                            params = {}
                                        if i == 0:
                                            compact_kv(
                                                "search params",
                                                parallel=effective_search_params.get("parallel"),
                                                top=effective_search_params.get("top"),
                                                test_duration=effective_search_params.get("test_duration"),
                                                use_query_plan_cache=effective_search_params.get("use_query_plan_cache"),
                                                query_plan_cache_enable_CAST=effective_search_params.get("query_plan_cache_enable_CAST"),
                                                query_plan_cache_only_vector=effective_search_params.get("query_plan_cache_only_vector"),
                                                vector_only_cache_query_plan=effective_search_params.get("vector_only_cache_query_plan"),
                                                use_query_cache=effective_search_params.get("use_query_cache"),
                                                string_to_vector_mode=effective_search_params.get("string_to_vector_parallel_mode"),
                                                string_to_vector_threads=effective_search_params.get("string_to_vector_parallel_threads"),
                                                sql_type=effective_search_params.get("sql_type"),
                                                **params,
                                            )

                                        get_queries = functools.partial(reader.read_queries)
                                        search_stats = current_searcher.search_all(
                                            dataset.config.distance,
                                            get_queries,
                                            reader.get_query_files(),
                                            dataset.config.queries_pool_size,
                                            dataset.config.schema,
                                            dataset.config,
                                            warn_memory=not recall_only,
                                            recall_only=recall_only,
                                        )
                                        all_search_stats.append(search_stats)

                                    # 多轮聚合：>2 轮用截尾均值（去掉一个最高分和
                                    # 一个最低分）抑制偶发抖动；<=2 轮直接平均。
                                    # 各轮原始数值一并保存，便于事后复核。
                                    if search_number > 2 and not recall_only:
                                        if "rps" in all_search_stats[0]:
                                            qps_values = [stats["rps"] for stats in all_search_stats]
                                            qps_values_sorted = sorted(qps_values)
                                            trimmed_qps = qps_values_sorted[1:-1]
                                            avg_qps = sum(trimmed_qps) / len(trimmed_qps) if trimmed_qps else qps_values_sorted[0]

                                            averaged_stats = dict(all_search_stats[0])
                                            averaged_stats["rps"] = avg_qps
                                            averaged_stats["original_rps_values"] = qps_values
                                            averaged_stats["trimmed_mean_rps"] = avg_qps
                                        else:
                                            averaged_stats = all_search_stats[0]
                                    elif not recall_only:
                                        if "rps" in all_search_stats[0]:
                                            qps_values = [stats["rps"] for stats in all_search_stats]
                                            avg_qps = sum(qps_values) / len(qps_values)
                                            averaged_stats = dict(all_search_stats[0])
                                            averaged_stats["rps"] = avg_qps
                                            averaged_stats["original_rps_values"] = qps_values
                                            averaged_stats["average_rps"] = avg_qps
                                        else:
                                            averaged_stats = all_search_stats[0]
                                    else:
                                        averaged_stats = all_search_stats[0] if all_search_stats else {}

                                    if recall_only:
                                        result_group = getattr(dataset.config, "result_group", "")
                                        if result_group in ("text_search", "hybrid_search"):
                                            metric_key = "mrr"
                                        else:
                                            metric_key = "mean_precisions"
                                        metric_value = averaged_stats.get(metric_key, 0.0)
                                        params_only = effective_search_params.get("params", {})
                                        if not isinstance(params_only, dict):
                                            params_only = {}
                                        recall_only_results.append(
                                            {
                                                "sql_type": sql_type,
                                                "params": params_only,
                                                metric_key: metric_value,
                                            }
                                        )
                                    else:
                                        self.save_search_and_upload_results(
                                            search_results=averaged_stats, search_id=search_id, search_params=effective_search_params,
                                            upload_params={
                                                **self.uploader.upload_params,
                                                **self.configurator.collection_params,
                                            },
                                            upload_results=upload_stats,
                                            result_group=dataset.config.result_group,
                                            cache_mode=cache_mode,
                                            CAST_mode=CAST_mode,
                                            only_vector=only_vector,
                                            only_cache_query_plan=only_cache_query_plan,
                                            result_cache=query_cache,
                                        )
                                        self.save_to_csv(
                                            search_results=averaged_stats,
                                            search_params=effective_search_params,
                                            dataset_config=dataset.config,
                                            cache_mode=cache_mode,
                                            CAST_mode=CAST_mode,
                                            only_vector=only_vector,
                                            only_cache_query_plan=only_cache_query_plan,
                                            query_cache=query_cache,
                                            sql_type=sql_type,
                                            sv_mode=sv_mode,
                                            sv_threads=effective_search_params.get("string_to_vector_parallel_threads", 0),
                                        )
                                if recall_only and recall_only_results:
                                    self.save_recall_only_results(recall_only_results)

    def _run_clickhouse_experiment(self, dataset: Dataset, skip_upload: bool, recall_only: bool, upload_stats, reader):
        """Complete ClickHouse-specific experiment implementation with query plan cache parameters.

        Uses explicit independent parameters: vector_only_cache_query_plan (only cache QueryPlan)
        and use_query_cache (query result cache), referencing MyScale's approach.

        NOTE: 与旧的编码方案不同，现在 vector_only_cache_query_plan 和 use_query_cache
              是独立的一等参数。这参照了 MyScale 引擎的实现方式，使得配置更清晰、
              组合更灵活。两个参数的说明：
              - vector_only_cache_query_plan: 仅缓存 QueryPlan，不缓存完整执行计划
              - use_query_cache: 启用查询结果缓存，重复查询直接返回缓存结果
        """
        import functools

        search_number = int(self.uploader.upload_params.get("search_number", 1))

        # 读取所有缓存相关参数，每个参数都是独立的一等配置项
        # 参照 MyScale 引擎的做法，不再使用旧的编码合并方案
        vector_query_plan_cache = self.uploader.upload_params.get("vector_query_plan_cache", [0])
        vector_use_cast = self.uploader.upload_params.get("vector_use_cast", [0])
        vector_query_plan_cache_only_vector = self.uploader.upload_params.get("vector_query_plan_cache_only_vector", [0])
        # vector_only_cache_query_plan: 独立控制"仅缓存 QueryPlan"模式
        # 0 = 缓存完整执行计划, 1 = 仅缓存 QueryPlan（减少缓存占用）
        vector_only_cache_query_plan = self.uploader.upload_params.get("vector_only_cache_query_plan", [0])
        # use_query_cache: 独立控制"查询结果缓存"
        # 0 = 关闭结果缓存, 1 = 开启结果缓存（相同查询直接返回缓存结果）
        use_query_cache = self.uploader.upload_params.get("use_query_cache", [0])
        sql_type_values = self.uploader.upload_params.get("sql_type", ["cast_array"])

        # Normalize to lists
        if not isinstance(vector_query_plan_cache, list):
            vector_query_plan_cache = [vector_query_plan_cache]
        if not isinstance(vector_use_cast, list):
            vector_use_cast = [vector_use_cast]
        if not isinstance(vector_query_plan_cache_only_vector, list):
            vector_query_plan_cache_only_vector = [vector_query_plan_cache_only_vector]
        if not isinstance(vector_only_cache_query_plan, list):
            vector_only_cache_query_plan = [vector_only_cache_query_plan]
        if not isinstance(use_query_cache, list):
            use_query_cache = [use_query_cache]
        if not isinstance(sql_type_values, list):
            sql_type_values = [sql_type_values]

        # 使用嵌套循环遍历所有参数组合，确保每种组合都被测试到
        # 每个参数都是独立维度，不再像旧方案那样编码到单个整数中
        for cache_mode in vector_query_plan_cache:
            for CAST_mode in vector_use_cast:
                for only_vector in vector_query_plan_cache_only_vector:
                    for only_cache_query_plan in vector_only_cache_query_plan:
                        for query_cache in use_query_cache:
                            for sql_type in sql_type_values:
                                # 将当前迭代的参数注入到 search_params 中，
                                # 传递给 ClickHouseSearcher 的 apply_query_plan_cache_settings
                                def with_cache_modes(search_params):
                                    """复制一份 search_params，写入本轮 ClickHouse 组合的参数。

                                    键名与 MyScale 那套有意不同（这里用
                                    ``vector_query_plan_cache`` /
                                    ``vector_use_cast``），因为两个引擎后端对同一
                                    语义的开关名不同；就地写原 dict 会污染后续组合。
                                    """
                                    params = dict(search_params or {})
                                    params["vector_query_plan_cache"] = int(cache_mode)
                                    params["vector_use_cast"] = int(CAST_mode)
                                    params["vector_query_plan_cache_only_vector"] = int(only_vector)
                                    # 直接传递独立的 only_cache_query_plan（仅缓存 QueryPlan）
                                    params["vector_only_cache_query_plan"] = int(only_cache_query_plan)
                                    # 直接传递独立的 query_cache（查询结果缓存）
                                    params["use_query_cache"] = int(query_cache)
                                    params["sql_type"] = sql_type
                                    return params

                                def log_clickhouse_params(params_dict, prefix):
                                    """打印本轮 ClickHouse 组合的关键参数（一行 kv 日志）。

                                    只挑会改变实验结果维度且容易在日志里丢失的字段，
                                    再把 ``params`` 子字典平铺出来——排查某个异常点
                                    属于哪一组时，靠这一行就能复现配置。
                                    """
                                    log_kwargs = {
                                        "parallel": params_dict.get("parallel"),
                                        "top": params_dict.get("top"),
                                        "test_duration": params_dict.get("test_duration"),
                                        "vector_query_plan_cache": params_dict.get("vector_query_plan_cache"),
                                        "vector_use_cast": params_dict.get("vector_use_cast"),
                                        "vector_only_cache_query_plan": params_dict.get("vector_only_cache_query_plan"),
                                        "use_query_cache": params_dict.get("use_query_cache"),
                                        "sql_type": params_dict.get("sql_type"),
                                    }
                                    params_only = (params_dict.get("params") or {})
                                    if not isinstance(params_only, dict):
                                        params_only = {}
                                    compact_kv(prefix, **log_kwargs, **params_only)

                                if self.searchers and not getattr(self, "_table_warmup_done", False):
                                    duration_searchers = [
                                        s for s in self.searchers if int((s.search_params or {}).get("test_duration", 0) or 0) > 0
                                    ]
                                    if duration_searchers:
                                        last_searcher = duration_searchers[-1]
                                        last_test_duration = int((last_searcher.search_params or {}).get("test_duration", 0) or 0)
                                        stage("WARMUP")
                                        # 预热时长取配置 test_duration，但至少 60 秒
                                        warmup_seconds = max(60, last_test_duration)
                                        warmup_search_params = with_cache_modes(last_searcher.search_params)
                                        warmup_search_params["test_duration"] = warmup_seconds
                                        warmup_search_params["_warmup"] = True
                                        warmup_searcher = last_searcher.__class__(
                                            last_searcher.host,
                                            connection_params={**(last_searcher.connection_params or {})},
                                            search_params=warmup_search_params,
                                        )
                                        log_clickhouse_params(warmup_search_params, "warmup params")
                                        get_queries = functools.partial(reader.read_queries)
                                        warmup_searcher.search_all(
                                            dataset.config.distance,
                                            get_queries,
                                            reader.get_query_files(),
                                            dataset.config.queries_pool_size,
                                            dataset.config.schema,
                                            dataset.config,
                                            warn_memory=False,
                                            recall_only=False,
                                        )
                                        warmup_searcher.post_warmup(dataset.config)
                                        self._table_warmup_done = True

                                stage("SEARCH")
                                if not recall_only:
                                    self._warn_search_memory(reader, dataset)
                                recall_only_results: list[dict] = [] if recall_only else []
                                for search_id, searcher in enumerate(self.searchers):
                                    if recall_only:
                                        stage(f"SEARCHER {search_id + 1}/{len(self.searchers)} (recall-only, sql_type={sql_type})")
                                    else:
                                        stage(f"SEARCHER {search_id + 1}/{len(self.searchers)} (sql_type={sql_type})")
                                    original_search_params = {**searcher.search_params}
                                    effective_search_params = with_cache_modes(original_search_params)
                                    if recall_only:
                                        effective_search_params["test_duration"] = 0

                                    all_search_stats = []
                                    for i in range(search_number):
                                        current_searcher = searcher.__class__(
                                            searcher.host,
                                            connection_params={**(searcher.connection_params or {})},
                                            search_params=effective_search_params,
                                        )

                                        if i == 0:
                                            log_clickhouse_params(effective_search_params, "search params")

                                        get_queries = functools.partial(reader.read_queries)
                                        search_stats = current_searcher.search_all(
                                            dataset.config.distance,
                                            get_queries,
                                            reader.get_query_files(),
                                            dataset.config.queries_pool_size,
                                            dataset.config.schema,
                                            dataset.config,
                                            warn_memory=not recall_only,
                                            recall_only=recall_only,
                                        )
                                        all_search_stats.append(search_stats)

                                    # Calculate trimmed mean (remove max and min, then average)
                                    # 多轮 QPS 取截尾均值；各轮原始值同时保留，
                                    # 便于判断抖动是否异常。
                                    if search_number > 2 and not recall_only:
                                        if "rps" in all_search_stats[0]:
                                            qps_values = [stats["rps"] for stats in all_search_stats]
                                            qps_values_sorted = sorted(qps_values)
                                            trimmed_qps = qps_values_sorted[1:-1]
                                            avg_qps = sum(trimmed_qps) / len(trimmed_qps) if trimmed_qps else qps_values_sorted[0]

                                            averaged_stats = dict(all_search_stats[0])
                                            averaged_stats["rps"] = avg_qps
                                            averaged_stats["original_rps_values"] = qps_values
                                            averaged_stats["trimmed_mean_rps"] = avg_qps
                                        else:
                                            averaged_stats = all_search_stats[0]
                                    elif not recall_only:
                                        if "rps" in all_search_stats[0]:
                                            qps_values = [stats["rps"] for stats in all_search_stats]
                                            avg_qps = sum(qps_values) / len(qps_values)
                                            averaged_stats = dict(all_search_stats[0])
                                            averaged_stats["rps"] = avg_qps
                                            averaged_stats["original_rps_values"] = qps_values
                                            averaged_stats["average_rps"] = avg_qps
                                        else:
                                            averaged_stats = all_search_stats[0]
                                    else:
                                        averaged_stats = all_search_stats[0] if all_search_stats else {}

                                    if recall_only:
                                        result_group = getattr(dataset.config, "result_group", "")
                                        if result_group in ("text_search", "hybrid_search"):
                                            metric_key = "mrr"
                                        else:
                                            metric_key = "mean_precisions"
                                        metric_value = averaged_stats.get(metric_key, 0.0)
                                        params_only = effective_search_params.get("params", {})
                                        if not isinstance(params_only, dict):
                                            params_only = {}
                                        recall_only_results.append(
                                            {
                                                "sql_type": sql_type,
                                                "params": params_only,
                                                metric_key: metric_value,
                                            }
                                        )
                                    else:
                                        self.save_search_and_upload_results(
                                            search_results=averaged_stats, search_id=search_id, search_params=effective_search_params,
                                            upload_params={
                                                **self.uploader.upload_params,
                                                **self.configurator.collection_params,
                                            },
                                            upload_results=upload_stats,
                                            result_group=dataset.config.result_group,
                                            cache_mode=cache_mode,
                                            CAST_mode=CAST_mode,
                                            only_vector=only_vector,
                                            only_cache_query_plan=only_cache_query_plan,
                                            result_cache=query_cache,
                                        )
                                        # CSV 中也记录完整的独立缓存参数，便于后续分析和对比
                                        self.save_clickhouse_to_csv(
                                            search_results=averaged_stats,
                                            search_params=effective_search_params,
                                            dataset_config=dataset.config,
                                            cache_mode=cache_mode,
                                            CAST_mode=CAST_mode,
                                            only_vector=only_vector,
                                            only_cache_query_plan=only_cache_query_plan,
                                            query_cache=query_cache,
                                            sql_type=sql_type,
                                        )
                                if recall_only and recall_only_results:
                                    self.save_recall_only_results(recall_only_results)

    def save_clickhouse_to_csv(self, search_results, search_params, dataset_config, cache_mode, CAST_mode, only_vector, only_cache_query_plan=0, query_cache=0, sql_type="cast_array"):
        """
        Save ClickHouse benchmark results to CSV file with all required parameters for comparison.

        注意：only_cache_query_plan 和 query_cache 作为独立参数存储，
              不再像旧方案那样从 cache_mode 编码推导。这确保了 CSV 中的
              记录与配置文件中的参数一一对应，便于后续分析和对比。

        Args:
            cache_mode: vector_query_plan_cache 值 (0/1)
            CAST_mode: vector_use_cast 值 (0/1)
            only_vector: vector_query_plan_cache_only_vector 值 (0/1)
            only_cache_query_plan: vector_only_cache_query_plan (仅缓存 QueryPlan, 0/1)
            query_cache: use_query_cache (查询结果缓存, 0/1)
            sql_type: SQL 查询类型
        """
        try:
            # Extract required parameters
            parallel = search_params.get("parallel", 0)
            test_duration = search_params.get("test_duration", 0)
            ef_s = search_params.get("params", {}).get("ef_s", 0) if isinstance(search_params.get("params"), dict) else 0

            # Extract QPS from search results
            qps = search_results.get("rps", 0)

            # Prepare CSV row data
            csv_row = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'experiment_name': self.name,
                'dataset': getattr(dataset_config, 'name', ''),
                'vector_size': getattr(dataset_config, 'vector_size', 0),
                'distance': getattr(dataset_config, 'distance', ''),
                'vector_query_plan_cache': int(cache_mode),
                'vector_use_cast': int(CAST_mode),
                'vector_query_plan_cache_only_vector': int(only_vector),
                # vector_only_cache_query_plan: 独立存储，不从 cache_mode 推导
                'vector_only_cache_query_plan': int(only_cache_query_plan),
                # use_query_cache: 独立存储，不从 cache_mode 推导
                'use_query_cache': int(query_cache),
                'sql_type': sql_type,
                'parallel': int(parallel),
                'top': int(search_params.get("top") or 0),
                'test_duration': int(test_duration),
                'ef_s': int(ef_s),
                'rps': float(qps),
                'recall': search_results.get("recall", 0),
                'mean_precisions': search_results.get("mean_precisions", 0),
                'mrr': search_results.get("mrr", 0),
                'mean_time': search_results.get("mean_time", 0),
                'p95_time': search_results.get("p95_time", 0),
                'p99_time': search_results.get("p99_time", 0),
            }

            # Write to CSV file
            _ensure_csv_schema(CLICKHOUSE_CSV_FILE, CLICKHOUSE_CSV_FIELDS)

            file_exists = os.path.exists(CLICKHOUSE_CSV_FILE)

            with open(CLICKHOUSE_CSV_FILE, 'a', newline='') as csvfile:
                fieldnames = CLICKHOUSE_CSV_FIELDS

                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                if not file_exists:
                    writer.writeheader()

                writer.writerow(csv_row)

        except Exception as e:
            warn(f"Failed to save CSV results: {e}")

    def _run_pgvector_experiment(self, dataset: Dataset, skip_upload: bool, recall_only: bool, upload_stats, reader):
        """PGvector / PolarDB experiment (cartesian over sql_type x enable_query_plan_cache x use_query_cache).

        All sql_type values are swept over the full enable_query_plan_cache range;
        the engine's _can_prepare() method internally decides whether PREPARE is
        applicable for each sql_type. Non-parameterizable types (array-based, etc.)
        simply execute inline SQL even when use_query_plan_cache=1.

        QPS (rps) and recall (mean_precisions) are measured together by the base searcher.

        pgvector 与 PolarDB 共用本函数：两者的 SQL 形态、计划缓存语义相同，
        差异封装在各自的 searcher 里。矩阵维度为 SQL 形态 × 计划缓存等级
        （``enable_query_plan_cache``） × 结果缓存 × 文本→向量解析模式
        （``vector_in_parse_mode``，仅 pgvector/PolarDB 扩展支持）。
        """
        import functools

        # search_number：同一配置重复测几轮，用于平滑 QPS 抖动。
        search_number = self.uploader.upload_params.get("search_number", 1)
        search_number = int(search_number or 1)

        # config.json uses the ck/myscale-style key `enable_query_plan_cache`;
        # the older `use_cache` key is kept as a fallback.
        use_cache_values = self.uploader.upload_params.get(
            "enable_query_plan_cache",
            self.uploader.upload_params.get("use_cache", [0]),
        )
        result_cache_values = self.uploader.upload_params.get("use_query_cache", [0])
        sql_type_values = self.uploader.upload_params.get("sql_type", ["text_literal"])
        in_parse_mode_values = self.uploader.upload_params.get("vector_in_parse_mode", [None])
        if not isinstance(use_cache_values, list):
            use_cache_values = [use_cache_values]
        if not isinstance(result_cache_values, list):
            result_cache_values = [result_cache_values]
        if not isinstance(sql_type_values, list):
            sql_type_values = [sql_type_values]
        if not isinstance(in_parse_mode_values, list):
            in_parse_mode_values = [in_parse_mode_values]

        for sql_type in sql_type_values:
            for use_cache_val in use_cache_values:
                for result_cache_val in result_cache_values:
                    for in_parse_mode_val in in_parse_mode_values:
                        dataset_dim = self.uploader.upload_params.get("_vector_size")
                        if dataset_dim:
                            dataset_dim = int(dataset_dim)

                        # 注入当前组合的缓存/解析器参数；``dims`` 仅在数据集维度
                        # 可用时补上，供服务端校验向量维度。
                        def with_pgvector_cache_modes(search_params):
                            """复制一份 search_params，写入本轮 pgvector 组合的参数。

                            ``dims`` 用 ``setdefault`` 而不是直接赋值：配置里显式
                            给了维度就以配置为准，只有缺省时才用数据集推断值。
                            """
                            params = dict(search_params or {})
                            params["use_query_plan_cache"] = int(use_cache_val)
                            params["use_result_cache"] = int(result_cache_val)
                            params["sql_type"] = sql_type
                            if in_parse_mode_val is not None:
                                params["vector_in_parse_mode"] = in_parse_mode_val
                            if dataset_dim:
                                params.setdefault("dims", dataset_dim)
                            return params

                        def log_pgvector_params(params_dict, prefix):
                            """打印本轮 pgvector 组合的关键参数（一行 kv 日志）。

                            与 ClickHouse 版本同构：固定字段 + 平铺 ``params``，
                            保证异常点能从日志反查回具体组合。
                            """
                            log_kwargs = {
                                "sql_type": params_dict.get("sql_type"),
                                "vector_in_parse_mode": params_dict.get("vector_in_parse_mode"),
                                "parallel": params_dict.get("parallel"),
                                "top": params_dict.get("top"),
                                "test_duration": params_dict.get("test_duration"),
                                "use_query_plan_cache": params_dict.get("use_query_plan_cache"),
                                "use_result_cache": params_dict.get("use_result_cache"),
                            }
                            params_only = (params_dict.get("params") or {})
                            if not isinstance(params_only, dict):
                                params_only = {}
                            compact_kv(prefix, **log_kwargs, **params_only)

                        if self.searchers and not getattr(self, "_table_warmup_done", False):
                            duration_searchers = [
                                s for s in self.searchers if int((s.search_params or {}).get("test_duration", 0) or 0) > 0
                            ]
                            if duration_searchers:
                                last_searcher = duration_searchers[-1]
                                last_test_duration = int((last_searcher.search_params or {}).get("test_duration", 0) or 0)
                                stage("WARMUP")
                                # 预热时长取配置 test_duration，但至少 60 秒
                                warmup_seconds = max(60, last_test_duration)
                                warmup_search_params = with_pgvector_cache_modes(last_searcher.search_params)
                                warmup_search_params["test_duration"] = warmup_seconds
                                warmup_search_params["_warmup"] = True
                                warmup_searcher = last_searcher.__class__(
                                    last_searcher.host,
                                    connection_params={**(last_searcher.connection_params or {})},
                                    search_params=warmup_search_params,
                                )
                                log_pgvector_params(warmup_search_params, "warmup params")
                                get_queries = functools.partial(reader.read_queries)
                                warmup_searcher.search_all(
                                    dataset.config.distance,
                                    get_queries,
                                    reader.get_query_files(),
                                    dataset.config.queries_pool_size,
                                    dataset.config.schema,
                                    dataset.config,
                                    warn_memory=False,
                                    recall_only=False,
                                )
                                warmup_searcher.post_warmup(dataset.config)
                                self._table_warmup_done = True

                        stage(f"SEARCH (sql_type={sql_type}, use_cache={use_cache_val})")
                        if not recall_only:
                            self._warn_search_memory(reader, dataset)
                        recall_only_results: list[dict] = [] if recall_only else []
                        for search_id, searcher in enumerate(self.searchers):
                            if recall_only:
                                stage(f"SEARCHER {search_id + 1}/{len(self.searchers)} (recall-only, sql_type={sql_type}, use_cache={use_cache_val})")
                            else:
                                stage(f"SEARCHER {search_id + 1}/{len(self.searchers)} (sql_type={sql_type}, use_cache={use_cache_val})")
                            original_search_params = {**searcher.search_params}
                            effective_search_params = with_pgvector_cache_modes(original_search_params)
                            if recall_only:
                                effective_search_params["test_duration"] = 0

                            all_search_stats = []
                            for i in range(search_number):
                                current_searcher = searcher.__class__(
                                    searcher.host,
                                    connection_params={**(searcher.connection_params or {})},
                                    search_params=effective_search_params,
                                )
                                if i == 0:
                                    log_pgvector_params(effective_search_params, "search params")
                                get_queries = functools.partial(reader.read_queries)
                                search_stats = current_searcher.search_all(
                                    dataset.config.distance,
                                    get_queries,
                                    reader.get_query_files(),
                                    dataset.config.queries_pool_size,
                                    dataset.config.schema,
                                    dataset.config,
                                    warn_memory=not recall_only,
                                    recall_only=recall_only,
                                )
                                all_search_stats.append(search_stats)

                            # 多轮聚合：>2 轮用截尾均值，<=2 轮直接平均。
                            if search_number > 2 and not recall_only:
                                if "rps" in all_search_stats[0]:
                                    rps_values = [stats["rps"] for stats in all_search_stats]
                                    rps_values_sorted = sorted(rps_values)
                                    trimmed_rps = rps_values_sorted[1:-1]
                                    avg_rps = sum(trimmed_rps) / len(trimmed_rps) if trimmed_rps else rps_values_sorted[0]
                                    averaged_stats = dict(all_search_stats[0])
                                    averaged_stats["rps"] = avg_rps
                                    averaged_stats["original_rps_values"] = rps_values
                                    averaged_stats["trimmed_mean_rps"] = avg_rps
                                else:
                                    averaged_stats = all_search_stats[0]
                            elif not recall_only:
                                if "rps" in all_search_stats[0]:
                                    rps_values = [stats["rps"] for stats in all_search_stats]
                                    avg_rps = sum(rps_values) / len(rps_values)
                                    averaged_stats = dict(all_search_stats[0])
                                    averaged_stats["rps"] = avg_rps
                                    averaged_stats["original_rps_values"] = rps_values
                                    averaged_stats["average_rps"] = avg_rps
                                else:
                                    averaged_stats = all_search_stats[0]
                            else:
                                averaged_stats = all_search_stats[0] if all_search_stats else {}

                            if recall_only:
                                result_group = getattr(dataset.config, "result_group", "")
                                if result_group in ("text_search", "hybrid_search"):
                                    metric_key = "mrr"
                                else:
                                    metric_key = "mean_precisions"
                                metric_value = averaged_stats.get(metric_key, 0.0)
                                params_only = effective_search_params.get("params", {})
                                if not isinstance(params_only, dict):
                                    params_only = {}
                                recall_only_results.append(
                                    {
                                        "sql_type": sql_type,
                                        "use_cache": use_cache_val,
                                        "params": params_only,
                                        metric_key: metric_value,
                                    }
                                )
                            else:
                                self.save_search_and_upload_results(
                                    search_results=averaged_stats, search_id=search_id, search_params=effective_search_params,
                                    upload_params={
                                        **self.uploader.upload_params,
                                        **self.configurator.collection_params,
                                    },
                                    upload_results=upload_stats,
                                    result_group=dataset.config.result_group,
                                    cache_mode=use_cache_val,
                                    CAST_mode=0,
                                    only_vector=0,
                                    only_cache_query_plan=0,
                                    result_cache=result_cache_val
                                )
                                self.save_pgvector_to_csv(
                                    search_results=averaged_stats,
                                    search_params=effective_search_params,
                                    dataset_config=dataset.config,
                                    use_cache=use_cache_val,
                                    result_cache=result_cache_val,
                                    sql_type=sql_type,
                                )
                        if recall_only and recall_only_results:
                            self.save_recall_only_results(recall_only_results)

    def save_pgvector_to_csv(self, search_results, search_params, dataset_config, use_cache, result_cache=0, sql_type="text_literal"):
        """
        Save PGvector / PolarDB benchmark results to a dedicated CSV file with
        sql_type and plan-cache columns for per-form / plan-cache comparison.

        Writes to PGVECTOR_CSV_FILE (separate from the MyScale/ClickHouse
        benchmark_results.csv, which uses a different column schema).

        ``mean_time`` / ``p95_time`` / ``p99_time`` 由 Python 侧直接写入，
        单位是**秒**；注意与 shell 脚本写出的 ``*_ms`` 列（毫秒）区分。
        """
        try:
            # Extract required parameters
            parallel = search_params.get("parallel", 0)
            test_duration = search_params.get("test_duration", 0)
            ef_s = search_params.get("params", {}).get("ef_s", 0) if isinstance(search_params.get("params"), dict) else 0
            if isinstance(ef_s, list):
                ef_s = ef_s[0] if ef_s else 0

            # Extract RPS from search results (PGvector uses RPS, not QPS)
            rps = search_results.get("rps", 0)

            # Effective number of rows loaded (upload_limit overrides the dataset's full count)
            upload_limit = self.uploader.upload_params.get("upload_limit", None)
            vector_count = int(upload_limit) if upload_limit else int(getattr(dataset_config, 'vector_count', 0) or 0)

            # Prepare CSV row data
            csv_row = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'experiment_name': self.name,
                'dataset': getattr(dataset_config, 'name', ''),
                'vector_size': getattr(dataset_config, 'vector_size', 0),
                'rows': vector_count,
                'distance': getattr(dataset_config, 'distance', ''),
                'sql_type': str(sql_type),
                # engine-specific text->vector parser swept for this row
                # (pgvector: vector.in_parse_mode; polardb: pase_in_parse_mode)
                'vector_in_parse_mode': search_params.get('vector_in_parse_mode') or '',
                'use_cache': int(use_cache),
                'use_query_cache': int(result_cache),
                'parallel': int(parallel),
                'top': int(search_params.get("top") or 0),
                'test_duration': int(test_duration),
                'ef_s': int(ef_s),
                'rps': float(rps),
                'recall': search_results.get("recall", 0),
                'mean_precisions': search_results.get("mean_precisions", 0),
                'mrr': search_results.get("mrr", 0),
                'mean_time': search_results.get("mean_time", 0),
                'p95_time': search_results.get("p95_time", 0),
                'p99_time': search_results.get("p99_time", 0),
            }

            fieldnames = PGVECTOR_CSV_FIELDS
            # Add the vector_in_parse_mode column to files written by older
            # versions (pad existing rows with '') before appending, otherwise
            # the new column would misalign every previously written row.
            _ensure_pgvector_csv_schema(fieldnames)
            file_exists = os.path.exists(PGVECTOR_CSV_FILE)

            with open(PGVECTOR_CSV_FILE, 'a', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

                if not file_exists:
                    writer.writeheader()

                writer.writerow(csv_row)

        except Exception as e:
            warn(f"Failed to save CSV results: {e}")

    def run_experiment(self, dataset: Dataset, skip_upload: bool = False, recall_only: bool = False, skip_search: bool = False):
        """跑完一个数据集上的完整实验：建表 → 写入 → （可选）搜索。

        Args:
            dataset: 数据集对象，提供 reader 与 ``config``。
            skip_upload: 跳过建表与写入，直接在已有表上搜索（调试用）。
            recall_only: 只测召回，不测 QPS。
            skip_search: 只建表/写入后返回；常配合 shell 的 QPS 脚本单独压测。

        Returns:
            写入阶段统计字典；``skip_search=True`` 时提前返回。
        """

        # 预热操作按"每张表一次"执行：在每次处理新表前重置预热标志，
        # 保证不同的表各预热一次（时长固定为 60 秒，见各 _run_*_experiment）。
        self._table_warmup_done = False

        execution_params = self.configurator.execution_params(
            distance=dataset.config.distance, vector_size=dataset.config.vector_size
        )
        reader = dataset.get_reader(execution_params.get("normalize", False))

        # Determine engine type directly from configuration
        engine_lower = self.engine.lower()
        is_myscale = engine_lower == 'myscale'
        is_pgvector = engine_lower == 'pgvector'
        is_polardb = engine_lower == 'polardb'
        is_clickhouse = engine_lower == 'clickhouse'
        upload_stats = {}
        if not skip_upload:
            extra_columns_name, extra_columns_type = reader.read_column_name_type()
            stage("CONFIGURE")
            self.configurator.configure(
                distance=dataset.config.distance,
                vector_size=dataset.config.vector_size,
                extra_columns_name=extra_columns_name,
                extra_columns_type=extra_columns_type,
            )

            self._warn_upload_memory(dataset)
            stage("UPLOAD")
            effective_vector_count = dataset.config.vector_count
            # 文本/混合检索写的是语料库（corpus）而不是向量条数，进度分母要用
            # corpus_count，否则进度百分比会失真甚至超过 100%。
            if getattr(dataset.config, "result_group", "") in ("text_search", "hybrid_search"):
                corpus_count = getattr(dataset.config, "corpus_count", 0) or 0
                if corpus_count:
                    effective_vector_count = corpus_count
            records = reader.read_data()
            # Optional subset: upload only the first `upload_limit` rows (e.g. a 2M slice of a 10M dataset)
            upload_limit = self.uploader.upload_params.get("upload_limit", None)
            if upload_limit:
                upload_limit = int(upload_limit)
                records = itertools.islice(records, upload_limit)
                if upload_limit < effective_vector_count:
                    effective_vector_count = upload_limit
                step(f"upload_limit: loading first {upload_limit} rows")
            upload_stats = self.uploader.upload(
                distance=dataset.config.distance,
                vector_count=effective_vector_count,
                records=records,
                extra_columns_name=extra_columns_name,
                extra_columns_type=extra_columns_type,
            )
            # self.save_upload_results(
            #     upload_stats,
            #     upload_params={
            #         **self.uploader.upload_params,
            #         **self.configurator.collection_params,
            #     },
            #     result_group=dataset.config.result_group
            # )
        
        if skip_search:
            stage("SKIP SEARCH")
            step("--skip-search: table + index built; skipping SEARCH phase (use the bash QPS scripts against this table)")
            return upload_stats

        # 按引擎分派到对应的矩阵执行器；polardb 复用 pgvector 的流程。
        if is_myscale:
            self._run_myscale_experiment(dataset, skip_upload, recall_only, upload_stats, reader)
        elif is_pgvector:
            self._run_pgvector_experiment(dataset, skip_upload, recall_only, upload_stats, reader)
        elif is_polardb:
            self._run_pgvector_experiment(dataset, skip_upload, recall_only, upload_stats, reader)
        elif is_clickhouse:
            self._run_clickhouse_experiment(dataset, skip_upload, recall_only, upload_stats, reader)
        else:
            pass
        stage("DONE")