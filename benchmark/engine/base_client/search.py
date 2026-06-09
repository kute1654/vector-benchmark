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

from dataset_reader.base_reader import Query
from dataset_reader.utils import _to_uint32_id
from benchmark.cli_output import compact_kv, step, warn
from engine.base_client.utils import mrr, intersect_precision, get_mem_available_bytes, format_bytes


DEFAULT_TOP = 100


def init_worker(stop_event, cls, init_args):
    cls.stop_event = stop_event
    cls.init_client(*init_args)


class BaseSearcher:
    shared_queries = []
    shared_queries_info = None
    stop_event = None

    def __init__(self, host, connection_params, search_params):
        self.host = host
        self.connection_params = connection_params
        self.search_params = dict(search_params) if search_params is not None else {}

    def post_warmup(self, dataset_config):
        return

    @staticmethod
    def _estimate_query_bytes(query: Query) -> int:
        if query is None:
            return 0
        vector = query.vector or []
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
        dataset_type = getattr(dataset_config, "type", "")
        if dataset_type == "h5":
            try:
                import h5py  # type: ignore
            except Exception:
                return int(default_pool_size or 0)
            files = list(query_files or [])
            selected_files = []
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
        raise NotImplementedError()

    @classmethod
    def get_mp_start_method(cls):
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
        raise NotImplementedError()

    @classmethod
    def _search_one(cls, query: Query, top: Optional[int] = None, schema: Optional[dict] = None):
        if top is None:
            top = DEFAULT_TOP

        start = time.perf_counter()
        search_res = cls.search_one(query.vector, query.meta_conditions, top, schema, query)
        end = time.perf_counter()

        precision = 0.0
        search_results = search_res
        debug_info = None
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
        parallel = self.search_params.get("parallel", 1)
        top = self.search_params.get("top", None)

        self.setup_search(self.host, distance, self.connection_params, self.search_params, dataset_config)

        search_batch = functools.partial(self.__class__._search_batch, top=top, schema=schema)

        query_meta = self.search_params.get("query_meta", None)

        if query_meta is not None:
            for query_file in query_files:
                if query_meta == query_file["meta"]:
                    queries_pool_size = query_file["queries_pool_size"]
                    break
        test_duration = self.search_params.get("test_duration", 0)
        if recall_only:
            test_duration = 0
            queries_pool_size = self._compute_total_queries_for_recall_only(
                dataset_config, query_files, query_meta, queries_pool_size
            )
            total_queries = int(queries_pool_size or 0)
            if total_queries <= 0:
                return {
                    "rps": 0.0,
                    "mean_time": 0.0,
                    "p95_time": 0.0,
                    "p99_time": 0.0,
                    "mean_precisions": 0.0,
                }
            BATCH_SIZE = 100
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

            def query_iterator():
                generator = get_queries(times=total_queries, query_meta=query_meta)
                for query in generator:
                    yield query

            def chunk_generator():
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
        elif test_duration > 0:
            pool_size = queries_pool_size
            pool_meta = {"pool_size": pool_size, "query_meta": query_meta, "dataset": dataset_config.name}
            if self.__class__.shared_queries and self.__class__.shared_queries_info == pool_meta:
                candidates_queries = self.__class__.shared_queries
                step(f"reuse query pool: {len(candidates_queries)}")
            else:
                multi_queries = get_queries(times=pool_size, query_meta=query_meta)
                candidates_queries = [query for query in multi_queries]
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

        else:
            queries_need = 1000 * parallel
            count = queries_pool_size if queries_need <= queries_pool_size else queries_need
            multi_queries = get_queries(times=count, query_meta=self.search_params.get("query_meta", None))
            candidates_queries = [query for query in multi_queries]
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

            def batch_index_generator(total, batch_size):
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

        total_time = time.perf_counter() - start

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
        pass

    def post_search(self):
        pass
