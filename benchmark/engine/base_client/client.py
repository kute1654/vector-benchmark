import functools
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
CSV_RESULTS_FILE = RESULTS_DIR / "benchmark_results.csv"


class BaseClient:
    def __init__(
            self,
            name: str,   # name of the experiment, for example: myscale-m-16-ef-128...
            meta: dict,  # information of your engine version
            configurator: BaseConfigurator,
            uploader: BaseUploader,
            searchers: List[BaseSearcher],
            engine: str = "myscale",  # engine type from configuration: "pgvector" or "myscale"
    ):
        # Fixme do not reference a dict, please deep copy
        self.name = name
        self.meta = meta
        self.configurator = configurator
        self.uploader = uploader
        self.searchers = searchers
        self.engine = engine.lower()
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
            upload_params: dict, upload_results: dict, result_group: str, cache_mode: int, CAST_mode: int, only_vector: int, use_number: int
    ):
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
        parallel = (search_params or {}).get("parallel", 1)
        top = (search_params or {}).get("top", None)
        top_label = top if top is not None else "default"
        experiments_file = (f"{self.name}-search-{search_id}-cache-{cache_mode}-CAST-{CAST_mode}-only_vector-{only_vector}-use_number-{use_number}-parallel-{parallel}"
                            f"-top-{top_label}-{timestamp}.json")
        step(f"saved search results: results/{experiments_file}")
        with open(RESULTS_DIR / experiments_file, "w") as out:
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
            if isinstance(params_output, dict):
                if result_group == "text_search":
                    params_output = {k: v for k, v in params_output.items() if k != "only_text_search"}
                if params_output:
                    search_params_output["params"] = params_output
                else:
                    search_params_output.pop("params", None)
            defaults = {"parallel": 1, "top": None, "test_duration": 0}
            for k, dv in list(defaults.items()):
                if search_params_output.get(k, dv) == dv and k in search_params_output:
                    search_params_output.pop(k, None)
            search_results["cache_mode"] = cache_mode
            search_results["CAST_mode"] = CAST_mode
            search_results["only_vector"] = only_vector
            search_results["use_number"] = use_number
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
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
        experiments_file = f"{self.name}-upload-{timestamp}.json"
        step(f"saved upload results: results/{experiments_file}")
        with open(RESULTS_DIR / experiments_file, "w") as out:
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
        now = datetime.now()
        timestamp = now.strftime("%Y-%m-%d-%H-%M-%S")
        experiments_file = f"{self.name}-recall-only-{timestamp}.json"
        step(f"saved recall-only results: results/{experiments_file}")
        with open(RESULTS_DIR / experiments_file, "w") as out:
            out.write(json.dumps(results, indent=2))

    def _estimate_h5_query_bytes(self, path: str) -> int:
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

    def save_to_csv(self, search_results, search_params, dataset_config, cache_mode, CAST_mode, only_vector, use_number, threads):
        """
        Save benchmark results to CSV file with all required parameters for comparison.
        """
        try:
            # Extract required parameters
            parallel = search_params.get("parallel", 0)
            test_duration = search_params.get("test_duration", 0)
            ef_s = search_params.get("params", {}).get("ef_s", 0) if isinstance(search_params.get("params"), dict) else 0
            
            # Extract QPS from search results
            qps = search_results.get("qps", 0)
            
            # Prepare CSV row data
            csv_row = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'experiment_name': self.name,
                'dataset': getattr(dataset_config, 'name', ''),
                'vector_size': getattr(dataset_config, 'vector_size', 0),
                'distance': getattr(dataset_config, 'distance', ''),
                'use_query_plan_cache': int(cache_mode),
                'query_plan_cache_enable_CAST': int(CAST_mode),
                'query_plan_cache_only_vector': int(only_vector),
                'query_plan_cache_use_number': int(use_number),
                'query_parameterizer_max_threads': int(threads),
                'parallel': int(parallel),
                'test_duration': int(test_duration),
                'ef_s': int(ef_s),
                'qps': float(qps),
                'recall': search_results.get("recall", 0),
                'mean_precisions': search_results.get("mean_precisions", 0),
                'mrr': search_results.get("mrr", 0),
            }
            
            # Write to CSV file
            file_exists = os.path.exists(CSV_RESULTS_FILE)
            
            with open(CSV_RESULTS_FILE, 'a', newline='') as csvfile:
                fieldnames = [
                    'timestamp', 'experiment_name', 'dataset', 'vector_size', 'distance',
                    'use_query_plan_cache', 'query_plan_cache_enable_CAST', 
                    'query_plan_cache_only_vector', 'query_plan_cache_use_number', 
                    'query_parameterizer_max_threads', 'parallel', 'test_duration', 'ef_s',
                    'qps', 'recall', 'mean_precisions', 'mrr'
                ]
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow(csv_row)
                
        except Exception as e:
            warn(f"Failed to save CSV results: {e}")
    
    def _run_myscale_experiment(self, dataset: Dataset, skip_upload: bool, recall_only: bool, upload_stats, reader):
        """Complete MyScale-specific experiment implementation with full query plan cache parameters"""
        
        search_number = self.uploader.upload_params.get("search_number", 1)
        
        for cache_mode in use_query_plan_cache:
            if cache_mode == 0:
                query_plan_cache_only_vector = [0]
                query_plan_cache_use_number = [0]
            else:
                query_plan_cache_only_vector = self.uploader.upload_params.get("query_plan_cache_only_vector", [0])
                query_plan_cache_use_number = self.uploader.upload_params.get("query_plan_cache_use_number", [0])   
            for CAST_mode in query_plan_cache_enable_CAST:
                if cache_mode == 0 or CAST_mode == 1:
                    query_parameterizer_max_threads = [0]
                else:
                    query_parameterizer_max_threads = self.uploader.upload_params.get("query_parameterizer_max_threads", [4])
                for only_vector in query_plan_cache_only_vector:
                    for use_number in query_plan_cache_use_number:
                        for threads in query_parameterizer_max_threads:
                            def with_cache_modes(search_params):
                                params = dict(search_params or {})
                                params["use_query_plan_cache"] = int(cache_mode)
                                params["query_plan_cache_enable_CAST"] = int(CAST_mode)
                                params["query_plan_cache_only_vector"] = int(only_vector)
                                params["query_plan_cache_use_number"] = int(use_number)
                                params["query_parameterizer_max_threads"] = int(threads)
                                return params

                            if self.searchers:
                                duration_searchers = [
                                    s for s in self.searchers if int((s.search_params or {}).get("test_duration", 0) or 0) > 0
                                ]
                                if duration_searchers:
                                    last_searcher = duration_searchers[-1]
                                    last_test_duration = int((last_searcher.search_params or {}).get("test_duration", 0) or 0)
                                    stage("WARMUP")
                                    warmup_seconds = int(round(last_test_duration * 0.1))
                                    warmup_seconds = max(1, min(5, warmup_seconds))
                                    # warmup_seconds = max(2, min(5, warmup_seconds))
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
                                        query_plan_cache_use_number=warmup_search_params.get("query_plan_cache_use_number"),
                                        query_parameterizer_max_threads=warmup_search_params.get("query_parameterizer_max_threads"),
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
                        
                            stage("SEARCH")
                            if not recall_only:
                                self._warn_search_memory(reader, dataset)
                            recall_only_results: list[dict] = [] if recall_only else []
                            for search_id, searcher in enumerate(self.searchers):
                                if recall_only:
                                    stage(f"SEARCHER {search_id + 1}/{len(self.searchers)} (recall-only)")
                                else:
                                    stage(f"SEARCHER {search_id + 1}/{len(self.searchers)}")
                                original_search_params = {**searcher.search_params}
                                effective_search_params = with_cache_modes(original_search_params)
                                if recall_only:
                                    effective_search_params["test_duration"] = 0
                                
                                # Collect multiple search results for averaging
                                all_search_stats = []
                                for i in range(search_number):
                                    # Create a fresh searcher instance for each iteration to avoid state contamination
                                    current_searcher = searcher.__class__(
                                        searcher.host,
                                        connection_params={**(searcher.connection_params or {})},
                                        search_params=effective_search_params,
                                    )
                                    
                                    params = (effective_search_params.get("params") or {})
                                    if not isinstance(params, dict):
                                        params = {}
                                    if i == 0:  # Only show params once for the first iteration
                                        compact_kv(
                                            "search params",
                                            parallel=effective_search_params.get("parallel"),
                                            top=effective_search_params.get("top"),
                                            test_duration=effective_search_params.get("test_duration"),
                                            use_query_plan_cache=effective_search_params.get("use_query_plan_cache"),
                                            query_plan_cache_enable_CAST=effective_search_params.get("query_plan_cache_enable_CAST"),
                                            query_plan_cache_only_vector=effective_search_params.get("query_plan_cache_only_vector"),
                                            query_plan_cache_use_number=effective_search_params.get("query_plan_cache_use_number"),
                                            query_parameterizer_max_threads=effective_search_params.get("query_parameterizer_max_threads"),
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
                                
                                # Calculate trimmed mean (remove max and min, then average)
                                if search_number > 2 and not recall_only:
                                    # For QPS values, we want to remove outliers
                                    if "qps" in all_search_stats[0]:
                                        qps_values = [stats["qps"] for stats in all_search_stats]
                                        qps_values_sorted = sorted(qps_values)
                                        # Remove min and max
                                        trimmed_qps = qps_values_sorted[1:-1]
                                        avg_qps = sum(trimmed_qps) / len(trimmed_qps) if trimmed_qps else qps_values_sorted[0]
                                        
                                        # Use the first search stats as base and update QPS with trimmed mean
                                        averaged_stats = dict(all_search_stats[0])
                                        averaged_stats["qps"] = avg_qps
                                        # Store original values for reference
                                        averaged_stats["original_qps_values"] = qps_values
                                        averaged_stats["trimmed_mean_qps"] = avg_qps
                                    else:
                                        averaged_stats = all_search_stats[0]  # Fallback if no QPS
                                elif not recall_only:
                                    # If search_number <= 2, just use the average of all results
                                    if "qps" in all_search_stats[0]:
                                        qps_values = [stats["qps"] for stats in all_search_stats]
                                        avg_qps = sum(qps_values) / len(qps_values)
                                        averaged_stats = dict(all_search_stats[0])
                                        averaged_stats["qps"] = avg_qps
                                        averaged_stats["original_qps_values"] = qps_values
                                        averaged_stats["average_qps"] = avg_qps
                                    else:
                                        averaged_stats = all_search_stats[0]
                                else:
                                    # For recall_only, we don't average, just collect results
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
                                        use_number = use_number,
                                        threads = threads
                                    )
                                    # Save results to CSV for easy comparison
                                    self.save_to_csv(
                                        search_results=averaged_stats,
                                        search_params=effective_search_params,
                                        dataset_config=dataset.config,
                                        cache_mode=cache_mode,
                                        CAST_mode=CAST_mode,
                                        only_vector=only_vector,
                                        use_number=use_number,
                                        threads=threads
                                    )
                            if recall_only and recall_only_results:
                                self.save_recall_only_results(recall_only_results)

    def _run_pgvector_experiment(self, dataset: Dataset, skip_upload: bool, recall_only: bool, upload_stats, reader):
        """Complete PGvector-specific experiment implementation with basic cache parameters only"""
        import functools
        
        search_number = self.uploader.upload_params.get("search_number", 1)
        
        use_cache_values = self.uploader.upload_params.get("use_cache", [0])
        
        for use_cache_val in use_cache_values:
            # PGvector-specific parameter injection - only use_cache
            def with_pgvector_cache_modes(search_params):
                params = dict(search_params or {})
                params["use_query_plan_cache"] = int(use_cache_val)
                return params

            def log_pgvector_params(params_dict, prefix):
                log_kwargs = {
                    "parallel": params_dict.get("parallel"),
                    "top": params_dict.get("top"),
                    "test_duration": params_dict.get("test_duration"),
                    "use_query_plan_cache": params_dict.get("use_query_plan_cache"),
                }
                params_only = (params_dict.get("params") or {})
                if not isinstance(params_only, dict):
                    params_only = {}
                compact_kv(prefix, **log_kwargs, **params_only)

            if self.searchers:
                duration_searchers = [
                    s for s in self.searchers if int((s.search_params or {}).get("test_duration", 0) or 0) > 0
                ]
                if duration_searchers:
                    last_searcher = duration_searchers[-1]
                    last_test_duration = int((last_searcher.search_params or {}).get("test_duration", 0) or 0)
                    stage("WARMUP")
                    warmup_seconds = int(round(last_test_duration * 0.1))
                    warmup_seconds = max(1, min(5, warmup_seconds))
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
            
            stage("SEARCH")
            if not recall_only:
                self._warn_search_memory(reader, dataset)
            recall_only_results: list[dict] = [] if recall_only else []
            for search_id, searcher in enumerate(self.searchers):
                if recall_only:
                    stage(f"SEARCHER {search_id + 1}/{len(self.searchers)} (recall-only)")
                else:
                    stage(f"SEARCHER {search_id + 1}/{len(self.searchers)}")
                original_search_params = {**searcher.search_params}
                effective_search_params = with_pgvector_cache_modes(original_search_params)
                if recall_only:
                    effective_search_params["test_duration"] = 0
                
                # Collect multiple search results for averaging
                all_search_stats = []
                for i in range(search_number):
                    # Create a fresh searcher instance for each iteration to avoid state contamination
                    current_searcher = searcher.__class__(
                        searcher.host,
                        connection_params={**(searcher.connection_params or {})},
                        search_params=effective_search_params,
                    )
                    
                    if i == 0:  # Only show params once for the first iteration
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
                
                # Calculate trimmed mean (remove max and min, then average)
                if search_number > 2 and not recall_only:
                    # For RPS values, we want to remove outliers
                    if "rps" in all_search_stats[0]:
                        rps_values = [stats["rps"] for stats in all_search_stats]
                        rps_values_sorted = sorted(rps_values)
                        # Remove min and max
                        trimmed_rps = rps_values_sorted[1:-1]
                        avg_rps = sum(trimmed_rps) / len(trimmed_rps) if trimmed_rps else rps_values_sorted[0]
                        
                        # Use the first search stats as base and update RPS with trimmed mean
                        averaged_stats = dict(all_search_stats[0])
                        averaged_stats["rps"] = avg_rps
                        # Store original values for reference
                        averaged_stats["original_rps_values"] = rps_values
                        averaged_stats["trimmed_mean_rps"] = avg_rps
                    else:
                        averaged_stats = all_search_stats[0]  # Fallback if no RPS
                elif not recall_only:
                    # If search_number <= 2, just use the average of all results
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
                    # For recall_only, we don't average, just collect results
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
                            "params": params_only,
                            metric_key: metric_value,
                        }
                    )
                else:
                    # For PGvector, always pass 0 for MyScale-specific parameters
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
                        use_number=0
                    )
                    # Save results to CSV for easy comparison
                    self.save_pgvector_to_csv(
                        search_results=averaged_stats,
                        search_params=effective_search_params,
                        dataset_config=dataset.config,
                        use_cache=use_cache_val
                    )
            if recall_only and recall_only_results:
                self.save_recall_only_results(recall_only_results)

    def save_pgvector_to_csv(self, search_results, search_params, dataset_config, use_cache):
        """
        Save PGvector benchmark results to CSV file with all required parameters for comparison.
        """
        try:
            # Extract required parameters
            parallel = search_params.get("parallel", 0)
            test_duration = search_params.get("test_duration", 0)
            ef_s = search_params.get("params", {}).get("ef_s", 0) if isinstance(search_params.get("params"), dict) else 0
            
            # Extract RPS from search results (PGvector uses RPS, not QPS)
            rps = search_results.get("rps", 0)
            
            # Prepare CSV row data
            csv_row = {
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'experiment_name': self.name,
                'dataset': getattr(dataset_config, 'name', ''),
                'vector_size': getattr(dataset_config, 'vector_size', 0),
                'distance': getattr(dataset_config, 'distance', ''),
                'use_cache': int(use_cache),
                'parallel': int(parallel),
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
            
            # Write to CSV file
            file_exists = os.path.exists(CSV_RESULTS_FILE)
            
            with open(CSV_RESULTS_FILE, 'a', newline='') as csvfile:
                fieldnames = [
                    'timestamp', 'experiment_name', 'dataset', 'vector_size', 'distance',
                    'use_cache', 'parallel', 'test_duration', 'ef_s',
                    'rps', 'recall', 'mean_precisions', 'mrr', 'mean_time', 'p95_time', 'p99_time'
                ]
                
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                if not file_exists:
                    writer.writeheader()
                
                writer.writerow(csv_row)
                
        except Exception as e:
            warn(f"Failed to save CSV results: {e}")

    def run_experiment(self, dataset: Dataset, skip_upload: bool = False, recall_only: bool = False):
        
        execution_params = self.configurator.execution_params(
            distance=dataset.config.distance, vector_size=dataset.config.vector_size
        )
        reader = dataset.get_reader(execution_params.get("normalize", False))
        
        # Determine engine type directly from configuration
        engine_lower = self.engine.lower()
        is_myscale = engine_lower == 'myscale'
        is_pgvector = engine_lower == 'pgvector'
        
        search_number = self.uploader.upload_params.get("search_number", 1)
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
            if getattr(dataset.config, "result_group", "") in ("text_search", "hybrid_search"):
                corpus_count = getattr(dataset.config, "corpus_count", 0) or 0
                if corpus_count:
                    effective_vector_count = corpus_count
            upload_stats = self.uploader.upload(
                distance=dataset.config.distance,
                vector_count=effective_vector_count,
                records=reader.read_data(),
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
        
        if is_myscale:
            self._run_myscale_experiment(dataset, skip_upload, recall_only, upload_stats, reader)
        elif is_pgvector:
            self._run_pgvector_experiment(dataset, skip_upload, recall_only, upload_stats, reader)
        else:
            pass
        stage("DONE")
