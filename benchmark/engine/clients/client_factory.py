from abc import ABC
from itertools import product
from typing import List, Dict, Any, Iterable

from engine.base_client.client import (
    BaseClient,
    BaseConfigurator,
    BaseSearcher,
    BaseUploader,
)
from engine.clients.myscale.configure import MyScaleConfigurator
from engine.clients.myscale.search import MyScaleSearcher
from engine.clients.myscale.upload import MyScaleUploader

# Import ClickHouse clients
try:
    from engine.clients.clickhouse.configure import ClickHouseConfigurator
    from engine.clients.clickhouse.search import ClickHouseSearcher
    from engine.clients.clickhouse.upload import ClickHouseUploader
    CLICKHOUSE_AVAILABLE = True
except ImportError:
    CLICKHOUSE_AVAILABLE = False

# Import PGVector clients
try:
    from engine.clients.pgvector.configure import PGVectorConfigurator
    from engine.clients.pgvector.search import PGVectorSearcher
    from engine.clients.pgvector.upload import PGVectorUploader
    PGVECTOR_AVAILABLE = True
except ImportError:
    PGVECTOR_AVAILABLE = False


class ClientFactory(ABC):
    def __init__(self, host):
        self.host = host

    @staticmethod
    def _expand_params(params: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not isinstance(params, dict) or not params:
            return [params]
        option_keys = []
        option_values = []
        fixed = {}
        for k, v in params.items():
            if isinstance(v, list):
                option_keys.append(k)
                option_values.append(v)
            else:
                fixed[k] = v
        if not option_keys:
            return [params]
        expanded = []
        for combo in product(*option_values):
            merged = {**fixed}
            for k, v in zip(option_keys, combo):
                merged[k] = v
            expanded.append(merged)
        return expanded

    @classmethod
    def _expand_search_params(cls, search_params: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if not isinstance(search_params, dict) or not search_params:
            yield search_params
            return
        base = {k: v for k, v in search_params.items() if k != "params"}
        params = search_params.get("params", {})
        if not isinstance(params, dict):
            params = {}

        expanded_bases = cls._expand_params(base)
        expanded_params_list = cls._expand_params(params)

        for expanded_base in expanded_bases:
            for expanded_params in expanded_params_list:
                yield {**expanded_base, "params": expanded_params}

    def _create_configurator(self, experiment) -> BaseConfigurator:
        engine_type = experiment.get("engine", "myscale").lower()

        if engine_type == "pgvector" and PGVECTOR_AVAILABLE:
            engine_configurator = PGVectorConfigurator(
                self.host,
                collection_params={**experiment.get("upload_params", {})},
                connection_params={**experiment.get("connection_params", {})},
            )
        elif engine_type == "clickhouse" and CLICKHOUSE_AVAILABLE:
            engine_configurator = ClickHouseConfigurator(
                self.host,
                collection_params={**experiment.get("upload_params", {})},
                connection_params={**experiment.get("connection_params", {})},
            )
        else:
            engine_configurator = MyScaleConfigurator(
                self.host,
                collection_params={**experiment.get("upload_params", {})},
                connection_params={**experiment.get("connection_params", {})},
            )
        return engine_configurator

    def _create_uploader(self, experiment) -> BaseUploader:
        engine_type = experiment.get("engine", "myscale").lower()
        upload_params = {**experiment.get("upload_params", {})}

        if engine_type == "pgvector" and PGVECTOR_AVAILABLE:
            # Handle PGVector specific parameters
            index_type_raw = upload_params.get("index_type", "hnsw")
            index_type_str = str(index_type_raw or "").strip()
            index_type = index_type_str.lower()

            if index_type not in ["hnsw"]:
                raise RuntimeError(f"PGVector only supports 'hnsw' index_type, got: {index_type}")

            upload_params["_index_type"] = index_type

            engine_uploader = PGVectorUploader(
                self.host,
                connection_params={**experiment.get("connection_params", {})},
                upload_params=upload_params,
            )
        elif engine_type == "clickhouse" and CLICKHOUSE_AVAILABLE:
            # ClickHouse logic
            index_type_raw = upload_params.get("index_type", "")
            index_type_str = str(index_type_raw or "").strip()
            index_type = index_type_str.upper()

            # ClickHouse 26.6.1.1 支持的索引类型
            # HNSWFLAT, HNSW, VECTOR_SIMILARITY 是有效的（都会映射到 vector_similarity）
            # ANNOY, USEARCH, FLAT 也是有效的
            supported_index_types = ["HNSWFLAT", "HNSW", "VECTOR_SIMILARITY", "ANNOY", "USEARCH", "FLAT"]
            if index_type and index_type not in supported_index_types:
                raise RuntimeError(f"ClickHouse only supports {supported_index_types} index_type, got: {index_type}")

            upload_params["_index_type"] = index_type

            engine_uploader = ClickHouseUploader(
                self.host,
                connection_params={**experiment.get("connection_params", {})},
                upload_params=upload_params,
            )
        else:
            # MyScale logic (existing code)
            index_type_raw = upload_params.get("index_type", "")
            index_type_str = str(index_type_raw or "").strip()
            index_type = index_type_str.upper()
            fts_idx_cols = upload_params.get("fts_idx_cols", None) or []
            if not index_type and not fts_idx_cols:
                raise RuntimeError("missing upload_params.index_type and/or fts_idx_cols")

            upload_params["_only_text_search"] = (not index_type) and bool(fts_idx_cols)
            upload_params["_index_type"] = index_type
            if index_type == "MSTG" or index_type == "MSRQ":
                raw_mode = upload_params.get("mstg_disk_mode", 0)
                if isinstance(raw_mode, str):
                    mode = raw_mode.strip().lower()
                    if mode in {"0", "memory", "mem", "in_memory", "ram", "false"}:
                        mstg_disk_mode = 0
                    elif mode in {"1", "disk", "ssd", "true"}:
                        mstg_disk_mode = 1
                    else:
                        raise RuntimeError(f"invalid mstg_disk_mode: {raw_mode}")
                elif isinstance(raw_mode, bool):
                    mstg_disk_mode = 1 if raw_mode else 0
                elif isinstance(raw_mode, (int, float)):
                    if isinstance(raw_mode, float) and not raw_mode.is_integer():
                        raise RuntimeError(f"mstg_disk_mode must be integer, got float: {raw_mode}")
                    mstg_disk_mode = int(raw_mode)
                    if mstg_disk_mode not in (0, 1):
                        raise RuntimeError(f"invalid mstg_disk_mode: {raw_mode}")
                else:
                    raise RuntimeError(f"unsupported type for mstg_disk_mode: {type(raw_mode)}")

                upload_params["_mstg_disk_mode"] = mstg_disk_mode

            engine_uploader = MyScaleUploader(
                self.host,
                connection_params={**experiment.get("connection_params", {})},
                upload_params=upload_params,
            )
        return engine_uploader

    def _create_searchers(self, experiment) -> List[BaseSearcher]:
        engine_type = experiment.get("engine", "myscale").lower()
        expanded_search_params = []
        raw_search_params = experiment.get("search_params", None)
        if raw_search_params is None:
            search_params_list = [{}]
        elif isinstance(raw_search_params, list):
            search_params_list = raw_search_params
        elif isinstance(raw_search_params, dict):
            search_params_list = [raw_search_params]
        else:
            search_params_list = [{}]

        # illegal checks
        upload_params = experiment.get("upload_params", {}) or {}
        result_group = upload_params.get("_result_group", "")

        for search_params in search_params_list:
            params = search_params.get("params", None)
            if params is not None:
                only_text = params.get("only_text_search", False)
                only_vector = params.get("only_vector_search", False)

                if result_group == "hybrid_search":
                    if only_text and only_vector:
                        raise RuntimeError(f"Cannot enable both only_text_search and only_vector_search in hybrid search")
                elif result_group in ("text_search", "vector_search"):
                    if only_text or only_vector:
                        params.pop("only_text_search", None)
                        params.pop("only_vector_search", None)
            expanded_search_params.extend(list(self._expand_search_params(search_params)))

        base_connection_params = {**experiment.get("connection_params", {})}
        if upload_params.get("fts_idx_cols", None) is not None:
            base_connection_params["fts_idx_cols"] = upload_params.get("fts_idx_cols")

        if engine_type == "pgvector" and PGVECTOR_AVAILABLE:
            engine_searchers = [
                PGVectorSearcher(
                    self.host,
                    connection_params={**base_connection_params},
                    search_params=search_params,
                )
                for search_params in expanded_search_params
            ]
        elif engine_type == "clickhouse" and CLICKHOUSE_AVAILABLE:
            engine_searchers = [
                ClickHouseSearcher(
                    self.host,
                    connection_params={**base_connection_params},
                    search_params=search_params,
                )
                for search_params in expanded_search_params
            ]
        else:
            engine_searchers = [
                MyScaleSearcher(
                    self.host,
                    connection_params={**base_connection_params},
                    search_params=search_params,
                )
                for search_params in expanded_search_params
            ]
        return engine_searchers

    def build_client(self, experiment, dataset_name, dataset_config):
        meta = {
            "dataset": dataset_name,
        }
        # print(experiment)
        # print(dataset_config)
        experiment_name = experiment.get("name") or f"{experiment.get('engine', 'myscale')}-{dataset_name}"
        engine_type = experiment.get("engine", "myscale")

        # 将数据集的 vector_size 注入到 experiment 的 upload_params 中
        # 这样 ClickHouse 构建向量索引时可以获取向量维度
        vector_size = dataset_config.get("vector_size", 0)
        if vector_size:
            upload_params = experiment.get("upload_params", {}) or {}
            upload_params["_vector_size"] = vector_size
            experiment["upload_params"] = upload_params

        return BaseClient(
            name=experiment_name,
            meta=meta,
            configurator=self._create_configurator(experiment),
            uploader=self._create_uploader(experiment),
            # init n search obj from search.py
            searchers=self._create_searchers(experiment),
            engine=engine_type,
        )
