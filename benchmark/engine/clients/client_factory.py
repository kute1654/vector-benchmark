"""引擎客户端工厂：按 ``experiment.engine`` 组装 configurator / uploader / searcher。

这是 driver 式抽象（``engine.clients.*`` + ``engine.base_client.client``）的
入口。一个 experiment 配置（来自 ``configurations/*.json``）会被翻译成：

* 一个 ``BaseConfigurator``：建表 / 建索引 / 清表；
* 一个 ``BaseUploader``：批量写入向量与标量；
* 一组 ``BaseSearcher``：配置里每个 search_params 组合对应一个 searcher，
  benchmark 会对它们逐一执行搜索并分别输出结果。

各引擎模块用 ``try/except ImportError`` 延迟导入：某个引擎的客户端依赖
（如 ``clickhouse_driver``）没装时，不影响使用其他引擎。
"""

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


def _format_count(count: int) -> str:
    """把数据量压成表名友好的短串：``2000000`` → ``2m``，``200000`` → ``200k``。

    只在能被整百/整百万整除时才压缩，否则原样返回，避免 ``1234567`` 变成
    有歧义的 ``1m``。非正数统一返回 ``"0"``。
    """
    if count <= 0:
        return "0"
    if count >= 1_000_000 and count % 1_000_000 == 0:
        return f"{count // 1_000_000}m"
    if count >= 1_000 and count % 1_000 == 0:
        return f"{count // 1_000}k"
    return str(count)


def generate_table_name(dataset_config: dict) -> str:
    """根据数据集配置生成默认表名，便于在库里一眼分辨规模与用途。

    规则按优先级：

    * 纯文本检索（``result_group == "text_search"`` 且无向量维度）
      → ``Benchmark_text_<条数>``；
    * 有向量维度且有数据量 → ``Benchmark_<维度>_<条数>``；
    * 只有维度 → ``Benchmark_<维度>``；
    * 都没有 → ``Benchmark``。

    调用方（``build_client``）只在 connection_params 未显式给 ``table`` 时
    使用这个默认值。
    """
    vector_size = dataset_config.get("vector_size", 0) or 0
    vector_count = dataset_config.get("vector_count", 0) or 0
    corpus_count = dataset_config.get("corpus_count", 0) or 0
    result_group = str(dataset_config.get("result_group", "") or "")

    if result_group == "text_search" and vector_size == 0:
        # 文本检索没有向量维度，只能用语料条数区分；拿不到条数时标 unknown。
        count_str = _format_count(corpus_count) if corpus_count > 0 else "unknown"
        return f"Benchmark_text_{count_str}"

    if vector_size > 0 and (vector_count > 0 or corpus_count > 0):
        # 纯文本/混合检索的数据量记在 corpus_count 上，向量检索记在 vector_count。
        effective_count = vector_count if vector_count > 0 else corpus_count
        count_str = _format_count(effective_count)
        return f"Benchmark_{vector_size}_{count_str}"

    if vector_size > 0:
        return f"Benchmark_{vector_size}"

    return "Benchmark"

# ---- 可选引擎的延迟导入 ----
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

# PolarDB uses PostgreSQL wire protocol, but its PASE data type, operator and
# index DDL are different from pgvector.  Keep it as a first-class engine so a
# PolarDB config can never silently fall through to MyScale.
# 中文补充：PolarDB 与 pgvector 驱动相同（psycopg2），但列类型 pase、距离
# 运算符 <#>、索引 DDL 都不同，因此单独注册，不能共用 PGVector* 类。
try:
    from engine.clients.polardb.configure import PolarDBConfigurator
    from engine.clients.polardb.search import PolarDBSearcher
    from engine.clients.polardb.upload import PolarDBUploader
    POLARDB_AVAILABLE = True
except ImportError:
    POLARDB_AVAILABLE = False


class ClientFactory(ABC):
    """把一份 experiment 配置展开成可直接运行的 ``BaseClient``。

    继承 ``ABC`` 只是标记"不应实例化后直接使用"，本类并未定义抽象方法；
    实际入口是 :meth:`build_client`。
    """

    def __init__(self, host):
        """保存 ``"host:port"`` 字符串，供后续实例化各阶段组件时复用。

        host 会原样传给各引擎的 configurator/uploader/searcher，因此这里
        不做拆分或校验；端口解析由各引擎客户端自己完成（不同引擎默认端口
        不同，提前解析反而会把默认值写死在这一层）。
        """
        self.host = host

    @staticmethod
    def _expand_params(params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """把"值可以是列表"的参数表展开成多个具体配置（笛卡尔积）。

        例：``{"m": [16, 32], "ef": [64]}`` → ``[{m:16,ef:64}, {m:32,ef:64}]``。
        列表值被当作"待遍历的候选"，标量值在所有组合里保持不变。这样配置文件
        里可以一次写多组参数做参数扫描，而不用手写重复的配置块。

        非 dict / 空 dict 原样包一层返回，保证调用方总能拿到列表。
        """
        if not isinstance(params, dict) or not params:
            return [params]
        option_keys = []       # 值为 list 的键（参与展开）
        option_values = []     # 上面这些键对应的候选值列表
        fixed = {}             # 其余键（所有组合共用）
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
        """展开一组 search_params，产出外层与内层 ``params`` 的交叉组合。

        search_params 结构分两层：顶层放引擎/客户端级开关，``params`` 放
        检索参数（如 ``limit``、``ef_search``）。两层都支持列表值，这里先分别
        展开，再两两组合，避免把所有键混在一起做笛卡尔积（那会生成语义上
        不存在的组合）。
        """
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
        """按 engine 类型实例化对应的 Configurator（建表/建索引/清表）。

        未知引擎或依赖缺失都直接抛错，而不是回退到默认引擎——静默回退会
        让配置写错的实验在错误的数据库上跑完并产出看似正常的结果。
        """
        if not experiment:
            raise ValueError(
                "experiment config is None or empty. "
                "Please check your configuration file format in configurations/*.json"
            )
        engine_type = (experiment.get("engine") or "myscale").lower()

        if engine_type == "pgvector" and PGVECTOR_AVAILABLE:
            engine_configurator = PGVectorConfigurator(
                self.host,
                collection_params={**experiment.get("upload_params", {})},
                connection_params={**experiment.get("connection_params", {})},
            )
        elif engine_type == "polardb" and POLARDB_AVAILABLE:
            engine_configurator = PolarDBConfigurator(
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
        elif engine_type == "myscale":
            # MyScale 客户端是硬依赖（文件顶部直接导入），无需可用性判断。
            engine_configurator = MyScaleConfigurator(
                self.host,
                collection_params={**experiment.get("upload_params", {})},
                connection_params={**experiment.get("connection_params", {})},
            )
        else:
            raise RuntimeError(f"unsupported engine or unavailable client: {engine_type}")
        return engine_configurator

    def _create_uploader(self, experiment) -> BaseUploader:
        """按 engine 类型实例化 Uploader，并在这里统一做索引类型归一化与校验。

        归一化规则：配置对全部引擎都写成 ClickHouse 风格的 ``HNSWFLAT``，
        pgvector/PolarDB 侧转成 ``hnsw``；ClickHouse/MyScale 则统一转大写再
        按各自白名单校验。校验后的值放进 ``_index_type``（下划线前缀表示
        引擎内部字段，不直接来自用户配置）。
        """
        engine_type = experiment.get("engine", "myscale").lower()
        upload_params = {**experiment.get("upload_params", {})}

        if engine_type == "pgvector" and PGVECTOR_AVAILABLE:
            # Handle PGVector specific parameters
            index_type_raw = upload_params.get("index_type", "hnsw")
            index_type_str = str(index_type_raw or "").strip()
            index_type = index_type_str.lower()
            # config.json uses the ClickHouse-style name HNSWFLAT for all engines
            # 中文：pgvector 没有 "HNSWFLAT" 这个名字，映射到它实际的索引类型 hnsw。
            if index_type == "hnswflat":
                index_type = "hnsw"

            if index_type not in ["hnsw", "ivfflat", "none"]:
                raise RuntimeError(f"PGVector only supports 'hnsw', 'ivfflat' or 'none', got: {index_type}")

            upload_params["_index_type"] = index_type

            engine_uploader = PGVectorUploader(
                self.host,
                connection_params={**experiment.get("connection_params", {})},
                upload_params=upload_params,
            )
        elif engine_type == "polardb" and POLARDB_AVAILABLE:
            index_type = str(upload_params.get("index_type", "hnsw") or "").strip().lower()
            # config.json uses the ClickHouse-style name HNSWFLAT for all engines
            # 中文：与 pgvector 相同的命名归一化；PASE 侧只用 hnsw/ivfflat/none 三种。
            if index_type == "hnswflat":
                index_type = "hnsw"
            if index_type not in {"hnsw", "ivfflat", "none"}:
                raise RuntimeError(f"PolarDB only supports 'hnsw', 'ivfflat' or 'none', got: {index_type}")
            upload_params["_index_type"] = index_type
            engine_uploader = PolarDBUploader(
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
        elif engine_type == "myscale":
            # MyScale logic (existing code)
            index_type_raw = upload_params.get("index_type", "")
            index_type_str = str(index_type_raw or "").strip()
            index_type = index_type_str.upper()
            fts_idx_cols = upload_params.get("fts_idx_cols", None) or []
            # 向量索引与全文索引至少要配一个，否则 uploader 无从建索引。
            if not index_type and not fts_idx_cols:
                raise RuntimeError("missing upload_params.index_type and/or fts_idx_cols")

            # 只配了 fts_idx_cols 没配 index_type ⇒ 纯文本检索实验。
            upload_params["_only_text_search"] = (not index_type) and bool(fts_idx_cols)
            upload_params["_index_type"] = index_type
            if index_type == "MSTG" or index_type == "MSRQ":
                # mstg_disk_mode 允许写成 0/1、bool 或多种字符串别名，
                # 这里统一收敛为 int（0=内存模式，1=磁盘模式）。
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
                    # 1.0 这种整值浮点可接受，1.5 属于配置错误。
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
        else:
            raise RuntimeError(f"unsupported engine or unavailable client: {engine_type}")
        return engine_uploader

    def _create_searchers(self, experiment) -> List[BaseSearcher]:
        """为配置中的每个 search_params 组合创建一个 Searcher。

        ``search_params`` 支持三种写法：缺省（一个空配置）、单个对象、对象
        数组。数组里的每一项既可以是"一个完整检索配置"，也可以通过列表值
        在 ``_expand_search_params`` 里再展开成多组参数；最终每组合并后的
        配置都会独立跑一轮搜索，并各自输出一行结果。
        """
        engine_type = experiment.get("engine", "myscale").lower()
        expanded_search_params = []
        raw_search_params = experiment.get("search_params", None)
        # 统一成列表形式，后面按项处理。
        if raw_search_params is None:
            search_params_list = [{}]
        elif isinstance(raw_search_params, list):
            search_params_list = raw_search_params
        elif isinstance(raw_search_params, dict):
            search_params_list = [raw_search_params]
        elif engine_type == "myscale":
            search_params_list = [{}]
        else:
            raise RuntimeError("search_params must be an object or array")

        # illegal checks
        # result_group 由 build_client/上游写入 upload_params（"文本/向量/混合"），
        # 用来判断 only_text_search / only_vector_search 这两个开关是否有意义。
        upload_params = experiment.get("upload_params", {}) or {}
        result_group = upload_params.get("_result_group", "")

        for search_params in search_params_list:
            params = search_params.get("params", None)
            if params is not None:
                only_text = params.get("only_text_search", False)
                only_vector = params.get("only_vector_search", False)

                if result_group == "hybrid_search":
                    # 混合检索要求两路都有输入，两个 only_* 同时开就自相矛盾。
                    if only_text and only_vector:
                        raise RuntimeError(f"Cannot enable both only_text_search and only_vector_search in hybrid search")
                elif result_group in ("text_search", "vector_search"):
                    # 单一通道的实验中这两个开关没有区分作用，直接剔除，
                    # 避免它们进入 SQL 生成逻辑造成歧义。
                    if only_text or only_vector:
                        params.pop("only_text_search", None)
                        params.pop("only_vector_search", None)
            expanded_search_params.extend(list(self._expand_search_params(search_params)))

        base_connection_params = {**experiment.get("connection_params", {})}
        # 全文索引列定义属于连接级参数，searcher 生成 SQL 时需要它来决定是否启用文本检索。
        if upload_params.get("fts_idx_cols", None) is not None:
            base_connection_params["fts_idx_cols"] = upload_params.get("fts_idx_cols")

        # 每个引擎的 searcher 构造函数签名一致（host / connection_params / search_params），
        # 因此这里按引擎类型做一次分派，再对展开后的每种参数各建一个实例。
        if engine_type == "pgvector" and PGVECTOR_AVAILABLE:
            engine_searchers = [
                PGVectorSearcher(
                    self.host,
                    connection_params={**base_connection_params},
                    search_params=search_params,
                )
                for search_params in expanded_search_params
            ]
        elif engine_type == "polardb" and POLARDB_AVAILABLE:
            engine_searchers = [
                PolarDBSearcher(
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
        elif engine_type == "myscale":
            engine_searchers = [
                MyScaleSearcher(
                    self.host,
                    connection_params={**base_connection_params},
                    search_params=search_params,
                )
                for search_params in expanded_search_params
            ]
        else:
            raise RuntimeError(f"unsupported engine or unavailable client: {engine_type}")
        return engine_searchers

    def build_client(self, experiment, dataset_name, dataset_config):
        """工厂主入口：补全配置缺省项后组装出 ``BaseClient``。

        做三件事：

        1. 复制一份 experiment（避免污染调用方持有的原始配置）；
        2. 注入内部字段：``upload_params["_vector_size"]`` 与
           ``connection_params["table"]``（未显式指定时按数据集规模生成）；
        3. 分别创建 configurator / uploader / searchers 并打包。
        """
        if not experiment:
            raise ValueError(
                f"experiment config is None for dataset '{dataset_name}'. "
                "Please check your configuration file format in configurations/*.json"
            )
        meta = {
            "dataset": dataset_name,
        }
        # 浅拷贝：后续要往里塞 _vector_size / table，不能改到原始配置文件对象。
        experiment = dict(experiment)
        experiment_name = experiment.get("name") or f"{experiment.get('engine', 'myscale')}-{dataset_name}"
        engine_type = (experiment.get("engine") or "myscale").lower()

        vector_size = dataset_config.get("vector_size", 0)
        if vector_size:
            # 维度用于建表语句和距离运算符的合法性校验，供 uploader 内部读取。
            upload_params = experiment.get("upload_params", {}) or {}
            upload_params["_vector_size"] = vector_size
            experiment["upload_params"] = upload_params

        connection_params = experiment.get("connection_params", {}) or {}
        if "table" not in connection_params:
            # 用户没指定表名时按数据集规模生成默认名，保证不同 experiment
            # 不会意外写到同一张表里。
            connection_params["table"] = generate_table_name(dataset_config)
            experiment["connection_params"] = connection_params

        if engine_type not in {"pgvector", "polardb", "clickhouse", "myscale"}:
            raise RuntimeError(f"unsupported engine: {engine_type}")

        return BaseClient(
            name=experiment_name,
            meta=meta,
            configurator=self._create_configurator(experiment),
            uploader=self._create_uploader(experiment),
            searchers=self._create_searchers(experiment),
            engine=engine_type,
        )
