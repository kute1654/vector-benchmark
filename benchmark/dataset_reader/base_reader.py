"""读取器抽象层：统一数据记录与查询的数据结构。

各具体 Reader（HDF5 / JSONL / gz_tsv / 复合目录）都实现
:class:`BaseReader` 的迭代器方法，把各自的磁盘格式转换为
:class:`Record`（入库数据）与 :class:`Query`（检索请求 + 标准答案）。

约定：
- ``read_data()`` 每次产出一条待上传记录，按需逐条读取，避免一次性载入全量数据。
- ``read_queries(times, query_meta)`` 产出用于压测的查询；``times`` 是期望产出的
  查询条数（不是迭代器重复次数）。
- 所有实现均为生成器（懒加载），调用方负责控制消费速度与内存。
"""

from dataclasses import dataclass
from typing import Iterator, List, Optional, Tuple


@dataclass
class Record:
    """一条待上传的向量记录。

    Attributes:
        id: 记录主键（由 Reader 生成的连续整数，或从原始数据解析的 uint32 id）。
        vector: 稠密向量，Python float 列表，长度应等于 dataset_config.vector_size。
        metadata: 附加列（payload）；无附加列时为 None。
    """

    id: int
    vector: List[float]
    metadata: Optional[dict]


@dataclass
class Query:
    """一条检索查询及其标准答案（ground truth）。

    Attributes:
        vector: 查询向量；text_search 等无向量场景下为空列表或零向量。
        meta_conditions: 元数据过滤条件（dict），由客户端拼接到查询语句中；
            无过滤时为 None。
        expected_result: 标准答案的文档 id 列表（neighbors / qrels），用于计算召回。
        expected_scores: 与 expected_result 对应的距离/分数（若数据提供），默认 None。
        score_type: 指标类型（如 "default"、"mrr"），影响结果评估方式。
        query_text: 文本查询内容（hybrid/text_search 使用）。
        query_text_column: 文本写入的列名，客户端据此选择查询列。
    """

    vector: List[float]
    meta_conditions: Optional[dict]
    expected_result: Optional[List[int]]
    expected_scores: Optional[List[float]] = None
    score_type: Optional[str] = "default"
    query_text: Optional[str] = None
    query_text_column: Optional[str] = None


class BaseReader:
    """所有数据集读取器的抽象基类。

    子类需实现下列方法；基类实现一律抛出 NotImplementedError，起到接口契约作用。
    """

    def read_data(self) -> Iterator[Record]:
        """逐条产出待上传的记录（懒加载生成器，可能跨越多个文件/分块）。"""
        raise NotImplementedError()

    def read_queries(self, times: Optional[int] = 1000, query_meta: Optional[dict] = None) -> Iterator[Query]:
        """产出大约 ``times`` 条查询。

        Args:
            times: 期望产出的查询条数；数据不足时各实现可能循环复用查询。
            query_meta: 仅选取 meta 匹配的查询文件；用于
                gist-960-euclidean-probability-l2 这类带多组过滤条件的数据集。

        Raises:
            NotImplementedError: 基类未实现。
        """
        # query_meta: to support the datasets gist-960-euclidean-probability-l2
        raise NotImplementedError()

    def prefetch(self, vector, *items) -> List:
        """预留的批量预取接口（当前主流程未调用），子类可按需实现。"""
        raise NotImplementedError()

    def read_column_name_type(self) -> Tuple[list, list]:
        """返回附加列的 (列名列表, 列类型列表)，供建表/上传时使用。"""
        raise NotImplementedError()

    def get_query_files(self) -> List[dict]:
        """返回查询文件描述列表；每项至少含 path，可能含 meta / score_type /
        queries_pool_size 等字段。"""
        raise NotImplementedError()
