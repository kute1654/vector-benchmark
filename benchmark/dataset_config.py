"""数据集配置的数据类定义。

本模块只声明结构，不做 I/O。``DatasetConfig`` 对应 ``datasets/datasets.json``
中的一条记录，由 ``benchmark.dataset.Dataset`` 用 ``DatasetConfig(**config)``
实例化；``QueryConfig`` 描述混合检索等多查询数据集的单个查询文件。

注意：``DatasetConfig.query_files`` 标注为 ``List[QueryConfig]``，但
``Dataset.download()`` 实际按字典下标（``query_config["path"]``）访问，
即当前 JSON 中该字段通常仍是原始 dict 列表。
"""

from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field


@dataclass
class QueryConfig:
    """一个附加查询数据集的配置（用于 hybrid_search 的多组查询）。

    字段：
        name: 查询集名称。
        meta: 任意元信息字典。
        queries_pool_size: 搜索时使用的查询数量（条）。
        path: 相对 ``datasets/`` 的本地路径。
        link: 下载地址。
        score_type: 评分方式，默认 ``"default"``。
    """
    name: str
    meta: dict
    queries_pool_size: int
    path: str
    link: str
    score_type: Optional[str] = "default"


@dataclass
class DatasetConfig:
    """单个数据集/实验组的完整描述。

    仅前四个字段必填，其余带默认值，便于 vector / text / hybrid 各类数据集
    共用同一结构。
    """
    name: str  # dataset name 数据集唯一名称
    result_group: str  # vector_search or hybrid_search or text_search 结果分组
    type: str  # file format: "h5", "jsonl", "tar", "gz_tsv", etc. 文件格式，决定读取器
    path: str  # local path to main dataset file 主数据文件（相对 datasets/）的本地路径
    link: Optional[str] = None  # dataset link, use it to download file 下载地址
    # dataset group, such as arxiv_title_no_filter and arxiv_title_filter belong to one group
    # 数据集分组名：同组内用 tag 区分不同变体
    group_name: Optional[str] = None
    tag: Optional[str] = None  # dataset tag, we use tag to differentiate dataset in one group 组内标签

    # search 检索相关
    queries_pool_size: int = 0  # number of queries used for search 搜索使用的查询条数
    score_type: Optional[str] = "default"  # "default", "mrr", "ndcg", etc. 评分方式

    # vector datasets 向量数据集
    vector_count: int = 0  # vector count 向量总条数
    vector_size: int = 0  # vector dimension 向量维度
    distance: str = "l2"  # "l2", "cosine", etc. 距离度量

    # text/hybrid datasets 文本/混合数据集
    corpus_count: int = 0  # corpus/doc count (for text/hybrid datasets) 语料/文档条数
    # hybrid_search queries dataset (they use same train dataset) 多组查询文件（共用主数据集）
    query_files: Optional[List[QueryConfig]] = None
    corpus_file: Optional[str] = None  # 语料文件路径
    queries_file: Optional[str] = None  # 查询文件路径
    qrels_file: Optional[str] = None  # 相关性标注（qrels）文件路径
    corpus_embedding_file: Optional[str] = None  # 语料向量文件
    queries_embedding_file: Optional[str] = None  # 查询向量文件
    query_cols: Optional[List[str]] = None  # 查询中使用的列名
    schema: Optional[Dict[str, str]] = field(default_factory=dict)  # 字段名 -> 类型 的映射