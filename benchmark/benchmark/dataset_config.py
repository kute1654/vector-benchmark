from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field

@dataclass
class QueryConfig:
    name: str
    meta: dict
    queries_pool_size: int
    path: str
    link: str
    score_type: Optional[str] = "default"


@dataclass
class DatasetConfig:
    name: str  # dataset name
    result_group: str  # vector_search or hybrid_search or text_search
    type: str  # file format: "h5", "jsonl", "tar", "gz_tsv", etc.
    path: str  # local path to main dataset file
    link: Optional[str] = None  # dataset link, use it to download file
    group_name: Optional[str] = None  # dataset group, such as arxiv_title_no_filter and arxiv_title_filter belong to one group
    tag: Optional[str] = None  # dataset tag, we use tag to differentiate dataset in one group

    # search
    queries_pool_size: int = 0  # number of queries used for search
    score_type: Optional[str] = "default"  # "default", "mrr", "ndcg", etc.

    # vector datasets
    vector_count: int = 0 # vector count
    vector_size: int = 0  # vector dimension
    distance: str = "l2"  # "l2", "cosine", etc.

    # text/hybrid datasets
    corpus_count: int = 0 # corpus/doc count (for text/hybrid datasets)
    query_files: Optional[List[QueryConfig]] = None  # hybrid_search queries dataset (they use same train dataset)
    corpus_file: Optional[str] = None
    queries_file: Optional[str] = None
    qrels_file: Optional[str] = None
    corpus_embedding_file: Optional[str] = None
    queries_embedding_file: Optional[str] = None
    query_cols: Optional[List[str]] = None
    schema: Optional[Dict[str, str]] = field(default_factory=dict)
