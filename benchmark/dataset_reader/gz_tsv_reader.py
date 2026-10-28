"""gzip 压缩的 JSONL 语料 + TSV qrels 读取器（文本/混合检索数据集）。

目录约定（``dataset_dir / dataset_config.path``）：

- ``corpus_file``           : gzip 压缩的 JSONL，一行一篇文档，字段 ``_id`` (或 ``id``)、
                              ``text`` 及 schema 中声明的 payload 字段。
- ``queries_file``          : gzip JSONL，一行一条查询，字段 ``_id``/``id`` 与
                              ``text``（兼容 ``query``）。
- ``qrels_file``            : 未压缩 TSV，每行 ``qid<TAB>docid[<TAB>score]``，
                              score 缺省为 1，<=0 视为不相关而丢弃。
- ``corpus_embedding_file`` : 可选，gzip JSONL，每行 ``{"_id":..., "vector"/"embedding":[...]}``；
                              仅 hybrid_search 且 vector_size>0 时读取。
- ``queries_embedding_file``: 可选，同上，用于查询向量。

典型配置见 ``datasets/datasets.json`` 的 quora-mini-text-dev（纯文本）与
quora-mini-hybrid-dev（文本+384 维向量）。

实现特点：所有文件流式读取；读取语料时用单向游标把逐行 id 与嵌入文件做归并，
因此要求嵌入文件按 id 单调递增。纯文本场景向量为空列表（vector_size=0）。
"""

import gzip
import json
import math
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple, Set

from ..dataset_config import DatasetConfig
from .base_reader import BaseReader, Query, Record
from .utils import _to_uint32_id


class GzTsvReader(BaseReader):
    """读取 quora 风格（JSONL 语料 + TSV qrels）的文本/混合数据集。"""

    def __init__(self, dataset_dir: Path, dataset_config: DatasetConfig, normalize: bool = False):
        """
        Args:
            dataset_dir: 数据集根目录（DATASETS_DIR）。
            dataset_config: 提供 path、corpus/queries/qrels 文件名、schema、vector_size 等。
            normalize: 是否对向量做 L2 归一化。
        """
        self.dataset_dir = dataset_dir
        self.dataset_config = dataset_config
        self.normalize = normalize

        # ---- 路径解析：所有相对文件名都拼在同一个 base_path 下 ----
        # 未配置文件名字段时退化为空字符串，path / "" 仍等于 base_path，
        # 后续 exists() 检查会失败并给出明确错误。
        self.base_path = dataset_dir / dataset_config.path
        self.corpus_path = self.base_path / str(dataset_config.corpus_file or "")
        self.queries_path = self.base_path / str(dataset_config.queries_file or "")
        self.qrels_path = self.base_path / str(dataset_config.qrels_file or "")
        self.corpus_embedding_path = (
            self.base_path / str(dataset_config.corpus_embedding_file)
            if getattr(dataset_config, "corpus_embedding_file", None)
            else None
        )
        self.queries_embedding_path = (
            self.base_path / str(dataset_config.queries_embedding_file)
            if getattr(dataset_config, "queries_embedding_file", None)
            else None
        )

        # 查询文本写入哪一列：优先 query_cols[0]，否则取 schema 的第一个键。
        query_cols = dataset_config.query_cols or []
        if query_cols:
            self.query_text_column = query_cols[0]
        else:
            self.query_text_column = next(iter((dataset_config.schema or {}).keys()), None)

    @staticmethod
    def _normalize_vector(vector: List[float]) -> List[float]:
        """对向量做 L2 归一化，返回新列表。

        与 HDF5/JSON Reader 不同，这里显式处理空向量与零向量：范数为 0 时
        原样返回，避免产生 NaN。输入/输出均为 Python float 列表。
        """
        if not vector:
            return vector
        s = 0.0
        for x in vector:
            s += float(x) * float(x)
        if s <= 0.0:
            return vector
        inv = 1.0 / math.sqrt(s)
        return [float(x) * inv for x in vector]

    @staticmethod
    def _iter_embedding_jsonl_gz(path: Path) -> Iterator[Tuple[int, List[float]]]:
        """流式解析嵌入 JSONL.gz，产出 (uint32 id, 向量列表)。

        每行 JSON 的 id 取 ``_id``，回退 ``id``；向量取 ``vector``，回退
        ``embedding``；没有向量的行跳过。空行忽略。
        id 经 ``_to_uint32_id`` 归一化，与语料端保持一致才能对齐。
        """
        with gzip.open(path, "rt", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                raw_id = row.get("_id", row.get("id", None))
                vec = row.get("vector", row.get("embedding", None))
                if vec is None:
                    continue
                yield _to_uint32_id(raw_id), list(vec)

    def _load_query_vectors(self, qids: Set[int]) -> Dict[int, List[float]]:
        """为给定 qid 集合加载查询向量。

        Args:
            qids: 需要的查询 id 集合。

        Returns:
            qid -> 向量列表；vector_size<=0 时直接返回空 dict（不需要向量）。

        Raises:
            FileNotFoundError: 未配置或文件不存在。
            ValueError: 某条向量维度与 vector_size 不符。
            RuntimeError: 文件遍历完仍有 qid 未找到（错误信息最多列 10 个样例）。

        副作用：遍历整份嵌入文件，直到所有 qid 都被找到才提前结束。
        """
        vector_size = int(self.dataset_config.vector_size or 0)
        if vector_size <= 0:
            return {}
        if not self.queries_embedding_path:
            raise FileNotFoundError("missing queries embedding file config")
        if not self.queries_embedding_path.exists():
            raise FileNotFoundError(f"missing queries embedding file: {self.queries_embedding_path}")

        vectors: Dict[int, List[float]] = {}
        remaining = set(qids)
        for qid, vec in self._iter_embedding_jsonl_gz(self.queries_embedding_path):
            # 嵌入文件可能包含与本次查询无关的 id，直接跳过。
            if qid not in remaining:
                continue
            if len(vec) != vector_size:
                raise ValueError(f"query embedding vector size mismatch: id={qid} got={len(vec)} expected={vector_size}")
            if self.normalize:
                vec = self._normalize_vector(vec)
            vectors[qid] = vec
            remaining.remove(qid)
            # 全部找到即可停止，避免读完整个文件。
            if not remaining:
                break
        if remaining:
            missing = sorted(list(remaining))[:10]
            raise RuntimeError(f"missing query embeddings: {len(remaining)} (sample: {missing})")
        return vectors

    def read_data(self) -> Iterator[Record]:
        """流式产出语料记录（id、向量、元数据）。

        - 纯文本数据集（vector_size=0）：vector 为空列表。
        - hybrid_search（vector_size>0）：从 corpus_embedding_file 归并加载向量，
          要求嵌入文件的 id 升序，否则会报 missing embedding。
        - 其它 result_group 即使 vector_size>0 也返回全零向量（default_vector）。

        副作用：打开两个 gzip 文件；不一次性载入语料。
        """
        if not self.corpus_path.exists():
            raise FileNotFoundError(f"missing corpus file: {self.corpus_path}")

        schema = self.dataset_config.schema or {}
        vector_size = int(self.dataset_config.vector_size or 0)
        # 无向量场景给空列表；有维度但未启用 embeddings 时给等长零向量占位。
        default_vector = [] if vector_size == 0 else [0.0] * vector_size

        embedding_iter = None
        current_embedding = None
        # 仅 hybrid_search 需要真实文档向量；先预读第一条，便于后续归并。
        if vector_size > 0 and self.dataset_config.result_group == "hybrid_search":
            if not self.corpus_embedding_path:
                raise FileNotFoundError("missing corpus embedding file config")
            if not self.corpus_embedding_path.exists():
                raise FileNotFoundError(f"missing corpus embedding file: {self.corpus_embedding_path}")
            embedding_iter = self._iter_embedding_jsonl_gz(self.corpus_embedding_path)
            current_embedding = next(embedding_iter, None)

        with gzip.open(self.corpus_path, "rt", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                raw_id = row.get("_id", None)
                if raw_id is None:
                    raw_id = row.get("id", None)
                record_id = _to_uint32_id(raw_id)

                # ---- 按 schema 抽取 payload；缺失列补 None/空串 ----
                metadata = {}
                for col in schema.keys():
                    if col in row:
                        metadata[col] = row.get(col)
                    elif col == "body" and "text" in row:
                        # quora 语料用 text 字段，而 schema 常命名为 body，做兼容映射。
                        metadata[col] = row.get("text")
                    else:
                        metadata[col] = None
                    col_type = str(schema.get(col, "") or "").lower()
                    # 字符串类型缺失值用 "" 而非 None，避免建表 NOT NULL 冲突。
                    if metadata[col] is None and col_type in {"string", "text", "blob"}:
                        metadata[col] = ""

                # ---- 归并两个有序流：跳过 id 更小的嵌入，要求精确相等 ----
                vector = default_vector
                if embedding_iter is not None:
                    while current_embedding is not None and current_embedding[0] < record_id:
                        current_embedding = next(embedding_iter, None)
                    if current_embedding is None or current_embedding[0] != record_id:
                        raise RuntimeError(f"missing corpus embedding for id={record_id}")
                    vec = current_embedding[1]
                    if len(vec) != vector_size:
                        raise ValueError(
                            f"corpus embedding vector size mismatch: id={record_id} got={len(vec)} expected={vector_size}"
                        )
                    if self.normalize:
                        vec = self._normalize_vector(vec)
                    vector = vec
                    # 每消费一条就推进到下一个嵌入，供下一条语料匹配。
                    current_embedding = next(embedding_iter, None)

                yield Record(id=record_id, vector=vector, metadata=metadata if metadata else None)

    def _load_qrels(self) -> Dict[int, List[int]]:
        """读取 TSV 相关性标注，返回 qid -> 相关文档 id 列表。

        行格式 ``qid\\tdocid[\\tscore]``：前两列必须为纯数字，否则跳过；
        score 缺省/解析失败按 1 处理；score<=0 的行视为不相关并丢弃。
        同一 qid 可有多行，按出现顺序累积。
        """
        if not self.qrels_path.exists():
            raise FileNotFoundError(f"missing qrels file: {self.qrels_path}")

        qrels: Dict[int, List[int]] = {}
        with open(self.qrels_path, "r", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                parts = line.split("\t")
                if len(parts) < 2:
                    continue
                if not parts[0].isdigit() or not parts[1].isdigit():
                    continue
                qid = _to_uint32_id(parts[0])
                docid = _to_uint32_id(parts[1])
                score = 1
                if len(parts) >= 3:
                    try:
                        score = int(parts[2])
                    except Exception:
                        score = 1
                if score <= 0:
                    continue
                qrels.setdefault(qid, []).append(docid)
        return qrels

    def _load_queries(self, qrels: Dict[int, List[int]]) -> List[Tuple[int, str, List[int]]]:
        """读取 gzip JSONL 查询，返回 (qid, 文本, 相关文档 id 列表)。

        没有文本字段（text/query 均缺失）或没有 qrels 标注的查询会被跳过，
        因此最终查询数可能小于文件行数。
        """
        if not self.queries_path.exists():
            raise FileNotFoundError(f"missing queries file: {self.queries_path}")

        queries: List[Tuple[int, str, List[int]]] = []
        with gzip.open(self.queries_path, "rt", encoding="utf-8") as fp:
            for line in fp:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                qid = _to_uint32_id(row.get("_id", row.get("id", None)))
                text = row.get("text", row.get("query", None))
                if text is None:
                    continue
                rel = qrels.get(qid, [])
                if not rel:
                    continue
                queries.append((qid, str(text), rel))
        return queries

    def read_queries(self, times: Optional[int] = 1000, query_meta: Optional[dict] = None) -> Iterator[Query]:
        """产出文本/混合查询。

        Args:
            times: 查询条数上限；文件内查询不足时循环复用。注意 ``times=None``
                会被当作 0，结果是不产出任何查询。
            query_meta: 本格式不支持多组 meta，非 None 时抛 RuntimeError。

        Raises:
            RuntimeError: 传了 query_meta，或没有任何带标注的查询。
            FileNotFoundError: queries/qrels 文件缺失。
        """
        if query_meta is not None:
            raise RuntimeError("Not support multi query meta in gz_tsv datasets...")

        qrels = self._load_qrels()
        queries = self._load_queries(qrels)
        if not queries:
            raise RuntimeError("no queries loaded")

        count = 0
        score_type = self.dataset_config.score_type or "default"
        # 文本/混合检索按 MRR 评估（与向量召回指标区分）。
        if self.dataset_config.result_group in ("text_search", "hybrid_search"):
            score_type = "mrr"

        vector_size = int(self.dataset_config.vector_size or 0)
        default_vector = [] if vector_size == 0 else [0.0] * vector_size
        query_vectors: Dict[int, List[float]] = {}
        # 仅 hybrid_search 需要查询向量；纯文本查询 vector 为空列表。
        if vector_size > 0 and self.dataset_config.result_group == "hybrid_search":
            qids = {qid for qid, _, _ in queries}
            query_vectors = self._load_query_vectors(qids)

        # 外层 while 支持 times > 查询数时循环复用。
        while True:
            exit_flag = 0
            for qid, text, expected in queries:
                count += 1
                if count > int(times or 0):
                    exit_flag = 1
                    break
                yield Query(
                    vector=query_vectors.get(qid, default_vector),
                    meta_conditions=None,
                    expected_result=expected,
                    score_type=score_type,
                    query_text=text,
                    query_text_column=self.query_text_column,
                )
            if exit_flag == 1:
                break

    def prefetch(self, vector, *items) -> List:
        """该格式不支持预取，调用即抛错。"""
        raise RuntimeError("gz_tsv reader does not support prefetch")

    def read_column_name_type(self) -> Tuple[list, list]:
        """返回 schema 声明的列名与类型（顺序与 dict 插入顺序一致）。"""
        extra_columns = []
        extra_columns_type = []
        for extra_column in list((self.dataset_config.schema or {}).keys()):
            extra_columns.append(extra_column)
            extra_columns_type.append(self.dataset_config.schema.get(extra_column))
        return extra_columns, extra_columns_type

    def get_query_files(self) -> List[dict]:
        """返回单一查询文件描述；该格式不支持按 meta 拆分多个查询文件。"""
        return [
            {
                "path": self.queries_path,
                "meta": None,
                "queries_pool_size": self.dataset_config.queries_pool_size,
                "score_type": self.dataset_config.score_type or "default",
            }
        ]