import gzip
import json
import math
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple, Set

from benchmark.dataset_config import DatasetConfig
from dataset_reader.base_reader import BaseReader, Query, Record
from dataset_reader.utils import _to_uint32_id


class GzTsvReader(BaseReader):
    def __init__(self, dataset_dir: Path, dataset_config: DatasetConfig, normalize: bool = False):
        self.dataset_dir = dataset_dir
        self.dataset_config = dataset_config
        self.normalize = normalize

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

        query_cols = dataset_config.query_cols or []
        if query_cols:
            self.query_text_column = query_cols[0]
        else:
            self.query_text_column = next(iter((dataset_config.schema or {}).keys()), None)

    @staticmethod
    def _normalize_vector(vector: List[float]) -> List[float]:
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
            if qid not in remaining:
                continue
            if len(vec) != vector_size:
                raise ValueError(f"query embedding vector size mismatch: id={qid} got={len(vec)} expected={vector_size}")
            if self.normalize:
                vec = self._normalize_vector(vec)
            vectors[qid] = vec
            remaining.remove(qid)
            if not remaining:
                break
        if remaining:
            missing = sorted(list(remaining))[:10]
            raise RuntimeError(f"missing query embeddings: {len(remaining)} (sample: {missing})")
        return vectors

    def read_data(self) -> Iterator[Record]:
        if not self.corpus_path.exists():
            raise FileNotFoundError(f"missing corpus file: {self.corpus_path}")

        schema = self.dataset_config.schema or {}
        vector_size = int(self.dataset_config.vector_size or 0)
        default_vector = [] if vector_size == 0 else [0.0] * vector_size

        embedding_iter = None
        current_embedding = None
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

                metadata = {}
                for col in schema.keys():
                    if col in row:
                        metadata[col] = row.get(col)
                    elif col == "body" and "text" in row:
                        metadata[col] = row.get("text")
                    else:
                        metadata[col] = None
                    col_type = str(schema.get(col, "") or "").lower()
                    if metadata[col] is None and col_type in {"string", "text", "blob"}:
                        metadata[col] = ""

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
                    current_embedding = next(embedding_iter, None)

                yield Record(id=record_id, vector=vector, metadata=metadata if metadata else None)

    def _load_qrels(self) -> Dict[int, List[int]]:
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
        if query_meta is not None:
            raise RuntimeError("Not support multi query meta in gz_tsv datasets...")

        qrels = self._load_qrels()
        queries = self._load_queries(qrels)
        if not queries:
            raise RuntimeError("no queries loaded")

        count = 0
        score_type = self.dataset_config.score_type or "default"
        if self.dataset_config.result_group in ("text_search", "hybrid_search"):
            score_type = "mrr"

        vector_size = int(self.dataset_config.vector_size or 0)
        default_vector = [] if vector_size == 0 else [0.0] * vector_size
        query_vectors: Dict[int, List[float]] = {}
        if vector_size > 0 and self.dataset_config.result_group == "hybrid_search":
            qids = {qid for qid, _, _ in queries}
            query_vectors = self._load_query_vectors(qids)

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
        raise RuntimeError("gz_tsv reader does not support prefetch")

    def read_column_name_type(self) -> Tuple[list, list]:
        extra_columns = []
        extra_columns_type = []
        for extra_column in list((self.dataset_config.schema or {}).keys()):
            extra_columns.append(extra_column)
            extra_columns_type.append(self.dataset_config.schema.get(extra_column))
        return extra_columns, extra_columns_type

    def get_query_files(self) -> List[dict]:
        return [
            {
                "path": self.queries_path,
                "meta": None,
                "queries_pool_size": self.dataset_config.queries_pool_size,
                "score_type": self.dataset_config.score_type or "default",
            }
        ]
