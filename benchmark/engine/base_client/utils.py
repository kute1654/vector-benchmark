import math
from typing import Any, Iterable, List, Optional
import os
from dataset_reader.base_reader import Record


def iter_batches(records: Iterable[Record], n: int) -> Iterable[Any]:
    ids = []
    vectors = []
    metadata = []

    for record in records:
        ids.append(record.id)
        vectors.append(record.vector)
        metadata.append(record.metadata)

        if len(vectors) >= n:
            yield [ids, vectors, metadata]
            ids, vectors, metadata = [], [], []
    if len(ids) > 0:
        yield [ids, vectors, metadata]


# Intersect Precision, IP
def intersect_precision(actual_ids: List[int], expected_ids: List[int], limit: int):
    ans_len = min(len(actual_ids), len(expected_ids), limit)
    if ans_len == 0:
        return 0.0
    expected_set = set(expected_ids[:limit])
    actual_set = set(actual_ids[:limit])
    intersect_len = len(actual_set.intersection(expected_set))
    return intersect_len / ans_len


# Average Precision, AP
def average_precision(actual_ids: List[int], expected_ids: List[int], limit: int):
    ans_len = min(len(actual_ids), len(expected_ids), limit)
    expected_set = set(expected_ids[:limit])

    precision_sum = 0
    num_hits = 0

    for i, id in enumerate(actual_ids[:limit]):
        if id in expected_set:
            num_hits += 1
            precision_sum += num_hits / (i + 1)

    if num_hits == 0:
        return 0

    return precision_sum / ans_len


# Discounted Cumulative Gain, DCG
def dcg(actual_ids: List[int], expected_ids: List[int], limit: int, expected_scores: List[Any] = None):
    dcg_score = 0
    for i, id in enumerate(actual_ids[:limit]):
        if id in expected_ids[:limit]:
            # rel_score = expected_scores[expected_ids.index(id)]
            rel_score = 1
            dcg_score += rel_score / math.log2(i + 2)
    return dcg_score


# Normalized Discounted Cumulative Gain, NDCG
def ndcg(actual_ids: List[int], expected_ids: List[int], limit: int):
    idcg_score = dcg(expected_ids, expected_ids, limit)
    dcg_score = dcg(actual_ids, expected_ids, limit)
    if idcg_score == 0:
        return 0
    return dcg_score / idcg_score


# MRR
def mrr(actual_ids: List[int], expected_ids: List[int], limit: int):
    mrr_result = 0.0
    actual_ids = actual_ids[:limit]

    for i in range(0, min(len(actual_ids), limit)):
        if actual_ids[i] in expected_ids:
            mrr_result += 1 / (i + 1)
            break

    return mrr_result


def get_mem_available_bytes() -> Optional[int]:
    def read_int(path: str) -> Optional[int]:
        try:
            with open(path, "r") as fp:
                raw = fp.read().strip()
            if raw.isdigit():
                return int(raw)
        except Exception:
            return None
        return None

    mem_available = None
    try:
        with open("/proc/meminfo", "r") as meminfo:
            for line in meminfo:
                if line.startswith("MemAvailable:"):
                    parts = line.split()
                    if len(parts) >= 2 and parts[1].isdigit():
                        mem_available = int(parts[1]) * 1024
                        break
    except Exception:
        mem_available = None

    cgroup_available = None
    if os.path.exists("/sys/fs/cgroup/memory.max"):
        try:
            with open("/sys/fs/cgroup/memory.max", "r") as fp:
                limit_raw = fp.read().strip()
            if limit_raw != "max" and limit_raw.isdigit():
                limit_val = int(limit_raw)
                current_val = read_int("/sys/fs/cgroup/memory.current")
                if current_val is not None:
                    cgroup_available = max(limit_val - current_val, 0)
        except Exception:
            cgroup_available = None

    if cgroup_available is None and os.path.exists("/sys/fs/cgroup/memory/memory.limit_in_bytes"):
        limit_val = read_int("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        current_val = read_int("/sys/fs/cgroup/memory/memory.usage_in_bytes")
        if limit_val is not None and current_val is not None and limit_val < (1 << 60):
            cgroup_available = max(limit_val - current_val, 0)

    if mem_available is None:
        return cgroup_available
    if cgroup_available is None:
        return mem_available
    return min(mem_available, cgroup_available)


def format_bytes(value: Optional[int]) -> str:
    if value is None:
        return "unknown"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(value)
    for unit in units:
        if size < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(size)}{unit}"
            return f"{size:.1f}{unit}"
        size /= 1024.0
