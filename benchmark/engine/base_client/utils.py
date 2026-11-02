"""驱动式客户端的通用小工具：分批、检索指标与内存探测。

不依赖任何引擎，可被上传阶段（分批写入）与搜索阶段（指标计算、内存
告警）共同使用。
"""

from typing import Any, Iterable, List, Optional
import os
from benchmark.dataset_reader.base_reader import Record


def iter_batches(records: Iterable[Record], n: int) -> Iterable[Any]:
    """把记录流按 ``n`` 条切批，产出 ``[ids, vectors, metadata]`` 三平行列表。

    采用流式读取：只有当前批次在内存里，适合大文件数据集；最后不足 ``n``
    条的尾巴也会产出一次，空输入则不产出任何批次。
    """
    ids = []
    vectors = []
    metadata = []

    for record in records:
        ids.append(record.id)
        vectors.append(record.vector)
        metadata.append(record.metadata)

        if len(vectors) >= n:
            # 每批都换新列表，避免调用方持有引用后被下一批内容覆盖。
            yield [ids, vectors, metadata]
            ids, vectors, metadata = [], [], []
    if len(ids) > 0:
        yield [ids, vectors, metadata]


# Intersect Precision, IP
def intersect_precision(actual_ids: List[int], expected_ids: List[int], limit: int):
    """交集精度（即 recall@limit）：命中数 / 可比较条数。

    分母取 ``min(实际返回数, ground truth 数, limit)``，因此查询返回不足
    ``limit`` 条时不会被当成漏召回。两侧都先去重再求交，重复 id 不会虚增
    分子。
    """
    ans_len = min(len(actual_ids), len(expected_ids), limit)
    if ans_len == 0:
        return 0.0
    expected_set = set(expected_ids[:limit])
    actual_set = set(actual_ids[:limit])
    intersect_len = len(actual_set.intersection(expected_set))
    return intersect_len / ans_len


# MRR
def mrr(actual_ids: List[int], expected_ids: List[int], limit: int):
    """平均倒数排名（单条查询）：只取第一个命中位置的倒数，未命中为 0。"""
    mrr_result = 0.0
    actual_ids = actual_ids[:limit]

    for i in range(0, min(len(actual_ids), limit)):
        if actual_ids[i] in expected_ids:
            mrr_result += 1 / (i + 1)
            break

    return mrr_result


def get_mem_available_bytes() -> Optional[int]:
    """估算当前进程还能用多少内存（字节），失败时返回 None。

    同时看两个来源并取较小值，因为容器里两者可能不一致：

    1. ``/proc/meminfo`` 的 ``MemAvailable``（单位 KB，乘以 1024）。
    2. cgroup 限额减当前用量，兼容 v2（``memory.max`` / ``memory.current``）
       与 v1（``memory.limit_in_bytes`` / ``memory.usage_in_bytes``）；
       v1 的无限额用超大值表示，故用 ``< 1 << 60`` 过滤掉。

    取 min 才是真正可用的上界：宿主机可能还有空闲，但容器配额已经快满。
    """

    def read_int(path: str) -> Optional[int]:
        """读取一个只含十进制数字的文件；不存在/不可读/非数字都返回 None。

        cgroup v1 的无限额用超大数表示、v2 用 ``max`` 表示，后者会走到
        ``isdigit()`` 为假的分支并被当作"未知"，调用方据此跳过该级限制。
        """
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
    """把字节数格式化成人类可读的 ``1.5GB`` 形式；None 显示为 ``unknown``。

    按 1024 进制逐级换算；字节单位不带小数，其余保留一位。
    """
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