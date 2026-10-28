"""纯 JSONL 目录格式读取器。

目录约定（``path / dataset_config.path``）：

- ``vectors.jsonl``    : 每行一个 JSON 数组，即一条文档向量（必需）。
- ``payloads.jsonl``   : 每行一个 JSON 对象，附加字段；与 vectors 按行号对应（可选）。
- ``queries.jsonl``    : 每行一个 JSON 数组，即一条查询向量（必需）。
- ``neighbours.jsonl`` : 每行一个 JSON 数组，query 的标准答案 id（可选）。

所有文件按行流式读取，不做全量预载；缺省文件用无限生成器占位，
由上层的 ``zip`` 以较短的一侧（vectors / query vectors）自然截断。

注意：``AnnCompoundReader`` 继承本类，并覆盖 ``read_vectors`` / ``read_queries``，
因此本模块的常量与实现也被复合目录格式复用。
"""

import json
import os
import sys
from pathlib import Path
from typing import Iterator, List, Optional, Tuple
import numpy as np

# 把 benchmark 的父目录加入 sys.path，使本模块既能作为包导入，
# 也能以脚本方式直接运行时找到同级包（历史兼容逻辑）。
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from ..dataset_config import DatasetConfig
from .base_reader import BaseReader, Query, Record

# 下面四个文件名是 JSONL 目录格式的固定约定，不可通过配置改名。
VECTORS_FILE = "vectors.jsonl"
PAYLOADS_FILE = "payloads.jsonl"
QUERIES_FILE = "queries.jsonl"
NEIGHBOURS_FILE = "neighbours.jsonl"


class JSONReader(BaseReader):
    """读取由 vectors/queries/payloads/neighbours 四个 JSONL 组成的目录。"""

    def __init__(self, path: Path, dataset_config: DatasetConfig, normalize=False):
        """
        Args:
            path: 数据集根目录（通常是 DATASETS_DIR）。
            dataset_config: 提供 path（相对根目录的子路径）与 schema。
            normalize: 是否对向量做 L2 归一化。
        """
        self.path = (path / dataset_config.path)
        # 保留原调试输出（副作用）：构造时会打印解析后的数据目录。
        print(self.path)
        self.dataset_config = dataset_config
        self.normalize = normalize

    def read_payloads(self) -> Iterator[dict]:
        """逐行产出 payload 字典，仅保留 schema 中声明的列。

        文件缺失时返回无限 ``{}``（由 ``read_data`` 中的 zip 与 vectors 对齐后截断）。
        注意 ``schema`` 为 None 时 ``.keys()`` 会抛 AttributeError。
        """
        if not (self.path / PAYLOADS_FILE).exists():
            while True:
                yield {}
        valid_columns = list(self.dataset_config.schema.keys())
        with open(self.path / PAYLOADS_FILE, "r") as json_fp:
            for json_line in json_fp:
                line = json.loads(json_line)
                # filter out non-stored fields
                # 仅挑选 schema 声明的列；缺失字段补 None。
                re_line = dict([(col, line.get(col)) for col in valid_columns])
                yield re_line

    def read_vectors(self) -> Iterator[List[float]]:
        """逐行读取文档向量；normalize=True 时按 L2 范数归一化（零向量会产生 NaN）。"""
        with open(self.path / VECTORS_FILE, "r") as json_fp:
            for json_line in json_fp:
                vector = json.loads(json_line)
                if self.normalize:
                    vector = vector / np.linalg.norm(vector)
                yield vector

    def read_neighbours(self) -> Iterator[Optional[List[int]]]:
        """逐行读取标准答案 id；文件缺失时无限产出 None。

        注意：缺失分支没有 return，``while True`` 之后的 open 永远不会执行。
        """
        if not (self.path / NEIGHBOURS_FILE).exists():
            while True:
                yield None

        with open(self.path / NEIGHBOURS_FILE, "r") as json_fp:
            for json_line in json_fp:
                line = json.loads(json_line)
                yield line

    def read_query_vectors(self) -> Iterator[List[float]]:
        """逐行读取查询向量；此处使用 ``/=`` 原地归一化（要求元素为可变数值标量）。"""
        with open(self.path / QUERIES_FILE, "r") as json_fp:
            for json_line in json_fp:
                vector = json.loads(json_line)
                if self.normalize:
                    vector /= np.linalg.norm(vector)
                yield vector

    def read_queries(self, times: Optional[int] = 1000, query_meta: Optional[dict] = None) -> Iterator[Query]:
        """产出最多 ``times`` 条查询；query 文件耗尽后循环重读实现复用。

        Args:
            times: 查询条数上限；达到后退出。
            query_meta: 本格式不支持多组 meta，传非 None 会被忽略（未做校验）。
        """
        count = 0
        while True:
            exit_flag = 0
            # zip 以查询向量与 neighbours 中较短者为准；neighbours 缺失时无限 None。
            for idx, (vector, neighbours) in enumerate(
                    zip(self.read_query_vectors(), self.read_neighbours())
            ):
                count += 1
                # 计数到上限后先置退出标志，再跳出内层循环（本条查询被丢弃）。
                if count > times:
                    exit_flag = 1
                    break
                yield Query(vector=vector, meta_conditions=None, expected_result=neighbours)
            if exit_flag == 1:
                break

    def read_data(self) -> Iterator[Record]:
        """
        Returns:Record(id=0, vector=[0.29879.....378], metadata={})

        id 为行号（从 0 开始），metadata 为 payload 字典（无 payload 文件时为 ``{}``）。
        """
        # zip 以 vectors 行数为准：payload 过多会被忽略，payload 不足则只输出公共部分。
        for idx, (vector, payload) in enumerate(
                zip(self.read_vectors(), self.read_payloads())
        ):
            yield Record(id=idx, vector=vector, metadata=payload)

    def read_column_name_type(self) -> Tuple[list, list]:
        """ Get the payloads data name and type """
        # 列类型直接来自 dataset_config.schema（例如 {"body": "string"}）。
        extra_columns = []
        extra_columns_type = []
        for extra_column in list(self.dataset_config.schema.keys()):
            extra_columns.append(extra_column)
            extra_columns_type.append(self.dataset_config.schema.get(extra_column))

        return extra_columns, extra_columns_type