"""qdrant 复合目录格式读取器。

对应 https://github.com/qdrant/ann-filtering-benchmark-datasets 的布局：
目录（``path / dataset_config.path``）下同时存放向量、payload 与查询，且
不同信息分散在不同文件：

- ``vectors.npy``   : numpy 二维数组，全部文档向量（一次性 ``np.load`` 进内存）。
- ``payloads.jsonl``: 每行一个 JSON 对象，文档附加字段；由父类 JSONReader 读取，
                      与 vectors.npy 按行号对齐。
- ``tests.jsonl``   : 每行一个 JSON 对象，字段：
                      ``query``（查询向量）、``conditions``（过滤条件）、
                      ``closest_ids`` / ``closest_scores``（标准答案 id 与分数）。

``read_data()`` 直接复用父类实现（vectors.npy + payloads.jsonl 的 zip）；
本类只覆盖向量读取与查询读取。
"""

import json
import os
import sys
from typing import Iterator, List, Optional
import numpy as np

# 同 json_reader：兼容脚本方式运行时补齐导入路径（历史逻辑，保留不动）。
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from .base_reader import Query
from .json_reader import JSONReader


class AnnCompoundReader(JSONReader):
    """
    A reader created specifically to read the format used in
    https://github.com/qdrant/ann-filtering-benchmark-datasets, in which vectors
    and their metadata are stored in separate files.

    继承 JSONReader：目录解析、payload 读取、read_data/read_column_name_type 均复用。
    """

    # 覆盖父类的同名常量：文档向量改用 npy，查询文件改用 tests.jsonl。
    # payloads.jsonl 仍沿用父类的 PAYLOADS_FILE。
    VECTORS_FILE = "vectors.npy"
    QUERIES_FILE = "tests.jsonl"

    def read_vectors(self) -> Iterator[List[float]]:
        """加载 vectors.npy 并逐行产出 float 列表。

        注意：``np.load`` 会把整个矩阵读入内存（N×dim×dtype 字节），
        对大集合内存占用显著；生成器只节省了逐行转换的开销。
        """
        vectors = np.load(str(self.path / self.VECTORS_FILE))
        for vector in vectors:
            if self.normalize:
                # 未保护零向量；归一化后转换为 Python list。
                vector = vector / np.linalg.norm(vector)
            yield vector.tolist()

    def read_queries(self, times: Optional[int] = 1000, query_meta: Optional[dict] = None) -> Iterator[Query]:
        """逐行读取 tests.jsonl，产出带过滤条件与标准答案的查询。

        Args:
            times: 查询条数上限；文件耗尽后会重新打开文件循环复用，直到达到上限。
            query_meta: 本格式不支持多组 meta，非 None 时直接抛 RuntimeError。

        Raises:
            RuntimeError: 传入了 query_meta。
            KeyError: 某行缺少 query/conditions/closest_ids/closest_scores 字段。
        """
        if query_meta is not None:
            raise RuntimeError("Not support multi query meta in jsonl datasets...")
        count = 0
        # 外层 while 用于在查询数少于 times 时重复读取整个文件。
        while True:
            exit_flag = 0
            with open(self.path / self.QUERIES_FILE) as payloads_fp:
                for idx, row in enumerate(payloads_fp):
                    row_json = json.loads(row)
                    vector = np.array(row_json["query"])
                    if self.normalize:
                        vector /= np.linalg.norm(vector)
                    count += 1
                    # 先计数再判断：达到上限时丢弃当前这条，置标志退出。
                    if count > times:
                        exit_flag = 1
                        break
                    yield Query(
                        vector=vector.tolist(),
                        meta_conditions=row_json["conditions"],
                        expected_result=row_json["closest_ids"],
                        expected_scores=row_json["closest_scores"],
                    )
            if exit_flag == 1:
                break


# 以下为保留的手工调试示例（已注释，不参与执行），
# 演示如何以 arxiv-titles 复合数据集构造 Reader 并遍历 read_data()。
# if __name__ == "__main__":
#     from benchmark.dataset_config import DatasetConfig
#     from pathlib import Path
#
#     my_test = AnnCompoundReader(
#         path=Path("/Users/mochix/workspace_mqdb_bench/vector-db-benchmark/datasets"),
#         dataset_config=DatasetConfig(name="arxiv-titles-384-angular-filters",
#                                      result_group="single-search",
#                                      vector_size=384,
#                                      distance='cosine',
#                                      type='tar',
#                                      path="arxiv-titles-384-angular/arxiv",
#                                      schema={
#                                          "update_date_ts": "int",
#                                          "label": "keyword"
#                                      }),
#         normalize=True
#     )
#     for i in my_test.read_data():
#         print(i)