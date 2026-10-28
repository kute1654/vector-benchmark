"""HDF5（ann-benchmarks 风格）数据集读取器。

文件约定（``dataset_dir / dataset_config.path`` 指向单个 .hdf5 文件）：

- ``train``       : 训练向量二维数组，形状 (N, dim)；N 由 vector_count 截断。
- ``test``        : 查询向量二维数组，形状 (Q, dim)。
- ``neighbors``   : 可选，形状 (Q, k) 的答案 id，用于计算召回率。
- ``distances``   : 可选，形状 (Q, k) 的答案距离/分数。
- ``filter``      : 可选，每条查询一个 bytes，内容是 dict 的字面量文本，
                    例如 ``b"{'conditions': [...]}"``；解析后取其中的 ``conditions``。
- 属性 ``extra_columns`` / ``extra_columns_type``：
                    附加列名及其 HDF5 类型字符串，仅 hybrid_search 才读取。
- 属性 ``query_columns_in_hdf5`` / ``query_columns_in_table``：
                    查询文本在 HDF5 中的数据集名，以及写入数据库时的列名。

``dataset_config.query_files`` 非空时表示多查询文件（每个文件可带不同 meta /
score_type），否则主文件自身即查询文件。读取全部为懒加载：``read_data`` 按
200k 行分块（见 ``HDF5_BATCH_PART_SIZE``），``read_queries`` 则一次性预载
查询文件（test/neighbors/distances/filter 全量进内存）。
"""

import ast
import math
from typing import Iterator, Optional, Tuple, List

import h5py
import numpy as np

from ..dataset_config import DatasetConfig
from ..cli_output import compact_kv, step
from .base_reader import BaseReader, Query, Record
from .utils import convert_H52py

# read_data 的分块行数：超过该阈值时把 train 切成若干块，控制单次内存峰值。
# 200000 行 768 维 float32 ≈ 600MB/块。
HDF5_BATCH_PART_SIZE = 200000


def convert_bytes_to_str(text):
    """把 HDF5 读出的标量转成 str。

    bytes 按 UTF-8 解码；非 str（如 numpy 标量）先 str()。解码失败会抛
    UnicodeDecodeError（不做容错）。
    """
    if isinstance(text, bytes):
        text = text.decode('utf-8')
    if not isinstance(text, str):
        text = str(text)
    return text


class AnnH5Reader(BaseReader):
    """读取单个 HDF5 文件中的向量、查询与可选过滤条件/附加列。"""

    def __init__(self, dataset_dir, dataset_config: DatasetConfig, normalize=False):
        """
        Args:
            dataset_dir: 数据集根目录（Path），与 config.path 拼接得到实际文件。
            dataset_config: 数据集配置，提供 path / vector_count / score_type 等。
            normalize: 是否把向量归一化为单位长度（cosine 场景常用）。
        """
        self.dataset_dir = dataset_dir
        self.dataset_config = dataset_config
        self.normalize = normalize
        # 初始化 query_files
        # ---- 查询文件描述 ----
        # 多查询文件模式：每个条目携带独立的 meta / score_type / queries_pool_size。
        # 注意这里把 "score_type" 默认补成 "default"，因此该分支的条目永远非 None。
        if self.dataset_config.query_files is not None:
            query_files = [
                {
                    "path": self.dataset_dir / query_file_op["path"],
                    "meta": query_file_op["meta"],
                    "score_type": query_file_op.get("score_type", "default"),
                    "queries_pool_size": query_file_op["queries_pool_size"],
                } for
                query_file_op in self.dataset_config.query_files]
        else:
            # 单文件模式：meta 为 None（表示不过滤 meta），不设 score_type 键，
            # read_queries 中会回退到 dataset_config.score_type。
            query_files = [{"path": self.dataset_dir / self.dataset_config.path,
                            "meta": None,
                            "queries_pool_size": self.dataset_config.queries_pool_size}]
        self.query_files = query_files

    def read_queries(self, times: Optional[int] = 1000, query_meta: Optional[dict] = None) -> Iterator[Query]:
        """产出查询及其标准答案。

        Args:
            times: 期望产出的查询条数；若大于文件内查询数会循环复用（见下方 while）。
            query_meta: 仅处理 meta 与之完全相等的查询文件；None 表示不过滤。

        行为要点：
        - 只有第一个未被跳过的查询文件会被读取（循环末尾无条件 break）。
        - test / neighbors / distances / filter / 文本列在进入循环前一次性读入内存，
          大查询集会有明显内存占用。
        - normalize=True 时对每条向量做 L2 归一化（未防零向量）。
        """
        for query_path in self.query_files:
            compact_kv(
                "queries",
                path=query_path.get("path"),
                meta=query_path.get("meta"),
                requested=times,
            )
            # skip mismatched query path
            # query_meta 采用字典精确相等比较（不是子集匹配）。
            if query_meta is not None and query_meta != query_path["meta"]:
                step("skip: meta not match")
                continue
            with h5py.File(query_path["path"], "r") as query_data:
                # Pre-load data into memory to avoid I/O during iteration
                # Load datasets
                # ``[:]`` 会把整个数据集复制进内存：test 常为 1000×dim，规模可控；
                # 但若换成大数据集需注意峰值内存。
                test_vectors = query_data["test"][:]
                total_queries = len(test_vectors)

                # ---- 可选数据集：缺失时用等长 None 列表占位，保证按下标对齐 ----
                if "filter" in query_data.keys():
                    raw_filters = query_data["filter"][:]
                else:
                    raw_filters = [None] * total_queries

                if "neighbors" in query_data.keys():
                    raw_neighbors = query_data["neighbors"][:]
                else:
                    raw_neighbors = [None] * total_queries

                if "distances" in query_data.keys():
                    raw_distances = query_data["distances"][:]
                else:
                    raw_distances = [None] * total_queries

                # Handle score type
                # ---- 指标类型优先级：查询文件级 > 数据集级 > "default" ----
                if query_path.get("score_type", None) is not None:
                    st = query_path["score_type"]
                elif self.dataset_config.score_type is not None:
                    st = self.dataset_config.score_type
                else:
                    st = "default"

                # Handle text columns
                # ---- 可选文本列：属性里记录 HDF5 数据集名与目标表列名，各取第一个 ----
                query_columns_in_hdf5 = query_data.attrs.get("query_columns_in_hdf5", [])
                query_columns_in_table = query_data.attrs.get("query_columns_in_table", [])

                if len(query_columns_in_hdf5) != 0:
                    raw_query_texts = [convert_bytes_to_str(bytes_str) for bytes_str in query_data[query_columns_in_hdf5[0]][:]]
                else:
                    raw_query_texts = [None] * total_queries

                if len(query_columns_in_table) != 0:
                    query_text_column = query_columns_in_table[0]
                else:
                    query_text_column = None

                # ---- 按 times 产出：不足时循环复用同一文件内的查询 ----
                count = 0
                # Use a while loop to support cycling through the dataset if times > total_queries
                while count < times:
                    # Iterate over pre-loaded data
                    for i in range(total_queries):
                        # times 可能为 None 之外的较小值；达到上限立即停止。
                        if count >= times:
                            break

                        vector = test_vectors[i]
                        if self.normalize:
                            # 未对零向量做保护；零向量会得到 NaN。
                            vector = vector / np.linalg.norm(vector)

                        filter_condition = raw_filters[i]
                        expected_result = raw_neighbors[i]
                        expected_scores = raw_distances[i]
                        query_text = raw_query_texts[i]

                        # Process meta conditions (AST eval is slow, but doing it here avoids HDF5 overhead)
                        # filter 存的是 Python 字面量文本（不是 JSON），用 ast.literal_eval 解析；
                        # 解析失败或结构不符时静默降级为 None（不中断压测）。
                        meta_conditions = None
                        if filter_condition is not None:
                             # Optimize: Only decode/eval if absolutely necessary and not None
                             # Assuming filter_condition is bytes
                             try:
                                 # "ignore" 允许非 ASCII 字节而不抛解码错误。
                                 decoded_cond = filter_condition.decode("ascii", "ignore")
                                 meta_conditions = ast.literal_eval(decoded_cond).get("conditions", None)
                             except Exception:
                                 meta_conditions = None

                        count += 1
                        yield Query(
                            vector=vector.tolist(), # Still converting to list, but from memory
                            meta_conditions=meta_conditions,
                            # 缺失的标准答案统一给空列表，避免下游判空分支。
                            expected_result=expected_result.tolist() if expected_result is not None else [],
                            expected_scores=expected_scores.tolist() if expected_scores is not None else [],
                            score_type=st,
                            query_text=query_text,
                            query_text_column=query_text_column,
                        )
                break  # Finished yielding required number of queries
                # 注意：该 break 位于 for query_files 内，因此只处理第一个匹配的查询文件；
                # 多查询文件场景需要调用方按 query_meta 多次调用。

    def read_data(self) -> Iterator[Record]:
        """按行产出训练数据（懒加载，分块读取）。

        Returns:
            Record(id=连续整数, vector=float 列表, metadata=附加列 dict 或 None)。

        副作用：对每个分块通过 ``step()`` 打印进度到 stdout。
        单位/规模：每块最多约 ``HDF5_BATCH_PART_SIZE`` 行，块大小 = data_size / 块数
        （整除，最后一块补齐余数）。
        """
        with h5py.File(self.dataset_dir / self.dataset_config.path, "r") as train_data:
            # 附加列只在 hybrid_search 场景读取；其它场景即使文件里有也不加载，
            # 以避免无谓的内存与转换开销。
            extra_columns = train_data.attrs.get("extra_columns",
                                                 []) if self.dataset_config.result_group == "hybrid_search" else []
            extra_columns_type = train_data.attrs.get("extra_columns_type",
                                                      []) if self.dataset_config.result_group == "hybrid_search" else []
            # get origin train datasets length
            # ---- 计算实际上传行数：取 min(文件行数, vector_count) ----
            # 注意：vector_count 为 0 时这里会得到 0 行（并不会回退到文件行数）。
            data_size = train_data["train"].shape[0]
            if self.dataset_config.vector_count < data_size:
                data_size = self.dataset_config.vector_count
            # default use one batch_part
            batch_parts = 1

            # 超过阈值才分块，向上取整保证覆盖全部行。
            if data_size > HDF5_BATCH_PART_SIZE:
                batch_parts = math.ceil(data_size / HDF5_BATCH_PART_SIZE)
            block_size = data_size // batch_parts

            global_idx = 0  # Add this line to initialize the global index.
            vectors_limit = -1  # 预留的截断开关：-1 表示不限制（下方判断当前永不触发）
            vector_count = 0
            for i in range(batch_parts):
                start = i * block_size
                # To handle the case of uneven data sizes, we allow the last block to contain all the remaining data.
                if i == batch_parts - 1:
                    end = data_size
                else:
                    end = start + block_size
                step(f"read data block {i + 1}/{batch_parts}: rows {start}:{end} of {data_size}")
                # avoid mess memory consume
                # 仅把当前分块读入内存，而不是整个 train。
                data_block = train_data["train"][start:end]

                if 0 < vectors_limit <= vector_count:
                    break

                # 附加列同样按块切片读取，与 data_block 下标一一对应。
                extra_columns_data = {col_name: train_data[col_name][start:end] for col_name in extra_columns}

                for idx, vector in enumerate(data_block):
                    # normalize the vector for some distance
                    if self.normalize:
                        # 原地除法；零向量会产生 NaN（不额外保护）。
                        vector /= np.linalg.norm(vector)

                    # read payload data
                    record_extra_data = {}
                    for col_name, col_type in zip(extra_columns, extra_columns_type):
                        value = extra_columns_data[col_name][idx]
                        # 声明为 string/text/blob 的列统一解码为 str；其它类型保持原值。
                        if convert_H52py(col_type) == str:
                            value = convert_bytes_to_str(value)
                        record_extra_data[col_name] = value

                    if 0 < vectors_limit <= vector_count:
                        break
                    yield Record(id=global_idx,
                                 # 维度不符时用 [0,1) 均匀随机向量兜底，保证上传不中断；
                                 # 注意这会产生“假数据”，排查维度问题时应关注此分支。
                                 vector=vector.tolist()
                                 if (len(vector) == self.dataset_config.vector_size)
                                 else np.random.uniform(0, 1, self.dataset_config.vector_size).tolist(),
                                 metadata=None if len(record_extra_data.keys()) == 0 else record_extra_data)
                    # id 由读取顺序生成（0,1,2,...），与 HDF5 内可能存在的原始 id 无关。
                    global_idx += 1
                    vector_count += 1

    def read_column_name_type(self) -> Tuple[list, list]:
        """ Get the payloads data name and type """
        # 读取 HDF5 属性中的附加列定义；与 read_data 不同，这里不做 result_group 过滤，
        # 因此 hybrid 之外的场景也可能返回非空列表（供建表阶段判断）。
        with h5py.File(self.dataset_dir / self.dataset_config.path, "r") as train_data:
            extra_columns = train_data.attrs.get("extra_columns", [])
            extra_columns_type = train_data.attrs.get("extra_columns_type", [])
            return extra_columns, extra_columns_type

    def get_query_files(self) -> List[dict]:
        """返回构造时归一化的查询文件列表（path 已拼成绝对/根目录路径）。"""
        return self.query_files