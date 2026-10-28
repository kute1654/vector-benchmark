"""数据集读取器包。

本包负责把多种磁盘格式统一成 ``BaseReader`` 定义的读取接口，向上层
（``benchmark.dataset.Dataset`` / ``engine.base_client``）提供一致的
``read_data()`` / ``read_queries()`` 迭代器：

- HDF5（ann-benchmarks 风格）      -> :class:`ann_h5_reader.AnnH5Reader`
- 纯 JSONL 目录                    -> :class:`json_reader.JSONReader`
- qdrant 复合目录（npy + jsonl）   -> :class:`ann_compound_reader.AnnCompoundReader`
- gzip 压缩的 JSONL + TSV qrels    -> :class:`gz_tsv_reader.GzTsvReader`

``benchmark.dataset.READER_TYPE`` 会依据 ``dataset_config.type``（h5 / jsonl /
tar / gz_tsv）选择具体 Reader；本 ``__init__`` 只导出 AnnH5Reader 作为快捷入口，
其余 Reader 由 ``benchmark.dataset`` 按模块路径直接导入。

无导入期副作用：不会打开文件或下载数据。
"""

from .ann_h5_reader import AnnH5Reader

__all__ = ["AnnH5Reader"]