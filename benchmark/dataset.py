"""数据集描述与下载/读取工厂。

``Dataset`` 包装 ``datasets/datasets.json`` 中的一条数据集配置，负责：

- ``download()``：按配置中的 ``link`` 把主数据文件（以及 ``query_files``
  中的多组查询文件）下载到 ``DATASETS_DIR`` 下；
- ``get_reader(normalize)``：按 ``type`` 字段选择对应的数据集读取器。

与其它模块的关系：``run.py`` 通过 ``Dataset`` 准备数据，再把 reader 产出的
``Record`` 交给上传器；目录常量来自 ``benchmark/__init__.py``。

重要副作用：``download`` 会访问网络、在**当前工作目录**写入临时下载文件，
随后移动到/解压到 ``DATASETS_DIR``，并删除临时文件；目标已存在时跳过。
"""

import os
import shutil
import tarfile
import urllib.request

from . import DATASETS_DIR
from .dataset_config import DatasetConfig
from .dataset_reader.ann_compound_reader import AnnCompoundReader
from .dataset_reader.json_reader import JSONReader
from .dataset_reader.ann_h5_reader import AnnH5Reader
from .dataset_reader.gz_tsv_reader import GzTsvReader
from .dataset_reader.base_reader import BaseReader

# 数据集 type -> 读取器实现；新增文件格式时在此登记
READER_TYPE = {"h5": AnnH5Reader, "jsonl": JSONReader, "tar": AnnCompoundReader, "gz_tsv": GzTsvReader}


def download_core(config_path: str, link: str):
    """下载单个数据集文件到 ``DATASETS_DIR / config_path``。

    参数：
        config_path: 相对 ``DATASETS_DIR`` 的目标路径。若对应的是压缩包，
            该路径会被当作解压目录；否则被当作目标文件路径。
        link: 下载地址；为空/None 表示必须由用户手动放置文件。

    行为：
        1. 目标已存在则直接返回（不校验完整性）。
        2. ``urllib.request.urlretrieve`` 先下载到**当前工作目录**下以 URL
           末段命名的临时文件。
        3. ``.tgz`` / ``.tar.gz``：创建目标目录并全部解压到其中；
           其它格式：复制到目标文件路径（自动创建父目录）。
        4. 删除临时文件。

    异常：目标不存在且 ``link`` 为空时抛 ``FileNotFoundError``。
    副作用：网络下载、写文件、解压、删除临时文件。
    """
    target_path = DATASETS_DIR / config_path
    if target_path.exists():
        print(f"{target_path} already exists")
        return

    if link is None or str(link).strip() == "":
        raise FileNotFoundError(
            f"Dataset file not found: {target_path}. "
            f"Please download it manually and place it at this path."
        )

    # 仅取 URL 最后一段作为临时文件名（不含查询串处理）
    file_name = f"{str(link).split('/')[-1]}"
    print(f"Downloading {link} to file {file_name}")
    # 注意：urlretrieve 将文件写到当前工作目录；urlretrieve 可能返回实际文件名
    file_name, _ = urllib.request.urlretrieve(link, file_name)

    if file_name.endswith(".tgz") or file_name.endswith(".tar.gz"):
        # 压缩包：target_path 作为解压目录
        print(f"Mkdir if not exists: {target_path} -> Extracting: {file_name} -> {target_path}")
        os.makedirs(target_path, exist_ok=True)
        file = tarfile.open(file_name)
        file.extractall(target_path)
        file.close()
        os.remove(file_name)
    else:
        # 普通文件：target_path 即目标文件，复制后删除下载到的临时文件
        print(f"Moving: {file_name} -> {target_path.parent}")
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(file_name, target_path)
        os.remove(file_name)


class Dataset:
    """一条数据集配置的运行时封装。"""

    def __init__(self, config: dict):
        """用原始配置字典构造；字段校验/默认值由 ``DatasetConfig`` 完成。

        参数：
            config: ``datasets.json`` 中的单条配置（含 ``name``、``type``、
                ``path``、``link`` 等字段）。
        """
        self.config = DatasetConfig(**config)

    def download(self):
        """下载主数据文件及所有附加查询文件（若配置了 ``query_files``）。

        每个文件都委托 ``download_core``，已存在的会被跳过。
        副作用：见 ``download_core``（网络 + 磁盘写入）。
        """
        # download train data
        download_core(config_path=self.config.path, link=self.config.link)
        # download multi queries data
        if self.config.query_files is not None:
            for query_config in self.config.query_files:
                download_core(config_path=query_config["path"], link=query_config["link"])

    def get_reader(self, normalize: bool) -> BaseReader:
        """按 ``config.type`` 实例化数据集读取器。

        参数：
            normalize: 是否对向量做归一化（由各读取器解释，通常用于 cosine）。
        返回：``BaseReader`` 子类实例，数据根目录固定为 ``DATASETS_DIR``。
        异常：``config.type`` 不在 ``READER_TYPE`` 中时抛 ``KeyError``。
        """
        reader_class = READER_TYPE[self.config.type]
        return reader_class(DATASETS_DIR, self.config, normalize=normalize)