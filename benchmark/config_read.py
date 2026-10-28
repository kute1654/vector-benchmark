"""配置文件读取。

本模块负责把磁盘上的 JSON 配置读入内存，是 ``run.py`` 与各实验脚本的
配置入口：

- ``read_engine_configs()``：读取 ``configurations/*.json``（每个文件是
  配置对象数组），按配置的 ``name`` 汇总为一个字典，并附加来源文件信息。
- ``read_dataset_config()``：读取 ``datasets/datasets.json``。

与其它模块的关系：目录常量来自 ``benchmark/__init__.py``；返回的配置字典
后续由 ``Dataset`` / ``ClientFactory`` 使用。

重要副作用：只读文件；遇到坏 JSON、非数组、非对象或缺 ``name`` 的条目时
向 stderr 打印告警并跳过（不中断）。同名配置会被后读到的覆盖（文件按
文件名排序后再合并）。
"""

import glob
import json
import os
import sys

from benchmark import CONFIGURATIONS_DIR, DATASETS_DIR


def read_engine_configs() -> dict:
    """ Concatenate all engine configurations

    读取 ``CONFIGURATIONS_DIR`` 下全部 ``*.json`` 并合并为单个字典。

    返回：``{config_name: config_dict}``。每个 config_dict 是原配置的浅拷贝，
    额外带有 ``_source_file``（相对工程根目录的文件路径，用于报错溯源；
    若跨盘符等导致 ``relpath`` 失败则退化为文件名）。

    异常：目录下没有任何 JSON 文件时抛 ``FileNotFoundError``。
    副作用：读取多个文件；解析失败/格式不符的条目仅在 stderr 告警并跳过。
    注意：同名 ``name`` 后者覆盖前者（文件先按名称排序，保证结果稳定）。
    """
    all_configs = {}
    config_dir = str(CONFIGURATIONS_DIR)
    # 以 configurations/ 的上一级（工程根）为基准计算来源文件的相对路径
    root_dir = os.path.dirname(config_dir)
    # 先排序再 set：既有稳定顺序，又避免重复路径（glob 一般不会重复）
    config_files = sorted(set(glob.glob(os.path.join(config_dir, "*.json"))))
    if not config_files:
        raise FileNotFoundError(
            f"No experiment config files found in {config_dir}. "
            f"Please copy your myscale experiment json files into configurations."
        )
    for config_file in config_files:
        with open(config_file, "r") as fd:
            try:
                configs = json.load(fd)
            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse {config_file}: {e}", file=sys.stderr)
                continue

            # 约定每个配置文件是一个配置对象数组
            if not isinstance(configs, list):
                print(f"Warning: {config_file} should contain a JSON array, got {type(configs)}", file=sys.stderr)
                continue

            for config in configs:
                if not isinstance(config, dict):
                    print(f"Warning: Skipping invalid config entry in {config_file}: expected dict, got {type(config)}", file=sys.stderr)
                    continue
                # name 是配置的唯一标识，缺失则无法索引
                if "name" not in config:
                    print(f"Warning: Skipping config without 'name' field in {config_file}", file=sys.stderr)
                    continue

                config_with_meta = dict(config)  # 浅拷贝，避免修改调用方/缓存中的原始对象
                try:
                    config_with_meta["_source_file"] = os.path.relpath(config_file, start=root_dir)
                except Exception:
                    # 例如 Windows 跨盘符时 relpath 会失败，退化为文件名
                    config_with_meta["_source_file"] = os.path.basename(config_file)
                all_configs[config["name"]] = config_with_meta

    return all_configs


def read_dataset_config():
    """读取 ``datasets/datasets.json`` 并返回 ``{name: config_dict}``。

    与 ``read_engine_configs`` 不同，此文件按约定始终存在且格式正确，
    因此没有逐条容错；文件缺失或 JSON 非法会直接抛异常。
    """
    all_configs = {}
    datasets_config_path = DATASETS_DIR / "datasets.json"
    with open(datasets_config_path, "r") as fd:
        configs = json.load(fd)
        for config in configs:
            all_configs[config["name"]] = config
    return all_configs