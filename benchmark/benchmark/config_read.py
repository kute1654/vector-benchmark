import glob
import json
import os

from benchmark import CONFIGURATIONS_DIR, DATASETS_DIR


def read_engine_configs() -> dict:
    """ Concatenate all engine configurations """
    all_configs = {}
    config_dir = str(CONFIGURATIONS_DIR)
    root_dir = os.path.dirname(config_dir)
    config_files = sorted(set(glob.glob(os.path.join(config_dir, "*.json"))))
    if not config_files:
        raise FileNotFoundError(
            f"No experiment config files found in {config_dir}. "
            f"Please copy your myscale experiment json files into configurations."
        )
    for config_file in config_files:
        with open(config_file, "r") as fd:
            configs = json.load(fd)
            for config in configs:
                config_with_meta = dict(config)
                try:
                    config_with_meta["_source_file"] = os.path.relpath(config_file, start=root_dir)
                except Exception:
                    config_with_meta["_source_file"] = os.path.basename(config_file)
                all_configs[config["name"]] = config_with_meta

    return all_configs


def read_dataset_config():
    all_configs = {}
    datasets_config_path = DATASETS_DIR / "datasets.json"
    with open(datasets_config_path, "r") as fd:
        configs = json.load(fd)
        for config in configs:
            all_configs[config["name"]] = config
    return all_configs

# print(read_engine_configs())
# print(read_dataset_config())
