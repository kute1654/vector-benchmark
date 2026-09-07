import glob
import json
import os
import sys

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
            try:
                configs = json.load(fd)
            except json.JSONDecodeError as e:
                print(f"Warning: Failed to parse {config_file}: {e}", file=sys.stderr)
                continue

            if not isinstance(configs, list):
                print(f"Warning: {config_file} should contain a JSON array, got {type(configs)}", file=sys.stderr)
                continue

            for config in configs:
                if not isinstance(config, dict):
                    print(f"Warning: Skipping invalid config entry in {config_file}: expected dict, got {type(config)}", file=sys.stderr)
                    continue
                if "name" not in config:
                    print(f"Warning: Skipping config without 'name' field in {config_file}", file=sys.stderr)
                    continue

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