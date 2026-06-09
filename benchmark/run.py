import fnmatch
import argparse
from benchmark.config_read import read_dataset_config, read_engine_configs
from benchmark.cli_output import header, step
from benchmark.dataset import Dataset
from engine.base_client import IncompatibilityError
from engine.clients.client_factory import ClientFactory


def run(
    engines: str = "*",
    datasets: str = "*",
    host: str | None = "127.0.0.1",
    port: int | None = 9000,
    skip_upload: bool = False,
    recall_only: bool = False,
):

    """
    Example:
        python3 run --engines *-m-16-* --datasets glove-*
    """
    try:
        all_engines = read_engine_configs()
    except FileNotFoundError:
        header("未找到配置文件")
        step("未找到 configurations/ 下的实验配置 JSON 文件。")
        step("请确认 configurations/ 目录下存在配置文件，并检查配置文件中的 name 字段。")
        raise SystemExit(2)

    try:
        all_datasets = read_dataset_config()
    except FileNotFoundError:
        header("未找到数据集配置")
        step("未找到 datasets/datasets.json，请确认数据集配置文件存在。")
        raise SystemExit(2)

    selected_engines = {
        name: config
        for name, config in all_engines.items()
        if fnmatch.fnmatch(name, engines)
    }
    selected_datasets = {
        name: config
        for name, config in all_datasets.items()
        if fnmatch.fnmatch(name, datasets)
    }

    if not selected_engines:
        header("未找到 Engines")
        step(f"未找到匹配的 engines: {engines}")
        step(f"请确认 configurations/ 目录下存在配置文件，并检查是否有配置文件中的 name 字段与 {engines} 一致。")
        if not all_engines:
            step("检测到 configurations/ 目录下没有任何配置文件。")
            step("configurations/templates 下的测试配置文件不会被自动检测到，需要复制到 configurations/ 目录。")
        raise SystemExit(2)

    if datasets != "*" and not selected_datasets:
        header("未找到 Datasets")
        step(f"未找到匹配的 datasets: {datasets}")
        step(f"请检查 datasets/datasets.json 中的数据集 name 字段是否存在 {datasets}。")
        raise SystemExit(2)

    targets: list[tuple[str, dict, str, dict]] = []
    engine_missing_dataset: list[tuple[str, str]] = []
    engine_dataset_not_found: list[tuple[str, str, str]] = []
    engine_dataset_filter_mismatch: list[tuple[str, str, str]] = []

    for engine_name, engine_config in selected_engines.items():
        dataset_in_engine = str(engine_config.get("dataset", "") or "").strip()
        source_file = str(engine_config.get("_source_file", "") or "").strip()

        if dataset_in_engine == "":
            engine_missing_dataset.append((engine_name, source_file))
            for dataset_name, dataset_config in selected_datasets.items():
                targets.append((engine_name, engine_config, dataset_name, dataset_config))
            continue

        if dataset_in_engine not in all_datasets:
            engine_dataset_not_found.append((engine_name, source_file, dataset_in_engine))
            continue

        if dataset_in_engine not in selected_datasets:
            engine_dataset_filter_mismatch.append((engine_name, source_file, dataset_in_engine))
            continue

        targets.append((engine_name, engine_config, dataset_in_engine, all_datasets[dataset_in_engine]))

    if engine_missing_dataset:
        header("提示")
        for engine_name, source_file in engine_missing_dataset:
            src = f"（{source_file}）" if source_file else ""
            step(f"已匹配 engines: {engine_name}{src}，但该配置未填写 dataset，将运行 --datasets 选中的数据集。")

    if engine_dataset_not_found:
        header("数据集未找到")
        for engine_name, source_file, dataset_in_engine in engine_dataset_not_found:
            src = f"（{source_file}）" if source_file else ""
            step(f"已匹配 engines: {engine_name}{src}，但 dataset='{dataset_in_engine}' 在 datasets/datasets.json 中不存在，请检查该配置文件的数据集名称。")

    if engine_dataset_filter_mismatch and datasets == "*":
        header("数据集未匹配")
        for engine_name, source_file, dataset_in_engine in engine_dataset_filter_mismatch:
            src = f"（{source_file}）" if source_file else ""
            step(f"已匹配 engines: {engine_name}{src}，但 dataset='{dataset_in_engine}' 不匹配 --datasets '{datasets}'，请检查该配置文件的数据集名称或调整 --datasets。")

    if not targets:
        header("无可执行任务")
        if datasets != "*" and selected_datasets:
            step("已选择 datasets，但没有任何匹配的配置会对这些数据集执行。")
            step("请检查 configurations/*.json 中是否存在 dataset 字段与命令行中指定的数据集一致。")
        raise SystemExit(2)

    used_datasets = {dataset_name for _, _, dataset_name, _ in targets}
    if datasets != "*" and selected_datasets:
        unused = sorted(set(selected_datasets.keys()) - used_datasets)
        if unused:
            header("未找到对应配置文件")
            step(f"datasets 已匹配到: {', '.join(sorted(selected_datasets.keys()))}")
            step(f"但没有任何 engines 配置的 dataset 字段匹配: {', '.join(unused)}")
            step("请检查 configurations/*.json 中的 dataset 字段。")

    for engine_name, engine_config, dataset_name, dataset_config in targets:
        engine_config = dict(engine_config)
        engine_config.pop("_source_file", None)
        conn = dict(engine_config.get("connection_params", {}))
        conn.pop("host", None)
        conn.pop("port", None)
        conn["host"] = host or "127.0.0.1"
        conn["port"] = port or 9000
        engine_config["connection_params"] = {
            **conn,
        }
        upload_params = dict(engine_config.get("upload_params", {}) or {})
        result_group = str(dataset_config.get("result_group", "") or "")
        if result_group:
            upload_params["_result_group"] = result_group
        engine_config["upload_params"] = upload_params
        effective_host = engine_config["connection_params"]["host"]
        effective_port = engine_config["connection_params"]["port"]
        header(f"EXPERIMENT: {engine_name}")
        client = ClientFactory(effective_host).build_client(engine_config, dataset_name, dataset_config)
        dataset = Dataset(dataset_config)
        dataset.download()
        try:
            client.run_experiment(dataset, skip_upload, recall_only=recall_only)
        except IncompatibilityError as e:
            step(f"skipped: {engine_name} - {dataset_name} - {e}")
            continue


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="myscale-bench")
    parser.add_argument(
        "--engines",
        default="*",
        help="experiment name (configurations/* JSON 'name' field); single argument; supports glob to match multiple"
    )
    parser.add_argument(
        "--datasets",
        default="*",
        help="dataset name (datasets/datasets.json 'name' field); single argument; supports glob to match multiple; selects all configs targeting the dataset"
    )
    parser.add_argument("--host", default="127.0.0.1", help="server IP (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=9000, help="server port (default: 9000)")
    parser.add_argument("--skip-upload", action="store_true", help="skip data upload and index build stages")
    parser.add_argument(
        "--recall-only",
        action="store_true",
        help="only run recall/metric evaluation on all test queries; ignores queries_pool_size limits"
    )
    args = parser.parse_args()

    run(
        engines=args.engines,
        datasets=args.datasets,
        host=args.host,
        port=args.port,
        skip_upload=args.skip_upload,
        recall_only=args.recall_only,
    )
