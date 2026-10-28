"""benchmark 主编排入口。

职责：
1. 读取 ``configurations/*.json``（实验/引擎配置，允许一个文件包含 JSON 数组）
   与 ``datasets/datasets.json``（数据集定义）；
2. 按 ``--engines`` / ``--datasets`` 的 glob 表达式筛选，并解析每个实验配置
   绑定的数据集（配置里的 ``dataset`` 字段）；
3. 对每个 (engine, dataset) 组合：用 ``ClientFactory`` 构建客户端，下载数据集，
   再调用 ``client.run_experiment()`` 依次执行
   CONFIGURE（建表）→ UPLOAD（写入/建索引）→ post_upload → SEARCH（压测）
   → DONE，结果由各客户端写入 ``results/`` 下的 CSV/JSON；
4. 支持 ``--skip-upload`` / ``--skip-search`` / ``--recall-only`` 等裁剪阶段。

路径约定：脚本自身定位 ``benchmark/`` 与项目根目录并加入 ``sys.path``，
因此 ``cd benchmark && python run.py`` 与 ``python benchmark/run.py`` 都可用。

退出码：配置缺失、筛选结果为空等“无任务可跑”的情况统一 ``SystemExit(2)``。
"""

import fnmatch
import argparse
import sys
from pathlib import Path

# Support both documented invocations:
#   cd benchmark && python run.py
#   python benchmark/run.py
# The project has a top-level ``benchmark`` package and a sibling-style
# ``engine`` import tree, so both roots must be importable.
# ---- 导入路径准备 ----
# benchmark/ 与项目根目录都插入 sys.path：保证 `benchmark.*` 与 `engine.*`
# 两种导入方式在两种启动方式下都能解析。insert(0) 使项目根优先。
_BENCHMARK_DIR = Path(__file__).resolve().parent
_PROJECT_DIR = _BENCHMARK_DIR.parent
for _path in (str(_PROJECT_DIR), str(_BENCHMARK_DIR)):
    if _path not in sys.path:
        sys.path.insert(0, _path)
from benchmark.config_read import read_dataset_config, read_engine_configs
from benchmark.cli_output import header, step
from benchmark.dataset import Dataset
from engine.base_client import IncompatibilityError
from engine.clients.client_factory import ClientFactory


def run(
    engines: str = "*",
    datasets: str = "*",
    engine_type: str | None = None,
    host: str | None = None,
    port: int | None = None,
    skip_upload: bool = False,
    skip_search: bool = False,
    recall_only: bool = False,
):

    """
    Example:
        python3 run.py --engines *-m-16-* --datasets glove-*

    参数与命令行同义：
    - engines: 匹配 configurations/*.json 里 name 字段的 glob（fnmatch 语义）。
    - datasets: 匹配 datasets/datasets.json 里 name 字段的 glob。
    - engine_type: 按配置的 engine 字段（clickhouse/myscale/pgvector/polardb）再过滤。
    - host/port: 覆盖配置中的连接参数；None 表示沿用配置/默认值。
    - skip_upload: 跳过建表与上传阶段（假定表已存在且有数据/索引）。
    - skip_search: 只建表+索引，不进入 SEARCH 阶段。
    - recall_only: 只做召回/指标评估，忽略 queries_pool_size 限制。
    """
    # ---- 读取配置 ----
    try:
        all_engines = read_engine_configs()
    except FileNotFoundError:
        # configurations/ 下没有任何可解析的 *.json
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

    # ---- 按 glob 筛选实验（配置 name 字段）与数据集 ----
    selected_engines = {
        name: config
        for name, config in all_engines.items()
        if fnmatch.fnmatch(name, engines)
    }

    # 如果指定了 engine_type，进一步过滤引擎
    # 比较前统一小写；engine 字段缺失时视为 ""，不会匹配任何具体类型。
    if engine_type:
        engine_type_lower = engine_type.lower()
        before_count = len(selected_engines)
        selected_engines = {
            name: config
            for name, config in selected_engines.items()
            if (config.get("engine") or "").lower() == engine_type_lower
        }
        if len(selected_engines) < before_count:
            step(f"已按 engine_type='{engine_type}' 过滤: {before_count} -> {len(selected_engines)} 个配置")

    selected_datasets = {
        name: config
        for name, config in all_datasets.items()
        if fnmatch.fnmatch(name, datasets)
    }

    # ---- 筛选结果为空的早退分支 ----
    if not selected_engines:
        header("未找到 Engines")
        step(f"未找到匹配的 engines: {engines}")
        step(f"请确认 configurations/ 目录下存在配置文件，并检查是否有配置文件中的 name 字段与 {engines} 一致。")
        if not all_engines:
            step("检测到 configurations/ 目录下没有任何配置文件。")
            step("configurations/templates 下的测试配置文件不会被自动检测到，需要复制到 configurations/ 目录。")
        raise SystemExit(2)

    if datasets != "*" and not selected_datasets:
        # 只在显式指定数据集时视为错误；默认 "*" 允许为空（由后续 targets 判断）。
        header("未找到 Datasets")
        step(f"未找到匹配的 datasets: {datasets}")
        step(f"请检查 datasets/datasets.json 中的数据集 name 字段是否存在 {datasets}。")
        raise SystemExit(2)

    # ---- 构建 (engine, dataset) 执行计划 ----
    # targets 元素含义：(实验名, 引擎配置, 数据集名, 数据集配置)
    targets: list[tuple[str, dict, str, dict]] = []
    # 下面三个列表只用于诊断输出：
    #   配置未写 dataset / dataset 不存在 / dataset 被 --datasets 排除。
    engine_missing_dataset: list[tuple[str, str]] = []
    engine_dataset_not_found: list[tuple[str, str, str]] = []
    engine_dataset_filter_mismatch: list[tuple[str, str, str]] = []

    for engine_name, engine_config in selected_engines.items():
        dataset_in_engine = str(engine_config.get("dataset", "") or "").strip()
        # _source_file 由 config_read 注入，记录配置来源文件（诊断信息用）。
        source_file = str(engine_config.get("_source_file", "") or "").strip()

        # 情况 1：配置未绑定数据集 -> 对 --datasets 选中的每个数据集各跑一次。
        if dataset_in_engine == "":
            engine_missing_dataset.append((engine_name, source_file))
            for dataset_name, dataset_config in selected_datasets.items():
                targets.append((engine_name, engine_config, dataset_name, dataset_config))
            continue

        # 情况 2：配置引用的数据集在 datasets.json 中不存在 -> 跳过该配置。
        if dataset_in_engine not in all_datasets:
            engine_dataset_not_found.append((engine_name, source_file, dataset_in_engine))
            continue

        # 情况 3：数据集存在但被本次 --datasets 过滤掉 -> 跳过该配置。
        if dataset_in_engine not in selected_datasets:
            engine_dataset_filter_mismatch.append((engine_name, source_file, dataset_in_engine))
            continue

        # 正常绑定：一个配置对应一个数据集。
        targets.append((engine_name, engine_config, dataset_in_engine, all_datasets[dataset_in_engine]))

    # ---- 诊断信息输出（不改变执行计划）----
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

    if engine_dataset_filter_mismatch:
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

    # 选中的数据集若没有任何配置使用，仅提示（不终止），便于发现配置遗漏。
    used_datasets = {dataset_name for _, _, dataset_name, _ in targets}
    if datasets != "*" and selected_datasets:
        unused = sorted(set(selected_datasets.keys()) - used_datasets)
        if unused:
            header("未找到对应配置文件")
            step(f"datasets 已匹配到: {', '.join(sorted(selected_datasets.keys()))}")
            step(f"但没有任何 engines 配置的 dataset 字段匹配: {', '.join(unused)}")
            step("请检查 configurations/*.json 中的 dataset 字段。")

    # ---- 顺序执行每个实验 ----
    for engine_name, engine_config, dataset_name, dataset_config in targets:
        # 复制配置后移除内部字段，避免把它传给客户端/写入结果。
        engine_config = dict(engine_config)
        engine_config.pop("_source_file", None)
        configured_engine = str(engine_config.get("engine", "") or "").lower()
        conn = dict(engine_config.get("connection_params", {}))
        # Command-line values are overrides.  When omitted, preserve the
        # per-engine values from the JSON (5432/5433 for PostgreSQL, 9000 for
        # ClickHouse-compatible servers).
        # ---- 连接参数：命令行 > 配置文件 > 内置默认 ----
        if host is not None:
            conn["host"] = host
        elif not conn.get("host"):
            conn["host"] = "127.0.0.1"
        if port is not None:
            conn["port"] = port
        elif not conn.get("port"):
            # 默认端口：polardb 5433（避免与本机 PostgreSQL 5432 冲突），
            # pgvector 5432；其余（ClickHouse/MyScale 兼容服务）统一 9000。
            conn["port"] = {"polardb": 5433, "pgvector": 5432}.get(configured_engine, 9000)
        engine_config["connection_params"] = {
            **conn,
        }
        # 把数据集的 result_group 注入 upload_params，供上传器选择建模/写入策略。
        upload_params = dict(engine_config.get("upload_params", {}) or {})
        result_group = str(dataset_config.get("result_group", "") or "")
        if result_group:
            upload_params["_result_group"] = result_group
        engine_config["upload_params"] = upload_params
        effective_host = engine_config["connection_params"]["host"]
        header(f"EXPERIMENT: {engine_name}")

        # 构建客户端 -> 准备数据（必要时下载/解压）-> 执行完整实验流程。
        client = ClientFactory(effective_host).build_client(engine_config, dataset_name, dataset_config)
        dataset = Dataset(dataset_config)
        dataset.download()
        try:
            client.run_experiment(dataset, skip_upload, recall_only=recall_only, skip_search=skip_search)
        except IncompatibilityError as e:
            # 引擎与数据集不兼容（如类型/维度不支持）时跳过该组合，继续后续实验。
            step(f"skipped: {engine_name} - {dataset_name} - {e}")
            continue


if __name__ == "__main__":
    # ---- 命令行参数 ----
    # 所有字符串筛选参数只接受单个 glob，需匹配多个时用通配符而不是列表。
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
    parser.add_argument("--engine-type", default=None, help="filter by engine type: 'clickhouse', 'myscale', 'pgvector', 'polardb' (default: None, no filter)")
    parser.add_argument("--host", default=None, help="override server IP; otherwise use each config")
    parser.add_argument("--port", type=int, default=None, help="override server port; otherwise use each config")
    parser.add_argument("--skip-upload", action="store_true", help="skip data upload and index build stages")
    parser.add_argument(
        "--skip-search",
        action="store_true",
        help="only create table + build index (CONFIGURE/UPLOAD/post_upload), then exit without running SEARCH. "
        "Used to prepare tables for sql-bench/polardb-pase-query-forms-benchmark.sh",
    )
    parser.add_argument(
        "--recall-only",
        action="store_true",
        help="only run recall/metric evaluation on all test queries; ignores queries_pool_size limits"
    )
    args = parser.parse_args()

    # 将解析结果原样传给 run()（命令行参数优先于函数默认值）。
    run(
        engines=args.engines,
        datasets=args.datasets,
        engine_type=args.engine_type,
        host=args.host,
        port=args.port,
        skip_upload=args.skip_upload,
        skip_search=args.skip_search,
        recall_only=args.recall_only,
    )