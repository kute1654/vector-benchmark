"""Benchmark module entry point.

``python -m benchmark <subcommand> ...`` 的统一入口：本文件只做子命令分发，
真正的实现位于 ``benchmark.generate_sql_files`` 与 ``benchmark.query_forms``。

Usage:
    python -m benchmark generate_sql_files --engine pgvector --table benchmark_sift_128_1k
    python -m benchmark generate_sql_files --engine polardb --table benchmark_sift_1m
    python -m benchmark generate_sql_files --engine clickhouse --table Benchmark_768_1m
    python -m benchmark query_forms --config tmp/query-forms-targets.json

关键副作用：分发前会把 ``sys.argv`` 重写为“去掉子命令后的参数列表”，
使子模块中基于 argparse 的 ``parse_args()`` 能按常规方式解析；子模块可能
生成 SQL 文件或读取配置，本文件自身不读写文件。
"""

import sys
import os


def main():
    """解析子命令并委派给对应模块。

    - 无参数或 ``-h/--help``：打印模块 docstring 与子命令列表后返回。
    - ``generate_sql_files``：延迟导入模块；协助其处理 ``--help``，
      否则调用其 ``main()``。
    - ``query_forms``：调用 ``run(parse_args())`` 并以返回值作为退出码。

    副作用：原地修改 ``sys.argv``（移除子命令），并可能引发子模块的文件写入。
    异常：未知命令只打印提示，不抛异常、不返回非零退出码。
    """
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        print("Subcommands:")
        print("  generate_sql_files  Generate SQL files for the bash QPS tests")
        print("                       (pgbench / clickhouse-benchmark)")
        print("  query_forms         Compare all SQL query forms and cache profiles")
        print()
        print("For detailed options, run:")
        print("  python -m benchmark generate_sql_files --help")
        return

    command = sys.argv[1]

    if command == "generate_sql_files":
        # Import directly to avoid heavy benchmark/__init__.py
        # 延迟导入：避免为了执行子命令而加载较重的包级初始化（引擎、数据集读取器等）
        import importlib
        mod = importlib.import_module("benchmark.generate_sql_files")
        # 去掉 argv[1] 的子命令名，让子模块看到 `--engine ...` 等选项
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        if len(sys.argv) <= 1 or sys.argv[1] in ("-h", "--help"):
            # 仅解析参数并打印帮助（argparse 的 parse_args 在 --help 时会退出）
            mod.parse_args()
        else:
            mod.main()
    elif command == "query_forms":
        from benchmark import query_forms
        # 同样剥离子命令名，再以 run() 的返回值作为进程退出码
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        raise SystemExit(query_forms.run(query_forms.parse_args()))
    else:
        # 未知子命令：提示到 stderr 并打印用法；不返回非零退出码
        print(f"Unknown command: {command}", file=sys.stderr)
        print(__doc__)


if __name__ == "__main__":
    main()
