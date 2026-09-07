"""Benchmark module entry point.

Usage:
    python -m benchmark generate_sql_files --engine pgvector --table benchmark_sift_128_1k
    python -m benchmark generate_sql_files --engine polardb --table benchmark_sift_1m
    python -m benchmark generate_sql_files --engine clickhouse --table Benchmark_768_1m
    python -m benchmark query_forms --config query-forms/targets.json
"""

import sys
import os


def main():
    if len(sys.argv) < 2 or sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        print("Subcommands:")
        print("  generate_sql_files  Generate SQL files for bash-based QPS testing")
        print("                       (pgbench / clickhouse-benchmark)")
        print("  query_forms         Compare all SQL query forms and cache profiles")
        print()
        print("For detailed options, run:")
        print("  python -m benchmark generate_sql_files --help")
        return

    command = sys.argv[1]

    if command == "generate_sql_files":
        # Import directly to avoid heavy benchmark/__init__.py
        import importlib
        mod = importlib.import_module("benchmark.generate_sql_files")
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        if len(sys.argv) <= 1 or sys.argv[1] in ("-h", "--help"):
            mod.parse_args()
        else:
            mod.main()
    elif command == "query_forms":
        from benchmark import query_forms
        sys.argv = [sys.argv[0]] + sys.argv[2:]
        raise SystemExit(query_forms.run(query_forms.parse_args()))
    else:
        print(f"Unknown command: {command}", file=sys.stderr)
        print(__doc__)


if __name__ == "__main__":
    main()
