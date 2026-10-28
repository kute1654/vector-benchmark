"""Run one query-form QPS comparison across all configured engines.

The regular benchmark measures one canonical query per engine.  This module
measures every SQL shape emitted by :mod:`generate_sql_files` using the same
sampled query pool, concurrency, duration and cache profile, so the result is
directly comparable.  Table creation and index building remain owned by
``benchmark.run``; use ``python run.py`` once per target before this command.

Cache profiles on the ClickHouse-family engines are expressed through the query's
own SETTINGS clause (e.g. ``... SETTINGS vector_query_plan_cache=1``).  For those
per-query settings to reach the server-side query-plan-cache probe, the server must
run with the ``enable_vector_performance_test`` mode on; each worker turns it on for
its session (``_session_setup``) and the server caches are cleared once per profile
(``_clear_server_caches``).  PostgreSQL-compatible engines keep using prepared
statements (``PREPARE``/``EXECUTE``) for their plan cache.

ClickHouse and MyScale listen on the same port and cannot be up at the same time, so
they are measured in separate, sequential invocations: ``--engines clickhouse`` while
the ClickHouse server runs, then ``--engines myscale`` while the MyScale server runs.
A single invocation that would measure two engines against the same host:port is
rejected up front (``_check_endpoint_conflicts``).

中文说明
--------
本模块是 ``python -m benchmark query_forms`` 的实现主体，对外提供 ``run(args)``
与 ``parse_args()``；``benchmark/__main__.py`` 直接调用它们。它不属于 ``run.py``
的驱动式路径（``engine/base_client/*`` + ``engine/clients/*`` + ``ClientFactory``），
而是自己连接数据库、自己采样、自己跑多线程 QPS 循环的独立脚本。

职责：对同一份采样查询池，遍历“SQL 形态 × 缓存档位”，逐行把 QPS 写入 CSV。
建表、建索引、导入数据仍由 ``run.py`` 负责，本模块只执行只读查询，不会建表。
``run.py`` 负责创建数据库/表/索引并测召回；本模块只测 QPS，不计算 recall。
CSV 中 ``duration_s`` 是配置的计时窗口（秒），``queries`` 是该形态包含的 SQL
条数（每条 SQL 对应一条采样向量），``qps`` 以实际墙钟耗时为分母。
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable

from .generate_sql_files import (
    CLICKHOUSE_SQL_TYPES,
    PGVECTOR_SQL_TYPES,
    POLARDB_SQL_TYPES,
    generate_clickhouse_sql_forms,
    generate_pgvector_sql_forms,
    generate_polardb_sql_forms,
    sample_vectors_clickhouse,
    sample_vectors_pg,
)


# ---- 参数解析与配置加载 ----

def _int_list(value: str) -> list[int]:
    """把 ``"1,2,3"`` 形式的参数解析为 ``[1, 2, 3]``。

    设计为 argparse 的 ``type`` 回调；空片段会被忽略，非法整数由 ``int``
    自行抛错。
    """
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def _load_targets(path: str) -> list[dict[str, Any]]:
    """读取目标配置，返回带 ``engine`` 字段的字典列表。

    ``path`` 可以是单个 JSON 文件（顶层是对象或数组），也可以是目录：目录会
    按文件名排序后递归读取其中所有 ``*.json``，便于把多份 target 放在一起。
    缺少 ``engine`` 的条目会被静默忽略，避免把纯说明性 JSON 当作可测目标。
    """
    source = Path(path)
    if source.is_dir():
        targets = []
        for item in sorted(source.glob("*.json")):
            targets.extend(_load_targets(str(item)))
        return targets
    with source.open(encoding="utf-8") as fp:
        payload = json.load(fp)
    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list):
        raise ValueError(f"{path} must contain a JSON object or array")
    return [dict(item) for item in payload if isinstance(item, dict) and item.get("engine")]


# ---- SQL 拼装 ----

def _strip_statement_terminator(sql: str) -> str:
    """去掉语句末尾的空白和分号，便于在其后追加 ``SETTINGS`` 子句。

    先 ``rstrip`` 空白再剥一个 ``;``、再 ``rstrip``，这样 ``"... ;  "`` 也能
    被正确处理（不会因为分号前有空格而漏剥）。
    """
    return sql.rstrip().rstrip(";").rstrip()


def _setting_clause(engine: str, profile: dict[str, Any]) -> str:
    """把缓存档位渲染成 ClickHouse 系引擎的 ``SETTINGS`` 子句。

    仅 clickhouse/myscale 生效：把 profile 中除 ``name``/``mode`` 外的键值拼成
    `` SETTINGS k=v, k2=v2``（例如 ``vector_query_plan_cache=1, use_query_cache=0``）。
    渲染规则：bool 先转 0/1（服务端只认数值）；字符串若不是数字则加单引号，
    并转义内部单引号。其它引擎返回空串——pg/polardb 的缓存开关通过
    ``PREPARE``/连接内结果缓存实现，不存在 SQL 级 SETTINGS。
    """
    if engine in {"clickhouse", "myscale"}:
        settings = []
        for key, value in profile.items():
            if key in {"name", "mode"}:
                continue
            if isinstance(value, bool):
                value = int(value)
            if isinstance(value, str) and not value.replace(".", "", 1).isdigit():
                value = "'" + value.replace("'", "\'") + "'"
            settings.append(f"{key}={value}")
        return (" SETTINGS " + ", ".join(settings)) if settings else ""
    return ""


# ---- 缓存档位 ----

def _profiles(target: dict[str, Any], requested: list[str]) -> list[dict[str, Any]]:
    """返回本次要跑的缓存档位列表，可按 ``--cache-profiles`` 过滤名称。

    优先使用 target 中显式配置的 ``cache_profiles``；没有时按引擎给出默认矩阵：
      * ``myscale``：``enable_query_plan_cache`` / ``enable_cast_vector`` /
        ``query_plan_cache_only_vector`` / ``use_query_cache``；
      * ``clickhouse``：``vector_query_plan_cache`` / ``vector_use_cast`` /
        ``vector_query_plan_cache_only_vector`` / ``use_query_cache``；
      * ``pgvector``/``polardb``：``use_query_plan_cache``（PREPARE 路径）与
        ``use_result_cache``（连接内结果缓存）。
    每个档位都带 ``name``，写入 CSV 的 ``cache_profile`` 列。``requested`` 为空
    表示全跑；否则按名称筛选（未知名称不会报错，只是被过滤掉）。
    """
    configured = target.get("cache_profiles") or []
    if not configured:
        engine = str(target["engine"]).lower()
        if engine == "myscale":
            # MyScale plan-cache settings. Each profile is expressed as the server-side
            # setting names the myscaledb-oss-queryplancache branch understands. The values
            # are attached to the query's own SETTINGS clause and only take effect for the
            # plan-cache probe once `enable_vector_performance_test` is on at the session.
            # 中文：六个档位分别覆盖关闭 / 仅结果缓存 / 计划缓存 / 计划+CAST /
            # 计划缓存且只缓存向量计划 / 计划+结果缓存，用于隔离各缓存的收益。
            configured = [
                {"name": "off", "enable_query_plan_cache": 0, "enable_cast_vector": 0, "query_plan_cache_only_vector": 0, "use_query_cache": 0},
                {"name": "result-cache", "enable_query_plan_cache": 0, "enable_cast_vector": 0, "query_plan_cache_only_vector": 0, "use_query_cache": 1},
                {"name": "plan-cache", "enable_query_plan_cache": 1, "enable_cast_vector": 0, "query_plan_cache_only_vector": 0, "use_query_cache": 0},
                {"name": "plan-cast", "enable_query_plan_cache": 1, "enable_cast_vector": 1, "query_plan_cache_only_vector": 0, "use_query_cache": 0},
                {"name": "plan-only-vector", "enable_query_plan_cache": 1, "enable_cast_vector": 0, "query_plan_cache_only_vector": 1, "use_query_cache": 0},
                {"name": "plan-result-cache", "enable_query_plan_cache": 1, "enable_cast_vector": 0, "query_plan_cache_only_vector": 0, "use_query_cache": 1},
            ]
        elif engine == "clickhouse":
            configured = [
                {"name": "off", "vector_query_plan_cache": 0, "vector_use_cast": 0, "vector_query_plan_cache_only_vector": 0, "use_query_cache": 0},
                {"name": "result-cache", "vector_query_plan_cache": 0, "vector_use_cast": 0, "vector_query_plan_cache_only_vector": 0, "use_query_cache": 1},
                {"name": "plan-cache", "vector_query_plan_cache": 1, "vector_use_cast": 0, "vector_query_plan_cache_only_vector": 0, "use_query_cache": 0},
                {"name": "plan-cast", "vector_query_plan_cache": 1, "vector_use_cast": 1, "vector_query_plan_cache_only_vector": 0, "use_query_cache": 0},
                {"name": "plan-only-vector", "vector_query_plan_cache": 1, "vector_use_cast": 0, "vector_query_plan_cache_only_vector": 1, "use_query_cache": 0},
                {"name": "plan-result-cache", "vector_query_plan_cache": 1, "vector_use_cast": 0, "vector_query_plan_cache_only_vector": 0, "use_query_cache": 1},
            ]
        else:
            configured = [
                {"name": "direct", "use_query_plan_cache": 0},
                {"name": "result-cache", "use_query_plan_cache": 0, "use_result_cache": 1},
                {"name": "prepared-plan", "use_query_plan_cache": 1},
                {"name": "prepared-result-cache", "use_query_plan_cache": 1, "use_result_cache": 1},
            ]
    if not requested:
        return [dict(x) for x in configured]
    names = set(requested)
    return [dict(x) for x in configured if str(x.get("name")) in names]


def _check_endpoint_conflicts(targets: list[dict[str, Any]]) -> None:
    """Refuse to measure two engines against the same host:port in one run.

    ClickHouse and MyScale both listen on the same port (typically 9000), so at most one
    of them can be up at a time.  A config that lists both on the same endpoint would
    silently query whichever server happens to own the port, and the other engine's
    queries would be executed with setting names its server ignores - giving fake
    plan-cache numbers.  Such engines must be measured in separate, sequential runs
    (``--engines <target>``), each against its own running server.

    中文：同一 ``host:port`` 只允许出现一个引擎。ClickHouse 与 MyScale 默认都
    监听 9000，且各自只认自己那套 SETTINGS 名；混跑会把请求发给错误的服务端，
    得到"设置被忽略但仍返回成功"的假计划缓存数据。默认端口按引擎推断：
    pgvector 5432、polardb 5433、ClickHouse 系 9000。
    """
    owner: dict[tuple[str, int], str] = {}
    for target in targets:
        conn = target.get("connection_params") or {}
        engine = str(target.get("engine", "")).lower()
        default_port = {"pgvector": 5432, "polardb": 5433}.get(engine, 9000)
        endpoint = (conn.get("host", "127.0.0.1"), int(conn.get("port", default_port)))
        other = owner.get(endpoint)
        if other is not None and other != engine:
            raise SystemExit(
                f"targets of engines '{other}' and '{engine}' both use "
                f"{endpoint[0]}:{endpoint[1]}, which the two databases cannot share at the "
                f"same time. Run them sequentially, one engine per invocation, e.g. "
                f"'--engines {other}' then '--engines {engine}', and write each result to "
                f"its own CSV."
            )
        owner[endpoint] = engine


def _session_setup(engine: str) -> list[str]:
    """Session-level SQL each worker must run once on its connection.

    The ClickHouse and MyScale branches drive the query plan cache from the query's own
    SETTINGS clause (see ``_setting_clause``). That only works while the server is in
    ``enable_vector_performance_test`` mode: without it, per-query SETTINGS clauses are
    applied after AST parsing, i.e. after the plan-cache probe has already run. The flag
    is session state (putting it in the SETTINGS clause would be circular), so every worker
    enables it on its own session before measuring.

    中文：返回每个 worker 连接启动时必须执行一次的 SQL。只有 clickhouse/myscale
    需要执行 ``SET enable_vector_performance_test = 1``；pg/polardb 返回空列表。
    该开关是会话状态，不能写进每条 SQL 的 SETTINGS（否则形成循环依赖）。
    """
    if engine not in {"clickhouse", "myscale"}:
        return []
    return ["SET enable_vector_performance_test = 1"]


def _clear_server_caches(target: dict[str, Any], profile: dict[str, Any]) -> None:
    """Start each cache profile from an empty server cache so its first QPS row is
    measured the same way across runs.

    中文：只清理当前档位实际会用到的缓存——
      * ``use_query_cache``：``SYSTEM DROP QUERY CACHE``（结果缓存）；
      * clickhouse 且 ``vector_query_plan_cache``：``SYSTEM DROP VECTOR QUERY PLAN CACHE``；
      * myscale 且 ``enable_query_plan_cache``：``SYSTEM DROP QUERY PLAN CACHE``。
    无缓存档位直接返回，不建立连接。清理失败只打印 warning 并继续
    （无权限用户也要能跑），连接关闭放在 ``finally``。
    """
    engine = str(target["engine"]).lower()
    if engine not in {"clickhouse", "myscale"}:
        return
    drops = []
    if int(profile.get("use_query_cache", 0) or 0):
        drops.append("SYSTEM DROP QUERY CACHE")
    if engine == "clickhouse" and int(profile.get("vector_query_plan_cache", 0) or 0):
        drops.append("SYSTEM DROP VECTOR QUERY PLAN CACHE")
    if engine == "myscale" and int(profile.get("enable_query_plan_cache", 0) or 0):
        drops.append("SYSTEM DROP QUERY PLAN CACHE")
    if not drops:
        return
    connection = _connect(target)
    try:
        for statement in drops:
            connection.execute(statement)
    except Exception as exc:  # noqa: BLE001 - the benchmark must survive unprivileged users
        print(f"warning: failed to clear server caches for {profile.get('name', '?')}: {exc}")
    finally:
        if hasattr(connection, "close"):
            connection.close()


# ---- 采样、SQL 形态与连接 ----

def _sample(target: dict[str, Any], count: int):
    """从目标表随机采样 ``count`` 条查询向量，返回 ``[(id, vector), ...]``。

    pgvector/polardb 调用 ``sample_vectors_pg``（psycopg2），ClickHouse 系调用
    ``sample_vectors_clickhouse``。表名优先取 ``connection_params.table``，回退到
    target 顶层 ``table``；两者都没有时直接报错，避免静默查询错误对象。
    """
    engine = str(target["engine"]).lower()
    conn = target.get("connection_params") or {}
    table = conn.get("table") or target.get("table")
    if not table:
        raise ValueError(f"target {target.get('name', engine)} has no connection_params.table")
    if engine in {"pgvector", "polardb"}:
        return sample_vectors_pg(
            conn.get("host", "127.0.0.1"), int(conn.get("port", 5432)),
            conn.get("user", "postgres"), conn.get("password", ""),
            conn.get("database", "postgres"), table, count,
        )
    return sample_vectors_clickhouse(
        conn.get("host", "127.0.0.1"), int(conn.get("port", 9000)), table, count
    )


def _forms(target: dict[str, Any], vectors):
    """按引擎生成全部 SQL 形态，返回 ``{形态名: 多行 SQL 文本}``。

    参数全部来自 target：``top_k``、``distance``、``dimension``、``sql_types``
    （缺省用各引擎的完整注册表）。pgvector/polardb 调用各自生成器；ClickHouse
    系共用 ``generate_clickhouse_sql_forms``，用 ``engine=`` 切换函数家族。
    """
    engine = str(target["engine"]).lower()
    conn = target.get("connection_params") or {}
    table = conn["table"]
    top = int(target.get("top_k", 10))
    distance = target.get("distance", "l2")
    dimension = int(target.get("dimension", 0) or 0)
    selected = target.get("sql_types")
    if engine == "pgvector":
        return generate_pgvector_sql_forms(table, vectors, top, distance, dimension, selected or PGVECTOR_SQL_TYPES)
    if engine == "polardb":
        return generate_polardb_sql_forms(
            table, vectors, top, distance, int(target.get("pase_extra", 5)),
            int(target.get("pase_ds", 0)), selected or POLARDB_SQL_TYPES,
        )
    # The ClickHouse-family generator serves both engines: engine="clickhouse"
    # keeps the plain L2Distance/cosineDistance forms, while engine="myscale"
    # emits distance(...)(vector, q) forms and receives the search parameters
    # (ef_s etc.) nested under search_params.params exactly as run.py's MyScale
    # configuration expresses them.
    # 中文：MyScale 的 ANN 规划器只识别 ``distance(...)`` 函数家族，因此必须把
    # search 参数（如 ``ef_s``）按 run.py 配置里的 ``search_params.params`` 结构
    # 原样取出并下传；结构不合法时退化为空 dict（生成不带参数的 distance(...)）。
    search_params = None
    if engine == "myscale":
        sp = target.get("search_params") or {}
        search_params = sp.get("params") if isinstance(sp, dict) else None
        if not isinstance(search_params, dict):
            search_params = {}
    return generate_clickhouse_sql_forms(
        table, vectors, top, distance, dimension,
        selected or CLICKHOUSE_SQL_TYPES, engine=engine, search_params=search_params,
    )


def _connect(target: dict[str, Any]):
    """按引擎建立一条新连接（调用方负责关闭）。

    pg/polardb 用 psycopg2，默认端口 5432/5433；clickhouse/myscale 用
    ``clickhouse_driver.Client``，默认端口 9000、用户 default。两种驱动接口
    不同：前者需要 cursor 并显式提交/回滚，后者直接 ``execute`` 返回行。
    """
    engine = str(target["engine"]).lower()
    conn = target.get("connection_params") or {}
    if engine in {"pgvector", "polardb"}:
        import psycopg2
        default_port = 5433 if engine == "polardb" else 5432
        return psycopg2.connect(
            host=conn.get("host", "127.0.0.1"), port=int(conn.get("port", default_port)),
            user=conn.get("user", "postgres"), password=conn.get("password", ""),
            dbname=conn.get("database", "postgres"),
        )
    from clickhouse_driver import Client
    return Client(
        host=conn.get("host", "127.0.0.1"), port=int(conn.get("port", 9000)),
        user=conn.get("user", "default"), password=conn.get("password", ""),
        database=conn.get("database", "default"),
    )


def _execute(connection, engine: str, statement: str):
    """执行一条语句并返回结果行，抹平两种驱动的差异。

    psycopg2 路径需要 cursor，``fetchall`` 后立即 ``rollback`` 结束只读事务，
    避免 worker 长时间持有快照；clickhouse-driver 的 ``execute`` 直接返回行。
    """
    if engine in {"pgvector", "polardb"}:
        with connection.cursor() as cursor:
            cursor.execute(statement)
            rows = cursor.fetchall()
        connection.rollback()
        return rows
    else:
        return connection.execute(statement)


def _prepare(connection, engine: str, statements: list[str], profile: dict[str, Any]) -> list[str]:
    """Prepare sampled statements for PostgreSQL-compatible engines.

    中文：仅当引擎为 pgvector/polardb 且档位 ``use_query_plan_cache`` 非 0 时
    执行 ``PREPARE``，否则返回空列表（调用方据此改走直接执行）。每条语句命名为
    ``vb_qf_<i>``；先尝试 ``DEALLOCATE`` 清理同名旧语句，失败则 ``rollback``
    后继续，避免同名语句冲突。成功后 ``commit`` 并返回语句名列表，worker 之后用
    ``EXECUTE <name>`` 复用同一个执行计划。任一语句 PREPARE 失败会向上抛出，
    由 ``_measure`` 的 worker 捕获并回退到直接执行路径。
    """
    if engine not in {"pgvector", "polardb"} or not int(profile.get("use_query_plan_cache", 0) or 0):
        return []
    names = []
    for index, statement in enumerate(statements):
        name = f"vb_qf_{index}"
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DEALLOCATE {name}")
        except Exception:
            connection.rollback()
        with connection.cursor() as cursor:
            cursor.execute(f"PREPARE {name} AS {statement}")
        connection.commit()
        names.append(name)
    return names


# ---- 测量内核 ----

def _measure(target, sql_statements: list[str], profile: dict[str, Any], concurrency: int, duration: float):
    """跑一轮定时多线程 QPS 测量，返回 ``(qps, error_count)``。

    ``sql_statements`` 是某一种形态的多条语句（每条对应一个采样向量）。流程：
      1. 给每条语句去掉分号并追加本档位的 ``SETTINGS`` 子句；
      2. 启动 ``concurrency`` 个 worker，每个 worker 建独立连接并执行
         ``_session_setup``（ClickHouse 系开启 ``enable_vector_performance_test``）；
      3. PG 计划缓存档位先 ``_prepare`` 全部语句；PREPARE 失败则回退为直接执行；
      4. 在 ``duration`` 秒内随机挑选语句循环执行，成功计入 ``counts``、
         异常计入 ``errors``（单条失败不会中止整轮）；
      5. 置位 ``stop`` 后等待所有线程退出，用实际墙钟耗时计算 QPS。

    结果缓存档位（``use_result_cache``，ClickHouse 系也接受 ``use_query_cache``）
    在 worker 内用 ``result_cache`` 字典模拟：同一语句第二次起直接命中本地字典、
    完全不访问服务端，用于观察"纯结果缓存"的 QPS 上限。
    """
    engine = str(target["engine"]).lower()
    statements = [_strip_statement_terminator(sql) + _setting_clause(engine, profile) for sql in sql_statements]
    stop = threading.Event()
    counts = [0] * int(concurrency)
    errors = [0] * int(concurrency)

    def worker(slot: int):
        """单个并发槽的执行循环：先建连/准备语句，再按随机顺序反复执行。

        ``slot`` 只用于构造确定性的随机种子，使不同槽的语句顺序不同、但
        同一槽在重复运行时可复现。PREPARE 失败时退回直接执行，保证该
        SQL 形态仍被纳入对比，而不是整轮实验失败。
        """
        connection = _connect(target)
        prepared = []
        try:
            # Enable the per-query-SETTINGS-clause plan-cache path on this session
            # (ClickHouse-family engines only; no-op for pg/polardb).
            for setup_statement in _session_setup(engine):
                connection.execute(setup_statement)
            prepared = _prepare(connection, engine, statements, profile)
        except Exception:
            # A server may reject one particular SQL shape when preparing it.
            # Keep that form in the comparison and measure its direct path.
            if engine in {"pgvector", "polardb"}:
                connection.rollback()
            prepared = []
        # 每个 worker 独立的结果缓存：键为语句下标，值为上次查询返回的行列表。
        result_cache = {}
        # ClickHouse 系用 ``use_query_cache`` 表达结果缓存，PG 系用 ``use_result_cache``。
        use_result_cache = int(profile.get("use_result_cache", profile.get("use_query_cache", 0)) or 0)
        rng = random.Random(slot + 17)
        try:
            while not stop.is_set():
                try:
                    index = rng.randrange(len(statements))
                    statement_key = index
                    if use_result_cache and statement_key in result_cache:
                        result_cache[statement_key]
                    elif prepared:
                        with connection.cursor() as cursor:
                            cursor.execute(f"EXECUTE {prepared[index]}")
                            rows = cursor.fetchall()
                        connection.rollback()
                        if use_result_cache:
                            result_cache[statement_key] = rows
                    else:
                        rows = _execute(connection, engine, statements[index])
                        if use_result_cache:
                            result_cache[statement_key] = rows
                    counts[slot] += 1
                except Exception:
                    errors[slot] += 1
                    if engine in {"pgvector", "polardb"}:
                        connection.rollback()
        finally:
            if hasattr(connection, "close"):
                connection.close()

    # 计时窗口由主线程统一控制：启动全部 worker 后 sleep(duration)，再置位停止。
    with ThreadPoolExecutor(max_workers=int(concurrency)) as pool:
        futures = [pool.submit(worker, i) for i in range(int(concurrency))]
        start = time.perf_counter()
        time.sleep(max(0.01, float(duration)))
        stop.set()
        for future in futures:
            future.result()
    # 分母取实际墙钟耗时（含线程启动/退出），下限 1ms 以避免除零。
    elapsed = max(0.001, time.perf_counter() - start)
    return sum(counts) / elapsed, sum(errors)


# ---- 入口 ----

def run(args) -> int:
    """命令行入口：遍历目标 × 缓存档位 × SQL 形态，逐行写出 QPS CSV。

    流程：
      1. 校验 ``--query-count`` / ``--concurrency`` / ``--duration`` 必须为正；
      2. 读取 target 配置，并按 ``--engines``（匹配 target 名或引擎名）过滤；
      3. ``_check_endpoint_conflicts`` 拒绝同 host:port 的多引擎混跑；
      4. 每个 target 只 ``_sample`` 一次，所有档位/形态复用同一查询池；
      5. 每个档位开始前 ``_clear_server_caches``，每个形态调用 ``_measure``，
         立即写一行并 ``flush``，避免长跑中断时丢失已有结果。
    返回 0；CSV 列固定为 ``timestamp/target/engine/table/cache_profile/sql_type/
    queries/concurrency/duration_s/qps/errors``，时间戳是写行时刻。
    """
    if args.query_count <= 0:
        raise SystemExit("--query-count must be greater than zero")
    if args.concurrency <= 0:
        raise SystemExit("--concurrency must be greater than zero")
    if args.duration <= 0:
        raise SystemExit("--duration must be greater than zero")
    targets = _load_targets(args.config)
    if args.engines:
        wanted = set(args.engines.split(","))
        targets = [t for t in targets if t.get("name") in wanted or t.get("engine") in wanted]
    if not targets:
        raise SystemExit("no matching query-form targets")
    _check_endpoint_conflicts(targets)
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    # CSV 列顺序固定，后续分析脚本可按列名解析；qps 以 3 位小数写入。
    fields = ["timestamp", "target", "engine", "table", "cache_profile", "sql_type", "queries", "concurrency", "duration_s", "qps", "errors"]
    with output.open("w", newline="", encoding="utf-8") as fp:
        writer = csv.DictWriter(fp, fieldnames=fields)
        writer.writeheader()
        # 逐 target → 逐缓存档位 → 逐 SQL 形态；每个档位开始前清一次服务端缓存。
        for target in targets:
            vectors = _sample(target, args.query_count)  # 同一 target 的所有档位共用查询池
            forms = _forms(target, vectors)
            for profile in _profiles(target, args.cache_profiles):
                _clear_server_caches(target, profile)
                for sql_type, sql_text in forms.items():
                    # 生成器以换行分隔每条 SQL；丢弃空行得到本形态的语句列表。
                    statements = [line for line in sql_text.splitlines() if line.strip()]
                    qps, errors = _measure(target, statements, profile, args.concurrency, args.duration)
                    row = {
                        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "target": target.get("name", target["engine"]),
                        "engine": target["engine"],
                        "table": target.get("connection_params", {}).get("table", ""),
                        "cache_profile": profile.get("name", "custom"),
                        "sql_type": sql_type, "queries": len(statements),
                        "concurrency": args.concurrency, "duration_s": args.duration,
                        "qps": f"{qps:.3f}", "errors": errors,
                    }
                    writer.writerow(row)
                    fp.flush()
                    print(f"{row['target']} {row['cache_profile']} {sql_type}: QPS={row['qps']} errors={errors}")
    print(f"saved: {output}")
    return 0


def parse_args(argv=None):
    """解析命令行参数；默认输出到 ``benchmark/results/csv/query-forms-qps.csv``。

    ``--cache-profiles`` 为空表示使用 ``_profiles`` 里按引擎给出的默认档位；
    传入名称列表时只跑匹配的档位。
    """
    parser = argparse.ArgumentParser(description="Compare all vector SQL query forms and cache profiles")
    parser.add_argument("--config", required=True, help="JSON target file or directory")
    parser.add_argument("--engines", default="", help="comma-separated target name or engine filter")
    parser.add_argument("--query-count", type=int, default=100, help="sampled query vectors per target")
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--duration", type=float, default=10)
    parser.add_argument(
        "--cache-profiles", nargs="*", default=[],
        help="profile names; default is per engine: clickhouse/myscale "
             "off/result-cache/plan-cache/plan-cast/plan-only-vector/plan-result-cache, "
             "pgvector/polardb direct/result-cache/prepared-plan/prepared-result-cache",
    )
    parser.add_argument(
        "--output",
        default=str(Path(__file__).resolve().parent / "results" / "csv" / "query-forms-qps.csv"),
    )
    return parser.parse_args(argv)


# 包内入口由 ``benchmark/__main__.py`` 分发（``python -m benchmark query_forms``）；
# 这行只在以模块方式直接执行时生效，需保证相对导入的包上下文存在。
if __name__ == "__main__":
    raise SystemExit(run(parse_args()))
