#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Query plan cache correctness and invalidation test suite.

This script implements the ``E7`` / ``A7`` acceptance items from
``查询计划缓存论文补充实验与Benchmark完善指南-单模板DDL失效版.docx`` for the two
vector query plan cache implementations:

* ``clickhouse``: the CK tree at ``/home/ClickHouse`` (``VectorQueryPlanCache``).
* ``myscale``: the MS tree at ``/home/clickhouse`` (``QueryPlanCache``).

The suite is intentionally self-contained. It creates a scratch database, creates
tables with the same vector literal shape used by the benchmark, executes each
case, and compares every cache-enabled query against the same query executed
with the plan cache disabled. Cache hits and misses are read from
``system.query_log.ProfileEvents`` using unique ``query_id`` values; CK entry
counts are read from ``system.metrics``.

Usage::

    python3 plan_cache_correctness_invalidation.py --engine clickhouse
    python3 plan_cache_correctness_invalidation.py --engine myscale

Exit code is 0 only when no case ends in ``FAIL`` or ``XPASS``. ``XFAIL`` marks
a reproduced known defect, ``SKIP`` marks a case that cannot run on that engine.

The script never touches benchmark datasets; all objects live in its own
``pcache_ci`` database and are dropped on exit unless ``--keep`` is given.
"""

from __future__ import annotations

import argparse
import itertools
import json
import os
import random
import sys
import threading
import time
import traceback
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Sequence

from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException


DEFAULT_DIM = 8
DEFAULT_TOP_K = 3
DEFAULT_TABLE_ROWS = 200
TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

# Tolerate float noise between the cached-plan restore path and a fresh plan.
FLOAT_TOLERANCE = 1e-6


# ---------------------------------------------------------------------------
# Test data
# ---------------------------------------------------------------------------

def make_vectors(count: int, dim: int = DEFAULT_DIM, seed: int = 20240517) -> list[list[float]]:
    """生成确定性的伪随机向量；固定种子保证用例可复现、结果可比较。"""
    rng = random.Random(seed)
    return [[round(rng.random(), 6) for _ in range(dim)] for _ in range(count)]


VECTORS = make_vectors(256, DEFAULT_DIM, seed=20240517)
VECTORS_16 = make_vectors(64, 16, seed=20240518)


def make_rows(count: int = DEFAULT_TABLE_ROWS, dim: int = DEFAULT_DIM, id_base: int = 0) -> list[tuple[int, list[float]]]:
    """构造 ``(id, vector)`` 行；维度不是默认值时另生成一套向量并以 id 取模复用。"""
    vectors = VECTORS if dim == DEFAULT_DIM else make_vectors(count, dim, seed=20240519)
    return [(id_base + i, vectors[i % len(vectors)]) for i in range(count)]


def literal(vector: Sequence[float], form: str = "normal") -> str:
    """把向量渲染成三种 SQL 字面量形态之一，用于验证不同写法是否命中同一缓存。

    * ``normal``：裸 ``[..]``；
    * ``cast_array``：``CAST([..] AS Array(Float32))``；
    * ``type_hint``：``[..]::Array(Float32)``。
    另外两种写法应生成语法等价但 AST 不同的查询，正好用来检验缓存键是否过于敏感。
    """
    inner = ",".join(f"{value:.6f}" for value in vector)
    array = f"[{inner}]"
    if form == "normal":
        return array
    if form == "cast_array":
        return f"CAST({array} AS Array(Float32))"
    if form == "type_hint":
        return f"{array}::Array(Float32)"
    raise ValueError(f"Unknown literal form: {form}")


def rows_match(left: Sequence[Sequence[Any]], right: Sequence[Sequence[Any]]) -> bool:
    """比较两次查询结果：行数、id 必须完全一致，
    距离允许 ``FLOAT_TOLERANCE`` 的浮点噪声。"""
    if len(left) != len(right):
        return False
    for left_row, right_row in zip(left, right):
        if left_row[0] != right_row[0]:
            return False
        if abs(float(left_row[1]) - float(right_row[1])) > FLOAT_TOLERANCE:
            return False
    return True


# ---------------------------------------------------------------------------
# Engine profiles
# ---------------------------------------------------------------------------

class Engine:
    """Per-engine SQL, settings, and observability knowledge."""

    name = ""
    label = ""
    distance_function = ""
    clear_sql = ""
    hit_event = ""
    miss_event = ""
    supports_entries_metric = False
    supports_cache_tag = False
    supports_index_alter = True
    supports_dimension_change = True

    def __init__(self, host: str, port: int, user: str, password: str):
        """保存连接参数并建立一个管理连接（不走被测缓存设置）。

        ``admin`` 用于建库/建表/清缓存和查 event 计数；它不带 profile
        settings，避免管理操作本身污染计划缓存状态。
        """
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.admin = Client(host=host, port=port, user=user, password=password)

    # -- settings -----------------------------------------------------------
    def settings(self, cache: bool, plan_only: bool = False, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        """返回该引擎的会话级缓存开关字典；子类必须覆盖。"""
        raise NotImplementedError

    def new_client(self, cache: bool, plan_only: bool = False, extra: dict[str, Any] | None = None, timeout: int = 30) -> Client:
        """按缓存 profile 新建一条被测连接；``timeout`` 同时作为发送/接收超时。"""
        return Client(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            settings=self.settings(cache, plan_only, extra),
            send_receive_timeout=timeout,
        )

    # -- DDL ----------------------------------------------------------------
    def create_table_sql(self, table: str, dim: int = DEFAULT_DIM) -> str:
        """返回建表 DDL（含向量索引定义）；子类必须覆盖。"""
        raise NotImplementedError

    def add_index_sql(self, table: str, dim: int) -> str:
        """返回 ALTER 方式补建索引的 DDL；
        不支持该能力的引擎应抛出 NotImplementedError。"""
        raise NotImplementedError

    def drop_index_sql(self, table: str) -> str:
        """返回 ALTER 方式删除索引的 DDL；
        不支持该能力的引擎应抛出 NotImplementedError。"""
        raise NotImplementedError

    def wait_for_index(self, table: str) -> None:
        """No-op on engines that build vector indexes synchronously."""

    def system_clear(self, tag: str | None = None) -> None:
        """清空计划缓存；给出 ``tag`` 时只清该标签下的条目（引擎支持时）。"""
        if tag is not None:
            self.admin.execute(f"{self.clear_sql} TAG {quote_string(tag)}")
        else:
            self.admin.execute(self.clear_sql)

    def entries(self) -> int | None:
        """读取计划缓存条目数；引擎没有该指标时返回 None（调用方跳过断言）。"""
        if not self.supports_entries_metric:
            return None
        rows = self.admin.execute(
            "SELECT value FROM system.metrics WHERE metric = 'VectorQueryPlanCacheEntries'"
        )
        return int(rows[0][0]) if rows else None

    # -- queries ------------------------------------------------------------
    def query_sql(
        self,
        table: str,
        vector: Sequence[float],
        limit: int = DEFAULT_TOP_K,
        form: str = "normal",
        subquery: bool = False,
    ) -> str:
        """构造按距离升序取 top-k 的查询；``subquery`` 会额外套一层子查询。

        子查询形态用于验证"计划缓存是否对 SELECT 外层形态敏感"。
        """
        expression = f"{self.distance_function}(vector, {literal(vector, form)})"
        if subquery:
            return (
                f"SELECT * FROM (SELECT id, {expression} AS dis FROM {table} "
                f"ORDER BY dis ASC LIMIT {limit})"
            )
        return f"SELECT id, {expression} AS dis FROM {table} ORDER BY dis ASC LIMIT {limit}"


class ClickHouseEngine(Engine):
    """ClickHouse 实现：计划缓存用 ``vector_query_plan_cache``，且暴露条目数指标。"""

    name = "clickhouse"
    label = "CK"
    distance_function = "L2Distance"
    clear_sql = "SYSTEM DROP VECTOR QUERY PLAN CACHE"
    hit_event = "VectorQueryPlanCacheHits"
    miss_event = "VectorQueryPlanCacheMisses"
    supports_entries_metric = True
    supports_cache_tag = False
    supports_index_alter = True
    supports_dimension_change = True

    def settings(self, cache: bool, plan_only: bool = False, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        """ClickHouse profile：关结果缓存，按需开计划缓存/仅缓存计划。"""
        result: dict[str, Any] = {
            "use_query_cache": 0,
            "vector_query_plan_cache": int(cache),
            "enable_vector_performance_test": 1,
        }
        if plan_only:
            result["vector_only_cache_query_plan"] = 1
        if extra:
            result.update(extra)
        return result

    def create_table_sql(self, table: str, dim: int = DEFAULT_DIM) -> str:
        """建表时把 HNSW 向量索引写在列定义里（CK 的 skip index 形态）。"""
        index = (
            "INDEX vi vector TYPE vector_similarity("
            f"'hnsw', 'L2Distance', {dim}, 'bf16', 16, 100) GRANULARITY 100000000"
        )
        return (
            f"CREATE TABLE {table} (id UInt32, vector Array(Float32), {index}) "
            "ENGINE = MergeTree ORDER BY id"
        )

    def add_index_sql(self, table: str, dim: int) -> str:
        """通过 ALTER 在已有表上补挂向量索引（CK 支持）。"""
        return (
            f"ALTER TABLE {table} ADD INDEX vi vector TYPE vector_similarity("
            f"'hnsw', 'L2Distance', {dim}, 'bf16', 16, 100) GRANULARITY 100000000"
        )

    def drop_index_sql(self, table: str) -> str:
        """通过 ALTER 删除向量索引，用于验证索引 DDL 是否会失效缓存。"""
        return f"ALTER TABLE {table} DROP INDEX vi"


class MyScaleEngine(Engine):
    """MyScale 实现：缓存开关是 ``enable_query_plan_cache``，
    且只支持按标签清理缓存。"""

    name = "myscale"
    label = "MS"
    distance_function = "distance"
    clear_sql = "SYSTEM DROP QUERY PLAN CACHE"
    hit_event = "QueryPlanCacheHits"
    miss_event = "QueryPlanCacheMisses"
    supports_entries_metric = False
    supports_cache_tag = True
    supports_index_alter = False
    supports_dimension_change = False

    def settings(self, cache: bool, plan_only: bool = False, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        """MyScale profile：开关名与 CK 不同，``plan_only`` 对应 ``only_cache_query_plan``。"""
        result: dict[str, Any] = {
            "use_query_cache": 0,
            "enable_query_plan_cache": int(cache),
            "enable_vector_performance_test": 1,
        }
        if plan_only:
            result["only_cache_query_plan"] = 1
        if extra:
            result.update(extra)
        return result

    def create_table_sql(self, table: str, dim: int = DEFAULT_DIM) -> str:
        """建表并声明 ``VECTOR INDEX ... TYPE HNSWFLAT``，同时用 CHECK 约束维度。"""
        return (
            f"CREATE TABLE {table} (id UInt32, vector Array(Float32), "
            "VECTOR INDEX vi vector TYPE HNSWFLAT, "
            f"CONSTRAINT check_length CHECK length(vector) = {dim}) "
            "ENGINE = MergeTree ORDER BY id"
        )

    def add_index_sql(self, table: str, dim: int) -> str:
        """MyScale 不支持 ALTER 补建向量索引；相关用例会被跳过。"""
        raise NotImplementedError("MS cannot re-add a vector index through ALTER")

    def drop_index_sql(self, table: str) -> str:
        """MyScale 不支持 ALTER 删除向量索引；相关用例会被跳过。"""
        raise NotImplementedError("MS cannot drop a vector index through ALTER")

    def wait_for_index(self, table: str) -> None:
        """等待异步索引构建完成（MyScale 建索引是后台任务）。"""
        self.admin.execute(f"SYSTEM WAIT BUILDING VECTOR INDICES {table}")


ENGINES: dict[str, type[Engine]] = {
    "clickhouse": ClickHouseEngine,
    "ck": ClickHouseEngine,
    "myscale": MyScaleEngine,
    "ms": MyScaleEngine,
}


def quote_string(value: str) -> str:
    """把字符串转义成单引号 SQL 字面量（先转义反斜杠，再转义单引号）。"""
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


# ---------------------------------------------------------------------------
# Test plumbing
# ---------------------------------------------------------------------------

class SkipTest(Exception):
    """Raised when a case cannot run in this environment."""


@dataclass
class CaseResult:
    """单个用例的执行结果，最终序列化为 JSON 报告的一行。"""

    case_id: str
    title: str
    status: str
    detail: str
    metrics: dict[str, Any] = field(default_factory=dict)
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """转成可 JSON 序列化的字典；耗时保留 3 位小数。"""
        return {
            "case_id": self.case_id,
            "title": self.title,
            "status": self.status,
            "detail": self.detail,
            "metrics": self.metrics,
            "duration_seconds": round(self.duration_seconds, 3),
        }


class Environment:
    """Owns the scratch database, clients, and event bookkeeping."""

    def __init__(self, engine: Engine, database: str):
        """绑定引擎与临时库名；客户端按 profile 懒创建并缓存于 ``clients``。

        ``_qid_counter`` 为每个 query 生成唯一 query_id，后续靠它去
        ``system.query_log`` 里取 ProfileEvents；加锁是因为并发用例会同时取号。
        """
        self.engine = engine
        self.database = database
        self.clients: dict[tuple[Any, ...], Client] = {}
        self._client_lock = threading.Lock()
        self._qid_counter = itertools.count(1)
        self._qid_lock = threading.Lock()

    # -- lifecycle ----------------------------------------------------------
    def setup(self) -> None:
        """重建临时库并清空计划缓存，保证每个用例从确定状态开始。"""
        self.engine.admin.execute(f"DROP DATABASE IF EXISTS {self.database}")
        self.engine.admin.execute(f"CREATE DATABASE {self.database}")
        self.engine.system_clear()

    def teardown(self) -> None:
        """尽力清理缓存与临时库；两步都吞异常，避免清理失败掩盖用例结果。"""
        try:
            self.engine.system_clear()
        except Exception:
            pass
        try:
            self.engine.admin.execute(f"DROP DATABASE IF EXISTS {self.database}")
        except Exception:
            pass

    # -- names --------------------------------------------------------------
    def table(self, name: str) -> str:
        """把短表名拼成 ``<临时库>.<表名>`` 的全限定名。"""
        return f"{self.database}.{name}"

    def next_qid(self, prefix: str) -> str:
        """生成 ``pci_<prefix>_<序号>`` 形式的唯一 query_id（线程安全）。"""
        with self._qid_lock:
            return f"pci_{prefix}_{next(self._qid_counter)}"

    # -- clients ------------------------------------------------------------
    def client(
        self,
        cache: bool,
        plan_only: bool = False,
        extra: dict[str, Any] | None = None,
        timeout: int = 30,
    ) -> Client:
        """按 (缓存开关, 仅计划, 额外设置) 复用客户端；同 profile 只建一条连接。

        复用连接是为了让会话级 setting 生效范围稳定；不同的 ``extra`` 会
        产生不同 key，避免两个用例互相覆盖对方的 setting。
        """
        key = (cache, plan_only, tuple(sorted((extra or {}).items())))
        with self._client_lock:
            if key not in self.clients:
                self.clients[key] = self.engine.new_client(cache, plan_only, extra, timeout)
            return self.clients[key]

    # -- DDL helpers --------------------------------------------------------
    def drop_table(self, name: str) -> None:
        """删除临时库中的表（不存在时静默通过）。"""
        self.engine.admin.execute(f"DROP TABLE IF EXISTS {self.table(name)}")

    def create_table(self, name: str, dim: int = DEFAULT_DIM) -> str:
        """先删同名表再建表，返回全限定表名。"""
        table = self.table(name)
        self.drop_table(name)
        self.engine.admin.execute(self.engine.create_table_sql(table, dim))
        return table

    def insert(self, client: Client, table: str, rows: Iterable[tuple[int, Sequence[float]]]) -> None:
        """用 client 的批量 VALUES 接口写入 ``(id, vector)`` 行。"""
        client.execute(f"INSERT INTO {table} (id, vector) VALUES", list(rows))

    def fresh_table(
        self,
        name: str,
        rows: int | Iterable[tuple[int, Sequence[float]]] = DEFAULT_TABLE_ROWS,
        dim: int = DEFAULT_DIM,
        insert_client: Client | None = None,
    ) -> str:
        """建表 → 写入（可指定写入连接）→ 等索引就绪，返回表名。

        ``rows`` 可以是行数（自动生成数据）或现成的行序列；MyScale 的
        索引是异步构建的，因此最后统一调用 ``wait_for_index``。
        """
        table = self.create_table(name, dim)
        if isinstance(rows, int):
            rows = make_rows(rows, dim)
        self.insert(insert_client or self.engine.admin, table, rows)
        self.engine.wait_for_index(table)
        return table

    # -- execution ----------------------------------------------------------
    def run(
        self,
        sql: str,
        cache: bool,
        plan_only: bool = False,
        extra: dict[str, Any] | None = None,
        client: Client | None = None,
        prefix: str = "q",
    ) -> tuple[list[tuple[Any, ...]], str]:
        """执行一条查询并返回 ``(结果行, query_id)``，query_id 供事后查事件。"""
        target = client or self.client(cache, plan_only, extra)
        qid = self.next_qid(prefix)
        rows = target.execute(sql, query_id=qid)
        return rows, qid

    def events(self, qids: Sequence[str]) -> dict[str, dict[str, int]]:
        """从 ``system.query_log`` 汇总每个 query_id 的命中/未命中计数。

        先 ``SYSTEM FLUSH LOGS`` 保证日志可见；同一 query_id 可能有多条
        QueryFinish（MyScale 会这样），用 ``max`` 聚合避免重复行造成重复计数。
        """
        if not qids:
            return {}
        self.engine.admin.execute("SYSTEM FLUSH LOGS")
        # MS can emit more than one QueryFinish row for one query_id; aggregate
        # with MAX so duplicated rows do not distort hit/miss detection.
        rows = self.engine.admin.execute(
            "SELECT query_id, "
            f"max(ProfileEvents['{self.engine.hit_event}']), "
            f"max(ProfileEvents['{self.engine.miss_event}']) "
            "FROM system.query_log "
            "WHERE query_id IN %(ids)s AND type = 'QueryFinish' "
            "GROUP BY query_id",
            {"ids": tuple(qids)},
        )
        return {qid: {"hits": int(hits), "misses": int(misses)} for qid, hits, misses in rows}

    def hit(self, qid: str) -> bool:
        """该查询是否至少命中一次计划缓存。"""
        return self.events([qid]).get(qid, {}).get("hits", 0) > 0

    def miss(self, qid: str) -> bool:
        """该查询是否至少未命中一次计划缓存。"""
        return self.events([qid]).get(qid, {}).get("misses", 0) > 0

    def last_run(self, sql: str, cache: bool, prefix: str = "q", **kwargs: Any) -> tuple[list[tuple[Any, ...]], bool]:
        """执行一次查询并直接返回 ``(结果行, 是否命中)``，省去调用方再查一次。"""
        rows, qid = self.run(sql, cache=cache, prefix=prefix, **kwargs)
        return rows, self.hit(qid)

    def wait_ttl(self, seconds: float = 2.2) -> None:
        """等待超过缓存 TTL（默认 2s，留 0.2s 余量）以验证过期失效。"""
        time.sleep(seconds)


# ---------------------------------------------------------------------------
# Cases
# ---------------------------------------------------------------------------

class Cases:
    """Case implementations. Each returns a metrics dict on success."""

    def __init__(self, env: Environment):
        """保存共享环境；``engine`` 单独留一份便于用例直接读引擎能力标志。"""
        self.env = env
        self.engine = env.engine

    # -- helpers ------------------------------------------------------------
    def warm(self, sql: str, plan_only: bool = False, times: int = 2, extra: dict[str, Any] | None = None, prefix: str = "warm") -> list[str]:
        """重复执行同一 SQL 预热缓存，并断言最后一次确实命中。

        预热失败说明该 profile 根本不会缓存（或缓存键不稳定），后续对
        "失效/保持"的断言就没有意义，因此这里直接抛 AssertionError。
        """
        qids = []
        for _ in range(times):
            _, qid = self.env.run(sql, cache=True, plan_only=plan_only, extra=extra, prefix=prefix)
            qids.append(qid)
        events = self.env.events(qids)
        if times >= 2 and events.get(qids[-1], {}).get("hits", 0) == 0:
            raise AssertionError(f"warm-up did not reach a cache hit: {events}")
        return qids

    def assert_equal(self, left: Iterable[Sequence[Any]], right: Iterable[Sequence[Any]], message: str) -> None:
        """断言两次查询结果等价；失败信息同时打印两侧结果便于定位。"""
        if not rows_match(list(left), list(right)):
            raise AssertionError(f"{message}: cache-on={list(left)} cache-off={list(right)}")

    def assert_entries(self, expected: int, message: str) -> None:
        """断言缓存条目数等于期望值；引擎不支持该指标（None）时跳过。"""
        value = self.engine.entries()
        if value is not None and value != expected:
            raise AssertionError(f"{message}: entries={value}, expected={expected}")

    # -- C: correctness -----------------------------------------------------
    def case_c01(self) -> dict[str, Any]:
        """单模板 100 个查询实例：缓存开/关逐条结果一致，CK 条目数保持 1。"""
        table = self.env.fresh_table("t_c01")
        self.engine.system_clear()
        hits = 0
        on_qids = []
        for index in range(100):
            vector = VECTORS[index]
            sql = self.engine.query_sql(table, vector)
            off_rows, _ = self.env.run(sql, cache=False, plan_only=True, prefix="c01off")
            on_rows, qid = self.env.run(sql, cache=True, plan_only=True, prefix="c01on")
            on_qids.append(qid)
            self.assert_equal(on_rows, off_rows, f"query {index} differs")
        for event in self.env.events(on_qids).values():
            hits += event["hits"]
        if hits < 99:
            raise AssertionError(f"expected at least 99 cache hits over 100 queries, got {hits}")
        self.assert_entries(1, "single template must keep one entry")
        return {"queries": 100, "hits": hits, "entries": self.engine.entries()}

    def case_c02(self) -> dict[str, Any]:
        """预热后连续 20 个不同向量稳定命中，条目数不变。"""
        table = self.env.fresh_table("t_c02")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        warm_qids = self.warm(sql, plan_only=True, times=1, prefix="c02warm")
        qids = []
        for index in range(20):
            qids.append(self.env.run(self.engine.query_sql(table, VECTORS[index + 1]), cache=True, plan_only=True, prefix="c02")[1])
        events = self.env.events(qids)
        hits = sum(event["hits"] for event in events.values())
        if hits != 20:
            raise AssertionError(f"expected 20 hits after warm-up, got {hits}: {events}")
        self.assert_entries(1, "entries must stay at one")
        return {"hits": hits, "warm_qid": warm_qids[0], "entries": self.engine.entries()}

    def case_c03(self) -> dict[str, Any]:
        """SQL 表示形式（normal/cast_array/type_hint）的常量恢复正确。"""
        table = self.env.fresh_table("t_c03")
        self.engine.system_clear()
        details: dict[str, Any] = {}
        forms = ("normal", "cast_array", "type_hint")
        for form in forms:
            comparison = 0
            hits = 0
            try:
                for index in range(5):
                    sql = self.engine.query_sql(table, VECTORS[index], form=form)
                    off_rows, _ = self.env.run(sql, cache=False, plan_only=True, prefix=f"c03off_{form}")
                    on_rows, qid = self.env.run(sql, cache=True, plan_only=True, prefix=f"c03on_{form}")
                    self.assert_equal(on_rows, off_rows, f"form {form} query {index} differs")
                    hits += self.env.events([qid]).get(qid, {}).get("hits", 0)
                    comparison += 1
            except ServerException as exc:
                details[form] = {"status": "SKIPPED", "error": str(exc).splitlines()[0]}
                continue
            details[form] = {"status": "OK", "queries": comparison, "hits": hits}
        if not any(item.get("status") == "OK" for item in details.values()):
            raise SkipTest(f"no SQL form executed on this engine: {details}")
        return {"forms": details}

    def case_c04(self) -> dict[str, Any]:
        """命中后常量替换：不同查询向量返回与无缓存一致的 Top-K。"""
        table = self.env.fresh_table("t_c04")
        self.engine.system_clear()
        self.warm(self.engine.query_sql(table, VECTORS[0]), plan_only=True, times=2, prefix="c04warm")
        hits = 0
        for index in range(1, 11):
            sql = self.engine.query_sql(table, VECTORS[index])
            off_rows, _ = self.env.run(sql, cache=False, plan_only=True, prefix="c04off")
            on_rows, qid = self.env.run(sql, cache=True, plan_only=True, prefix="c04on")
            self.assert_equal(on_rows, off_rows, f"patched literal {index} differs")
            hits += self.env.events([qid]).get(qid, {}).get("hits", 0)
        if hits < 10:
            raise AssertionError(f"expected 10 hits while patching literals, got {hits}")
        return {"queries": 10, "hits": hits}

    def case_c05(self) -> dict[str, Any]:
        """plan-only 与 AST+Plan 两种模式的查询结果一致。"""
        table = self.env.fresh_table("t_c05")
        self.engine.system_clear()
        for index in range(10):
            sql = self.engine.query_sql(table, VECTORS[index])
            off_rows, _ = self.env.run(sql, cache=False, prefix="c05off")
            plan_rows, _ = self.env.run(sql, cache=True, plan_only=True, prefix="c05plan")
            ast_rows, _ = self.env.run(sql, cache=True, plan_only=False, prefix="c05ast")
            self.assert_equal(plan_rows, off_rows, f"plan-only literal {index} differs")
            self.assert_equal(ast_rows, off_rows, f"ast+plan literal {index} differs")
        return {"queries": 10, "modes": ["plan_only", "ast_plan", "off"]}

    def case_c06(self) -> dict[str, Any]:
        """LIMIT 参与缓存键：不同 Top-K 形成不同条目且各自命中。"""
        table = self.env.fresh_table("t_c06")
        self.engine.system_clear()
        results: dict[int, Any] = {}
        for limit in (3, 5):
            sql = self.engine.query_sql(table, VECTORS[0], limit=limit)
            off_rows, _ = self.env.run(sql, cache=False, plan_only=True, prefix=f"c06off{limit}")
            first_rows, first_qid = self.env.run(sql, cache=True, plan_only=True, prefix=f"c06a{limit}")
            second_rows, second_qid = self.env.run(sql, cache=True, plan_only=True, prefix=f"c06b{limit}")
            self.assert_equal(first_rows, off_rows, f"limit {limit} first run differs")
            self.assert_equal(second_rows, off_rows, f"limit {limit} second run differs")
            if self.env.events([first_qid])[first_qid]["hits"] != 0:
                raise AssertionError(f"limit {limit} first run must miss")
            if self.env.events([second_qid])[second_qid]["hits"] == 0:
                raise AssertionError(f"limit {limit} second run must hit")
            results[limit] = first_rows
        self.assert_entries(2, "LIMIT must produce a separate cache key")
        return {"limits": [3, 5], "entries": self.engine.entries()}

    def case_c07(self) -> dict[str, Any]:
        """结果缓存必须关闭：重复查询命中的是计划缓存而不是结果缓存。"""
        table = self.env.fresh_table("t_c07")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        _, first_qid = self.env.run(sql, cache=True, prefix="c07a")
        _, second_qid = self.env.run(sql, cache=True, prefix="c07b")
        if self.env.events([second_qid])[second_qid]["hits"] == 0:
            raise AssertionError("second identical query did not hit the plan cache")
        events = self.engine.admin.execute(
            "SELECT query_id, max(ProfileEvents['QueryCacheHits']) FROM system.query_log "
            "WHERE query_id IN %(ids)s AND type = 'QueryFinish' GROUP BY query_id",
            {"ids": (first_qid, second_qid)},
        )
        cache_hits = {qid: int(value) for qid, value in events}
        if any(value != 0 for value in cache_hits.values()):
            raise AssertionError(f"query result cache was used: {cache_hits}")
        return {"query_cache_hits": cache_hits}

    def case_c08(self) -> dict[str, Any]:
        """含子查询的 SELECT 不写入计划缓存。"""
        table = self.env.fresh_table("t_c08")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0], subquery=True)
        off_rows, _ = self.env.run(sql, cache=False, plan_only=True, prefix="c08off")
        first_rows, first_qid = self.env.run(sql, cache=True, plan_only=True, prefix="c08a")
        second_rows, second_qid = self.env.run(sql, cache=True, plan_only=True, prefix="c08b")
        self.assert_equal(first_rows, off_rows, "subquery first run differs")
        self.assert_equal(second_rows, off_rows, "subquery second run differs")
        events = self.env.events([first_qid, second_qid])
        if any(event["hits"] for event in events.values()):
            raise AssertionError(f"subquery unexpectedly hit the cache: {events}")
        self.assert_entries(0, "subquery must not populate the cache")
        return {"events": events, "entries": self.engine.entries()}

    def case_c09(self) -> dict[str, Any]:
        """默认容量下不静默淘汰：20 个不同查询形状全部保留并再次命中。"""
        table = self.env.fresh_table("t_c09")
        self.engine.system_clear()
        qids = []
        for limit in range(1, 21):
            sql = self.engine.query_sql(table, VECTORS[0], limit=limit)
            _, qid = self.env.run(sql, cache=True, plan_only=True, prefix="c09")
            qids.append(qid)
        events = self.env.events(qids)
        misses = sum(1 for event in events.values() if event["misses"])
        if misses != 20:
            raise AssertionError(f"expected 20 cold misses, got {misses}: {events}")
        self.assert_entries(20, "all distinct shapes must be retained")
        sql = self.engine.query_sql(table, VECTORS[0], limit=1)
        _, qid = self.env.run(sql, cache=True, plan_only=True, prefix="c09re")
        if self.env.events([qid])[qid]["hits"] == 0:
            raise AssertionError("first shape was evicted under default capacity")
        return {"shapes": 20, "entries": self.engine.entries()}

    # -- I: invalidation ----------------------------------------------------
    def case_i01(self) -> dict[str, Any]:
        """ALTER ADD COLUMN（DDL 会话开启缓存）必须清空相关条目。"""
        table = self.env.fresh_table("t_i01")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="i01warm")
        self.assert_entries(1, "entry must exist before DDL")
        # Invalidation is gated by the *DDL session's* cache setting, so the DDL
        # must be issued from a client that has the plan cache enabled.
        self.env.client(cache=True).execute(f"ALTER TABLE {table} ADD COLUMN payload UInt8 DEFAULT 0")
        self.assert_entries(0, "ALTER must clear the entry")
        rows, qid = self.env.run(sql, cache=True, prefix="i01after")
        if self.env.events([qid])[qid]["hits"] != 0:
            raise AssertionError("first query after ALTER must miss")
        off_rows, _ = self.env.run(sql, cache=False, prefix="i01off")
        self.assert_equal(rows, off_rows, "result after ALTER differs")
        return {"entries": self.engine.entries()}

    def case_i02(self) -> dict[str, Any]:
        """已知缺陷：DDL 会话关闭缓存时 ALTER 不会失效，旧条目仍可命中。"""
        table = self.env.fresh_table("t_i02")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="i02warm")
        self.assert_entries(1, "entry must exist before DDL")
        off_client = self.env.client(cache=False)
        off_client.execute(f"ALTER TABLE {table} ADD COLUMN payload UInt8 DEFAULT 0")
        entries = self.engine.entries()
        _, qid = self.env.run(sql, cache=True, prefix="i02after")
        events = self.env.events([qid])
        stale_hit = events[qid]["hits"] > 0
        stale_entries = entries not in (None, 0)
        if stale_hit or stale_entries:
            raise AssertionError(
                f"cache-off ALTER left entries={entries} and hit={stale_hit}; invalidation was skipped"
            )
        return {"entries": entries, "invalidation": "works"}

    def case_i03(self) -> dict[str, Any]:
        """维度变化（8 -> 16 维）：列重建后旧计划不得复用。"""
        if not self.engine.supports_dimension_change:
            raise SkipTest("engine cannot drop a column referenced by a vector index")
        table = self.env.fresh_table("t_i03")
        self.engine.system_clear()
        old_sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(old_sql, prefix="i03warm")
        client = self.env.client(cache=True)
        client.execute(self.engine.drop_index_sql(table))
        self.assert_entries(0, "DROP INDEX must clear the entry")
        client.execute(f"ALTER TABLE {table} DROP COLUMN vector")
        client.execute(f"ALTER TABLE {table} ADD COLUMN vector Array(Float32)")
        client.execute(self.engine.add_index_sql(table, 16))
        client.execute(f"TRUNCATE TABLE {table}")
        self.assert_entries(0, "schema change must not leave entries")
        rows = [(1000 + i, VECTORS_16[i]) for i in range(50)]
        self.env.insert(self.env.client(cache=False), table, rows)
        self.engine.wait_for_index(table)
        new_sql = self.engine.query_sql(table, VECTORS_16[0])
        on_rows, qid = self.env.run(new_sql, cache=True, prefix="i03after")
        if self.env.events([qid])[qid]["hits"] != 0:
            raise AssertionError("query after dimension change must miss")
        off_rows, _ = self.env.run(new_sql, cache=False, prefix="i03off")
        self.assert_equal(on_rows, off_rows, "dimension change result differs")
        if not on_rows or on_rows[0][0] < 1000:
            raise AssertionError(f"dimension change returned stale rows: {on_rows}")
        return {"rows": len(on_rows), "entries": self.engine.entries()}

    def case_i04(self) -> dict[str, Any]:
        """已知缺陷：DDL 会话关闭缓存时 DROP+CREATE 同名表会复用旧计划。"""
        table = self.env.fresh_table("t_i04")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="i04warm")
        off_client = self.env.client(cache=False)
        off_client.execute(f"DROP TABLE {table}")
        off_client.execute(self.engine.create_table_sql(table, DEFAULT_DIM))
        self.env.insert(off_client, table, make_rows(50, DEFAULT_DIM, id_base=1000))
        self.engine.wait_for_index(table)
        try:
            on_rows, qid = self.env.run(sql, cache=True, prefix="i04after")
        except ServerException as exc:
            raise AssertionError(f"stale plan failed with code {exc.code}: {exc}") from exc
        events = self.env.events([qid])
        stale_hit = events[qid]["hits"] > 0
        stale_rows = not on_rows or on_rows[0][0] < 1000
        if stale_hit or stale_rows:
            raise AssertionError(
                f"cache-off DROP+CREATE reused the old plan: hit={stale_hit}, rows={on_rows}"
            )
        return {"events": events, "rows": on_rows}

    def case_i05(self) -> dict[str, Any]:
        """DROP+CREATE 同名表（DDL 会话开启缓存）必须失效并返回新表数据。"""
        table = self.env.fresh_table("t_i05")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="i05warm")
        on_client = self.env.client(cache=True)
        on_client.execute(f"DROP TABLE {table}")
        on_client.execute(self.engine.create_table_sql(table, DEFAULT_DIM))
        self.env.insert(on_client, table, make_rows(50, DEFAULT_DIM, id_base=1000))
        self.engine.wait_for_index(table)
        self.assert_entries(0, "DROP+CREATE must clear entries")
        on_rows, qid = self.env.run(sql, cache=True, prefix="i05after")
        if self.env.events([qid])[qid]["hits"] != 0:
            raise AssertionError("query after DROP+CREATE must miss")
        if not on_rows or on_rows[0][0] < 1000:
            raise AssertionError(f"new table data was not used: {on_rows}")
        off_rows, _ = self.env.run(sql, cache=False, prefix="i05off")
        self.assert_equal(on_rows, off_rows, "DROP+CREATE result differs")
        return {"rows": on_rows, "entries": self.engine.entries()}

    def case_i06(self) -> dict[str, Any]:
        """索引删除/重建必须失效，后续查询重新命中。"""
        if not self.engine.supports_index_alter:
            raise SkipTest("engine cannot ALTER a vector index")
        table = self.env.fresh_table("t_i06")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="i06warm")
        client = self.env.client(cache=True)
        client.execute(self.engine.drop_index_sql(table))
        self.assert_entries(0, "DROP INDEX must clear entries")
        rows, qid = self.env.run(sql, cache=True, prefix="i06drop")
        if self.env.events([qid])[qid]["hits"] != 0:
            raise AssertionError("query after DROP INDEX must miss")
        off_rows, _ = self.env.run(sql, cache=False, prefix="i06off")
        self.assert_equal(rows, off_rows, "result after DROP INDEX differs")
        client.execute(self.engine.add_index_sql(table, DEFAULT_DIM))
        self.assert_entries(0, "ADD INDEX must clear entries")
        rows, qid = self.env.run(sql, cache=True, prefix="i06add")
        if self.env.events([qid])[qid]["hits"] != 0:
            raise AssertionError("query after ADD INDEX must miss")
        self.assert_equal(rows, off_rows, "result after ADD INDEX differs")
        _, second_qid = self.env.run(sql, cache=True, prefix="i06rehit")
        if self.env.events([second_qid])[second_qid]["hits"] == 0:
            raise AssertionError("plan was not re-cached after index rebuild")
        return {"entries": self.engine.entries()}

    def case_i07(self) -> dict[str, Any]:
        """TRUNCATE 必须失效；重新写入后查询命中新数据。"""
        table = self.env.fresh_table("t_i07")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="i07warm")
        self.env.client(cache=True).execute(f"TRUNCATE TABLE {table}")
        self.assert_entries(0, "TRUNCATE must clear entries")
        # Insert through a cache-off session so the INSERT itself cannot be the
        # reason the entries disappeared.
        self.env.insert(self.env.client(cache=False), table, make_rows(20, DEFAULT_DIM, id_base=2000))
        self.engine.wait_for_index(table)
        rows, qid = self.env.run(sql, cache=True, prefix="i07after")
        if self.env.events([qid])[qid]["hits"] != 0:
            raise AssertionError("query after TRUNCATE must miss")
        if not rows or rows[0][0] < 2000:
            raise AssertionError(f"TRUNCATE left stale rows: {rows}")
        return {"rows": rows, "entries": self.engine.entries()}

    def case_i07b(self) -> dict[str, Any]:
        """已知缺陷（MS）：TRUNCATE 后空表的缓存开启查询无法克隆计划。"""
        table = self.env.fresh_table("t_i07b")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="i07bwarm")
        self.env.client(cache=True).execute(f"TRUNCATE TABLE {table}")
        try:
            rows, _ = self.env.run(sql, cache=True, prefix="i07bafter")
        except ServerException as exc:
            raise AssertionError(f"query on empty table failed with code {exc.code}: {exc}") from exc
        if rows:
            raise AssertionError(f"truncated table returned rows: {rows}")
        return {"rows": 0}

    def case_i08(self) -> dict[str, Any]:
        """RENAME 必须失效；改回原名后首次查询 miss。"""
        table = self.env.fresh_table("t_i08")
        renamed = self.env.table("t_i08_renamed")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="i08warm")
        client = self.env.client(cache=True)
        client.execute(f"RENAME TABLE {table} TO {renamed}")
        self.assert_entries(0, "RENAME must clear entries")
        client.execute(f"RENAME TABLE {renamed} TO {table}")
        rows, qid = self.env.run(sql, cache=True, prefix="i08after")
        if self.env.events([qid])[qid]["hits"] != 0:
            raise AssertionError("query after RENAME round-trip must miss")
        off_rows, _ = self.env.run(sql, cache=False, prefix="i08off")
        self.assert_equal(rows, off_rows, "RENAME result differs")
        return {"entries": self.engine.entries()}

    def case_i09a(self) -> dict[str, Any]:
        """INSERT（写会话开启缓存）必须失效，新行参与 Top-K。"""
        table = self.env.fresh_table("t_i09a")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="i09awarm")
        on_client = self.env.client(cache=True)
        self.env.insert(on_client, table, [(9999, VECTORS[0])])
        self.engine.wait_for_index(table)
        self.assert_entries(0, "INSERT must clear entries")
        rows, qid = self.env.run(sql, cache=True, prefix="i09aafter")
        if self.env.events([qid])[qid]["hits"] != 0:
            raise AssertionError("query after INSERT must miss")
        if not any(row[0] == 9999 for row in rows):
            raise AssertionError(f"new row is missing from Top-K: {rows}")
        return {"rows": rows, "entries": self.engine.entries()}

    def case_i09b(self) -> dict[str, Any]:
        """已知缺陷：写会话关闭缓存时 INSERT 不失效，Top-K 漏掉新行。"""
        table = self.env.fresh_table("t_i09b")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="i09bwarm")
        off_client = self.env.client(cache=False)
        self.env.insert(off_client, table, [(8888, VECTORS[0])])
        self.engine.wait_for_index(table)
        try:
            rows, qid = self.env.run(sql, cache=True, prefix="i09bafter")
        except ServerException as exc:
            raise AssertionError(f"stale plan failed with code {exc.code}: {exc}") from exc
        events = self.env.events([qid])
        stale_hit = events[qid]["hits"] > 0
        row_visible = any(row[0] == 8888 for row in rows)
        if stale_hit and not row_visible:
            raise AssertionError(f"stale plan omits the newly inserted row: {rows}, events={events}")
        self.engine.system_clear()
        return {"rows": rows, "events": events}

    def case_i10(self) -> dict[str, Any]:
        """无关表 DDL 不得清空其它表的缓存条目。"""
        table = self.env.fresh_table("t_i10")
        other = self.env.fresh_table("t_i10_other", rows=20)
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="i10warm")
        self.assert_entries(1, "entry must exist before unrelated DDL")
        client = self.env.client(cache=True)
        client.execute(f"ALTER TABLE {other} ADD COLUMN extra UInt8 DEFAULT 0")
        client.execute(f"CREATE TABLE {self.env.table('t_i10_new')} (id UInt32) ENGINE = Memory")
        self.assert_entries(1, "unrelated DDL must not clear entries")
        _, qid = self.env.run(sql, cache=True, prefix="i10after")
        if self.env.events([qid])[qid]["hits"] == 0:
            raise AssertionError("unrelated DDL invalidated the cached entry")
        return {"entries": self.engine.entries()}

    def case_i11(self) -> dict[str, Any]:
        """TTL 到期后首次查询必须 miss。"""
        table = self.env.fresh_table("t_i11")
        self.engine.system_clear()
        extra = {"vector_query_plan_cache_ttl": 1} if self.engine.name == "clickhouse" else {"query_plan_cache_ttl": 1}
        sql = self.engine.query_sql(table, VECTORS[0])
        _, first_qid = self.env.run(sql, cache=True, extra=extra, prefix="i11a")
        _, second_qid = self.env.run(sql, cache=True, extra=extra, prefix="i11b")
        if self.env.events([second_qid])[second_qid]["hits"] == 0:
            raise AssertionError("second query did not hit before TTL expiry")
        self.env.wait_ttl(2.2)
        _, third_qid = self.env.run(sql, cache=True, extra=extra, prefix="i11c")
        if self.env.events([third_qid])[third_qid]["hits"] > 0:
            raise AssertionError("query still hit after TTL expiry")
        return {"ttl_seconds": 1, "sleep_seconds": 2.2}

    def case_i12(self) -> dict[str, Any]:
        """SYSTEM DROP ... CACHE TAG 只清除指定 tag 的条目。"""
        if not self.engine.supports_cache_tag:
            raise SkipTest("tag-scoped cache clear is not exposed by this engine's parser")
        table = self.env.fresh_table("t_i12")
        self.engine.system_clear()
        tag_setting = "query_plan_cache_tag"
        client_a = self.env.client(cache=True, plan_only=True, extra={tag_setting: "tag_a"})
        client_b = self.env.client(cache=True, plan_only=True, extra={tag_setting: "tag_b"})
        sql_a = self.engine.query_sql(table, VECTORS[0], limit=3)
        sql_b = self.engine.query_sql(table, VECTORS[0], limit=4)
        qids = []
        for prefix, client, sql in (("i12a1", client_a, sql_a), ("i12a2", client_a, sql_a), ("i12b1", client_b, sql_b), ("i12b2", client_b, sql_b)):
            qids.append((prefix, self.env.next_qid(prefix)))
            client.execute(sql, query_id=qids[-1][1])
        self.engine.system_clear(tag="tag_a")
        qid_a = self.env.next_qid("i12a3")
        qid_b = self.env.next_qid("i12b3")
        client_a.execute(sql_a, query_id=qid_a)
        client_b.execute(sql_b, query_id=qid_b)
        events = self.env.events([qid_a, qid_b])
        if events[qid_a]["hits"] != 0:
            raise AssertionError(f"tag_a entry survived tag-scoped clear: {events}")
        if events[qid_b]["hits"] == 0:
            raise AssertionError(f"tag_b entry was cleared by tag_a clear: {events}")
        return {"events": events}

    def case_i13(self) -> dict[str, Any]:
        """容量淘汰需要服务端配置；默认容量行为由 C09 覆盖。"""
        raise SkipTest("capacity eviction requires changing server-level max_entries; C09 covers default-capacity retention")

    def case_i14(self) -> dict[str, Any]:
        """其它数据库的 DROP DATABASE 不得失效当前数据库的条目。"""
        table = self.env.fresh_table("t_i14")
        aux = f"{self.env.database}_aux"
        admin = self.engine.admin
        admin.execute(f"DROP DATABASE IF EXISTS {aux}")
        admin.execute(f"CREATE DATABASE {aux}")
        admin.execute(f"CREATE TABLE {aux}.t (id UInt32) ENGINE = Memory")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="i14warm")
        self.assert_entries(1, "entry must exist before database DDL")
        self.env.client(cache=True).execute(f"DROP DATABASE {aux}")
        self.assert_entries(1, "unrelated DROP DATABASE must not clear entries")
        _, qid = self.env.run(sql, cache=True, prefix="i14after")
        if self.env.events([qid])[qid]["hits"] == 0:
            raise AssertionError("unrelated DROP DATABASE invalidated the entry")
        return {"entries": self.engine.entries()}

    def case_i15(self) -> dict[str, Any]:
        """进程重启清空内存缓存，需人工执行。"""
        raise SkipTest("requires a server restart; run manually and verify entries=0 plus a cold miss")

    # -- K: concurrency -----------------------------------------------------
    def _start_query_threads(self, sql: str, count: int, stop: threading.Event, errors: list[str], tolerated: set[int] | None = None) -> list[threading.Thread]:
        """启动 ``count`` 个后台查询线程并立即返回（不 join）。

        线程持续执行同一 SQL 直到 ``stop`` 置位；``tolerated`` 里的服务端
        错误码视为预期（例如并发 DDL 期间的 Code 60），其余错误写入
        ``errors`` 并让该线程退出。
        """
        tolerated = tolerated or set()

        def worker(index: int) -> None:
            """查询线程主体：独占连接，循环执行直到收到停止信号。"""
            client = self.engine.new_client(cache=True)
            while not stop.is_set():
                try:
                    client.execute(sql, query_id=self.env.next_qid(f"k{index}"))
                except ServerException as exc:
                    if exc.code not in tolerated:
                        errors.append(f"query thread {index}: {exc.code} {exc}")
                        return
                except Exception as exc:  # noqa: BLE001 - report unexpected failure
                    errors.append(f"query thread {index}: {exc!r}")
                    return

        threads = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(count)]
        for thread in threads:
            thread.start()
        return threads

    def case_k01(self) -> dict[str, Any]:
        """并发查询 + ALTER ADD/DROP COLUMN：无异常、无错误结果。"""
        table = self.env.fresh_table("t_k01")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="k01warm")
        stop = threading.Event()
        errors: list[str] = []
        threads = self._start_query_threads(sql, 4, stop, errors)
        ddl = self.engine.new_client(cache=True)
        try:
            for index in range(5):
                ddl.execute(f"ALTER TABLE {table} ADD COLUMN c{index} UInt8 DEFAULT 0")
                time.sleep(0.05)
                ddl.execute(f"ALTER TABLE {table} DROP COLUMN c{index}")
                time.sleep(0.05)
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=10)
        if errors:
            raise AssertionError(f"concurrent errors: {errors}")
        on_rows, _ = self.env.run(sql, cache=True, prefix="k01final")
        off_rows, _ = self.env.run(sql, cache=False, prefix="k01off")
        self.assert_equal(on_rows, off_rows, "result after concurrent ALTER differs")
        return {"threads": 4, "alterations": 5}

    def case_k02(self) -> dict[str, Any]:
        """并发查询 + DROP/CREATE 同名表：允许表缺失窗口，不允许崩溃或旧结果。"""
        table = self.env.fresh_table("t_k02")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="k02warm")
        stop = threading.Event()
        errors: list[str] = []
        # Creating under a temporary name and renaming it keeps the table from
        # being visible while empty; queries only see UNKNOWN_TABLE / UNKNOWN_DATABASE.
        threads = self._start_query_threads(sql, 4, stop, errors, tolerated={60, 81})
        ddl = self.engine.new_client(cache=True)
        try:
            for index in range(3):
                temp = self.env.table(f"t_k02_tmp{index}")
                ddl.execute(f"DROP TABLE IF EXISTS {table}")
                ddl.execute(self.engine.create_table_sql(temp, DEFAULT_DIM))
                self.env.insert(ddl, temp, make_rows(20, DEFAULT_DIM, id_base=1000 * (index + 1)))
                self.engine.wait_for_index(temp)
                ddl.execute(f"RENAME TABLE {temp} TO {table}")
                time.sleep(0.05)
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=10)
        if errors:
            raise AssertionError(f"concurrent errors: {errors}")
        on_rows, _ = self.env.run(sql, cache=True, prefix="k02final")
        off_rows, _ = self.env.run(sql, cache=False, prefix="k02off")
        self.assert_equal(on_rows, off_rows, "result after concurrent DROP+CREATE differs")
        if not on_rows or on_rows[0][0] < 1000:
            raise AssertionError(f"stale result after concurrent DROP+CREATE: {on_rows}")
        return {"threads": 4, "recreations": 3}

    def case_k03(self) -> dict[str, Any]:
        """并发查询 + 周期清空缓存：无异常、结果正确。"""
        table = self.env.fresh_table("t_k03")
        self.engine.system_clear()
        sql = self.engine.query_sql(table, VECTORS[0])
        self.warm(sql, prefix="k03warm")
        stop = threading.Event()
        errors: list[str] = []
        threads = self._start_query_threads(sql, 4, stop, errors)
        try:
            for _ in range(20):
                self.engine.system_clear()
                time.sleep(0.02)
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=10)
        if errors:
            raise AssertionError(f"concurrent errors: {errors}")
        on_rows, _ = self.env.run(sql, cache=True, prefix="k03final")
        off_rows, _ = self.env.run(sql, cache=False, prefix="k03off")
        self.assert_equal(on_rows, off_rows, "result after concurrent cache clear differs")
        return {"threads": 4, "clears": 20}


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

CASE_CATALOG: list[tuple[str, str, str]] = [
    ("C01", "单模板 100 实例：缓存开/关逐条一致", "case_c01"),
    ("C02", "预热后连续查询稳定命中", "case_c02"),
    ("C03", "SQL 表示形式常量恢复", "case_c03"),
    ("C04", "命中后常量替换正确", "case_c04"),
    ("C05", "plan-only 与 AST+Plan 结果一致", "case_c05"),
    ("C06", "LIMIT 参与缓存键", "case_c06"),
    ("C07", "查询结果缓存强制关闭", "case_c07"),
    ("C08", "子查询不缓存", "case_c08"),
    ("C09", "默认容量下无静默淘汰", "case_c09"),
    ("I01", "ALTER（DDL 会话开启缓存）失效", "case_i01"),
    ("I02", "ALTER（DDL 会话关闭缓存）失效", "case_i02"),
    ("I03", "维度变化后旧计划不复用", "case_i03"),
    ("I04", "DROP+CREATE 同名表（缓存关闭）", "case_i04"),
    ("I05", "DROP+CREATE 同名表（缓存开启）", "case_i05"),
    ("I06", "索引删除/重建失效", "case_i06"),
    ("I07", "TRUNCATE 失效", "case_i07"),
    ("I07b", "TRUNCATE 后空表查询", "case_i07b"),
    ("I08", "RENAME 失效", "case_i08"),
    ("I09a", "INSERT（写会话开启缓存）失效", "case_i09a"),
    ("I09b", "INSERT（写会话关闭缓存）失效", "case_i09b"),
    ("I10", "无关表 DDL 不越界失效", "case_i10"),
    ("I11", "TTL 到期失效", "case_i11"),
    ("I12", "TAG 范围失效", "case_i12"),
    ("I13", "容量淘汰（需服务端配置）", "case_i13"),
    ("I14", "其它数据库 DDL 不越界失效", "case_i14"),
    ("I15", "进程重启清空缓存", "case_i15"),
    ("K01", "并发查询 + ALTER", "case_k01"),
    ("K02", "并发查询 + DROP/CREATE 同名表", "case_k02"),
    ("K03", "并发查询 + 周期清空缓存", "case_k03"),
]

# Defects confirmed on both engines: DDL executed by a session with the plan
# cache disabled never invalidates the cache. MS additionally cannot clone a
# plan for a vector-indexed table that has no data (Code 48).
KNOWN_DEFECTS: dict[str, set[str]] = {
    "clickhouse": {"I02", "I04", "I09b"},
    "myscale": {"I02", "I04", "I09b", "I07b"},
}


def parse_only(value: str | None) -> set[str] | None:
    """解析 ``--only`` 的逗号分隔用例 id；空值返回 None 表示"全部执行"。"""
    if not value:
        return None
    return {item.strip() for item in value.split(",") if item.strip()}


def main(argv: Sequence[str] | None = None) -> int:
    """CLI 入口：连接引擎、依次执行用例、输出 JSON 报告并返回退出码。"""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", default="clickhouse", choices=sorted(ENGINES))
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=None)
    parser.add_argument("--user", default="default")
    parser.add_argument("--password", default="")
    parser.add_argument("--database", default="pcache_ci")
    parser.add_argument("--only", default=None, help="Comma-separated case ids, e.g. C01,I01")
    parser.add_argument("--keep", action="store_true", help="Keep the scratch database and cached entries")
    parser.add_argument("--report", default=None, help="Report JSON path (default: tmp/plan_cache_ci_<engine>_<ts>.json)")
    args = parser.parse_args(argv)

    engine_class = ENGINES[args.engine]
    port = args.port if args.port is not None else (9000 if engine_class is ClickHouseEngine else 19000)
    engine = engine_class(args.host, port, args.user, args.password)

    try:
        engine.admin.execute("SELECT 1")
    except Exception as exc:  # noqa: BLE001 - fail fast with context
        print(f"Cannot connect to {engine.name} at {args.host}:{port}: {exc}", file=sys.stderr)
        return 2

    selected = parse_only(args.only)
    known_defects = KNOWN_DEFECTS.get(engine.name, set())
    env = Environment(engine, args.database)
    cases = Cases(env)
    results: list[CaseResult] = []

    env.setup()
    print(f"engine={engine.name} {args.host}:{port} database={args.database}")
    try:
        for case_id, title, method_name in CASE_CATALOG:
            if selected and case_id not in selected:
                continue
            start = time.monotonic()
            try:
                metrics = getattr(cases, method_name)()
                if case_id in known_defects:
                    # The case passed but is expected to fail: the defect is fixed,
                    # which needs the marker removed, not a red build.
                    status = "XPASS"
                    detail = "known defect no longer reproduces; remove the XFAIL marker"
                else:
                    status = "PASS"
                    detail = "ok"
            except SkipTest as exc:
                status = "SKIP"
                metrics = {}
                detail = str(exc)
            except AssertionError as exc:
                if case_id in known_defects:
                    status = "XFAIL"
                    detail = f"known defect reproduced: {exc}"
                else:
                    status = "FAIL"
                    detail = str(exc)
                metrics = {}
            except Exception as exc:  # noqa: BLE001 - keep the suite running
                status = "FAIL"
                detail = f"{type(exc).__name__}: {exc}"
                metrics = {}
                traceback.print_exc()
            duration = time.monotonic() - start
            results.append(CaseResult(case_id, title, status, detail, metrics, duration))
            print(f"[{status:5s}] {case_id:5s} {title} ({duration:.2f}s)")
            if status in ("FAIL", "SKIP", "XFAIL"):
                print(f"        {detail}")
    finally:
        if not args.keep:
            env.teardown()

    summary: dict[str, int] = {}
    for result in results:
        summary[result.status] = summary.get(result.status, 0) + 1

    report = {
        "engine": engine.name,
        "version": engine.admin.execute("SELECT version()")[0][0],
        "host": args.host,
        "port": port,
        "database": args.database,
        "timestamp": datetime.now().strftime(TIME_FORMAT),
        "summary": summary,
        "results": [result.to_dict() for result in results],
    }

    report_path = args.report or os.path.join(
        "tmp", f"plan_cache_ci_{engine.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    os.makedirs(os.path.dirname(report_path) or ".", exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as handle:
        json.dump(report, handle, ensure_ascii=False, indent=2)

    print("\nSummary: " + ", ".join(f"{key}={value}" for key, value in sorted(summary.items())))
    print(f"Report: {report_path}")
    return 1 if summary.get("FAIL") or summary.get("XPASS") else 0


if __name__ == "__main__":
    sys.exit(main())
