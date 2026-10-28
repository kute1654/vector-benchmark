# 查询计划缓存“正确性与失效”测试设计

本文档依据《查询计划缓存论文补充实验与Benchmark完善指南-单模板DDL失效版.docx》第 5.7 节
`E7` 与表 17、表 18、表 25、表 29 的 `A7` 条目，给出可执行的“正确性与失效”测试用例设计，
覆盖两套独立实现：

- **CK**：`/home/ClickHouse` 的 `VectorQueryPlanCache`（向量查询计划缓存）。
- **MS**：`/home/clickhouse` 的 `QueryPlanCache`（MyScale 查询计划缓存）。

测试脚本为 `benchmark/plan_cache_correctness_invalidation.py`，与性能基准解耦：它只验证
“缓存是否命中、命中后结果是否正确、DDL/DML 后是否失效”，不测量 QPS，也不读写任何基准数据集。
所有测试对象都建在脚本自有的 `pcache_ci` 数据库中，运行结束即删除（`--keep` 可保留）。

---

## 1. 测试目标

对应指南中的验收表述，本套用例回答四个问题：

1. **正确性**：同一条 SQL 模板下替换常量（查询向量、`LIMIT`、字面量写法）后，缓存命中返回的
   Top-K 必须与关闭缓存逐条一致；结果缓存必须处于关闭状态，避免把结果缓存命中误判为计划缓存命中。
2. **表级失效**：对目标表执行 `ALTER`、索引删除/重建、`DROP`+同名 `CREATE`、`TRUNCATE`、
   `RENAME`、`INSERT` 后，相关条目必须被清除，DDL 后第一条查询必须 miss，并读到新数据。
3. **失效边界**：无关表、无关数据库的 DDL 不得越界清除其它表的条目；`LIMIT` 等参与缓存键的
   维度必须形成独立条目；默认容量下不得发生静默淘汰。
4. **并发安全**：查询与 DDL（`ALTER`、`DROP`/`CREATE`）、查询与缓存清空并发执行时不得崩溃、
   不得返回旧表数据。

单模板稳定性（表 25“单模板稳定”）单独由 `C01`/`C02`/`C09` 覆盖：预热后 CK 的
`VectorQueryPlanCacheEntries` 必须保持 `1`（`C09` 中为不同 `LIMIT` 的 `20`），不能随查询实例数增长。

---

## 2. 两套实现的关键差异

两套实现共享“AST 缓存 + QueryPlan 缓存”两级结构，但缓存键与可观测性差异明显，测试设计必须分别处理。

### 2.1 缓存键：归一化 SQL 只在 plan-only 模式生效

| 项目 | CK | MS |
| --- | --- | --- |
| 启用设置 | `vector_query_plan_cache=1` | `enable_query_plan_cache=1` |
| 仅计划缓存 | `vector_only_cache_query_plan=1` | `only_cache_query_plan=1` |
| 键中的 SQL | plan-only 模式下为归一化 SQL；默认模式为原始 SQL | 同上（`only_cache_query_plan` 时为归一化 SQL） |
| 清空 | `SYSTEM DROP VECTOR QUERY PLAN CACHE` | `SYSTEM DROP QUERY PLAN CACHE` |
| 按 tag 清空 | 解析器不支持 `TAG` | `SYSTEM DROP QUERY PLAN CACHE TAG 'x'` |
| TTL 设置 | `vector_query_plan_cache_ttl` | `query_plan_cache_ttl` |

**这是单模板用例最重要的前提**：只有在 plan-only 模式下，不同向量字面量才会归一到同一个缓存键。
默认模式下 `query_string` 是原始 SQL，每条查询实例天然是不同键，无法验证“一条模板服务 100 个实例”。
因此 `C01`–`C04`、`C06`、`C09` 全部显式设置 plan-only；`C05` 则对比两种模式的结果一致性。

### 2.2 查询与可观测性

| 项目 | CK | MS |
| --- | --- | --- |
| 距离函数 | `L2Distance`、`cosineDistance` | `distance`（L2） |
| 向量索引 | `INDEX vi vector TYPE vector_similarity('hnsw', 'L2Distance', dim, 'bf16', 16, 100)` | `VECTOR INDEX vi vector TYPE HNSWFLAT` + `CHECK length(vector) = dim` |
| 索引构建等待 | 同步 | `SYSTEM WAIT BUILDING VECTOR INDICES <db>.<table>` |
| 命中事件 | `VectorQueryPlanCacheHits` | `QueryPlanCacheHits` |
| 未命中事件 | `VectorQueryPlanCacheMisses` | `QueryPlanCacheMisses` |
| 条目数指标 | `system.metrics.VectorQueryPlanCacheEntries` | 无对应指标，只能用“首次 miss / 次次 hit”推断 |
| 结果缓存事件 | `QueryCacheHits`（必须恒为 0） | `QueryCacheHits`（必须恒为 0） |

命中判定统一走 `system.query_log`：每条查询带唯一 `query_id`，执行后 `SYSTEM FLUSH LOGS`，再按
`query_id` 读取 `type='QueryFinish'` 行的 `ProfileEvents`。不断言 SQL 文本，因为 CK 的 `query_log`
可能记录模板中的首个字面量，用 `query_id` 才能避免误判。

MS 的同一条查询可能产生多行 `QueryFinish`，脚本按 `query_id` 取 `max()` 聚合，防止重复行干扰。

CK 的 `.sql` 回归测试同时覆盖 `L2Distance` 与 `cosineDistance`：`C10`/`C11` 验证余弦模板的
单模板稳定与独立缓存键，`I01` 验证一次 `ALTER` 同时清除两种度量的条目，`I06` 在索引重建后
把索引度量从 L2 换成 cosine。Python 脚本的 CK/MS 对比仍只用 L2。

### 2.3 引擎能力限制

- MS 支持 `ALTER TABLE ... DROP VECTOR INDEX` 与 `DROP COLUMN`，也支持把索引列重建为不同维度
  （`00057` 实测）；但 `DROP CONSTRAINT` 只允许在空表上执行（非空表报 Code 44），因此“原地维度
  变化”的顺序必须是：删索引 → `TRUNCATE` → 删约束 → 删列 → 加列 → 写数据 → 加约束 → 加索引。
- MS 对**空表**且带向量索引的表，在计划缓存开启时无法 `clone` 计划，抛 Code 48
  （`ReadFromPreparedSource cannot be cloned without pipe_builder`），因此 `TRUNCATE` 后立即查询只能在
  CK 通过；MS 该用例记为已知缺陷 `XFAIL`。
- MS 的唯一向量索引建表时必须带长度 `CHECK` 约束，脚本按维度生成 `CHECK length(vector) = dim`。

---

## 3. 判定口径与状态码

| 状态 | 含义 | 是否影响退出码 |
| --- | --- | --- |
| `PASS` | 断言全部成立 | 否 |
| `FAIL` | 断言失败且不属于已知缺陷 | 是 |
| `XFAIL` | 复现了已确认缺陷 | 否 |
| `XPASS` | 已知缺陷未复现（缺陷已修复），需要移除标记 | 是 |
| `SKIP` | 该引擎不支持，或需要改服务端配置/重启 | 否 |

脚本退出码为 0 当且仅当没有 `FAIL` 和 `XPASS`。

每个用例同时校验三件事中的若干项：

1. **事件判定**：`hits > 0` / `misses > 0`；
2. **结果判定**：缓存开/关的 Top-K 逐行一致（`id` 全等，距离容差 `1e-6`）；
3. **条目判定**（仅 CK）：`VectorQueryPlanCacheEntries` 等于期望值。

`C07` 额外断言所有被测查询的 `QueryCacheHits == 0`，确保命中的是计划缓存而不是结果缓存。

---

## 4. 用例目录

### 4.1 正确性用例（C）

| ID | 标题 | 关键动作 | 通过判据 |
| --- | --- | --- | --- |
| `C01` | 单模板 100 实例 | plan-only 下 100 个不同向量，每条与缓存关闭结果对比 | 100 条全部一致；至少 99 次 hit；CK `entries=1` |
| `C02` | 预热后稳定命中 | 预热 1 条后连续 20 个不同向量 | 20 次全部 hit；CK `entries=1` |
| `C03` | SQL 字面量形式 | `normal` / `CAST([...] AS Array(Float32))` / `[...]::Array(Float32)` 各 5 条 | 各形式开/关结果一致并命中；不支持的形式按子项跳过 |
| `C04` | 常量替换 | 命中后替换 10 个查询向量 | 10 条结果一致，10 次 hit |
| `C05` | 模式一致性 | 同一查询分别用 plan-only、AST+Plan、关闭 | 三种模式结果一致 |
| `C06` | `LIMIT` 参与缓存键 | `LIMIT 3` 与 `LIMIT 5` 各执行两次 | 首次均 miss、次次 hit；CK `entries=2` |
| `C07` | 结果缓存关闭 | 同一条查询执行两次 | 第二次计划缓存 hit；`QueryCacheHits` 全为 0 |
| `C08` | 子查询不缓存 | `SELECT * FROM (SELECT ...)` 执行两次 | 两次均 miss；CK `entries=0`；结果正确 |
| `C09` | 默认容量无静默淘汰 | 20 个不同 `LIMIT` 形状，再回到 `LIMIT 1` | 20 次冷 miss；CK `entries=20`；回访命中 |
| `C10` | 余弦距离单模板 | `cosineDistance` 下 100 个不同向量 | 99 次 hit；CK `entries=1` |
| `C11` | 距离函数参与缓存键 | 同一字面量分别用 `L2Distance`、`cosineDistance` 各执行两次 | 各 1 miss + 1 hit；CK `entries=2` |

`C01` 对应指南表 25 的“单模板稳定”与 `A6`（≥100 次实例对比、`entries=1`）；
`C09` 对应表 25 的“默认容量下无静默淘汰”；`C10`/`C11` 把这两条结论扩展到
`cosineDistance`，并验证距离函数本身参与缓存键。

### 4.2 失效用例（I）

| ID | 标题 | 触发方式 | 通过判据 | 引擎 |
| --- | --- | --- | --- | --- |
| `I01` | `ALTER` 失效（DDL 会话开启缓存） | 缓存开启的连接执行 `ADD COLUMN` | 条目归零；下一条 miss；结果正确 | CK/MS |
| `I02` | `ALTER` 失效（DDL 会话关闭缓存） | 缓存关闭的连接执行 `ADD COLUMN` | 条目归零且下一条 miss | CK/MS（已知缺陷） |
| `I03` | 维度变化 | CK：删索引→删列→加 16 维列→加索引→`TRUNCATE`→写入 16 维数据 | 旧计划不复用；返回新数据；条目归零 | CK/MS |
| `I04` | `DROP`+同名 `CREATE`（缓存关闭） | 缓存关闭的连接重建同名表并写入新数据 | 下一条 miss 且读到新数据 | CK/MS（已知缺陷） |
| `I05` | `DROP`+同名 `CREATE`（缓存开启） | 缓存开启的连接重建同名表并写入新数据 | 条目归零；下一条 miss；读到新数据 | CK/MS |
| `I06` | 索引删除/重建 | 删除向量索引→查询→重建索引→查询 | 每次 DDL 后条目归零、首查 miss；重建后再次命中 | CK/MS |
| `I07` | `TRUNCATE` | 缓存开启的连接 `TRUNCATE`，缓存关闭的连接写入 20 行 | 条目归零；下一条 miss；只返回新行 | CK/MS |
| `I07b` | `TRUNCATE` 后空表查询 | 清空后立即查询 | 返回空集且不报错 | CK；MS 已知缺陷 |
| `I08` | `RENAME` | 改名后改回 | 两次 DDL 后条目归零；首查 miss | CK/MS |
| `I09a` | `INSERT`（写会话开启缓存） | 缓存开启的连接插入距离为 0 的新行 | 条目归零；首查 miss；新行进入 Top-K | CK/MS |
| `I09b` | `INSERT`（写会话关闭缓存） | 缓存关闭的连接插入新行 | 首查 miss 且新行可见 | CK/MS（已知缺陷） |
| `I10` | 无关表 DDL 不越界 | 对另一张表 `ALTER`，另建新表 | CK 条目保持 1；原查询仍 hit | CK/MS |
| `I11` | TTL 到期 | 设置 TTL=1s，正常命中后等待 2.2s | 到期后首查 miss | CK/MS |
| `I12` | TAG 范围失效 | 两个连接使用不同 `query_plan_cache_tag`，按 tag 清空 | 目标 tag miss，另一 tag 仍 hit | MS；CK `SKIP` |
| `I13` | 容量淘汰 | 需要修改服务端 `max_entries` | 由 `C09` 覆盖默认容量，不单独执行 | `SKIP` |
| `I14` | 其它数据库 DDL 不越界 | 删除另一个数据库 | CK 条目保持 1；原查询仍 hit | CK/MS |
| `I15` | 进程重启 | 需要重启服务端 | 手动验证 `entries=0` 且首查 miss | `SKIP` |

`I02`/`I04`/`I09b` 检验的是同一个根因：`executeQuery` 中的 DDL 失效逻辑被
`enable...plan_cache && cache && !is_select` 门控，**执行 DDL 的会话本身没有开启计划缓存时，
失效被整体跳过**。`I01`/`I05` 作为对照组，证明同一 DDL 在缓存开启的会话中能正常失效。
注意 `I01` 必须用缓存开启的客户端执行 `ALTER`，若用默认会话执行，会与 `I02` 混为一谈。

### 4.3 并发用例（K）

| ID | 标题 | 并发方式 | 通过判据 |
| --- | --- | --- | --- |
| `K01` | 查询 + `ALTER` | 4 个查询线程，主线程交替 `ADD/DROP COLUMN` 5 轮 | 无异常；结束后开/关结果一致 |
| `K02` | 查询 + `DROP`/`CREATE` 同名表 | 4 个查询线程；主线程在临时名下建表、写入、`RENAME` 为正式名，共 3 轮 | 只允许 `UNKNOWN_TABLE`/`UNKNOWN_DATABASE`（Code 60/81）；最终结果为最新表数据 |
| `K03` | 查询 + 周期清空缓存 | 4 个查询线程；主线程清空缓存 20 次 | 无异常；结束后开/关结果一致 |

`K02` 采用“临时表建好再 `RENAME`”的方式，避免查询看到空表：MS 在空表上开启缓存会触发
Code 48，而该用例考察的是并发 DDL 本身，不是空表缺陷。

---

## 5. 与指南表 17 / 18 / 25 / 29 的对应关系

| 指南条目 | 设计要点 | 本文档落点 |
| --- | --- | --- |
| 表 17 依赖记录 | 缓存条目记录的表依赖必须足以支撑反向失效 | `I01`/`I05`/`I06`/`I08` 验证 DDL 后条目归零；`I10`/`I14` 验证不越界 |
| 表 17 反向映射 | 表 → 条目的映射用于表级清除 | `I05`（同名重建）、`I14`（数据库级） |
| 表 17 触发范围 | 只清除受影响表的条目 | `I10`（无关表）、`I14`（无关库） |
| 表 17 失效时序 | DDL 返回后条目已清除，后续首查 miss | 所有 `I01`–`I09b` 的首查 miss 断言 |
| 表 17 在途查询 | 并发 DDL 不得破坏正在执行的查询 | `K01`/`K02`/`K03` |
| 表 17 DDL 后查询 | 首查 miss，随后重新命中 | `I06`（重建后再次 hit）、`I05` |
| 表 17 设计取舍 | 一致性优先于命中率；失效可保守 | `I02`/`I04`/`I09b` 将“漏失效”记为缺陷而非可接受取舍 |
| 表 18 `ALTER TABLE`/维度变化 | ALTER 与列类型/维度变化 | `I01`/`I02`（列变化）、`I03`（维度变化，CK） |
| 表 18 索引删除或重建 | 删除、重建向量索引 | `I06`（CK） |
| 表 18 `DROP` 后创建同名表 | 同名表不能复用旧计划 | `I04`/`I05`（两引擎） |
| 表 18 并发查询 + DDL | 并发安全 | `K01`/`K02` |
| 表 25 结果缓存强制关闭 | 命中必须来自计划缓存 | `C07` |
| 表 25 单模板稳定 | 预热后 `entries=1` | `C01`/`C02`/`C09` |
| 表 25 DDL 表级失效 | 条目归零、首查 miss、后续 hit | `I01`–`I09b` |
| 表 25 并发失效安全 | 并发下不崩溃、不返回旧数据 | `K01`–`K03` |
| 表 29 `A7` | 正确性与失效四项 | `C01`–`C09` + `I01`–`I09b` + `K01`–`K03` |

---

## 6. 实测结果（2026-10-26）

环境：CK `26.11.1.1` @ `127.0.0.1:9000`，MS `24.8.8.1` @ `127.0.0.1:19000`。
报告文件：`tmp/plan_cache_ci_clickhouse_<ts>.json`、`tmp/plan_cache_ci_myscale_<ts>.json`。

| 引擎 | PASS | FAIL | SKIP | XFAIL | XPASS |
| --- | --- | --- | --- | --- | --- |
| CK | 23 | 0 | 3 | 3 | 0 |
| MS | 21 | 0 | 4 | 4 | 0 |

CK 跳过项：`I12`（解析器不支持 `TAG`）、`I13`（需服务端配置）、`I15`（需重启）。
MS 跳过项：`I03`、`I06`、`I13`、`I15`。其中 `I03`/`I06` 是脚本中
`supports_dimension_change`/`supports_index_alter` 守卫的结果，该判断对应更早的 MS 构建；
在本报告的 `24.8.8.1` 构建上，`00057` 已验证 MS 支持这两类 DDL（见 2.3、9.4）。守卫与脚本
尚未同步，`I03`/`I06` 的 MS 回归以 `.sql` 测试为准。

两引擎均复现的已知缺陷：`I02`、`I04`、`I09b`。MS 额外复现 `I07b`（空表 Code 48）。
其余用例全部通过，包括两引擎的 `K01`–`K03` 并发场景。

---

## 7. 已确认的缺陷与限制

### 7.1 DDL 会话关闭缓存时漏失效（两引擎）

`executeQuery` 中只有满足 `enable...plan_cache && cache && !is_select` 才调用
`query_plan_cache->invalidateByQuery(...)`。因此一个未开启计划缓存的会话执行 `ALTER`、
`DROP`+`CREATE`、`INSERT` 后，缓存保留旧条目：

- `I02`：CK 条目仍为 1，下一条查询命中；
- `I04`：两引擎均复用旧计划，返回旧表的行（`id < 1000`）；
- `I09b`：两引擎均命中旧计划，Top-K 中缺少新插入的行。

对照组 `I01`/`I05`/`I09a` 在缓存开启的会话中执行同样的 DDL，均正确失效。建议将失效判定与
“DDL 会话的缓存设置”解耦：DDL 要么总是尝试失效，要么在写入缓存时就登记依赖，由表级事件驱动清理。

### 7.2 MS 空表计划无法克隆（Code 48）

向量索引表为空且计划缓存开启时，`QueryPlan::clone()` 在 `ReadFromPreparedSource` 上抛
`ReadFromPreparedSource cannot be cloned without pipe_builder`（Code 48）。`I07b` 在 `TRUNCATE`
后立即查询即可稳定复现。影响：清空后的向量表在缓存开启时无法查询。建议对含
`ReadFromPreparedSource` 的计划跳过缓存写入或改为可克隆的来源。

### 7.3 引擎能力限制（非缺陷）

- CK 的 `SYSTEM DROP VECTOR QUERY PLAN CACHE TAG 'x'` 语法不被解析器接受，无法按 tag 清理。
- MS 可以 `ALTER TABLE ... DROP/ADD VECTOR INDEX`，也可以删除被索引引用的列；只有
  `DROP CONSTRAINT` 要求表为空（Code 44），见 2.3。
- MS 没有 `QueryPlanCacheEntries` 指标，条目数只能由 miss/hit 序列推断。
- MS 的参数化只识别直接出现在投影里的 `distance(...)`：`round(distance(...), 4)`、带 `WHERE`
  的查询、以及子查询都不产生任何缓存事件；`cosineDistance` 也不被识别。MS 的余弦覆盖因此
  改用 `float_vector_search_metric_type = 'Cosine'` 表，距离函数名不进入缓存键，CK 的 `C11`
  没有 MS 对应项。

### 7.4 可观测性注意事项

- 默认会话（脚本的 admin 客户端）未开启计划缓存，用它执行 DDL 会落入 7.1 的漏失效路径；
  因此 `I01` 的 DDL 必须走缓存开启的客户端。
- MS 的 `system.query_log` 可能出现同一 `query_id` 的多行 `QueryFinish`，聚合时取 `max()`。
- 判断命中必须用 `query_id`；CK 的 `query_log.query` 可能显示模板中的首个字面量，不能反映本次实例。
- 所有查询显式设置 `use_query_cache=0`，并用 `QueryCacheHits=0` 交叉验证结果缓存未被使用。

---

## 8. 运行方式

```bash
# CK
python3 benchmark/plan_cache_correctness_invalidation.py \
    --engine clickhouse --host 127.0.0.1 --port 9000

# MS
python3 benchmark/plan_cache_correctness_invalidation.py \
    --engine myscale --host 127.0.0.1 --port 19000

# 只跑部分用例
python3 benchmark/plan_cache_correctness_invalidation.py \
    --engine clickhouse --only C01,I01,I02,K02

# 保留测试库与缓存条目，便于事后检查
python3 benchmark/plan_cache_correctness_invalidation.py --engine myscale --keep
```

参数说明：

- `--engine`：`clickhouse`/`ck` 或 `myscale`/`ms`；
- `--only`：逗号分隔的用例 ID；
- `--keep`：不删除 `pcache_ci` 数据库与缓存条目；
- `--report`：报告 JSON 路径，默认写入 `tmp/`。

脚本依赖 `clickhouse_driver`，复用仓库现有的 Python 环境。它不修改 `configurations/`、
不读写 `datasets/`，可以随时重复运行。

---

## 9. 原生 `.sql` 测试

CK 侧的核心用例同时落成了 ClickHouse 自带的 stateless 测试，位于
`/home/ClickHouse/tests/queries/2_vector/`：

| 文件 | 覆盖用例 | 内容 |
| --- | --- | --- |
| `0003_vector_query_plan_cache_correctness_test.sql` | `C01`–`C06`、`C08`–`C11` | 单模板稳定（L2 与 cosine）、字面量形式、常量替换、模式一致性、`LIMIT` 参与缓存键、子查询不缓存、默认容量不淘汰、距离函数参与缓存键 |
| `0004_vector_query_plan_cache_invalidation_test.sql` | `I01`、`I05`、`I07`、`I08`、`I09a`、`I10`、`I14` | `ALTER`/`DROP`+`CREATE`/`TRUNCATE`/`RENAME`/`INSERT` 失效（`I01` 同时覆盖 L2 与 cosine 条目），以及无关表、无关库不越界 |
| `0005_vector_query_plan_cache_index_dimension_test.sql` | `I03`、`I06` | 向量索引删除/重建（L2 与 cosine 两种索引度量）、索引列维度从 8 变成 16 |

### 9.1 与 Python 脚本的分工

`.sql` 测试只断言确定性的 CK 行为，并作为仓库回归测试长期运行；下列用例留在 Python 脚本中：

- `I02`、`I04`、`I09b`：两引擎的已知缺陷（DDL 会话关闭缓存时漏失效），stateless 测试必须通过，
  因此不在 `.sql` 中断言；
- `I11`（TTL 到期）、`I15`（进程重启）、`K01`–`K03`（并发）：需要等待、重启或多线程客户端，
  超出单文件 `.sql` 的能力；
- `I12`（TAG 范围失效）、`I13`（容量淘汰）：CK 解析器不支持 `TAG`，容量淘汰需要改服务端配置；
- `C07` 不再单列：三个 `.sql` 文件的每个用例都断言 `QueryCacheHits = 0`；
- 跨引擎对比与 MS 侧其余用例仍留在 Python 脚本中；MS 的 `.sql` 端口见 9.4。

### 9.2 编写约束

- **`no-parallel` 是必需的**：`VectorQueryPlanCacheEntries` 是服务端全局指标，命中/未命中事件也
  来自全局缓存。同一服务器上只要有其它负载执行带向量字面量的查询，条目数就会被污染。
  开发期曾观察到并发的基准脚本把 `entries` 从 `0` 顶到 `1`；`0003`–`0005` 在独占实例上连续
  运行 5 次输出逐字节一致。
- **`no-fasttest`**：向量检索依赖 `usearch` 第三方库。
- **同一缓存键的两次执行必须使用相同的 `FORMAT` 和相同的 `log_comment`**：`FORMAT` 子句进入
  归一化 SQL，`log_comment` 属于参与缓存键的 `changed settings`。
- **断言查询使用 `<case>_assert` 注释**：开启缓存时任何 `SELECT` 都计一次 plan-cache miss，
  断言读 `system.metrics`/`system.query_log` 若沿用用例注释会污染命中/未命中计数。
- **运行边界用表内时间戳**：开启缓存时 `system.query_log.query` 保存的是归一化模板，不能用
  标记查询的时间来界定本次运行。

### 9.3 运行方式

```bash
# 在 ClickHouse 仓库根目录，指向完整的 tests/queries 根目录；位置参数匹配测试文件名
CLICKHOUSE_PORT_TCP=9000 CLICKHOUSE_PORT_HTTP=8123 \
    ./tests/clickhouse-test -q tests/queries \
    -b build/programs/clickhouse --configserver /etc/clickhouse-server/config.xml \
    "0003_vector_query_plan_cache_correctness" \
    "0004_vector_query_plan_cache_invalidation" \
    "0005_vector_query_plan_cache_index_dimension"
```

`-q` 必须是 `tests/queries` 根目录：脚本把该目录下的每一项当作 `NN_suite` 套件目录，
直接指向 `2_vector` 会把每个 `.sql` 文件误当作套件名，结果报 `No tests were run.`。

### 9.4 MS 端口

同样三个用例集落成 MS 原生 `.sql` 测试，位于 `/home/clickhouse/tests/queries/2_vector_search/`：

| 文件 | 覆盖用例 | 内容 |
| --- | --- | --- |
| `00055_mqvs_query_plan_cache_correctness_test.sql` | `C01`–`C06`、`C08`–`C10` | 单模板稳定、字面量形式、常量替换、模式一致性、`LIMIT` 参与缓存键、子查询不缓存、默认容量不淘汰；`C10` 用 cosine 度量表覆盖余弦 |
| `00056_mqvs_query_plan_cache_invalidation_test.sql` | `I01`、`I05`、`I07`、`I08`、`I09a`、`I10`、`I14` | `ALTER`/`DROP`+`CREATE`/`TRUNCATE`/`RENAME`/`INSERT` 失效，以及无关表、无关库不越界 |
| `00057_mqvs_query_plan_cache_index_dimension_test.sql` | `I03`、`I06` | 向量索引删除/重建、索引列维度从 8 变成 16 |

与 CK 版本相比的适配：

- **只参数化投影中的裸 `distance(...)`**：`round(distance(...), 4)`、带 `WHERE` 的查询、子查询
  在 MS 上不产生缓存事件，因此用例只投影原始距离值；`C08`（子查询）只断言“不缓存”，
  不断言命中数。
- **余弦覆盖方式不同**：MS 不识别 `cosineDistance`，`C10` 改用
  `float_vector_search_metric_type = 'Cosine'` 的表，距离函数名不进入缓存键，CK 的 `C11`
  没有 MS 对应项。
- **`I01` 用两个 `LIMIT` 条目代替 L2/cosine 两个条目**，验证一次 `ALTER` 同时清除同一模板的
  多个 `LIMIT` 计划。
- **没有 `QueryPlanCacheEntries` 指标**：条目存在性由“同一模板先 miss、后续 hit”的序列推断。
- **`I03` 的顺序受 `DROP CONSTRAINT` 限制**：删索引 → `TRUNCATE` → 删约束 → 删列 → 加 16 维列
  → 缓存关闭的会话写入数据 → 加约束 → 加索引 → `SYSTEM WAIT BUILDING VECTOR INDICES`。
- **断言查询在 `enable_query_plan_cache = 0` 下执行**：真正开启缓存时聚合读取
  `system.query_log` 会抛 `Code: 48. DB::Exception: Clone is not implemented`；
  断言使用独立的 `<case>_assert` 注释，不污染用例计数。
- **保留 `no-parallel`**：计划缓存是服务端全局对象，其它测试的 `SYSTEM DROP QUERY PLAN CACHE`
  或大量条目写入会改变命中序列；断言虽按 `log_comment` 与 `current_database` 过滤，精确的
  hits/misses 仍要求测试期间没有别的缓存清理。

实测（`24.8.8.1` @ `127.0.0.1:19000`）各用例计数：

| 用例 | hits | misses | result cache hits |
| --- | --- | --- | --- |
| `C01` / `C10` | 99 | 1 | 0 |
| `C02` / `C04` | 20 / 10 | 1 | 0 |
| `C03` | 3 | 3 | 0 |
| `C05`（含 AST 1/1） | 2 | 2 | 0 |
| `C06` | 2 | 2 | 0 |
| `C08` | 0 | 0 | 0 |
| `C09`（20 个 `LIMIT` 后回访 2 次） | 2 | 20 | 0 |
| `I01`（首次 / `ALTER` 后） | 0 / 1 | 2 / 4 | 0 |
| `I06`（预热 / 删索引后 / 建索引后） | 0 / 1 / 2 | 2 / 4 / 5 | 0 |
| `I03`（8 维 / 重建后） | 0 | 1 / 2 | 0 |
| `I05` / `I07` / `I08` / `I09a` | 0 | 2 | 0 |
| `I10` / `I14` | 1 | 1 | 0 |

运行方式（MS 仓库根目录）：

```bash
cd /home/clickhouse
CLICKHOUSE_PORT_TCP=19000 CLICKHOUSE_PORT_HTTP=18123 \
    ./tests/clickhouse-test -q tests/queries \
    -b build/programs/clickhouse \
    --no-random-settings --no-random-merge-tree-settings --no-left-queries-check \
    00055_mqvs_query_plan_cache_correctness_test \
    00056_mqvs_query_plan_cache_invalidation_test \
    00057_mqvs_query_plan_cache_index_dimension_test
```

位置参数是匹配文件名的正则，只写 basename；写成 `2_vector_search/00055_...` 不会匹配到任何
用例。`CLICKHOUSE_PORT_TCP`/`CLICKHOUSE_PORT_HTTP` 必须在启动脚本前导出：脚本的连接检查走
HTTP 端口，`--client-option port=...` 会被当成服务器设置并报 `Unknown setting 'port'`。
三个测试在独立的随机数据库中各建各的表，输出与 `.reference` 逐字节一致，连续运行结果稳定。
