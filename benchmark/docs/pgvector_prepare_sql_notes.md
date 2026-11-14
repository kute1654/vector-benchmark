# pgvector PREPARE 写法笔记

记录 pgvector 各 SQL 类型在 **PREPARE / EXECUTE** 形态下的写法、代价归属与踩过的坑，
供后续修改 `bash-test/pgvector-benchmark.sh`、`engine/clients/pgvector/search.py` 或解读
`pgvector_log_analysis-*.csv` 时查阅。

- 相关脚本：`bash-test/pgvector-benchmark.sh`、`engine/clients/pgvector/search.py`、
  `analyze_pgvector_log.py`
- 相关产物：`results/csv/pgvector_log_analysis-<dataset>.csv`、`results/charts/*_qps_*.png`

---

## 1. 为什么 PREPARE 写法会影响测量

阶段耗时口径把单查询划分为

```
pgvector：parse + analyze + plan + exec ，命中后另有 eval_params + get_cached_plan
```

脚本统计 `vi_ms`（`vector_in` / `array_to_vector` 的耗时）**单列输出，不计入上面任何一段**。
因此"查询向量从文本/数组转成 vector"这笔成本必须落在某个**已被计时的阶段**里，否则
`parse + analyze + plan + exec + eval + gcp` 加总会小于实测总耗时，表对不上。

这笔成本落在哪一段，完全由 PREPARE/EXECUTE 的写法决定：

| 写法 | 转换发生的位置 | 计入列 |
|---|---|---|
| 参数声明 `vector`，正文 `$1` | Bind（`ExecuteQuery → EvaluateParams`） | `eval_ms` |
| 参数声明 `text`/`real[]`，正文 `$1::vector` | 语句体内：custom 计划在规划期折叠；generic 计划在执行期 | `gcp_ms` / `exec_ms` |
| 参数声明 `text`/`real[]`，正文 `(SELECT $1::vector)` | 语句体内，InitPlan 每次查询求值一次 | `exec_ms` |

---

## 2. 三种历史写法

### 2.1 旧版：全类型统一 `(vector)`（2026-10-30 及以前）

```sql
PREPARE p(vector) AS
SELECT t.id, (t.vector <=> $1) AS dis FROM t ORDER BY dis ASC LIMIT 10;
EXECUTE p('[0.01,0.02,...]');
```

- 文本 `'[...]'` 在 Bind 阶段由 `vector_in` 解析一次，进入计划的 `$1` 已是 vector。
- 四种写法在 prepared 档**完全等价**：768 维 n=1 实测 `exec` = 0.586 / 0.601 / 0.577 / 0.578，
  `vi_ms` 全为 0.038，`vi#/exec#` = 1.00。
- 缺点：SQL 写法差异在计划缓存档无法体现，H2 只能靠 raw 档观察。

### 2.2 中间版：按 sql_type 声明 `text` / `real[]`（2026-11-12）

```sql
PREPARE p(text) AS SELECT t.id, (t.vector <=> $1::vector) AS dis FROM t ORDER BY dis ASC LIMIT 10;
EXECUTE p('[0.01,0.02,...]');

PREPARE p(real[]) AS SELECT t.id, (t.vector <=> CAST($1 AS vector)) AS dis FROM t ORDER BY dis ASC LIMIT 10;
EXECUTE p('{0.01,0.02,...}'::real[]);
```

- 保留了写法差异，但埋了一个坑：**generic 计划下 `$1::vector` 会按输出行重复求值**。
  `LIMIT 10` → 每条查询 11 次转换，`vi#/exec#` 实测 **10.95（auto）/ 11.00（generic）**，raw/custom 为 1.00。
- 文本类吃亏最大：768 维 n=1 的 `text_literal`，`exec` 从 0.681（custom）涨到 0.973/0.969（auto/generic），
  多出的 0.29 ms ≈ 11 × 0.032 ms（单次文本解析）。数组类单次仅 0.001–0.003 ms，累计 0.01–0.03 ms，被噪声掩盖。
- 原因链：auto 档每连接先走 5 次 custom（各 1 次转换）再转 generic，故均值 10.95 略低于 11.00；
  差值 0.05 恰为 `5 次 × 连接数 × 10 / 总语句数`，可反推验证。

### 2.3 过渡版：全数组族 `numeric[]` + 体内包装（2026-11-13 上午，已被第 4 节取代）

```sql
-- 文本族：参数 vector，正文无转换，文本在 Bind 解析一次
PREPARE p(vector) AS
SELECT t.id, (t.vector <=> $1) AS dis FROM t ORDER BY dis ASC LIMIT 10;
EXECUTE p('[0.01,0.02,...]');

-- 数组族（过渡版统一用 numeric[]）：转换在语句体内，包一层保证每条语句只求值一次
PREPARE p(numeric[]) AS
SELECT t.id, (t.vector <=> (SELECT $1::vector)) AS dis FROM t ORDER BY dis ASC LIMIT 10;
EXECUTE p('{0.01,0.02,...}'::numeric[]);

-- array_real_cast（保留 numeric[] -> real[] 这一步）
PREPARE p(numeric[]) AS
SELECT t.id, (t.vector <=> (SELECT $1::real[]::vector)) AS dis FROM t ORDER BY dis ASC LIMIT 10;
EXECUTE p('{0.01,0.02,...}'::numeric[]);

-- with_* 族：CTE 保持可内联，包装放在 CTE 内部
PREPARE p(numeric[]) AS
WITH kv AS (SELECT (SELECT $1::real[]::vector) AS v)
SELECT t.id, (t.vector <=> kv.v) AS dis FROM t CROSS JOIN kv ORDER BY dis ASC LIMIT 10;
EXECUTE p('{0.01,0.02,...}'::numeric[]);
```

- 文本族转换计入 `eval_ms`，数组族计入 `exec_ms`，两笔都在已计时列里，阶段加总能对上实测总耗时。
- 过渡版让 `array_int_cast` 的 eval 落到 0.149（≈raw 0.153），但 `array_real_cast` / `with_array_cast`
  的 eval 也被压到 0.15，与 raw 的 0.45 相差 3 倍，因此最终只对 `array_int_cast` 保留这条写法。
- 最终三条路线的类型归属见第 4 节与 `pgvector-benchmark.sh::render_prepare_param_type`。

---

## 3. 关键机制（改写法前必读）

### 3.1 没有 `text → vector` 的隐式转换

`pgvector/sql/vector.sql` 注册的转换只有：

```sql
CREATE CAST (vector AS vector)     AS IMPLICIT;
CREATE CAST (vector AS real[])     AS IMPLICIT;
CREATE CAST (integer[] AS vector)  AS ASSIGNMENT;
CREATE CAST (real[] AS vector)     AS ASSIGNMENT;
CREATE CAST (numeric[] AS vector)  AS ASSIGNMENT;
```

**没有 `text AS vector`**。所以下面这种写法会在 `PREPARE` 阶段直接报错
`operator does not exist: vector <=> text`：

```sql
PREPARE p(text) AS SELECT t.id, (t.vector <=> $1) AS dis FROM t ...;   -- 不可用
```

`'[...]'` 能直接用是因为它是"未知类型字面量"，按目标类型走输入函数；一旦参数被声明成
`text`，就必须显式写 `$1::vector`（显式 I/O 转换允许）。

### 3.2 `real[] → vector` 是 ASSIGNMENT，可用于 EXECUTE 实参

`EXECUTE` 的实参按 `COERCION_ASSIGNMENT` 转成参数类型（`postgres/src/backend/commands/prepare.c`），
而 `real[] → vector` 正是 ASSIGNMENT，所以

```sql
PREPARE p(vector) AS ... $1 ...;   EXECUTE p('{...}'::real[]);
```

是合法的（Bind 阶段做一次 `array_to_vector`，计入 `eval_ms`）。

### 3.3 generic 计划下，语句体内的转换会逐行求值

- custom 计划：规划期已知参数值 → `$1::vector` 被折叠成 Const，转换只发生 1 次（记 `gcp_ms`）。
- generic 计划：参数未知 → `CoerceViaIO` 保留，出现在目标列表/ORDER BY 中时**每输出一行求值一次**。
- `LIMIT 10` → 11 次；`vi#/exec#` 是最快的自查指标，正常应 **= 1.00**。

### 3.4 包装成 `(SELECT $1::vector)` 可保证"每条语句一次"

不相关标量子查询会被规划为 InitPlan：查询开始时算一次，之后每行只读结果参数。
同时 `preprocess_expression` 会把子查询转成 Param，`ORDER BY t.vector <=> $0` 仍满足
"列 op 常量"，pgvector 的 HNSW 有序扫描可以命中。

### 3.5 不要用 `MATERIALIZED` CTE

物化后 `kv.v` 是 CTE 扫描输出的普通列，`ORDER BY (vector <=> kv.v)` 不再满足"列 op 常量"，
HNSW 有序扫描失效 → Seq Scan + Sort。实测 1536 维、100 万行时 `exec = 4341 ms`（约 4 秒），
日志同时显示 `array_to_vector` 只调用 1 次（0.009 ms），可据此与"重复解析"区分。

正确做法：CTE 不加 `MATERIALIZED`（单次引用会被内联），把包装放进 CTE 内部。

---

## 4. 当前规则（2026-11-13 定稿：按 raw analyze 量级分三条路线）

> 目标：prepared 档的 `eval_ms` 尽量贴近 raw 档同 sql_type 的 `analyze_ms`。
> 按 raw 实测把 sql_type 分成三组，每组用各自的"参数类型 + 语句体 + EXECUTE 实参"组合。

| 路线 | sql_type | raw analyze（1536 维） | PREPARE 参数 | 语句体 | EXECUTE 实参 | 转换落点 / 目标 eval |
|---|---|---|---|---|---|---|
| 1 文本族 | text_literal / text_cast / vector_fn / halfvec_literal / with_text_literal / with_vector_fn | ≈0.09–0.15（文本解析） | `vector` | `<=> $1`；with 族 `WITH kv AS (SELECT $1 AS v)` | `'[...]'::vector` / `CAST('[..]' AS vector)` / `vector('[..]')` / `'[...]'::halfvec(dim)::vector` | Bind 的 `vector_in`，eval ≈0.09–0.15 |
| 2 低 analyze 数组 | array_int_cast | ≈0.15 | `numeric[]` | `<=> (SELECT $1::vector)` | `'{...}'::numeric[]` | Bind 只解析 `numeric[]`（eval≈0.15）；`numeric[]→vector` 在体内由 InitPlan 求值一次，计入 `exec` |
| 3 高 analyze 数组 | array_real_cast / with_array_cast（及 array_cast / array_to_vector_fn） | ≈0.45 | `vector` | `<=> $1`；with 族 `WITH kv AS (SELECT $1 AS v)` | `'{...}'::numeric[]::real[]::vector`（array_cast / array_to_vector_fn 保留各自形态） | 整条 cast 链在 Bind 完成，eval ≈0.29 |

成本归属：

- 路线 1/3：字面量解析与 cast 链都在 Bind（`eval_ms`），正文只有 `$1`，generic 计划也不会逐行重复转换。
- 路线 2：Bind 只做 `array_in(numeric[])`，转换留在语句体内，**必须用 `(SELECT ...)` 包一层**（InitPlan）
  保证每条语句只求值一次；否则 generic 计划会按输出行重复求值（`vi#/exec#` ≈ 11）。
- `with_*` 保留 CTE 外形，但绝不可 `MATERIALIZED`（会使 HNSW 有序扫描失效，见 3.5）。

为什么不统一成一条路线：

- 全部走路线 3（参数 `vector` + 完整 cast 链）：`array_int_cast` 的实参变成 `'{...}'::numeric[]::vector`，
  实测 `eval = 0.311`，约为其 raw analyze（0.158）的两倍；
- 全部走路线 2（参数 `numeric[]` + 体内转换）：`array_real_cast` / `with_array_cast` 的 `eval` 只有约 0.15
  （与 int 类相同），而 raw analyze 是 0.45，eval 与 analyze 差 3 倍；且 `exec` 多出约 0.13 ms 的
  `array_to_vector`，单查询总耗时反而更高；
- 全部走路线 1（参数 `vector` + `ARRAY[...]` 实参）：`ARRAY[...]` 在 `EvaluateParams` 里被折叠+求值两次，
  `eval ≈ 2 × raw analyze`（见下方告警）。

> 不要用 `ARRAY[...]` 形式做实参：数组表达式会在 `ExecPrepareExpr` 折叠一次、`ExecEvalExpr` 再求值
> 一次，实测 `eval ≈ 2 × raw analyze`（1536 维：int 0.378 vs 0.156、real 0.938 vs 0.479）。

### SQL 文件复用校验

`pgvector-benchmark.sh` 复用已有 `pgv_prep_*.sql` 前会校验，不匹配即重建：

1. `PREPARE` 行参数类型必须等于该 sql_type 的路线值（`array_int_cast` = `numeric[]`，其余 = `vector`）；
2. `array_int_cast`：正文必须含 `(SELECT $1::vector)`，`EXECUTE` 实参必须以 `::numeric[]);` 结尾；
3. 其余类型：正文不得出现 `(SELECT `；`EXECUTE` 实参不得是 `ARRAY[...]` 形态。

---

## 5. 实测对照

### 5.1 1536 维、n=1（raw 与 prepared 的阶段对照）

| sql_type | cache | analyze | plan | exec | eval | gcp | vi_ms |
|---|---|---|---|---|---|---|---|
| array_int_cast | 0 | 0.158 | 0.248 | 1.568 | – | – | 0.122 |
| array_real_cast | 0 | **0.482** | 0.753 | 1.743 | – | – | 0.002 |
| with_array_cast | 0 | 0.457 | 1.949 | 1.970 | – | – | 0.003 |
| array_int_cast | 1/2/3 | 0 | 0 | 1.56–1.63 | **0.156** | 0.005–0.036 | 0.003 |
| array_real_cast | 1/2/3 | 0 | 0 | 1.56–1.64 | **0.157** | 0.005–0.037 | 0.003 |
| with_array_cast | 1/2/3 | 0 | 0 | 1.57–1.63 | **0.157** | 0.005–0.042 | 0.003 |

raw 档 `array_real_cast` 的 0.482 ms 来自三步：`ARRAY[...]`（numeric[]）→ `::real[]`（逐元素
numeric→float4，最贵）→ `::vector`。prepared 档参数类型是 `real[]`，`array_in` 在 Bind 阶段
直接按 float4 解析，前两步被"提前做完"，体内只剩 `$1::vector`，于是四类数组写法塌成同一条路线。

> 注：上表的 prepared 行（`eval ≈ 0.156`）是 **`real[]` 参数时期**的实测值，已被 11-13 最终方案取代，
> 仅用于说明"参数类型会把两族写法塌成一条路线"这一现象。当前三条路线的实测见 5.3。

### 5.2 custom 档的 gcp 为什么偏高（1536 维、n=1000）

| sql_type | gcp | vi_ms |
|---|---|---|
| text_literal | 0.1404 | 0.085 |
| array_int_cast | 0.0510 | 0.003 |
| array_real_cast | 0.0639 | 0.004 |
| with_array_cast | 0.0727 | 0.003 |

`force_custom_plan` 下每条查询都要 `GetCachedPlan`：失效校验 → 带参数值重规划（含一次向量解析）
→ 构建并拷贝计划。文本类多出的约 0.09 ms 正是一次文本解析；数组类本底约 0.05–0.07 ms。
因此 **gcp 不能与 raw 的 `plan` 列直接比较**（后者只含纯规划）。

### 5.3 三条路线定稿后的重跑结果（1536 维、n=1000，2026-11-13）

命令（容器内）：`bash run-benchmark-loop.sh -t "pgvector" -n "1000" --tables "1" --with-analysis`

| sql_type | raw analyze | prepared eval（auto） | eval/analyze | prepared eval（custom） | vi#/exec# |
|---|---|---|---|---|---|
| text_literal | 0.096 | **0.0902** | 94% | 0.0906 | 1.00 |
| array_int_cast | 0.153 | **0.1477** | 97% | 0.1516 | 1.00 |
| array_real_cast | 0.473 | **0.2880** | 61% | 0.2926 | 1.00 |
| with_array_cast | 0.454 | **0.2890** | 64% | 0.2924 | 1.00 |

- 路线 1/2 的 `eval` 与 raw `analyze` 基本重合（94%/97%），符合预期；
- 路线 3 的 `eval` 约为 raw 的 6 成，是三套写法中最接近的一档（另两种写法分别偏高到 2 倍、
  偏低到 1/3，见第 8 节），同量级且排序正确；
- `vi#/exec#` 全为 1.00，`with_array_cast` 的 `exec`（8.847 / 8.955）与普通查询同量级，
  说明没有落入全表扫描或 n+1；
- `vi_ms` 与预期一致：文本 0.083（`vector_in`）、int 0.123（`array_in(numeric[])` 后体内
  `array_to_vector(numeric 元素)`）、real/with 0.002–0.003（`array_to_vector(real 元素)`）。
- 同一轮的 raw 档 `parse_ms`：文本 0.039，数组族 0.148–0.154（`ARRAY[...]` 的解析成本），
  与 5.1 的口径一致。

---

## 6. 自查清单

1. `vi#/exec#` 应 **= 1.00**；10.95/11.00 说明 generic 计划在逐行重复转换。解析脚本已加告警：
   `analyze_pgvector_log.py` 中 `per_q > 1.5` 会打印提示。
2. 各阶段加总是否等于实测：`parse + analyze + plan + exec + eval + gcp`，与 QPS 倒数对比。
   注意脚本的 `est_QPS` 只加前四项，prepared 档会偏乐观。
3. `EXPLAIN (COSTS OFF) EXECUTE p(...)` 必须是 `Index Scan ... ORDER BY (vector <=> ...)`；
   出现 `Seq Scan` + `Sort` 就说明索引有序扫描没命中（多半是 `MATERIALIZED` 或 CTE 未被内联）。
4. 文本类看 `eval_ms` 是否包含一次解析（旧式），数组类看 `exec_ms` 是否只多一次转换。

---

## 7. 已知局限与待决事项

1. **数组族在 prepared 档的写法区分**：已按 raw analyze 分两条路线（2026-11-13 定稿）：
   `array_int_cast` 用 `numeric[]` 参数 + 体内 `(SELECT $1::vector)`；
   `array_real_cast` / `with_array_cast` 用 `vector` 参数 + 实参保留整条 cast 链。见第 4 节。
2. **文本族内部子写法退化**：`text_cast`、`vector_fn`、`with_text_literal`、`with_vector_fn` 在 prepared 档
   正文都变成 `$1`，与 `text_literal` 相同，差异只存在于 raw 档。论文目前只用到 `text_literal`，暂不影响。
3. **Python 客户端口径**：`search.py::_param_type` 仍把四个数组类型统一成 `real[]`；若要与 bash 一致，
   需改为 `array_int_cast → numeric[]`、其余数组类型 `vector`，且 `array_int_cast` 正文加 `(SELECT ...)` 包装。
4. **`est_QPS` 口径**：不含 `eval` / `gcp`，prepared 档偏低；论文作"命中总耗时"时应计入复用开销。

---

## 8. 尝试记录与最终选择（2026-11-13）

本节记录为让 prepared 档 `eval_ms` 贴近 raw 档 `analyze_ms` 所试过的写法、实测结果，
以及最终为什么只保留第 4 节那三条路线。

### 8.1 尝试清单

| # | 写法 | 代表 sql_type | 实测（1536 维） | 结论 |
|---|---|---|---|---|
| A | 参数 `vector`，正文 `$1`，实参 `ARRAY[...]` | 全部数组族 | eval 翻倍：int 0.378 vs raw analyze 0.156；real 0.938 vs 0.479 | 弃用：`ARRAY[]` 被折叠+求值两次 |
| B | 参数 `text` / `real[]`，正文 `$1::vector` | 文本族 / 数组族 | generic 档逐行求值，`vi#/exec#` = 10.95/11.00；1536 文本 exec 0.973（custom 0.681） | 弃用：n+1 次重复解析 |
| C | 参数 `text` / `real[]`，正文 `(SELECT $1::vector)` | 数组族 | `vi#/exec#` = 1.00，但 `real[]` 参数把 `numeric[]→real[]` 吃掉，int/real 两族 eval 都是 0.157 | 弃用：两族不可区分 |
| D | 参数 `text` / `real[]` + `MATERIALIZED` CTE | with_array_cast | exec = 4341 ms（全表扫描；`array_to_vector` 仅 1 次 0.009 ms） | 弃用：HNSW 有序扫描失效 |
| E | 参数 `numeric[]`，正文 `(SELECT $1::vector / $1::real[]::vector)` | 全部数组族 | int 类 eval ≈ 0.149（≈ raw 0.158）；real 类 eval 仍 ≈0.15，而 raw analyze 0.482 | int 类保留（路线 2），real 类弃用 |
| F | 参数 `vector`，实参 `'{...}'::numeric[]::vector` | array_int_cast | eval = 0.311（raw analyze 0.158），其中 `array_to_vector`（numeric 元素）0.127 | 弃用：eval 是 analyze 的 2 倍 |
| G | 参数 `vector`，实参 `'{...}'::numeric[]::real[]::vector` | array_real_cast / with_array_cast | eval = 0.289（raw analyze 0.482），`array_to_vector`（float4 元素）仅 0.003 | 保留（路线 3） |
| H | 参数 `vector`，实参 `'[...]'::vector` | 文本族 | eval ≈ 0.09–0.15，与 raw analyze 同量级 | 保留（路线 1） |

两条关键对照：

- **E vs F（int 类）**：同样一次 `numeric[] → vector`，放进 Bind 的实参（F）要为整个 cast 链
  付 0.311；拆成"Bind 只做 `array_in(numeric[])`"+"体内 InitPlan 做转换"（E）后，eval 降到
  0.149，正好落在 raw analyze 0.158 附近，转换那 0.127 ms 落到 `exec`。这是路线 2 的由来。
- **E vs G（real 类）**：real 类的 raw analyze 是 0.482。若用 E，eval 只有约 0.15，偏离 3 倍；
  用 G 让整条 `numeric[]::real[]::vector` 在 Bind 完成，eval 0.289，是各写法中最接近 0.482 的。
  这是路线 3 的由来。

### 8.2 最终选择与理由

三条路线的定义见第 4 节。取舍依据：

1. 判据是 **`eval_ms` ≈ `analyze_ms`**，而不是单查询总耗时最小。prepared 档的 `parse/plan`
   已被计划缓存消掉，若 `eval` 再明显偏离 raw 的 `analyze`，两档的阶段对比就失去意义。
2. raw analyze 天然分三档：文本族 ≈0.09–0.15、`array_int_cast` ≈0.15、`array_real_cast` /
   `with_array_cast` ≈0.45。固定一种参数类型无法同时贴合三档。
3. 能贴合且不引入副作用的写法各只有一种：文本族用 Bind 的 `vector_in`（路线 1）；
   int 类用 `numeric[]` 参数 + 体内 InitPlan 转换（路线 2）；real/with 类用 Bind 上的完整
   cast 链（路线 3）。
4. 早先"参数统一 `vector`、实参统一 cast 链"的版本虽然最简单、`vi#/exec#` = 1.00，但 int 类
   eval 0.311 ≈ 2×analyze，三档被压成两档，故被本方案取代。
5. 全数组族 `numeric[]` 参数（8.1 的 E）会把 real 类 eval 压到 0.15，与 analyze 0.45 相差 3 倍，
   且 `exec` 多出约 0.13 ms，故最终只保留 int 类使用它。

### 8.3 验证结果（1536 维、n=1000，容器内 `run-benchmark-loop.sh -t pgvector -n 1000 --tables 1 --with-analysis`）

| sql_type | raw analyze | prepared eval（auto / custom） | eval/analyze（auto） | vi#/exec# |
|---|---|---|---|---|
| text_literal | 0.096 | 0.0902 / 0.0906 | 94% | 1.00 |
| array_int_cast | 0.153 | 0.1477 / 0.1516 | 97% | 1.00 |
| array_real_cast | 0.473 | 0.2880 / 0.2926 | 61% | 1.00 |
| with_array_cast | 0.454 | 0.2890 / 0.2924 | 64% | 1.00 |

结论：路线 1/2 达标（eval 与 analyze 基本重合），路线 3 达到本组写法的最接近值（其余写法分别为
2 倍或 1/3）；`vi#/exec#` 全为 1.00，`with_array_cast` 未退化为全表扫描。原始数据见
`results/csv/pgvector_log_analysis-benchmark_1536_1m-1000.csv`，详表见 5.3。

### 8.4 实施与风险

实施（2026-11-13，`bash-test/pgvector-benchmark.sh`）：

- `render_prepare_param_type`：`array_int_cast` → `numeric[]`，其余 → `vector`；
- `render_prepare_body`：仅 `array_int_cast` 用 `(SELECT $1::vector)`，其余正文只有 `$1`；
  `with_*` 保留 CTE 且不加 `MATERIALIZED`；
- `render_execute_arg`：`array_int_cast` → `'{__I__}'::numeric[]`，real/with 类保留整条 cast 链；
- 复用校验按路线判定（见第 4 节末尾）。

风险：

- 路线 1/3 的 `eval` 与 raw `analyze` 仍有 0.02–0.19 ms 的差（路 3 约 0.29 vs 0.48），
  只能保证同量级、同排序，不可当作逐毫秒等值；
- 与 Python 客户端的绑定类型（`search.py::_param_type`）若不同步，两套数据不可混用；
- 若后续 PG 版本改变 `numeric[] → vector` / `numeric[] → real[]` 的实现，数值需要重新基线。

---

## 9. 变更记录

| 时间 | 变更 |
|---|---|
| 2026-10-30 及以前 | 全类型 `(vector)` + `$1` + `'[...]'`；prepared 档四写法等价，`vi#/exec#` = 1.00 |
| 2026-11-12 | 改为按 sql_type 声明 `text` / `real[]`，转换写进语句体；generic 档出现 n+1 次重复解析（`vi#/exec#` ≈ 11），文本类 `exec` 被抬高（768 维 +43%、1536 维 +57%） |
| 2026-11-12 晚 | 改用 `MATERIALIZED` CTE 包住转换；导致 `with_array_cast` 命中不了 HNSW 有序扫描，`exec` 达 4341 ms |
| 2026-11-13 | 定稿：文本族回旧式（`vector` + `$1`），数组族保留体内转换并包成 `(SELECT ...)`；CTE 不加 `MATERIALIZED`；复用校验按族判定；解析脚本新增逐行重复转换告警 |
| 2026-11-13 | 数组族参数由 `real[]` 改为 `numeric[]` 的中间尝试：体内保留各自转换链（`$1::vector` / `$1::real[]::vector`），EXECUTE 传 `'{...}'::numeric[]`。int 类契合（eval≈0.149），real 类 eval 被压到 0.15、偏离 raw 0.482，遂只保留 int 类 |
| 2026-11-13 | 中间版本：参数统一 `vector`、正文只留 `$1`，构造表达式整条搬进 EXECUTE 实参。简单且 `vi#/exec#`=1.00，但 int 类 eval 0.311 ≈ 2×raw analyze，三档被压成两档 |
| 2026-11-13 | **最终定稿（取代以上两条）**：按 raw analyze 分三条路线——文本族 `vector`+`$1`+`'[...]'::vector`；`array_int_cast` `numeric[]`+`(SELECT $1::vector)`+`'{...}'::numeric[]`；`array_real_cast`/`with_array_cast` `vector`+`$1`+`'{...}'::numeric[]::real[]::vector`。目标是 prepared 的 `eval` 分别贴近 raw 的 `analyze`，且 `vi#/exec#` 保持 1.00 |
