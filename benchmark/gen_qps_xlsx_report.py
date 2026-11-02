# -*- coding: utf-8 -*-
"""QPS 缓存状态对比图（数据来自结果 XLSX）：

数据源（默认在 /home 下查找，可用 --xlsx-dir 指定）：
    clickhouse-benchmark-results.xlsx
    myscale-benchmark-results.xlsx
    pgvector_benchmark_results.xlsx

口径：8 并发、4 种 sql_type、5 个维度、3(CK/MS)/4(pg) 种查询计划缓存状态。
  - CK/MS 取不使用结果缓存的三档：
        ck_off / ck_only_plan_cache / ck_plan_cache
        off / only_plan_cache / plan_cache
    4 种 sql_type 为 normal / cast / cast_array / raw_bytes（不画 raw_bytes_x）。
  - pgvector 取 use_query_cache=0（不使用结果缓存）的 4 档：
        use_cache 0/1/2/3 = raw / prepare_auto / prepare_custom / prepare_generic
    4 种 sql_type 为 text_literal / array_int_cast / array_real_cast / halfvec_literal；
    按召回率分两组出图：mean_precisions=1.0 为 1 向量用例，!=1.0 为 1000 向量用例，
    同一组合按 3 种 vector_in_parse 取平均。
  - 所有图都以“不使用查询计划缓存”为基准：基准柱内标 QPS，其它状态柱顶标
    相对基准的倍数（保留 2 位小数）。
  - X 轴按 sql_type 分组；组内按维度(128→1536)分簇；簇内并排各缓存状态柱。

输出：
    ck_qps_c8_cache_compare.png
    myscale_qps_c8_cache_compare.png
    pg_qps_c8_cache_compare_n1.png        # 1 向量用例（召回率=1）
    pg_qps_c8_cache_compare_n1000.png     # 1000 向量用例（召回率!=1）
"""
import os
import sys
import zipfile
import xml.etree.ElementTree as ET

from gen_qps_report import _annotate, _plt

BASE = os.path.dirname(os.path.abspath(__file__))
CHART_DIR = os.path.join(BASE, 'results', 'charts')

# ---------- 固定：源文件名 / sql_type / 缓存状态 ----------
CK_SRC = 'clickhouse-benchmark-results.xlsx'
MS_SRC = 'myscale-benchmark-results.xlsx'
PG_SRC = 'pgvector_benchmark_results.xlsx'

CKMS_TYPES = ['normal', 'cast', 'cast_array', 'raw_bytes']   # 不画第 5 种 raw_bytes_x
PG_TYPES = ['text_literal', 'array_int_cast', 'array_real_cast', 'halfvec_literal']

# profile -> (图例名, 颜色)；第一个为基准（不使用查询计划缓存）
CK_STATES = {'ck_off': ('no plan cache', '#9aa5b1'),
             'ck_only_plan_cache': ('plan-only cache', '#8fb3e8'),
             'ck_plan_cache': ('plan cache', '#2c5fc4')}
MS_STATES = {'off': ('no plan cache', '#9aa5b1'),
             'only_plan_cache': ('plan-only cache', '#8fb3e8'),
             'plan_cache': ('plan cache', '#2c5fc4')}
# use_cache 数值 -> (图例名, 颜色)；PG_CACHE_MODES 顺序 raw→auto→generic→custom
PG_STATES = {0: ('no plan cache (raw)', '#9aa5b1'),
             1: ('auto (plan cache)', '#8fb3e8'),
             3: ('generic (plan cache)', '#2c5fc4'),
             2: ('custom (plan cache)', '#b07aa1')}
PG_MODES = [0, 1, 3, 2]   # 绘图顺序：基准在前，其余按 gen_qps_report 的 PG_CACHE_MODES
PG_CONC = '8'             # 只取 8 并发
PG_DUR = '60'             # 只取 60 秒测量

# pgvector 实验名 -> 向量维度（xlsx 没有 vector_size 列，用实验名里的数据集信息）
PG_EXP_DIM = [('sift-128', 128), ('nytimes-256', 256), ('laion-768', 768),
              ('gist-960', 960), ('dbpedia-openai', 1536)]

# ---------- 最小 XLSX 读取（仅标准库，容器内无需 openpyxl/pandas） ----------
_NS = {'m': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
_R_ID = '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id'


def _col_idx(ref):
    """单元格列号字母(A/AA)转 0 起的下标。"""
    n = 0
    for ch in ref:
        if ch.isalpha():
            n = n * 26 + ord(ch.upper()) - 64
    return n - 1


def _cell_value(c, shared):
    """取单元格值：共享字符串 / 内联字符串 / 数字或布尔。"""
    t = c.get('t')
    v = c.find('m:v', _NS)
    if t == 's':
        return shared[int(v.text)] if v is not None else ''
    if t == 'inlineStr':
        return ''.join(x.text or '' for x in c.iter('{%s}t' % _NS['m']))
    return v.text if v is not None else ''


def read_xlsx(path, sheet_name):
    """读 XLSX 某张工作表为 [dict]；第一行当表头，缺失单元格补空串。"""
    z = zipfile.ZipFile(path)
    shared = []
    if 'xl/sharedStrings.xml' in z.namelist():
        root = ET.fromstring(z.read('xl/sharedStrings.xml'))
        for si in root.findall('m:si', _NS):
            shared.append(''.join(t.text or '' for t in si.iter('{%s}t' % _NS['m'])))
    wb = ET.fromstring(z.read('xl/workbook.xml'))
    rels = ET.fromstring(z.read('xl/_rels/workbook.xml.rels'))
    rid_map = {r.get('Id'): r.get('Target') for r in rels}
    target = None
    for s in wb.find('m:sheets', _NS):
        name = s.get('name') or ''
        if sheet_name in name:
            tgt = rid_map[s.get(_R_ID)].lstrip('/')
            target = tgt if tgt.startswith('xl/') else 'xl/' + tgt
            break
    if not target:
        raise SystemExit('[ERR] %s 中找不到工作表 %s' % (path, sheet_name))
    root = ET.fromstring(z.read(target))
    raw = []
    for row in root.iter('{%s}row' % _NS['m']):
        cells = {}
        for c in row.findall('m:c', _NS):
            cells[_col_idx(c.get('r'))] = _cell_value(c, shared)
        if cells:
            raw.append(cells)
    if not raw:
        return []
    width = max(raw[0]) + 1
    header = [raw[0].get(i) for i in range(width)]
    out = []
    for cells in raw[1:]:
        vals = [cells.get(i) for i in range(width)]
        if any(v not in (None, '') for v in vals):
            out.append(dict(zip(header, vals)))
    return out


def find_xlsx(name, xlsx_dir=None):
    """按 --xlsx-dir / XLSX_DIR / /home / 项目目录 / 工作区根 的顺序查找源文件。"""
    cands = [xlsx_dir, os.environ.get('XLSX_DIR'), '/home', os.path.dirname(BASE),
             BASE, os.getcwd(), '/fog/xxb/workspace']
    for d in cands:
        if not d:
            continue
        p = os.path.join(d, name)
        if os.path.exists(p):
            return p
    raise SystemExit('[ERR] 找不到 %s；可用 --xlsx-dir 指定所在目录' % name)


def _mean(vals):
    """同组合多行/多解析模式取算术平均。"""
    return sum(vals) / len(vals) if vals else 0.0


def load_ckms(path, states):
    """读 CK/MS xlsx：返回 {(维度, profile, sql_type): qps}。"""
    rows = read_xlsx(path, 'benchmark-results')
    recs = {}
    dims = set()
    for r in rows:
        p, st = r.get('settings_profile'), r.get('sql_type')
        if p not in states or st not in CKMS_TYPES:
            continue
        try:
            d = int(float(r.get('维度')))
            q = float(r.get('qps_avg'))
        except (TypeError, ValueError):
            continue
        if q <= 0:
            continue
        dims.add(d)
        recs.setdefault((d, p, st), []).append(q)
    return {k: _mean(v) for k, v in recs.items()}, sorted(dims)


def _pg_dim(exp):
    """由 pgvector 实验名取向量维度。"""
    for key, dim in PG_EXP_DIM:
        if key in exp:
            return dim
    return None


def load_pg(path, recall1):
    """读 pgvector xlsx：use_query_cache=0，按召回率分组，每组按 3 种 parse 平均。

    recall1=True  -> 召回率=1 的 1 向量用例；
    recall1=False -> 召回率!=1 的 1000 向量用例。"""
    rows = read_xlsx(path, 'pgvector_benchmark_results')
    recs = {}
    dims = set()
    for r in rows:
        if r.get('parallel') != PG_CONC or r.get('test_duration') != PG_DUR:
            continue
        if r.get('use_query_cache') != '0':
            continue
        st, uc = r.get('sql_type'), r.get('use_cache')
        if st not in PG_TYPES or uc not in ('0', '1', '2', '3'):
            continue
        try:
            is1 = abs(float(r.get('mean_precisions')) - 1.0) <= 1e-9
            q = float(r.get('rps'))
        except (TypeError, ValueError):
            continue
        if is1 != recall1 or q <= 0:
            continue
        d = _pg_dim(r.get('experiment_name') or '')
        if d is None:
            continue
        dims.add(d)
        recs.setdefault((d, int(uc), st), []).append(q)
    return {k: _mean(v) for k, v in recs.items()}, sorted(dims)


CLUSTER_W = 1.0     # 每个维度簇占的横向宽度（组宽单位）
GROUP_GAP = 0.6     # 相邻 sql_type 组之间的间隔


def draw(recs, dims, modes, types, base, title, fname, color, label, out_dir):
    """sql_type 组 × 维度簇 × 缓存状态柱：基准柱标 QPS，其余柱顶标倍数。"""
    plt = _plt()
    from matplotlib.transforms import blended_transform_factory
    n, nd = len(modes), len(dims)
    group_w = nd * CLUSTER_W + GROUP_GAP
    span = len(types) * group_w - GROUP_GAP
    bw = CLUSTER_W * 0.86 / n
    fig, ax = plt.subplots(figsize=(max(10.0, span * 0.82), 6.4), dpi=200)
    tr = blended_transform_factory(ax.transData, ax.transAxes)
    bars, vals, bvals, maxv = [], [], [], 0.0
    for g, st in enumerate(types):
        for j, d in enumerate(dims):
            cx = g * group_w + (j + 0.5) * CLUSTER_W
            if j % 2 == 0:
                ax.axvspan(cx - CLUSTER_W / 2.0, cx + CLUSTER_W / 2.0,
                           color='#eef3fa', zorder=0, lw=0)
            ax.text(cx, -0.012, str(d), transform=tr, ha='center', va='top',
                    fontsize=7.5, color='#5b6673')
            for i, m in enumerate(modes):
                v = recs.get((d, m, st), 0.0)
                bar = ax.bar(cx + (i - (n - 1) / 2.0) * bw, v, bw * 0.92,
                             color=color(m), zorder=3,
                             label=(label(m) if (g == 0 and j == 0) else None))
                bars.append(bar[0])
                vals.append(v)
                bvals.append(None if i == 0 else recs.get((d, base, st), 0.0))
                maxv = max(maxv, v)
    top = maxv * 1.2
    ax.set_ylim(0, top * 1.10)
    _annotate(ax, bars, vals, bvals, top, qps_size=4.2, mult_size=5.8)
    ax.set_xlim(-0.2, span + 0.2)
    ax.set_xticks([g * group_w + nd * CLUSTER_W / 2.0 for g in range(len(types))])
    ax.set_xticklabels(types, fontsize=11)
    ax.tick_params(axis='x', pad=18)
    ax.set_ylabel('QPS (queries/sec)', fontsize=10)
    ax.set_title(title, fontsize=12)
    ax.grid(axis='y', color='#dfe6f0', lw=0.8, zorder=0)
    for s in ('top', 'right'):
        ax.spines[s].set_visible(False)
    ax.legend(fontsize=8, ncol=n, frameon=False, loc='upper right', reverse=True,
              columnspacing=1.0, handlelength=1.2, handletextpad=0.5)
    fig.tight_layout()
    fig.savefig(os.path.join(out_dir, fname), bbox_inches='tight')
    plt.close(fig)
    return fname


def main():
    import argparse
    ap = argparse.ArgumentParser(description='按结果 XLSX 生成 8 并发 QPS 缓存状态对比图')
    ap.add_argument('--engine', choices=['all', 'clickhouse', 'myscale', 'pgvector'],
                    default='all', help='只画某一类（默认 all）')
    ap.add_argument('--xlsx-dir', default='', help='三个结果 XLSX 所在目录')
    ap.add_argument('-o', '--out', default=CHART_DIR, help='输出目录（默认 results/charts）')
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)
    made = []

    if args.engine in ('all', 'clickhouse'):
        recs, dims = load_ckms(find_xlsx(CK_SRC, args.xlsx_dir), CK_STATES)
        if not recs:
            raise SystemExit('[ERR] ClickHouse XLSX 中没有目标口径数据')
        made.append(draw(
            recs, dims, list(CK_STATES), CKMS_TYPES, 'ck_off',
            'ClickHouse · 8 concurrency · 4 sql_types · 5 dimensions · QPS vs cache state'
            ' (baseline=no plan cache)',
            'ck_qps_c8_cache_compare.png',
            lambda m: CK_STATES[m][1], lambda m: CK_STATES[m][0], args.out))

    if args.engine in ('all', 'myscale'):
        recs, dims = load_ckms(find_xlsx(MS_SRC, args.xlsx_dir), MS_STATES)
        if not recs:
            raise SystemExit('[ERR] MyScale XLSX 中没有目标口径数据')
        made.append(draw(
            recs, dims, list(MS_STATES), CKMS_TYPES, 'off',
            'MyScale · 8 concurrency · 4 sql_types · 5 dimensions · QPS vs cache state'
            ' (baseline=no plan cache)',
            'myscale_qps_c8_cache_compare.png',
            lambda m: MS_STATES[m][1], lambda m: MS_STATES[m][0], args.out))

    if args.engine in ('all', 'pgvector'):
        pg_path = find_xlsx(PG_SRC, args.xlsx_dir)
        # 召回率=1 -> 1 向量用例；召回率!=1 -> 1000 向量用例，各出一张图
        for recall1, fname, case in (
                (True, 'pg_qps_c8_cache_compare_n1.png', '1 SQL case (recall=1)'),
                (False, 'pg_qps_c8_cache_compare_n1000.png', '1000 SQL case (recall!=1)')):
            recs, dims = load_pg(pg_path, recall1)
            if not recs:
                raise SystemExit('[ERR] pgvector XLSX 中没有 %s 的目标口径数据' % case)
            made.append(draw(
                recs, dims, PG_MODES, PG_TYPES, 0,
                'pgvector · 8 concurrency · %s · 4 sql_types · 5 dimensions · QPS vs cache state'
                ' (baseline=no plan cache)' % case,
                fname,
                lambda m: PG_STATES[m][1], lambda m: PG_STATES[m][0], args.out))

    print('charts saved to', args.out)
    for f in made:
        print('  ', f)


if __name__ == '__main__':
    main()
