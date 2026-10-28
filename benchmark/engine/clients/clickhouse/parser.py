"""ClickHouse 的标量过滤条件解析器。

把 benchmark 内部的过滤 DSL 翻译成 ClickHouse SQL 片段。与向量相关的过滤
不在这里（向量近邻由 search 阶段的 ``ORDER BY distance(...)`` 完成），本文件
只负责 ``WHERE`` 子句里的标量条件。

所有 ``build_*`` 方法返回的都是 SQL **字符串**，由调用方拼进查询。
"""

from typing import Any, List, Optional, Dict

from engine.base_client.parser import BaseConditionParser, FieldValue


class ClickHouseConditionParser(BaseConditionParser):
    """生成 ClickHouse ``WHERE`` 条件字符串的条件解析器。"""

    def build_condition(
            self, and_subfilters: Optional[List[Any]], or_subfilters: Optional[List[Any]]
    ) -> Optional[Any]:
        """组合 and / or 两组子条件。

        两组各自先加括号再以 ``and`` 相连：``or`` 的优先级低于 ``and``，若不
        加括号，``A or B and C`` 会被解析成 ``A or (B and C)``，与配置语义不符。
        两组都为空时返回空串（调用方据此不生成 ``WHERE``）。
        """
        clauses = []
        if or_subfilters is not None and len(or_subfilters) > 0:
            clauses.append("(" + " or ".join(or_subfilters) + ")")
        if and_subfilters is not None and len(and_subfilters) > 0:
            clauses.append("(" + " and ".join(and_subfilters) + ")")
        return " and ".join(clauses)

    def build_exact_match_filter(self, column_name: str, expression_value: FieldValue) -> Any:
        """等值过滤；值按字符串加单引号（数据集里的标量列都是文本常量）。"""
        return f"{column_name} = '{expression_value}'"

    def build_range_filter(
            self,
            column_name: str,
            lt: Optional[FieldValue],
            gt: Optional[FieldValue],
            lte: Optional[FieldValue],
            gte: Optional[FieldValue],
    ) -> Any:
        """区间过滤：四个边界各自独立可选，非空的拼成比较再用 ``and`` 连接。

        先做一致性校验：``lt < gt``（或 ``lte < gte``）说明数据集写反了，
        这样的区间恒为空；直接报错比静默返回零结果更容易定位问题。
        """
        if lt is not None and gt is not None and lt < gt:
            raise RuntimeError(f"dataset condition error! lt:{lt}, gt:{gt}")
        if lte is not None and gte is not None and lte < gte:
            raise RuntimeError(f"dataset condition error! lt:{lte}, gt:{gte}")
        range_filter = []
        if lt is not None:
            range_filter.append(f"{column_name}<{lt}")
        if gt is not None:
            range_filter.append(f"{column_name}>{gt}")
        if lte is not None:
            range_filter.append(f"{column_name}<={lte}")
        if gte is not None:
            range_filter.append(f"{column_name}>={gte}")
        return "(" + " and ".join(range_filter) + ")"

    def build_geo_filter(
            self, column_name: str, lat: float, lon: float, radius: float
    ) -> Any:
        """地理半径过滤：``geoDistance(lon, lat, col.lon, col.lat) < radius``。

        ClickHouse 的 Point 列用 ``.1`` / ``.2`` 取经度/纬度（元组下标从 1
        开始），参数顺序是 ``(经度, 纬度)``，不要与常见的 ``(lat, lon)`` 混淆。
        """
        return f"geoDistance( {lon}, {lat}, {column_name}.1, {column_name}.2 )<{radius}"

    def build_in_filter(self, column_name: str, expression_value: FieldValue) -> Any:
        """集合包含过滤：``col in (v1, v2, ...)``。

        数据集里 IN 的值以**字符串**给出（例如 ``"('a','b')"``），这里用
        ``[1:-1]`` 去掉首尾的包裹字符再放进括号，因此要求配置端的值正好是
        "左括号 + 列表 + 右括号" 的形式。
        """
        # expression_value type is str
        return f"{column_name} in ({expression_value[1:-1]})"

