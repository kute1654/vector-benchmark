"""MyScale 过滤条件解析器。

把 benchmark 的抽象过滤条件（等值、范围、geo、in）翻译成 MyScale SQL
片段。生成的片段最终由 :class:`MyScaleSearcher` 拼进
``SELECT ... FROM <table> PREWHERE <cond>`` 中，用于带标量过滤的
向量检索（ANN + filter）。

所有片段都是字符串拼接，字段名来自数据集 schema，值来自数据集条件；
``FieldValue`` 由基类保证类型，但拼 SQL 时仍需注意引号/转义。
"""

from typing import Any, List, Optional, Dict

from engine.base_client.parser import BaseConditionParser, FieldValue


class MyScaleConditionParser(BaseConditionParser):
    """继承通用条件遍历逻辑，只实现 MyScale 方言的片段生成。"""

    def build_condition(
            self, and_subfilters: Optional[List[Any]], or_subfilters: Optional[List[Any]]
    ) -> Optional[Any]:
        """组合 AND/OR 子条件。

        MyScale 的布尔优先级是 ``AND`` 高于 ``OR``，而 benchmark 语义要求
        分别对 AND 组和 OR 组求值后再取交集，因此两组各自加括号，最后用
        ``AND`` 连接：``(or 组合) AND (and 组合)``。
        """
        clauses = []
        if or_subfilters is not None and len(or_subfilters) > 0:
            clauses.append("(" + " or ".join(or_subfilters) + ")")
        if and_subfilters is not None and len(and_subfilters) > 0:
            clauses.append("(" + " and ".join(and_subfilters) + ")")
        return " and ".join(clauses)

    def build_exact_match_filter(self, column_name: str, expression_value: FieldValue) -> Any:
        """等值过滤：``col = 'value'``。

        ClickHouse 字符串字面量用单引号；值中的单引号需由上游转义（本类
        不做转义，仅负责拼装）。
        """
        return f"{column_name} = '{expression_value}'"

    def build_range_filter(
            self,
            column_name: str,
            lt: Optional[FieldValue],
            gt: Optional[FieldValue],
            lte: Optional[FieldValue],
            gte: Optional[FieldValue],
    ) -> Any:
        """开/闭区间过滤，按需拼接 ``<``、``>``、``<=``、``>=`` 并以 AND 连接。

        先做基本一致性检查：``lt < gt`` 或 ``lte < gte`` 说明数据集条件本身
        不可能命中，直接报错而不是返回空结果，便于尽早发现数据问题。
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
        """半径过滤：``geoDistance(lon, lat, col.1, col.2) < radius``。

        geo 列在表中是 ``Tuple(Float64, Float64)``（Point），``.1`` 为经度、
        ``.2`` 为纬度；``geoDistance`` 的签名要求经度在前、纬度在后，故这里
        顺序是 ``lon, lat`` 而不是函数参数名中的 ``lat, lon``。距离单位与
        半径单位一致（通常为米）。
        """
        return f"geoDistance( {lon}, {lat}, {column_name}.1, {column_name}.2 )<{radius}"

    def build_in_filter(self, column_name: str, expression_value: FieldValue) -> Any:
        """把基类给的 ``"(v1, v2, ...)"`` 转成 ``col in (v1, v2, ...)``。

        ``expression_value`` 类型是 str。基类传入的表达式形如
        ``"(v1, v2, ...)"``，这里去掉首尾圆括号后复用，因为 IN 子句自身
        还需要一对括号——注意不能简单拼 ``in expression_value``，否则会
        变成 ``in (v1, v2)`` 之外再套一层括号的错误形态。
        """
        return f"{column_name} in ({expression_value[1:-1]})"

