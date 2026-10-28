"""标量过滤条件的解析基类：把 benchmark 内部过滤 DSL 翻译成引擎语法。

数据集里的过滤条件在配置中写成统一结构（``and`` / ``or`` + 操作数列表），
各引擎的 ``*ConditionParser`` 继承本类，只需实现 ``build_condition`` 与四个
``build_*_filter``，即可复用上层的遍历与类型分发逻辑。
"""

from enum import Enum
from typing import Any, Dict, List, Optional, Union


class FilterType(str, Enum):
    """过滤表达式类型；取值即内部 DSL 里 ``expression_type`` 的字符串。

    继承 ``str`` 是为了可以直接与配置中的字符串比较（也可用
    ``FilterType(value)`` 从字符串反查枚举成员）。
    """

    FULL_MATCH = "match"  # 等值匹配
    RANGE = "range"       # 区间比较（lt/gt/lte/gte）
    GEO = "geo"           # 地理半径过滤（lon/lat/radius）
    IN = "in"             # 集合包含


# 单个标量字段允许的取值类型；带类型的别名仅是文档用途。
FieldValue = Union[str, int, float]
# {"and": [...], "or": [...]}，list 元素形如 {"列名": {"表达式类型": {...}}}
MetaConditions = Dict[str, List[Any]]


class BaseConditionParser:
    """过滤条件解析器基类；子类负责生成引擎特有的条件对象。"""

    def parse(self, meta_conditions: Optional[MetaConditions]) -> Optional[Any]:
        """
        The parse method accepts the meta conditions stored in a dict-like
        internal benchmark structure and converts it into the representation
        used by a specific engine.

        The internal representation has the following structure:
        {
            "or": [
                {"a": {"match": {"value": 80}}},
                {"a": {"match": {"value": 2}}}
            ]
        }

        There is always an operator ("and" / "or") and a list of operands.

        :param meta_conditions:
        :return:
        """
        # 空条件表示"不过滤"：返回 None 由各引擎翻译成恒真条件（或无 WHERE）。
        if meta_conditions is None or 0 == len(meta_conditions):
            # print("base parser meta is None")
            return None
        # 先分别把 and / or 的操作数列表翻译成条件对象，再交给子类组合；
        # 若某个逻辑分组缺省，对应参数为 None。
        return self.build_condition(
            and_subfilters=self.create_condition_subfilters(meta_conditions.get("and")),
            or_subfilters=self.create_condition_subfilters(meta_conditions.get("or")),
        )

    def build_condition(
        self, and_subfilters: Optional[List[Any]], or_subfilters: Optional[List[Any]]
    ) -> Optional[Any]:
        """
        Combination and logical "and" and "or" conditions
        Args:
            and_subfilters:
            or_subfilters:
        """
        raise NotImplementedError

    def create_condition_subfilters(self, condition_list) -> Optional[List[Any]]:
        """把一个逻辑分组下的操作数逐个翻译成过滤对象。

        ``condition_list`` 为空/None 时返回 None（表示该逻辑分组不存在），
        否则返回与输入顺序一致的过滤对象列表。结构是三层嵌套字典：
        ``{列名: {表达式类型: 参数字典}}``，因此用三重循环展开。
        """
        if condition_list is None:
            return None

        output_filters = []
        for entry in condition_list:
            for column_name, column_filters in entry.items():
                for expression_type, expression_value_dict in column_filters.items():
                    condition = self.build_filter(
                        column_name=column_name,
                        expression_type=FilterType(expression_type),
                        expression_value_dict=expression_value_dict
                    )
                    output_filters.append(condition)
        return output_filters

    def build_filter(
        self, column_name: str, expression_type: FilterType, expression_value_dict: Dict[str, Any]
    ):
        """按表达式类型分发到对应的 ``build_*_filter`` 实现。

        未知类型直接抛 ``NotImplementedError``，避免配置里的拼写错误被
        静默忽略成"不过滤"。
        """
        if FilterType.FULL_MATCH == expression_type:
            return self.build_exact_match_filter(
                column_name, expression_value=expression_value_dict.get("value")
            )
        if FilterType.RANGE == expression_type:
            return self.build_range_filter(
                column_name,
                lt=expression_value_dict.get("lt"),    # less than '<'
                gt=expression_value_dict.get("gt"),    # grater than '>'
                lte=expression_value_dict.get("lte"),  # less than or equal '<='
                gte=expression_value_dict.get("gte"),  # greater than or equal '>='
            )
        if FilterType.GEO == expression_type:
            return self.build_geo_filter(
                column_name,
                lon=expression_value_dict.get("lon"),
                lat=expression_value_dict.get("lat"),
                radius=expression_value_dict.get("radius"),
            )
        if FilterType.IN == expression_type:
            return self.build_in_filter(
                column_name, expression_value=expression_value_dict.get("value")
            )
        raise NotImplementedError

    def build_exact_match_filter(self, column_name: str, expression_value: FieldValue) -> Any:
        """等值过滤（``column = value``）。子类实现。"""
        raise NotImplementedError

    def build_range_filter(
        self,
        column_name: str,
        lt: Optional[FieldValue],
        gt: Optional[FieldValue],
        lte: Optional[FieldValue],
        gte: Optional[FieldValue],
    ) -> Any:
        """区间过滤；四个边界均可为 None，子类只拼接非空的部分。子类实现。"""
        raise NotImplementedError

    def build_geo_filter(
        self, column_name: str, lat: float, lon: float, radius: float
    ) -> Any:
        """地理半径过滤（以 lat/lon 为中心、radius 为半径）。子类实现。"""
        raise NotImplementedError

    def build_in_filter(
        self, column_name: str, expression_value: FieldValue
    ) -> Any:
        """集合包含过滤（``column IN (...)``）。子类实现。"""
        raise NotImplementedError
