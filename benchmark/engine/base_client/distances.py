"""驱动式客户端使用的距离度量枚举。

各引擎的 ``configure.py`` / ``search.py`` / ``parser.py`` 通过本枚举把配置里的
距离名（如 ``"l2"``、``"cosine"``、``"dot"``）映射到该引擎底层的算子或索引参数，
避免在多个模块里重复写字符串比较。继承 ``str`` 使得枚举值可以直接序列化进
JSON / CSV（输出就是 ``"l2"`` 这样的字符串）。
"""

from enum import Enum


class Distance(str, Enum):
    """支持的距离度量。

    - ``DOT``：内积（越大越相似；部分引擎会转成负值排序）。
    - ``COSINE``：余弦距离（实现上通常是 1 - 余弦相似度）。
    - ``L2``：欧氏距离（平方 L2 还是开方 L2 由具体引擎决定）。
    """

    DOT = "dot"
    COSINE = "cosine"
    L2 = "l2"

    @classmethod
    def from_name(cls, name) -> "Distance":
        """按名称（大小写不敏感）解析距离度量。

        ``"inner-product"`` 这类连字符写法会先被归一化为下划线，因此调用方
        可以直接传入配置文件中的人类可读名称。未知名称抛 ``ValueError``，
        而不是静默回退到默认值，避免误用错误的度量却看不出问题。
        """
        name = name.upper().replace("-", "_")
        distance = cls.__members__.get(name)
        if distance is not None:
            return distance
        raise ValueError(f"Unknown distance: <{name}>")
