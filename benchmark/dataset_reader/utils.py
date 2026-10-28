"""读取器共用工具：HDF5 类型映射与 id 归一化。

本模块无 I/O，仅提供纯函数；被 HDF5 Reader（类型转换）与 gz_tsv Reader
（字符串/负数 id 转 uint32）复用。
"""

import zlib

# HDF5 属性中声明的列类型字符串 -> Python 类型。
# 注意：blob/string/text 统一映射为 str，调用方再用 convert_bytes_to_str 解码 bytes；
# 所有浮点宽度（float64/float32）都映射为 float，不保留精度信息。
H5_COLUMN_TYPES_MAPPING = {
    "float64": float,
    "float32": float,
    "float": float,
    "int32": int,
    "int": int,
    "integer": int,
    "text": str,
    "string": str,
    "blob": str,
}


def convert_H52py(h5_column_type: str):
    """ Convert the data type of a dataset recorded in an HDF5 file to a Python data type. """
    # 大小写不敏感；未登记的类型返回 None 并抛出 RuntimeWarning（注意：是异常而非警告）。
    py_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if py_type is None:
        raise RuntimeWarning(f"Not support h5 column type: {h5_column_type}")
    return py_type


def _to_uint32_id(raw_id) -> int:
    """把任意来源的 id 归一化为 32 位无符号整数。

    规则（按优先级）：
    1. None -> ValueError（id 缺失）；
    2. 已是 int -> 直接按位与 0xFFFFFFFF（负数会回绕）；
    3. 可解析为十进制整数的字符串 -> 负数按位与回绕，非负数原样返回；
    4. 其余字符串 -> 对其 UTF-8 字节做 CRC32，保证稳定且落在 32 位范围内。

    使用 CRC32 而非随机数，使同一字符串在不同进程/运行中得到相同 id，
    这样上传端与查询端的 id 能对上。
    """
    if raw_id is None:
        raise ValueError("missing id")
    if isinstance(raw_id, int):
        return raw_id & 0xFFFFFFFF
    raw_str = str(raw_id)
    try:
        num = int(raw_str)
        if num < 0:
            # Python 的 & 作用于负整数时按二进制补码语义回绕到 [0, 2^32)。
            return num & 0xFFFFFFFF
        return num
    except Exception:
        # 非数字 id（如哈希字符串）走确定性哈希，避免抛错中断整轮读取。
        return zlib.crc32(raw_str.encode("utf-8")) & 0xFFFFFFFF
