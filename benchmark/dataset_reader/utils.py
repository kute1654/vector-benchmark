import zlib

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
    py_type = H5_COLUMN_TYPES_MAPPING.get(h5_column_type.lower(), None)
    if py_type is None:
        raise RuntimeWarning(f"Not support h5 column type: {h5_column_type}")
    return py_type


def _to_uint32_id(raw_id) -> int:
    if raw_id is None:
        raise ValueError("missing id")
    if isinstance(raw_id, int):
        return raw_id & 0xFFFFFFFF
    raw_str = str(raw_id)
    try:
        num = int(raw_str)
        if num < 0:
            return num & 0xFFFFFFFF
        return num
    except Exception:
        return zlib.crc32(raw_str.encode("utf-8")) & 0xFFFFFFFF
