import decimal

import pyarrow


def python_type_to_arrow_dtype(ttype: type) -> pyarrow.DataType:
    if ttype in (float, decimal.Decimal):
        return pyarrow.float64()
    elif ttype is int:
        return pyarrow.int64()
    elif ttype is bool:
        return pyarrow.bool8()
    else:
        return pyarrow.string()
