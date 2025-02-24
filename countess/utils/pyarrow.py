import decimal

import pyarrow  # type: ignore


def python_type_to_arrow_dtype(ttype: type) -> pyarrow.DataType:
    if ttype in (float, decimal.Decimal):
        return pyarrow.float64()
    elif ttype is int:
        return pyarrow.int64()
    elif ttype is bool:
        return pyarrow.bool_()
    else:
        return pyarrow.string()
