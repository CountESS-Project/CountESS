""" pandas utility functions """
from typing import Any, Dict, Iterable

import pandas as pd


def collect_dataframes(data: Iterable[pd.DataFrame], preferred_size: int = 100000) -> Iterable[pd.DataFrame]:
    """Takes an iterable of dataframes, ignores any empty ones and tries to pack smaller ones
    together up to `preferred_size`."""
    # XXX Data doesn't necessarily come out in the same order it went in.
    buffer = None
    for dataframe in data:
        if dataframe is None or len(dataframe) == 0:
            continue
        if len(dataframe) > preferred_size:
            yield dataframe
        elif buffer is None:
            buffer = dataframe
        elif len(buffer) + len(dataframe) > preferred_size:
            yield buffer
            buffer = dataframe
        else:
            # XXX catch errors?
            buffer = pd.concat([buffer, dataframe])
    if buffer is not None and len(buffer) > 0:
        yield buffer


def get_all_indexes(dataframe: pd.DataFrame) -> Dict[str, Any]:
    """Indexes work a couple of different ways, so this massages them into a consistent
    dictionary of name => dtype.  A single RangeIndex or a nameless index isn't counted."""
    if isinstance(dataframe.index, pd.RangeIndex):
        return {}
    elif dataframe.index.name:
        return {str(dataframe.index.name): dataframe.index.dtype}
    elif (
        hasattr(dataframe.index, "names")
        and hasattr(dataframe.index, "dtypes")
        and dataframe.index.names[0] is not None
    ):
        return dict(zip(dataframe.index.names, dataframe.index.dtypes))
    else:
        # XXX I'm not sure how or if this ever happens
        return {}  # pragma: no cover


def get_all_columns(dataframe: pd.DataFrame) -> Dict[str, Any]:
    r = get_all_indexes(dataframe)
    r.update(dict(zip(dataframe.columns, dataframe.dtypes)))
    return r


def concat_dataframes(dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    """handles the special case where there's an empty list"""
    # XXX dask also had a special case for a single input dataframe I think?
    if len(dataframes) == 0:
        return pd.DataFrame()
    return pd.concat(dataframes)


def flatten_columns(dataframe: pd.DataFrame, inplace: bool = False) -> pd.DataFrame:
    """used to rename columns with tuple names like `('things', 'count')` to easier-to-refer-to-
    in-python names like `'things__count'`."""
    # XXX arguably this would be better done at python plugin and/or export time but for now
    # we flatten columns where possible.

    if not inplace:
        dataframe = dataframe.copy()

    dataframe.columns = [
        "__".join(a) if type(a) is tuple else a for a in dataframe.columns.to_flat_index()
    ]  # type: ignore [assignment]

    return dataframe
