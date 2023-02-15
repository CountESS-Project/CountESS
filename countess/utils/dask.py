from itertools import islice

import dask.dataframe as dd
import pandas as pd  # type: ignore

"""Utility functions for manipulating Dask DataFrames"""


def empty_dask_dataframe() -> dd.DataFrame:
    """Returns an empty dask DataFrame for consistency."""
    edf = dd.from_pandas(pd.DataFrame([]), npartitions=1)
    assert isinstance(edf, dd.DataFrame)  # reassure mypy
    return edf


def crop_dask_dataframe(ddf: dd.DataFrame, row_limit: int) -> dd.DataFrame:
    """Takes a dask dataframe `ddf` and returns a frame with at most `row_limit` rows"""
    if len(ddf) > row_limit:
        x, y = islice(ddf.index, 0, row_limit, row_limit - 1)
        ddf = ddf.loc[x:y]
    return ddf


def concat_dask_dataframes(ddfs: list[dd.DataFrame]) -> dd.DataFrame:
    """Concat dask dataframes, but include special cases for 0 and 1 inputs"""

    # extra special case for empty dataframes
    ddfs = [df for df in ddfs if len(df) > 0]

    if len(ddfs) == 0:
        return empty_dask_dataframe()
    elif len(ddfs) == 1:
        return ddfs[0].copy()
    else:
        return dd.concat(ddfs)


def merge_dask_dataframes(
    dfs: list[dd.DataFrame | pd.DataFrame], how: str = "outer"
) -> dd.DataFrame:
    """Merge multiple dask/pandas dataframes together on their index.
    Always returns a new Dask dataframe even if there's one or zero input dataframes."""

    # XXX wrong for inner & left joins
    left = empty_dask_dataframe()
    for right in dfs:
        left = left.merge(right, how=how)
    return left
