"""Utility functions for manipulating Dask DataFrames"""

from typing import Collection, Optional

import dask.dataframe as dd
import pandas as pd  # type: ignore


def empty_dask_dataframe() -> dd.DataFrame:
    """Returns an empty dask DataFrame for consistency."""
    edf = dd.from_pandas(pd.DataFrame([]), npartitions=1)
    assert isinstance(edf, dd.DataFrame)  # reassure mypy
    return edf


def crop_dataframe(
    df: pd.DataFrame | dd.DataFrame, row_limit: Optional[int]
) -> pd.DataFrame | dd.DataFrame:
    """Takes a dask dataframe `ddf` and returns a frame with at most `row_limit` rows"""
    if row_limit is not None:
        assert isinstance(row_limit, int)
        if isinstance(df, pd.DataFrame):
            return df.head(row_limit)
        elif isinstance(df, dd.DataFrame):
            return df.head(row_limit, npartitions=-1, compute=False)
        else:
            raise TypeError("Expecting pd.DataFrame or dd.DataFrame")
    return df


def concat_dataframes(dfs: Collection[pd.DataFrame | dd.DataFrame]) -> pd.DataFrame | dd.DataFrame:
    """Concat dask dataframes, but include special cases for 0 and 1 inputs"""

    # extra special case for empty dataframes
    dfs = [df for df in dfs if len(df) > 0]

    if len(dfs) == 0:
        return empty_dask_dataframe()
    elif len(dfs) == 1:
        return dfs[0].copy()
    else:
        return dd.concat(dfs)


def merge_dataframes(dfs: list[dd.DataFrame | pd.DataFrame], how: str = "outer") -> dd.DataFrame:
    """Merge multiple dask/pandas dataframes together on their index.
    Always returns a new Dask dataframe even if there's one or zero input dataframes."""

    # XXX should be smarter about merge strategies?

    if not dfs:
        return empty_dask_dataframe()

    left, *rest = dfs
    if isinstance(left, pd.Dataframe):
        left = dd.from_pandas(left, npartitions=1)
    for right in rest:
        left = left.merge(right, how=how)
    return left
