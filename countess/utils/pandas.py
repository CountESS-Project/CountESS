""" pandas utility functions """

from typing import Iterable, Mapping, Optional

import numpy as np
import pandas as pd


def get_all_indexes(dataframe: pd.DataFrame) -> Mapping[str, np.dtype]:
    if dataframe.index.name:
        return {dataframe.index.name: dataframe.index.dtype}
    elif hasattr(dataframe.index, "names") and dataframe.index.names[0] is not None:
        return dict(zip(dataframe.index.names, dataframe.index.dtypes))
    else:
        return {}


def get_all_columns(dataframe: pd.DataFrame) -> Mapping[str, np.dtype]:
    r = get_all_indexes(dataframe)
    r.update(dict(zip(dataframe.columns, dataframe.dtypes)))
    return r


def crop_dataframe(dataframe: pd.DataFrame, row_limit: Optional[int]) -> pd.DataFrame:
    if row_limit is None:
        return dataframe
    return dataframe[0:row_limit]


def concat_dataframes(dataframes: Iterable[pd.DataFrame]) -> pd.DataFrame:
    df_out = pd.DataFrame()
    for df_in in dataframes:
        df_out = pd.concat([df_out, df_in])
    return df_out
