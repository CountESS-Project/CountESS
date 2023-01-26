from collections.abc import Iterable, Mapping
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd

import itertools

from countess.utils.dask import empty_dask_dataframe
from countess.core.parameters import ArrayParam, ColumnChoiceParam
from countess.core.plugins import DaskTransformPlugin

VERSION = "0.0.1"


class DaskPivotPlugin(DaskTransformPlugin):
    """Groups a Dask Dataframe by an arbitrary column and rolls up rows"""

    name = "Pivot Tool"
    title = "Pivot Tool"
    description = "..."
    version = VERSION

    parameters = {
        "index": ArrayParam("Index By", ColumnChoiceParam("Column")),
        "pivot": ArrayParam("Pivot By", ColumnChoiceParam("Column")),
        "sum": ArrayParam("Aggregate Sum", ColumnChoiceParam("Column")),
    }
        
    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        index_cols = [ p.value for p in self.parameters['index'].params if p.value ]
        pivot_cols = [ p.value for p in self.parameters['pivot'].params if p.value ]
        sum_cols = [ p.value for p in self.parameters['sum'].params if p.value ]

        new_ddf = ddf[index_cols + sum_cols].copy()

        pivot_product = itertools.product(*[ list(ddf[c].unique()) for c in pivot_cols ])
        pivot_groups = sorted([
            list(zip(pivot_cols, pivot_values))
            for pivot_values in pivot_product
        ])

        new_sum_cols = []
        for sc in sum_cols:
            for pg in pivot_groups:
                new_series = ddf[sc]
                new_sum_col = sc + ''.join([f"__{col}_{val}" for col, val in pg])
                new_sum_cols.append(new_sum_col)
                new_ddf[new_sum_col] = ddf[sc]
                for col, val in pg:
                    new_ddf[new_sum_col] *= (ddf[col] == val).astype(int)

        ops = dict(
            [ (c, 'first') for c in index_cols ] +
            [ (c, 'sum') for c in new_sum_cols ]
        )
       
        return new_ddf.groupby(index_cols or new_ddf.index).agg(ops).reset_index(drop=True)
