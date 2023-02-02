from collections.abc import Iterable, Mapping
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd

import itertools

from countess.utils.dask import empty_dask_dataframe
from countess.core.parameters import ArrayParam, ColumnChoiceParam, ChoiceParam, MultiParam
from countess.core.plugins import DaskTransformPlugin

VERSION = "0.0.1"

AGG_FUNCTIONS=['first', 'sum', 'count', 'mean']

class DaskPivotPlugin(DaskTransformPlugin):
    """Groups a Dask Dataframe by an arbitrary column and rolls up rows"""

    name = "Pivot Tool"
    title = "Pivot Tool"
    description = "..."
    version = VERSION

    parameters = {
        "index": ArrayParam("Index By", ColumnChoiceParam("Column")),
        "pivot": ArrayParam("Pivot By", ColumnChoiceParam("Column")),
        "agg": ArrayParam("Aggregates", MultiParam("Aggregate", {
            "column": ColumnChoiceParam("Column"),
            "function": ChoiceParam("Function", choices=AGG_FUNCTIONS),
        })),
    }
        
    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        index_cols = [ p.value for p in self.parameters['index'].params if p.value ]
        pivot_cols = [ p.value for p in self.parameters['pivot'].params if p.value ]

        agg_cols = [
            (p.params['column'].value, p.params['function'].value)
            for p in self.parameters['agg'].params
            if p.params['column'].value and p.params['function'].value
        ]

        ddf = ddf.reset_index(drop=True)

        pivot_product = itertools.product(*[ list(ddf[c].unique()) for c in pivot_cols ])
        pivot_groups = sorted([
            list(zip(pivot_cols, pivot_values))
            for pivot_values in pivot_product
        ])

        # dask's built-in "dd.pivot_table" can only handle one index column
        # and one pivot column which is too limiting.  So this is a slightly
        # cheesy replacement.

        aggregate_ops = [(c, 'first') for c in index_cols]

        ddfs = []
        for pg in pivot_groups:
            query = ' and '.join(f"`{col}` == {val}" for col, val in pg)
            new_ddf = ddf.query(query)

            for col, agg_op in agg_cols:
                new_col = col + ''.join([f"__{pc}_{pv}__{agg_op}" for pc, pv in pg])
                aggregate_ops.append((new_col, agg_op))
                new_ddf.insert(len(new_ddf.columns), column=new_col, value=new_ddf[col])

            ddfs.append(new_ddf)

        return dd.concat(ddfs).groupby(index_cols or new_ddf.index).agg(dict(aggregate_ops))
