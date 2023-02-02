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
        "count": ArrayParam("Aggregate Count", ColumnChoiceParam("Column")),
        "mean": ArrayParam("Aggregate Mean", ColumnChoiceParam("Column")),
    }
        
    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        index_cols = [ p.value for p in self.parameters['index'].params if p.value ]
        pivot_cols = [ p.value for p in self.parameters['pivot'].params if p.value ]

        agg_cols = [
            (op, [ p.value for p in self.parameters[op].params if p.value ])
            for op in ('sum', 'count', 'mean')
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

        aggregate_ops = [ (c, 'first') for c in index_cols ]

        ddfs = []
        for pg in pivot_groups:
            rename_cols = []
            for agg_op, cols in agg_cols:
                new_cols = [
                    col + ''.join([f"__{pc}_{pv}__{agg_op}" for pc, pv in pg])
                    for col in cols
                ]
                aggregate_ops += [ (nc, agg_op) for nc in new_cols ]
                rename_cols += zip(cols, new_cols)
            query = ' and '.join(f"`{col}` == {val}" for col, val in pg)

            nddf = ddf.query(query)
            for oc, nc in rename_cols:
                nddf.insert(0, column=nc, value=nddf[oc])
            ddfs.append(nddf)

        return dd.concat(ddfs).groupby(index_cols or new_ddf.index).agg(dict(aggregate_ops))
