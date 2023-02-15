import itertools
from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore

from countess.core.parameters import (
    ArrayParam,
    ChoiceParam,
    ColumnChoiceParam,
    MultiParam,
)
from countess.core.plugins import DaskTransformPlugin
from countess.utils.dask import empty_dask_dataframe

VERSION = "0.0.1"

AGG_FUNCTIONS = ["first", "sum", "count", "mean"]


class DaskPivotPlugin(DaskTransformPlugin):
    """Groups a Dask Dataframe by an arbitrary column and rolls up rows"""

    name = "Pivot Tool"
    title = "Pivot Tool"
    description = "..."
    version = VERSION

    parameters = {
        "index": ArrayParam("Index By", ColumnChoiceParam("Column")),
        "pivot": ArrayParam("Pivot By", ColumnChoiceParam("Column")),
        "agg": ArrayParam(
            "Aggregates",
            MultiParam(
                "Aggregate",
                {
                    "column": ColumnChoiceParam("Column"),
                    "function": ChoiceParam("Function", choices=AGG_FUNCTIONS),
                },
            ),
        ),
    }

    # XXX It'd be nice to also have "non pivoted" aggregated columns as well.

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        assert isinstance(self.parameters["index"], ArrayParam)
        assert isinstance(self.parameters["pivot"], ArrayParam)
        assert isinstance(self.parameters["agg"], ArrayParam)

        index_cols = [
            p.value
            for p in self.parameters["index"].params
            if isinstance(p, ChoiceParam) and p.value
        ]
        pivot_cols = [
            p.value
            for p in self.parameters["pivot"].params
            if isinstance(p, ChoiceParam) and p.value
        ]

        # dask's built-in "dd.pivot_table" can only handle one index column
        # and one pivot column which is too limiting.  So this is a slightly
        # cheesy replacement.

        # First, collect all the aggregated columns together in one place.
        agg_cols = [
            (p.params["column"].value, p.params["function"].value)
            for p in self.parameters["agg"].params
            if isinstance(p, MultiParam)
            and p.params["column"].value
            and p.params["function"].value
        ]
        aggregate_ops = defaultdict(list, [(c, ["first"]) for c in index_cols])

        if pivot_cols:

            # This won't run on multiindexes, but we're about to trash the indexing
            # anyway so just drop the existing index, we don't care.
            ddf = ddf.reset_index(drop=True)

            # `pivot_product` is every combination of every pivot column, so eg: if you're
            # pivoting on a `bin` column with values 1..4 and a `rep` column with values
            # 1..3 you'll end up with 12 elements, [ [1,1],[1,2],[1,3],[1,4],[2,1],[2,2] etc ]
            pivot_product = itertools.product(
                *[list(ddf[c].unique()) for c in pivot_cols]
            )

            # `pivot_groups` then reattaches the labels to those values, eg:
            # [[('bin', 1), ('rep', 1)], [('bin', 1), ('rep', 2)] etc
            pivot_groups = sorted(
                [list(zip(pivot_cols, pivot_values)) for pivot_values in pivot_product]
            )

            # Each pivot group is a set of conditions to filter for in that pivot
            # group.
            ddfs = []
            for pg in pivot_groups:
                # We first filter the source dataframe using the values in `pivot_group`
                # then rename the aggregated columns to add a specific suffix for
                # this group.
                suffix = "".join([f"__{pc}_{pv}" for pc, pv in pg])
                query = " and ".join(f"`{col}` == {repr(val)}" for col, val in pg)

                # work out what columns we need to rename and accumulate the
                # required aggregation operations in `aggregate_ops`.
                rename_cols = {}
                for col, agg_op in agg_cols:
                    aggregate_ops[col + suffix].append(agg_op)
                    rename_cols[col] = col + suffix

                ddfs.append(ddf.query(query).rename(columns=rename_cols))
            ddf = dd.concat(ddfs)
        else:
            for col, agg_op in agg_cols:
                aggregate_ops[col].append(agg_op)

        # If there aren't multple aggregations for any one column,
        # squish to prevent column name mangling.
        # XXX this could be improved to make mangling column specific.
        if all((len(v) == 1) for k, v in aggregate_ops.items()):
            aggregate_ops = defaultdict(
                list, [(k, v[0]) for k, v in aggregate_ops.items()]
            )

        # Group by the index columns and aggregate.
        return ddf.groupby(index_cols or ddf.index).agg(aggregate_ops)
