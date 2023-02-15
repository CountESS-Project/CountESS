from collections.abc import Iterable, Mapping
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np

from countess.core.parameters import ChoiceParam
from countess.core.plugins import DaskTransformPlugin

VERSION = "0.0.1"


class GroupByPlugin(DaskTransformPlugin):
    """Groups a Dask Dataframe by an arbitrary column and rolls up rows"""

    # XXX should support an operation per column, using
    # dd.Aggregation to supply appropriate chunk/agg/finalize
    # functions which (potentially) work differently per column,
    # as opposed to the built-in aggregations which are the
    # same for every column.

    name = "Group By"
    title = "Groups records by a column"
    description = "Group records by a column"
    version = VERSION

    parameters = {
        "column": ChoiceParam("Group By", "Index", choices=[]),
        "operation": ChoiceParam(
            "Operation",
            "sum",
            choices=["sum", "size", "std", "var", "sem", "min", "max"],
        ),
    }

    def update(self):
        self.parameters["column"].choices = ["Index"] + self.input_columns

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        col_name = self.parameters["column"].value
        col = ddf.index if col_name == "Index" else ddf[col_name]
        oper = self.parameters["operation"].value

        return ddf.groupby(col).agg(oper)
