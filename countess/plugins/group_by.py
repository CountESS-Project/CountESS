from collections.abc import Iterable, Mapping
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np

from countess.core.parameters import ChoiceParam
from countess.core.plugins import DaskTransformPlugin


class GroupByPlugin(DaskTransformPlugin):
    """Groups a Dask Dataframe by an arbitrary column and rolls up rows"""

    # XXX should support multiple operations but we'd need to support MultiIndex columns for that.

    name = "Group By"
    title = "Groups records by a column"
    description = "XXX"

    parameters = {
        "column": ChoiceParam("Group By", "Index", choices=[]),
        "operation": ChoiceParam(
            "Operation",
            "sum",
            choices=["sum", "size", "std", "var", "sem", "min", "max"],
        ),
    }

    def update(self):
        input_columns = sorted(self.previous_plugin.output_columns())
        self.parameters["column"].choices = ["Index"] + input_columns

    def run(self, ddf_in: dd.DataFrame):
        col_name = self.parameters["column"].value
        oper = self.parameters["operation"].value
        col = ddf_in.index if col_name == "Index" else ddf_in[col_name]

        return ddf_in.groupby(col).agg(oper)
