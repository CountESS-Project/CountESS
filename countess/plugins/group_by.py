from typing import Iterable, List, Optional

import numpy as np
import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BaseParam, BooleanParam, PerColumnArrayParam, TabularMultiParam
from countess.core.plugins import PandasProcessPlugin
from countess.utils.pandas import get_all_columns


def _column_renamer(col):
    if isinstance(col, tuple):
        if col[-1] == "first":
            return "__".join(col[:-1])
        else:
            return "__".join(col)
    return col


class GroupByPlugin(PandasProcessPlugin):
    """Groups a Pandas Dataframe by an arbitrary column and rolls up rows"""

    name = "Group By"
    description = "Group records by column(s) and calculate aggregates"
    version = VERSION

    index_cols: set[BaseParam] = set()
    input_columns: dict[str, np.dtype] = {}

    parameters = {
        "columns": PerColumnArrayParam(
            "Columns",
            TabularMultiParam(
                "Column",
                {
                    "index": BooleanParam("Index"),
                    "count": BooleanParam("Count"),
                    "min": BooleanParam("Min"),
                    "max": BooleanParam("Max"),
                    "sum": BooleanParam("Sum"),
                    "mean": BooleanParam("Mean"),
                },
            ),
        ),
        "join": BooleanParam("Join Back?"),
    }

    dataframes: Optional[List[pd.DataFrame]] = None

    def prepare(self, *_):
        self.dataframes = []

    def process(self, data: pd.DataFrame, source: str, logger: Logger) -> Iterable:
        # XXX can do this in two stages
        assert self.dataframes is not None
        self.dataframes.append(data)
        self.input_columns.update(get_all_columns(data))
        return []

    def finalize(self, logger: Logger) -> Iterable[pd.DataFrame]:
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        assert self.dataframes
        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))
        index_cols = [col for col, col_param in column_parameters if col_param["index"].value]
        agg_ops = dict(
            (
                col,
                [k for k, pp in col_param.params.items() if pp.value and k != "index"],
            )
            for col, col_param in column_parameters
            if any(pp.value for k, pp in col_param.params.items() if k != "index")
        )

        data_in = pd.concat(self.dataframes)

        try:
            data_out = data_in.groupby(index_cols or data_in.index).agg(agg_ops)
            if self.parameters["join"].value:
                yield data_in.merge(data_out, how="left", left_on=index_cols, right_on=index_cols)
            else:
                yield data_out
        except ValueError:
            pass

        for p in self.parameters.values():
            p.set_column_choices(self.input_columns.keys())
