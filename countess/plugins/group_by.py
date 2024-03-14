from typing import Iterable, List

import numpy as np
import pandas as pd
from pandas.api.typing import DataFrameGroupBy  # type: ignore

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ArrayParam, BooleanParam, PerColumnArrayParam, TabularMultiParam
from countess.core.plugins import PandasProcessPlugin
from countess.utils.pandas import flatten_columns, get_all_columns


class GroupByPlugin(PandasProcessPlugin):
    """Groups a Pandas Dataframe by an arbitrary column and rolls up rows"""

    name = "Group By"
    description = "Group records by column(s) and calculate aggregates"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#group-by"

    input_columns: dict[str, np.dtype]

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

    dataframes: List[pd.DataFrame]

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.prepare()

    def prepare(self, *_):
        self.dataframes = []
        self.input_columns = {}

    def process(self, data: pd.DataFrame, source: str, logger: Logger) -> Iterable:
        # XXX should do this in two stages: group each dataframe and then combine.
        # that can wait for a more general MapReduceFinalizePlugin class though.
        assert self.dataframes is not None
        assert isinstance(self.parameters["columns"], ArrayParam)

        self.input_columns.update(get_all_columns(data))
        column_parameters = list(zip(self.input_columns.keys(), self.parameters["columns"].params))

        if not self.parameters["join"].value:
            # Dispose of any columns we don't use in the aggregations.
            # TODO: Reindex as well?
            keep_columns = [
                col
                for col, col_param in column_parameters
                if isinstance(col_param, TabularMultiParam) and
                    any(cp.value for cp in col_param.values()) and col in data.columns
            ]
            data = data[keep_columns]

        self.dataframes.append(data)
        return []

    def finalize(self, logger: Logger) -> Iterable[pd.DataFrame]:
        assert isinstance(self.parameters["columns"], ArrayParam)
        assert self.dataframes

        self.parameters["columns"].set_column_choices(self.input_columns.keys())

        column_parameters = list(zip(self.input_columns.keys(), self.parameters["columns"]))

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
        data_in.reset_index([col for col in agg_ops.keys() if col in data_in.index.names], inplace=True)

        try:
            # If there are no columns to index by, add a dummy column and group by that so we
            # still get a DataFrameGroupBy for the next operation
            data_grouped : DataFrameGroupBy = (
                data_in.groupby(index_cols)
                if index_cols else
                data_in.assign(__temp=1).groupby("__temp")
            )

            # If no aggregation operations have been selected then just count the groups.
            if agg_ops:
                data_out = data_grouped.agg(agg_ops)
            else:
                data_out = pd.DataFrame(data_grouped.size(), columns=["count"])

            # Get rid of that dummy column if we added it.
            if "__temp" in data_out.index.names:
                data_out.reset_index("__temp", drop=True, inplace=True)

            flatten_columns(data_out, inplace=True)

            if self.parameters["join"].value:
                if index_cols:
                    yield data_in.merge(data_out, how="left", left_on=index_cols, right_on=index_cols)
                else:
                    yield data_in.assign(**data_out.to_dict("records")[0])
            else:
                yield data_out
        except ValueError as exc:
            logger.exception(exc)
