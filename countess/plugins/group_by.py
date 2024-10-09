import logging
from typing import Iterable, Optional

import pandas as pd
from pandas.api.typing import DataFrameGroupBy  # type: ignore

from countess import VERSION
from countess.core.parameters import BooleanParam, PerColumnArrayParam, TabularMultiParam
from countess.core.plugins import PandasConcatProcessPlugin
from countess.utils.pandas import flatten_columns, get_all_columns

logger = logging.getLogger(__name__)


class ColumnMultiParam(TabularMultiParam):
    index = BooleanParam("Index")
    count = BooleanParam("Count")
    nunique = BooleanParam("Count Distinct")
    min = BooleanParam("Min")
    max = BooleanParam("Max")
    sum = BooleanParam("Sum")
    mean = BooleanParam("Mean")
    median = BooleanParam("Median")
    std = BooleanParam("Std")


class GroupByPlugin(PandasConcatProcessPlugin):
    """Groups a Pandas Dataframe by an arbitrary column and rolls up rows"""

    name = "Group By"
    description = "Group records by column(s) and calculate aggregates"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#group-by"

    columns = PerColumnArrayParam("Columns", ColumnMultiParam("Column"))
    join = BooleanParam("Join Back?")

    def process(self, data: pd.DataFrame, source: str) -> Iterable:
        # XXX should do this in two stages: group each dataframe and then combine.
        # that can wait for a more general MapReduceFinalizePlugin class though.
        self.input_columns.update(get_all_columns(data))

        if not self.join:
            # Dispose of any columns we don't use in the aggregations.
            # TODO: Reindex as well?
            keep_columns = [
                col_param.label
                for col_param in self.columns
                if any(cp.value for cp in col_param.values()) and col_param.label in data.columns
            ]
            data = data[keep_columns]

        yield from super().process(data, source)

    def process_dataframe(self, dataframe: pd.DataFrame) -> Optional[pd.DataFrame]:
        self.columns.set_column_choices(self.input_columns.keys())

        column_parameters = list(zip(self.input_columns.keys(), self.columns))
        index_cols = [col for col, col_param in column_parameters if col_param["index"].value]
        agg_ops = dict(
            (
                col,
                [k for k, pp in col_param.params.items() if pp.value and k != "index"],
            )
            for col, col_param in column_parameters
            if any(pp.value for k, pp in col_param.params.items() if k != "index")
        )

        # reset any indexes which are actually columns we want to aggregate
        data_in = dataframe.reset_index([col for col in agg_ops.keys() if col in dataframe.index.names])

        try:
            # If there are no columns to index by, add a dummy column and group by that so we
            # still get a DataFrameGroupBy for the next operation
            # XXX there's probably an easier way to do this ...
            data_grouped: DataFrameGroupBy = (
                data_in.groupby(index_cols) if index_cols else data_in.assign(__temp=1).groupby("__temp")
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

            if self.join:
                if index_cols:
                    return data_in.merge(data_out, how="left", left_on=index_cols, right_on=index_cols)
                else:
                    return data_in.assign(**data_out.to_dict("records")[0])
            else:
                return data_out
        except (KeyError, ValueError) as exc:
            logger.warning("Exception", exc_info=exc)
            return None
