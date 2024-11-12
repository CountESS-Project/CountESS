import warnings
from typing import Iterable

import pandas as pd
from pandas.api.types import is_numeric_dtype

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ChoiceParam,
    ColumnChoiceParam,
    ColumnOrStringParam,
    DataTypeChoiceParam,
    MultiParam,
    StringParam,
    TabularMultiParam,
)
from countess.core.plugins import PandasSimplePlugin
from countess.utils.pandas import get_all_columns

OPERATORS = ["equals", "greater than", "less than", "contains", "starts with", "ends with", "matches regex"]


class _FilterColumnMultiParam(TabularMultiParam):
    column = ColumnChoiceParam("Column")
    negate = BooleanParam("Negate?")
    operator = ChoiceParam("Operator", OPERATORS[0], OPERATORS)
    value = ColumnOrStringParam("Value")


class _FilterOutputMultiParam(TabularMultiParam):
    output = StringParam("Output Column")
    value = StringParam("Output Value")
    type = DataTypeChoiceParam("Output Type", "string")


class FilterMultiParam(MultiParam):
    columns = ArrayParam("Columns", _FilterColumnMultiParam("Column"))
    combine = ChoiceParam("Combine", "All", ["All", "Any"])
    outputs = ArrayParam("Outputs", _FilterOutputMultiParam("Output"))


class FilterPlugin(PandasSimplePlugin):
    name = "Filter Plugin"
    description = "Filter rows by simple expressions"
    additional = """
        Filter rows by multiple simple expressions.
        The same column can have multiple filters.
        "Negate?" inverts the filter, eg: "not equals", "not greater than", etc.
    """
    link = "https://countess-project.github.io/CountESS/included-plugins/#filter"

    version = VERSION

    filters = ArrayParam("Filters", FilterMultiParam("Filter"))

    def process(self, data: pd.DataFrame, source: str) -> Iterable[pd.DataFrame]:
        data = data.reset_index(drop=data.index.names == [None])

        self.input_columns.update(get_all_columns(data))

        for filt in self.filters:
            # build a dictionary of columns to assign to matched rows.
            assign_dict = {p.output.value: p.type.cast_value(p.value.value) for p in filt.outputs}

            if len(filt.columns) == 0:
                # If there are no filter columns at all, then we match
                # every row, and there's no rows left unmatched so we're
                # finished.
                yield data.assign(**assign_dict)
                return

            # Build up a pd.Series to mask dataframe entries into
            # matching or not matching.
            series_acc = None

            for param in filt.columns:
                column = param.column.value
                if column not in data.columns:
                    continue

                is_numeric = is_numeric_dtype(data[column])
                value = param.value.get_column_or_value(data, is_numeric)
                if param.operator == "equals":
                    series = data[column].eq(value)
                elif param.operator == "greater than":
                    series = data[column].gt(value)
                elif param.operator == "less than":
                    series = data[column].lt(value)
                elif param.operator == "contains":
                    series = data[column].str.contains(value, regex=False)
                elif param.operator == "starts with":
                    series = data[column].str.startswith(value)
                elif param.operator == "ends with":
                    series = data[column].str.endswith(value)
                elif param.operator == "matches regex":
                    with warnings.catch_warnings():
                        warnings.filterwarnings("ignore", ".*has match groups")
                        series = data[column].str.contains(value, regex=True)
                else:
                    continue

                if param.negate:
                    series = ~series

                if series_acc is None:
                    series_acc = series
                elif filt.combine == "All":
                    series_acc = series_acc & series
                else:
                    series_acc = series_acc | series

            # If no filters were valid, or if no rows match,
            # just move on to the next filter
            if series_acc is None or not any(series_acc):
                continue

            # If all rows match, just return everything and then quit.
            if all(series_acc):
                yield data.assign(**assign_dict)
                return

            # split the dataframe into matching and non-matching rows.
            # non-matching rows go in to `data` for the next filter.
            df_split = data.groupby(series_acc)
            data = df_split.get_group(False)

            # matching rows have output columns assigned and are yielded.
            yield df_split.get_group(True).assign(**assign_dict)

        # anything which has matched nothing at all is left behind
        # in `data` and is just thrown away.

    def process_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        # not used
        return dataframe
