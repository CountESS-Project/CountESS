from typing import Iterable

import pandas as pd
from pandas.api.types import is_numeric_dtype

from countess import VERSION
from countess.core.logger import Logger
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

    parameters = {
        "filters": ArrayParam(
            "Filters",
            MultiParam(
                "Filter",
                {
                    "columns": ArrayParam(
                        "Columns",
                        TabularMultiParam(
                            "Column",
                            {
                                "column": ColumnChoiceParam("Column"),
                                "negate": BooleanParam("Negate?"),
                                "operator": ChoiceParam("Operator", OPERATORS[0], OPERATORS),
                                "value": ColumnOrStringParam("Value"),
                            },
                        ),
                    ),
                    "combine": ChoiceParam("Combine", "All", ["All", "Any"]),
                    "outputs": ArrayParam(
                        "Outputs",
                        TabularMultiParam(
                            "Output",
                            {
                                "output": StringParam("Output Column"),
                                "value": StringParam("Output Value"),
                                "type": DataTypeChoiceParam("Output Type", "string"),
                            },
                        ),
                    ),
                },
            ),
        )
    }

    def process(self, data: pd.DataFrame, source: str, logger: Logger) -> Iterable[pd.DataFrame]:
        assert isinstance(self.parameters["filters"], ArrayParam)
        data = data.reset_index(drop=data.index.names == [None])

        self.input_columns.update(get_all_columns(data))

        for filt in self.parameters["filters"]:
            # build a dictionary of columns to assign to matched rows.
            assign_dict = {p["output"].value: p["type"].cast_value(p["value"].value) for p in filt["outputs"]}

            if not filt.columns.params:
                # If there are no filter columns at all, then we match
                # every row, and there's no rows left unmatched so we're
                # finished.
                yield data.assign(**assign_dict)
                return

            # Build up a pd.Series to mask dataframe entries into
            # matching or not matching.  This starts as either
            # `True` or `False` and then 'accumulates' logical
            # operations using either `&` or `|` to end up with
            # a boolean series.
            series_acc = filt["combine"].value == "All"

            for param in filt.columns.params:
                assert isinstance(param, TabularMultiParam)
                column = param["column"].value
                operator = param["operator"].value
                value = param["value"].get_column_or_value(data, is_numeric_dtype(data[column]))
                if operator == "equals":
                    series = data[column].eq(value)
                elif operator == "greater than":
                    series = data[column].gt(value)
                elif operator == "less than":
                    series = data[column].lt(value)
                elif operator == "contains":
                    series = data[column].str.contains(value, regex=False)
                elif operator == "starts with":
                    series = data[column].str.startswith(value)
                elif operator == "ends with":
                    series = data[column].str.endswith(value)
                elif operator == "matches regex":
                    series = data[column].str.contains(value, regex=True)
                else:
                    continue

                if param["negate"].value:
                    series = ~series

                if filt["combine"].value == "All":
                    series_acc = series_acc & series
                else:
                    series_acc = series_acc | series

            # If no rows match, just move on to the next filter
            if not any(series_acc):
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

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        # not used
        return dataframe
