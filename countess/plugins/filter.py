import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ChoiceParam,
    ColumnChoiceParam,
    MultiParam,
    StringParam,
    TabularMultiParam,
)
from countess.core.plugins import PandasSimplePlugin

OPERATORS = ["equals", "greater than", "less than", "contains", "starts with", "ends with", "matches regex"]


class FilterPlugin(PandasSimplePlugin):
    name = "Filter Plugin"
    description = "Filter rows by simple expressions"
    additional = """
        Filter rows by multiple simple expressions.
        The same column can have multiple filters.
        "Negate?" inverts the filter, eg: "not equals", "not greater than", etc.
    """

    version = VERSION

    parameters = {
        "filters": ArrayParam(
            "Filters",
            MultiParam("Filter", {
                "columns": ArrayParam(
                    "Columns",
                    TabularMultiParam(
                        "Column",
                        {
                            "column": ColumnChoiceParam("Column"),
                            "negate": BooleanParam("Negate?"),
                            "operator": ChoiceParam("Operator", OPERATORS[0], OPERATORS),
                            "value": StringParam("Value"),
                        },
                    )
                ),
                "combine": ChoiceParam("Combine", "All", ["All", "Any"]),
                "outputs": ArrayParam(
                    "Outputs",
                    TabularMultiParam(
                        "Output",
                        {
                            "output": StringParam("Output Column"),
                            "value": StringParam("Output Value"),
                        }
                    )
                )
            })
        )
    }

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["filters"], ArrayParam)
        for filt in self.parameters["filters"]:

            series_acc = filt["combine"].value == 'All'

            if not filt.columns.params:
                continue

            for param in filt.columns.params:
                assert isinstance(param, TabularMultiParam)
                column = param["column"].value
                operator = param["operator"].value
                value = param["value"].value

                if operator == "equals":
                    series = dataframe[column].eq(value)
                elif operator == "greater than":
                    series = dataframe[column].gt(value)
                elif operator == "less than":
                    series = dataframe[column].lt(value)
                elif operator == "contains":
                    series = dataframe[column].str.contains(value, regex=False)
                elif operator == "starts with":
                    series = dataframe[column].str.startswith(value)
                elif operator == "ends with":
                    series = dataframe[column].str.endswith(value)
                elif operator == "matches regex":
                    series = dataframe[column].str.contains(value, regex=True)
                else:
                    continue

                print(series)

                if param["negate"].value:
                    series = series.eq(False)

                if filt["combine"].value == 'All':
                    series_acc = series_acc & series
                else:
                    series_acc = series_acc | series

                print(series_acc)

            if filt["outputs"].params:
                output_columns = [ p["output"].value for p in filt["outputs"] ]
                output_values = [ p["value"].value for p in filt["outputs"] ]
                print(f"XXX {output_columns} {output_values}")
                dataframe.loc[series_acc, output_columns] = output_values
            else:
                print(f"YYY {series_acc}")
                dataframe = dataframe[series_acc]

        print(dataframe)
        return dataframe
