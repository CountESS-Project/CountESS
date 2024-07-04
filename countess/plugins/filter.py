import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ChoiceParam,
    ColumnChoiceParam,
    StringParam,
    TabularMultiParam,
)
from countess.core.plugins import PandasSimplePlugin

OPERATORS = ["equals", "greater than", "less than", "contains", "starts with", "ends with"]


class FilterPlugin(PandasSimplePlugin):
    name = "Filter Plugin"
    description = "Filter rows by simple expressions"

    version = VERSION

    parameters = {
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
            ),
        )
    }

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["columns"], ArrayParam)
        for param in self.parameters["columns"].params:
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
            else:
                continue

            if param["negate"].value:
                series = series.eq(False)

            dataframe = dataframe[series]

        return dataframe
