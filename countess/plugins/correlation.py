from typing import Iterable, Optional

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ChoiceParam, ColumnChoiceParam, ColumnOrNoneChoiceParam
from countess.core.plugins import PandasSimplePlugin


class CorrelationPlugin(PandasSimplePlugin):
    """Correlations"""

    name = "Correlation Tool"
    description = "Measures Pearsons / Kendall / Spearman correlation of columns"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#correlation-tool"

    parameters = {
        "method": ChoiceParam("Method", choices=["pearson", "kendall", "spearman"]),
        "group": ColumnOrNoneChoiceParam("Group"),
        "column1": ColumnChoiceParam("Column 1"),
        "column2": ColumnChoiceParam("Column 2"),
    }
    columns: list[str] = []
    dataframes: list[pd.DataFrame] = []

    def prepare(self, sources: list[str], row_limit: Optional[int]=None):
        assert isinstance(self.parameters["group"], ColumnOrNoneChoiceParam)
        column1 = self.parameters["column1"].value
        column2 = self.parameters["column2"].value
        self.columns = [column1, column2]
        if not self.parameters["group"].is_none():
            self.columns.append(self.parameters["group"].value)
        self.dataframes = []

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> None:
        self.dataframes.append(dataframe[self.columns])

    def finalize(self, logger: Logger) -> Iterable[pd.DataFrame]:
        assert isinstance(self.parameters["group"], ColumnOrNoneChoiceParam)
        column1 = self.parameters["column1"].value
        column2 = self.parameters["column2"].value
        groupby = None if self.parameters["group"].is_none() else self.parameters["group"].value

        method = self.parameters["method"].value

        dataframe = pd.concat(self.dataframes)
        if groupby:
            ds = dataframe.groupby(groupby)[column1]
            yield ds.corr(dataframe[column2], method=method).to_frame(name="correlation")
        else:
            corr = dataframe[column1].corr(dataframe[column2], method=method)
            yield pd.DataFrame([[corr]], columns=["correlation"])
