from typing import Iterable, Optional

import pandas as pd

from countess import VERSION
from countess.core.parameters import ChoiceParam, ColumnChoiceParam, ColumnOrNoneChoiceParam
from countess.core.plugins import PandasSimplePlugin


class CorrelationPlugin(PandasSimplePlugin):
    """Correlations"""

    name = "Correlation Tool"
    description = "Measures Pearsons / Kendall / Spearman correlation of columns"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#correlation-tool"

    method = ChoiceParam("Method", choices=["pearson", "kendall", "spearman"])
    group = ColumnOrNoneChoiceParam("Group")
    column1 = ColumnChoiceParam("Column 1")
    column2 = ColumnChoiceParam("Column 2")

    columns: list[str] = []
    dataframes: list[pd.DataFrame] = []

    def prepare(self, sources: list[str], row_limit: Optional[int] = None):
        column1 = self.column1.value
        column2 = self.column2.value
        self.columns = [column1, column2]
        if self.group.is_not_none():
            self.columns.append(self.group.value)
        self.dataframes = []

    def process_dataframe(self, dataframe: pd.DataFrame) -> None:
        self.dataframes.append(dataframe[self.columns])

    def finalize(self) -> Iterable[pd.DataFrame]:
        column1 = self.column1.value
        column2 = self.column2.value
        groupby = None if self.group.is_none() else self.group.value

        method = self.method.value

        dataframe = pd.concat(self.dataframes)
        if groupby:
            ds = dataframe.groupby(groupby)[column1]
            yield ds.corr(dataframe[column2], method=method).to_frame(name="correlation")
        else:
            corr = dataframe[column1].corr(dataframe[column2], method=method)
            yield pd.DataFrame([[corr]], columns=["correlation"])
