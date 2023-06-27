import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ChoiceParam, ColumnChoiceParam, ColumnOrNoneChoiceParam
from countess.core.plugins import PandasTransformPlugin


class CorrelationPlugin(PandasTransformPlugin):
    """Correlations"""

    name = "Correlation Tool"
    description = "Measures Pearsons / Kendall / Spearman correlation of columns"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/plugins/#correlation-tool"

    parameters = {
        "method": ChoiceParam("Method", choices=["pearson", "kendall", "spearman"]),
        "group": ColumnOrNoneChoiceParam("Group"),
        "column1": ColumnChoiceParam("Column 1"),
        "column2": ColumnChoiceParam("Column 2"),
    }

    def run_df(self, df: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["group"], ColumnOrNoneChoiceParam)

        method = self.parameters["method"].value
        column1 = self.parameters["column1"].value
        column2 = self.parameters["column2"].value

        if self.parameters["group"].is_none():
            corr = df[column1].corr(df[column2], method=method)
            return pd.DataFrame([[corr]], columns=["correlation"])
        else:
            ds = df.groupby(self.parameters["group"].value)[column1]
            return ds.corr(df[column2], method=method).to_frame(name="correlation")
