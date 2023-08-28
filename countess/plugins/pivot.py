import numpy as np
import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ChoiceParam, PerColumnArrayParam
from countess.core.plugins import PandasSimplePlugin

AGG_FUNCTIONS = ["first", "sum", "count", "mean"]


class PivotPlugin(PandasSimplePlugin):
    """Groups a Pandas Dataframe by an arbitrary column and rolls up rows"""

    name = "Pivot Tool"
    description = "Groups a dataframe and pivots column values into columns"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/plugins/#pivot-tool"

    parameters = {
        "columns": PerColumnArrayParam("Columns", ChoiceParam("Role", choices=["Index", "Pivot", "Expand", "Drop"]))
    }

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))
        index_cols = [col for col, col_param in column_parameters if col_param.value == "Index"]
        pivot_cols = [col for col, col_param in column_parameters if col_param.value == "Pivot"]
        expand_cols = [col for col, col_param in column_parameters if col_param.value == "Expand"]

        # XXX friendly error messages please
        assert pivot_cols

        df = pd.pivot_table(
            dataframe,
            values=expand_cols,
            index=index_cols,
            columns=pivot_cols,
            aggfunc=np.sum,
            fill_value=0,
        )

        if isinstance(df.columns, pd.MultiIndex):
            # Clean up MultiIndex names ... XXX until such time as CountESS supports them
            df.columns = [
                "__".join([f"{cn}_{cv}" if cn else cv for cn, cv in zip(df.columns.names, cc)]) for cc in df.columns
            ]  # type: ignore

        return df
