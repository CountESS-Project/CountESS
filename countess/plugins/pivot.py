from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ChoiceParam, PerColumnArrayParam
from countess.core.plugins import PandasProcessPlugin
from countess.utils.pandas import get_all_columns

AGG_FUNCTIONS = ["first", "sum", "count", "mean"]


class PivotPlugin(PandasProcessPlugin):
    """Groups a Pandas Dataframe by an arbitrary column and rolls up rows"""

    name = "Pivot Tool"
    description = "Groups a dataframe and pivots column values into columns"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#pivot-tool"

    parameters = {
        "columns": PerColumnArrayParam("Columns", ChoiceParam("Role", choices=["Index", "Pivot", "Expand", "Drop"]))
    }

    input_columns: Dict[str, np.dtype] = {}

    dataframes: List[pd.DataFrame] = []

    def prepare(self, sources: List[str], row_limit: Optional[int]=None):
        self.input_columns = {}

    def process(self, data: pd.DataFrame, source: str, logger: Logger):
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        self.input_columns.update(get_all_columns(data))

        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))
        index_cols = [col for col, param in column_parameters if param.value == "Index"]
        pivot_cols = [col for col, param in column_parameters if param.value == "Pivot"]
        expand_cols = [col for col, param in column_parameters if param.value == "Expand"]

        if not pivot_cols:
            return []

        df = pd.pivot_table(
            data,
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

        self.dataframes.append(df)
        return []

    def finalize(self, logger: Logger):
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))
        index_cols = [col for col, param in column_parameters if param.value == "Index"]
        if self.dataframes:
            df = pd.concat(self.dataframes).groupby(by=index_cols, group_keys=True).sum()
            self.dataframes = []
            yield df

        for p in self.parameters.values():
            p.set_column_choices(self.input_columns.keys())
