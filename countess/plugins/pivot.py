import functools
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ChoiceParam, PerColumnArrayParam
from countess.core.plugins import PandasProcessPlugin
from countess.utils.pandas import get_all_columns


def _product(iterable):
    return functools.reduce(lambda x, y: x * y, iterable, 1)


class PivotPlugin(PandasProcessPlugin):
    """Groups a Pandas Dataframe by an arbitrary column and rolls up rows"""

    name = "Pivot Tool"
    description = "Groups a dataframe and pivots column values into columns."
    additional = """Expanded column values are duplicated for each combination of pivot values.
        Missing values default to 0, and duplicate values are aggregated."""
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#pivot-tool"

    parameters = {
        "columns": PerColumnArrayParam(
            "Columns", ChoiceParam("Role", "Drop", choices=["Index", "Pivot", "Expand", "Drop"])
        ),
        "aggfunc": ChoiceParam("Aggregation Function", "sum", choices=["sum", "mean", "min", "max"]),
    }

    input_columns: Dict[str, np.dtype] = {}

    dataframes: Optional[List[pd.DataFrame]] = None

    def prepare(self, sources: List[str], row_limit: Optional[int] = None):
        self.input_columns = {}
        self.dataframes = []

    def process(self, data: pd.DataFrame, source: str, logger: Logger):
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        assert self.dataframes is not None
        self.input_columns.update(get_all_columns(data))

        data.reset_index(drop=data.index.names == [None], inplace=True)

        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))
        index_cols = [col for col, param in column_parameters if param.value == "Index"]
        pivot_cols = [col for col, param in column_parameters if param.value == "Pivot"]
        expand_cols = [col for col, param in column_parameters if param.value == "Expand"]

        if not index_cols:
            logger.warning("No columns to index!")

        if not expand_cols:
            logger.warning("No columns to expand!")

        if not pivot_cols:
            logger.error("No columns to pivot on!")
            return []

        for ec in expand_cols:
            if not is_numeric_dtype(data[ec]):
                logger.warning(f"Expanding non-numeric column {ec}")

        n_pivot = _product(data[pc].nunique() for pc in pivot_cols) * len(expand_cols)
        if n_pivot > 200:
            pivot_cols_str = ", ".join(pivot_cols)
            logger.error(f"Too many pivot combinations on {pivot_cols_str} ({n_pivot})")
            return []

        df = pd.pivot_table(
            data,
            values=expand_cols,
            index=index_cols,
            columns=pivot_cols,
            aggfunc=self.parameters["aggfunc"].value,
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
            df = pd.concat(self.dataframes)
            self.dataframes = []
            if index_cols:
                df = df.groupby(by=index_cols, group_keys=True).sum()
            yield df

        for p in self.parameters.values():
            p.set_column_choices(self.input_columns.keys())
