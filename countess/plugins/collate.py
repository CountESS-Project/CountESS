from typing import Iterable, List

import numpy as np
import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (
    ChoiceParam,
    IntegerParam,
    PerColumnArrayParam,
)
from countess.core.plugins import PandasProcessPlugin
from countess.utils.pandas import get_all_columns


class CollatePlugin(PandasProcessPlugin):
    """Collates, sorts and selects data"""

    name = "Collate"
    description = "Collate and sort records by column(s), taking the first N"
    version = VERSION

    input_columns: dict[str, np.dtype]

    parameters = {
        "columns": PerColumnArrayParam(
            "Columns", ChoiceParam("Role", choices=["—", "Group", "Sort (Asc)", "Sort (Desc)"])
        ),
        "limit": IntegerParam("First N records", 0),
    }

    dataframes: List[pd.DataFrame]

    def prepare(self, *_):
        self.dataframes = []

    def process(self, data: pd.DataFrame, source: str, logger: Logger) -> Iterable:
        # XXX need a more general MapReduceFinalizePlugin class though.
        assert self.dataframes is not None

        self.dataframes.append(data)
        return []

    def sort_and_limit(self, df):
        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))
        sort_cols = [col for col, param in column_parameters if param.value.startswith("Sort")]
        sort_dirs = [
            param.value.endswith("(Asc)") for param in self.parameters["columns"] if param.value.startswith("Sort")
        ]

        df = df.sort_values(by=sort_cols, ascending=sort_dirs)
        if self.parameters["limit"].value > 0:
            df = df.head(self.parameters["limit"].value)
        return df

    def finalize(self, logger: Logger) -> Iterable[pd.DataFrame]:
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        assert self.dataframes

        df = pd.concat(self.dataframes)
        self.parameters["columns"].set_column_choices(get_all_columns(df).keys())

        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))
        group_cols = [col for col, param in column_parameters if param.value == "Group"]

        try:
            if group_cols:
                df = df.groupby(group_cols, group_keys=False).apply(self.sort_and_limit)
            else:
                df = self.sort_and_limit(df)

            print(f">>>>> {type(df)}\n{df}")

            yield df
        except ValueError as exc:
            logger.exception(exc)
