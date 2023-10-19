from typing import Iterable, List

import numpy as np
import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, ChoiceParam, ColumnOrNoneChoiceParam, IntegerParam, PerColumnArrayParam, TabularMultiParam
from countess.core.plugins import PandasProcessPlugin
from countess.utils.pandas import flatten_columns, get_all_columns


class CollatePlugin(PandasProcessPlugin):
    """Collates, sorts and selects data"""

    name = "Collate"
    description = "Collate and sort records by column(s), taking the first N"
    version = VERSION

    input_columns: dict[str, np.dtype]

    parameters = {
        "columns": PerColumnArrayParam(
            "Columns",
            ChoiceParam("Role", choices=["—", "Group", "Sort (Asc)", "Sort (Desc)"])
        ),
        "limit": IntegerParam("First N records", 0),
    }

    dataframes: List[pd.DataFrame]

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.prepare()

    def prepare(self, *_):
        self.dataframes = []
        self.input_columns = {}

    def process(self, data: pd.DataFrame, source: str, logger: Logger) -> Iterable:
        # XXX should do this in two stages: group each dataframe and then combine.
        # that can wait for a more general MapReduceFinalizePlugin class though.
        assert self.dataframes is not None

        self.dataframes.append(data)
        self.input_columns.update(get_all_columns(data))
        return []

    def sort_and_limit(self, df):
        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))
        sort_cols = [col for col, param in column_parameters if param.value.startswith("Sort")]
        sort_dirs = [
            param.value.endswith("(Asc)")
            for param in self.parameters["columns"]
            if param.value.startswith("Sort")
        ]

        df = df.sort_values(by=sort_cols, ascending=sort_dirs)
        if self.parameters["limit"].value > 0:
            df = df.head(self.parameters["limit"].value)
        return df

    def finalize(self, logger: Logger) -> Iterable[pd.DataFrame]:
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        assert self.dataframes

        self.parameters["columns"].set_column_choices(self.input_columns.keys())

        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))
        group_cols = [col for col, param in column_parameters if param.value == "Group"]

        df = pd.concat(self.dataframes)

        try:
            if group_cols:
                df = df.groupby(group_cols, group_keys=False).apply(self.sort_and_limit)
            else:
                df = self.sort_and_limit(df)

            print(f">>>>> {type(df)}\n{df}")

            yield df
        except ValueError as exc:
            logger.exception(exc)
