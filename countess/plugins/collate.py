from typing import Iterable, List

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ChoiceParam, IntegerParam, PerColumnArrayParam
from countess.core.plugins import PandasProcessPlugin
from countess.utils.pandas import get_all_columns


class CollatePlugin(PandasProcessPlugin):
    """Collates, sorts and selects data"""

    name = "Collate"
    description = "Collate and sort records by column(s), taking the first N"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-p  lugins/#collate"

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

    def finalize(self, logger: Logger) -> Iterable[pd.DataFrame]:
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        assert self.dataframes

        df = pd.concat(self.dataframes)
        input_columns = get_all_columns(df).keys()
        self.parameters["columns"].set_column_choices(input_columns)
        column_parameters = list(zip(input_columns, self.parameters["columns"]))
        group_cols = [col for col, param in column_parameters if param.value == "Group"]
        sort_cols = {
            col: param.value.endswith("(Asc)") for col, param in column_parameters if param.value.startswith("Sort")
        }

        def sort_and_limit(df: pd.DataFrame) -> pd.DataFrame:
            df = df.sort_values(by=list(sort_cols.keys()), ascending=list(sort_cols.values()))
            if self.parameters["limit"].value > 0:
                df = df.head(self.parameters["limit"].value)
            return df

        try:
            if group_cols:
                df = df.groupby(group_cols, group_keys=False).apply(sort_and_limit)
            else:
                df = sort_and_limit(df)

            yield df
        except ValueError as exc:
            logger.exception(exc)
