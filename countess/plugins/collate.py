import logging
from typing import Iterable, List

import numpy as np
import pandas as pd

from countess import VERSION
from countess.core.parameters import ChoiceParam, IntegerParam, PerColumnArrayParam
from countess.core.plugins import PandasProcessPlugin
from countess.utils.pandas import get_all_columns

logger = logging.getLogger(__name__)


class CollatePlugin(PandasProcessPlugin):
    """Collates, sorts and selects data"""

    name = "Collate"
    description = "Collate and sort records by column(s), taking the first N"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#collate"

    COLUMN_CHOICES = ["—", "Group", "Sort (Asc)", "Sort (Desc)", "Drop"]

    columns = PerColumnArrayParam("Columns", ChoiceParam("Role", choices=COLUMN_CHOICES))
    limit = IntegerParam("First N records", 0)

    dataframes: List[pd.DataFrame]

    def prepare(self, *_):
        self.dataframes = []

    def process(self, data: pd.DataFrame, source: str) -> Iterable:
        # XXX need a more general MapReduceFinalizePlugin class though.
        assert self.dataframes is not None

        drop_cols = {p.label: np.NAN for p in self.columns if p.value == "Drop"}
        self.dataframes.append(data.assign(**drop_cols))
        return []

    def finalize(self) -> Iterable[pd.DataFrame]:
        assert self.dataframes

        df = pd.concat(self.dataframes)
        input_columns = get_all_columns(df).keys()
        self.columns.set_column_choices(input_columns)

        drop_cols = [p.label for p in self.columns if p.value == "Drop"]
        if drop_cols:
            df.drop(columns=drop_cols, inplace=True)

        group_cols = [p.label for p in self.columns if p.value == "Group"]
        sort_cols = {p.label: p.value.endswith("(Asc)") for p in self.columns if p.value.startswith("Sort")}

        def sort_and_limit(df: pd.DataFrame) -> pd.DataFrame:
            if sort_cols:
                df = df.sort_values(by=list(sort_cols.keys()), ascending=list(sort_cols.values()))
            if self.limit > 0:
                df = df.head(self.limit.value)
            return df

        try:
            if group_cols:
                df = df.groupby(group_cols, group_keys=False).apply(sort_and_limit)
            else:
                df = sort_and_limit(df)

            yield df
        except ValueError as exc:
            logger.error("Exception", exc_info=exc)
