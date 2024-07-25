from typing import List, Optional

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (
    ChoiceParam,
    ColumnChoiceParam,
    ColumnGroupChoiceParam,
    ColumnGroupOrNoneChoiceParam,
    StringParam,
)
from countess.core.plugins import PandasConcatProcessPlugin


def float_or_none(s):
    try:
        return float(s)
    except ValueError:
        return None


def func(x, a, b):
    return (2 * a - 1) * np.log2(x + 1) + b


def score(xs: list[float], ys: list[float]) -> Optional[float]:
    try:
        popt, *_ = curve_fit(func, xs, ys, bounds=(0, 1))
        return popt[0]
    except ValueError:
        return None


class ScoringPlugin(PandasConcatProcessPlugin):
    name = "Scoring"
    description = "Score variants using counts or frequencies"
    version = VERSION

    parameters = {
        "variant": ColumnChoiceParam("Variant Column"),
        "replicate": ColumnChoiceParam("Replicate Column"),
        "columns": ColumnGroupChoiceParam("Input Columns"),
        "inputtype": ChoiceParam("Input Type", "counts", ["counts", "fractions"]),
        "xaxis": ColumnGroupOrNoneChoiceParam("X Axis Columns"),
        "output": StringParam("Score Column", "score"),
    }

    def process_row(self, row, count_prefix: str, xaxis_prefix: str, suffixes: List[str]) -> Optional[float]:
        y_values = [row.get(count_prefix + s) for s in suffixes]
        if xaxis_prefix:
            x_values = [row.get(xaxis_prefix + s) for s in suffixes]
        else:
            x_values = [float_or_none(s) for s in suffixes]

        return score(x_values, y_values)

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> Optional[pd.DataFrame]:
        assert isinstance(self.parameters["variant"], ColumnChoiceParam)
        assert isinstance(self.parameters["replicate"], ColumnChoiceParam)
        assert isinstance(self.parameters["columns"], ColumnGroupChoiceParam)
        assert isinstance(self.parameters["xaxis"], ColumnGroupOrNoneChoiceParam)

        variant_col = self.parameters["variant"].value
        replicate_col = self.parameters["replicate"].value
        count_cols = self.parameters["columns"].get_column_names(dataframe)
        is_counts = self.parameters["inputtype"].value == "counts"
        output = self.parameters["output"].value

        if variant_col and replicate_col and is_counts:
            # convert counts to frequencies by finding totals
            dataframe = dataframe.set_index([variant_col, replicate_col])

            totals_df = dataframe.groupby(by=replicate_col).agg({c: "sum" for c in count_cols})
            dataframe[count_cols] = dataframe[count_cols].div(totals_df, level=replicate_col)

        suffix_set = set(self.parameters["columns"].get_column_suffixes(dataframe))
        if self.parameters["xaxis"].is_not_none():
            suffix_set.update(self.parameters["xaxis"].get_column_suffixes(dataframe))
        suffixes = sorted(suffix_set)

        dataframe[output] = dataframe.apply(
            self.process_row,
            axis="columns",
            count_prefix=self.parameters["columns"].get_column_prefix(),
            xaxis_prefix=self.parameters["xaxis"].get_column_prefix(),
            suffixes=suffixes,
        )

        return dataframe
