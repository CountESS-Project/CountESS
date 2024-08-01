from math import log
from typing import Any, List, Optional, Union

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (
    BooleanParam,
    ChoiceParam,
    ColumnChoiceParam,
    ColumnGroupChoiceParam,
    ColumnGroupOrNoneChoiceParam,
    StringParam,
)
from countess.core.plugins import PandasConcatProcessPlugin


def float_or_none(s: Any) -> Optional[float]:
    try:
        return float(s)
    except ValueError:
        return None


def func(x: Union[float, np.ndarray], a: float, b: float) -> Union[float, np.ndarray]:
    return a * x + b


def score(xs: list[float], ys: list[float]) -> Optional[tuple[float, float]]:
    if len(xs) < 2:
        return None
    try:
        popt, pcov, *_ = curve_fit(func, xs, ys, bounds=(-5, 5))
        return popt[0], pcov[0][0]  # type: ignore  # mypy: ignore index
    except (ValueError, TypeError):
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
        "log": BooleanParam("Use log(y+1)"),
        "normalize": BooleanParam("Normalize (scale Y so max(y) = 1)"),
        "xaxis": ColumnGroupOrNoneChoiceParam("X Axis Columns"),
        "output": StringParam("Score Column", "score"),
        "variance": StringParam("Variance Column", ""),
    }

    def process_row(
        self, row, count_prefix: str, xaxis_prefix: str, suffixes: List[str]
    ) -> Union[float, tuple[float, float], None]:
        if xaxis_prefix:
            x_values = [row.get(xaxis_prefix + s) for s in suffixes]
        else:
            x_values = [float_or_none(s) for s in suffixes]

        y_values = [row.get(count_prefix + s) for s in suffixes]
        if any(y is None for y in y_values):
            return None

        if self.parameters["log"].value:
            y_values = [log(y + 1) for y in y_values]
        if self.parameters["normalize"].value:
            max_y = max(y_values)
            y_values = [y / max_y for y in y_values]

        x_values, y_values = zip(*[(x, y) for x, y in zip(x_values, y_values) if x > 0 or y > 0])
        if len(x_values) < len(suffixes) / 2 + 1:
            return None

        if self.parameters["variance"].value:
            return score(x_values, y_values)
        else:
            s = score(x_values, y_values)
            return s[0] if s else None

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

        output = dataframe.apply(
            self.process_row,
            axis="columns",
            result_type="expand",
            count_prefix=self.parameters["columns"].get_column_prefix(),
            xaxis_prefix=self.parameters["xaxis"].get_column_prefix(),
            suffixes=suffixes,
        )

        if self.parameters["variance"].value:
            dataframe[[self.parameters["output"].value, self.parameters["variance"].value]] = output
        else:
            dataframe[self.parameters["output"].value] = output

        return dataframe
