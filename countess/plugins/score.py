from math import log
from typing import Any, List, Optional, Union

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit

from countess import VERSION
from countess.core.parameters import (
    BooleanParam,
    ChoiceParam,
    ColumnChoiceParam,
    ColumnGroupChoiceParam,
    ColumnGroupOrNoneChoiceParam,
    StringParam,
)
from countess.core.plugins import DuckdbTransformPlugin


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


class ScoringPlugin(DuckdbTransformPlugin):
    name = "Scoring"
    description = "Score variants using counts or frequencies"
    version = VERSION

    variant = ColumnChoiceParam("Variant Column")
    replicate = ColumnChoiceParam("Replicate Column")
    columns = ColumnGroupChoiceParam("Input Columns")
    inputtype = ChoiceParam("Input Type", "counts", ["counts", "fractions"])
    log = BooleanParam("Use log(y+1)")
    normalize = BooleanParam("Normalize (scale Y so max(y) = 1)")
    xaxis = ColumnGroupOrNoneChoiceParam("X Axis Columns")
    output = StringParam("Score Column", "score")
    variance = StringParam("Variance Column", "")
    drop_input = BooleanParam("Drop Input Columns?", False)

    suffixes: Optional[list[str]] = None

    def output_columns(self) -> dict[str, str]:
        if self.variance:
            return { self.output.value: "DOUBLE", self.variance.value: "DOUBLE" }
        else:
            return { self.output.value: "DOUBLE" }

    def prepare(self, source):
        yaxis_prefix = self.columns.get_column_prefix()
        suffix_set = set([k.removeprefix(yaxis_prefix) for k in source.columns if k.startswith(yaxis_prefix)])

        if self.xaxis.is_not_none():
            xaxis_prefix = self.xaxis.get_column_prefix()
            suffix_set.update([k.removeprefix(xaxis_prefix) for k in source.columns if k.startswith(xaxis_prefix)])

        self.suffixes = sorted(suffix_set)

    def transform(self, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        if self.xaxis.is_not_none():
            xaxis_prefix = self.xaxis.get_column_prefix()
            x_values = [data.get(xaxis_prefix + s) for s in self.suffixes]
        else:
            x_values = [float_or_none(s) for s in self.suffixes]

        count_prefix = self.columns.get_column_prefix()
        y_values = [data.get(count_prefix + s) for s in self.suffixes]
        if any(y is None for y in y_values):
            return None

        if self.log:
            y_values = [log(y + 1) for y in y_values]
        if self.normalize:
            max_y = max(y_values)
            y_values = [y / max_y for y in y_values]

        x_values, y_values = zip(*[(x, y) for x, y in zip(x_values, y_values) if x > 0 or y > 0])
        if len(x_values) < len(self.suffixes) / 2 + 1:
            return None

        if self.variance:
            return score(x_values, y_values)
        else:
            s = score(x_values, y_values)
            return s[:1] if s else None
