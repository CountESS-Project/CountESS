import logging
from math import log
from typing import Any, List, Optional

import statsmodels.api as statsmodels_api

from countess import VERSION
from countess.core.parameters import (
    BooleanParam,
    ChoiceParam,
    ColumnChoiceParam,
    ColumnOrNoneChoiceParam,
    ColumnGroupChoiceParam,
    ColumnGroupOrNoneChoiceParam,
    StringParam,
)
from countess.core.plugins import DuckdbParallelTransformPlugin

logger = logging.getLogger(__name__)


def float_or_none(s: Any) -> Optional[float]:
    try:
        return float(s)
    except ValueError:
        return None


def score(xs: list[float], ys: list[float], ws: Optional[list[float]|float] = 1.0) -> Optional[tuple[float, float]]:
    """
    Apply a linear least squares regression to xs and ys with option weights ws 
    and return the 'slope' and the stderr of slope.
    """

    # add constant element (we later ignore this coefficient)
    xs = [ [ x, 1.0 ] for x in xs ]

    # do weighted regression, if ws == 1 then this is same as unweighted.
    fit = statsmodels_api.WLS(ys, xs, ws).fit()

    logger.debug("scoring: %s %s %s => %f %f", xs, ys, ws, fit.params[0], fit.bse[0])
    print("scoring: %s %s %s => %f %f" % (xs, ys, ws, fit.params[0], fit.bse[0]))

    # return just the slope and stderr of slope.
    return fit.params[0], fit.bse[0]


class ScoringPlugin(DuckdbParallelTransformPlugin):
    name = "Scoring"
    description = "Score variants using counts or frequencies"
    version = VERSION

    variant = ColumnChoiceParam("Variant Column")
    replicate = ColumnChoiceParam("Replicate Column")
    columns = ColumnGroupChoiceParam("Input Columns")
    variances = ColumnGroupOrNoneChoiceParam("Variance Columns")
    wildtype = ColumnOrNoneChoiceParam("Wildtype Indicator")

    log = BooleanParam("Use log(y)")
    normalize = BooleanParam("Normalize (scale X so max(x) = 1)")
    xaxis = ColumnGroupOrNoneChoiceParam("X Axis Columns")
    output = StringParam("Score Column", "score")
    variance = StringParam("Variance Column", "")
    drop_input = BooleanParam("Drop Input Columns?", False)

    suffixes: Optional[list[str]] = None

    def output_columns(self) -> dict[str, str]:
        if self.variance:
            return {self.output.value: "DOUBLE", self.variance.value: "DOUBLE"}
        else:
            return {self.output.value: "DOUBLE"}

    def prepare(self, ddbc, source):
        logger.debug("ScoringPlugin.prepare")
        super().prepare(ddbc, source)
        yaxis_prefix = self.columns.get_column_prefix()
        suffix_set = {k.removeprefix(yaxis_prefix) for k in source.columns if k.startswith(yaxis_prefix)}

        if self.xaxis.is_not_none():
            xaxis_prefix = self.xaxis.get_column_prefix()
            suffix_set.update([k.removeprefix(xaxis_prefix) for k in source.columns if k.startswith(xaxis_prefix)])

        self.suffixes = sorted(suffix_set)
        logger.debug("ScoringPlugin.prepare suffixes %s", self.suffixes)

#    def execute(self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
#    ) -> Optional[DuckDBPyRelation]:
#        yaxis_prefix = self.columns.get_column_prefix()
#        suffix_set = {k.removeprefix(yaxis_prefix) for k in source.columns if k.startswith(yaxis_prefix)}
#
#        if self.xaxis.is_not_none():
#            xaxis_prefix = self.xaxis.get_column_prefix()
#            suffix_set.update([k.removeprefix(xaxis_prefix) for k in source.columns if k.startswith(xaxis_prefix)])
#
#        self.suffixes = sorted(suffix_set)

    def add_fields(self):
        return {self.output.value: float, self.variance.value: float}

    def remove_fields(self, field_names: list[str]):
        if self.drop_input:
            return [
                name
                for name in field_names
                if name.startswith(self.columns.get_column_prefix())
                or (self.xaxis.is_not_none() and name.startswith(self.xaxis.get_column_prefix()))
            ]
        else:
            return []

    def transform(self, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        assert self.suffixes is not None

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
            y_values = [log(y) if y is not None and y > 0 else None for y in y_values]

        if self.normalize:
            max_x = max(x for x in x_values if x is not None)
            x_values = [x / max_x if x is not None else None for x in x_values]
            #max_y = max(y for y in y_values if y is not None)
            #y_values = [y / max_y if y is not None else None for y in y_values]

        if self.variances.is_not_none():
            variance_prefix = self.variances.get_column_prefix()
            w_values = [ 1 / data.get(variance_prefix + s) for s in self.suffixes]
        else:
            w_values = [ 1 for _ in x_values ]

        valid_x_values: List[float] = []
        valid_y_values: List[float] = []
        valid_w_values: List[float] = []
        for x, y, w in zip(x_values, y_values, w_values):
            if x is not None and y is not None and w is not None:
                valid_x_values.append(x)
                valid_y_values.append(y)
                valid_w_values.append(w)

        if len(valid_x_values) < len(self.suffixes) / 2 + 1:
            return None

        score_var = score(valid_x_values, valid_y_values, valid_w_values)
        if score_var is None:
            return None
        data[self.output.value] = score_var[0]
        if self.variance:
            data[self.variance.value] = score_var[1]

        if self.drop_input:
            for name in list(data.keys()):
                if name.startswith(self.columns.get_column_prefix()) or (
                    self.xaxis.is_not_none() and name.startswith(self.xaxis.get_column_prefix()) or (
                    self.variances.is_not_none() and name.startswith(self.variances.get_column_prefix()))
                ):
                    del data[name]

        return data
