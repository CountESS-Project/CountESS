import logging
from math import log
import secrets
from typing import Any, List, Optional

import statsmodels.api as statsmodels_api
from duckdb import DuckDBPyConnection, DuckDBPyRelation
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
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_source_to_view, duckdb_escape_literal

logger = logging.getLogger(__name__)


def score(xs: list[float], ys: list[float], ws: Optional[list[float]|float] = 1.0) -> Optional[tuple[float, float]]:
    """
    Apply a linear least squares regression to xs and ys with option weights ws 
    and return the 'slope' and the stderr of slope.
    """

    if None in xs or None in ys or None is ws:
        return None

    # add constant element (we later ignore this coefficient)
    xs = [ [ x, 1.0 ] for x in xs ]

    # do weighted regression, if ws == 1 then this is same as unweighted.
    fit = statsmodels_api.WLS(ys, xs, ws).fit()

    logger.debug("scoring: %s %s %s => %f %f", xs, ys, ws, fit.params[0], fit.bse[0])

    # return just the slope and stderr of slope.
    return fit.params[0], fit.bse[0]


class ScoringPlugin(DuckdbSimplePlugin):
    name = "Scoring"
    description = "Score variants using counts or frequencies"
    version = VERSION

    variant = ColumnChoiceParam("Variant Column")
    replicate = ColumnOrNoneChoiceParam("Replicate Column")
    columns = ColumnGroupChoiceParam("Input Columns")
    variances = ColumnGroupOrNoneChoiceParam("Variance Columns")
    wildtype = ColumnOrNoneChoiceParam("Wildtype Indicator")

    log = BooleanParam("Use log(y)")
    normalize = BooleanParam("Normalize (scale X so max(x) = 1)")
    xaxis = ColumnGroupOrNoneChoiceParam("X Axis Columns")
    wildtype = ColumnOrNoneChoiceParam("Wildtype Indicator Column")
    output = StringParam("Score Column", "score")
    variance = StringParam("Variance Column", "")
    drop_input = BooleanParam("Drop Input Columns?", False)

    def execute(self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        view = duckdb_source_to_view(ddbc, source)

        yaxis_prefix = self.columns.get_column_prefix()
        suffix_set = {k.removeprefix(yaxis_prefix) for k in source.columns if k.startswith(yaxis_prefix)}

        if self.xaxis.is_not_none():
            xaxis_prefix = self.xaxis.get_column_prefix()
            suffix_set.update([k.removeprefix(xaxis_prefix) for k in source.columns if k.startswith(xaxis_prefix)])

        suffixes = sorted(suffix_set)

        variances_prefix = self.variances.get_column_prefix()

        y_columns = [ duckdb_escape_identifier(yaxis_prefix+suffix) for suffix in suffixes ]

        wildtype_clause = f"WHERE {duckdb_escape_identifier(self.wildtype.value)} IN ('W','p.=')" if self.wildtype.is_not_none() else ""

        if self.xaxis.is_not_none():
            x_columns = [ duckdb_escape_identifier(xaxis_prefix+suffix) for suffix in suffixes ]
            if self.normalize:
                x_fields = [ f"A.{x} / C.MAX" for n, x in enumerate(x_columns)]
            else:
                x_fields = [ f"A.{x}" for n, x in enumerate(x_columns)]
        else:
            x_values = [ float(x) for x in suffixes ]
            x_max = max(x_values) if self.normalize else 1.0
            x_fields = [ f"{duckdb_escape_literal(x / x_max)}" for n, x in enumerate(x_values) ]
            x_columns = []

        if self.variances.is_not_none():
            v_columns = [ duckdb_escape_identifier(variances_prefix+suffix) for suffix in suffixes ]
            w_fields = [ f"1 / A.{v}" for n, v in enumerate(v_columns) ]
        else:
            w_fields = [ "1" for _ in suffixes ]
            v_columns = []

        if self.log:
            y_fields = [ f"log(A.{y} / B._{n})" for n, y in enumerate(y_columns) ]
        else:
            y_fields = [ f"A.{y} / B._{n}" for n, y in enumerate(y_columns)]

        y_sum_fields = [ f"SUM({y}) AS _{n}" for n, y in enumerate(y_columns)]

        if self.replicate.is_none():
            sums_query = f"SELECT {', '.join(y_sum_fields)} FROM {view.alias} {wildtype_clause}"
            join_on = "ON (1=1)"
        else:
            replicate_field = duckdb_escape_identifier(self.replicate.value)
            sums_query = f"SELECT {replicate_field}, {', '.join(y_sum_fields)} FROM {view.alias} {wildtype_clause} GROUP BY {replicate_field}"
            join_on = f"USING ({replicate_field})"

        if self.drop_input:
            drop_fields = set(x_columns + y_columns + v_columns)
            keep_fields = ','.join([
                f"A.{duckdb_escape_identifier(f)}" 
                for f in view.columns
                if duckdb_escape_identifier(f) not in drop_fields
            ])
        else:
            keep_fields = "A.*"

        return_type = { 'score': 'float', 'variance': 'float' }
        function_name = "f_" + secrets.token_hex(16)
        ddbc.create_function(
            function_name,
            score,
            return_type=return_type,
            null_handling="special",  # type: ignore[arg-type]
        )
        view = duckdb_source_to_view(ddbc, source)

        query = f"SELECT {keep_fields}, UNNEST({function_name}([{', '.join(x_fields)}], [{', '.join(y_fields)}], [{', '.join(w_fields)}])) FROM {view.alias} AS A JOIN ({sums_query}) AS B {join_on}"

        if self.xaxis.is_not_none() and self.normalize:
            x_max_fields = [ f"X({x}) AS _{n}" for n, x in enumerate(x_columns) ]
            if self.replicate.is_none():
                query += f" CROSS JOIN (SELECT MAX(GREATEST({','.join(x_columns)})) AS MAX FROM {view.alias}) AS C"
            else:
                replicate_field = duckdb_escape_identifier(self.replicate.value)
                query += f" JOIN (SELECT {replicate_field}, MAX(GREATEST({','.join(x_columns)})) AS MAX FROM {view.alias} GROUP BY {replicate_field}) AS C USING ({replicate_field})"

        print(query)

        return ddbc.sql(query)
