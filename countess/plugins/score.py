import logging
import secrets
from math import log, sqrt
from typing import Optional

import statsmodels.api as statsmodels_api
from duckdb import DuckDBPyConnection, DuckDBPyRelation, NotImplementedException
from duckdb.sqltypes import DuckDBPyType

from countess import VERSION
from countess.core.parameters import BooleanParam, ColumnGroupChoiceParam, ColumnOrNoneChoiceParam, StringParam
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import (
    duckdb_dtype_is_boolean,
    duckdb_dtype_is_numeric,
    duckdb_escape_identifier,
    duckdb_escape_literal,
    duckdb_source_to_view,
)

logger = logging.getLogger(__name__)


def score_function(times: list[float], counts: list[float], populations: list[float]) -> Optional[tuple[float, float]]:
    """
    Called with three equal-length lists of floats:
    * `times` is the time points
    * `counts` is the variant count at that point
    * `populations` is the population count at that point

    Apply the formulae from the Enrich2 paper and then perform
    a linear least squares regression to xs and ys with option weights ws
    and return the 'slope' and the stderr of slope.
    """

    if None in times or None in counts or None in populations:
        return None
    assert len(times) == len(counts) == len(populations) > 1

    if len(times) == 2:
        # 2 time point estimate
        score = log((counts[1] + 0.5) / (populations[1] + 0.5)) - log((counts[0] + 0.5) / (populations[0] + 0.5))
        sigma = sqrt(
            (1 / (counts[0] + 0.5) + 1 / (counts[1] + 0.5) + 1 / (populations[0] + 0.5) + 1 / (populations[1] + 0.5))
        )
    else:
        # multi time point least squares regression
        xs = [[t, 1.0] for t in times]
        ys = [log((c + 0.5) / (p + 0.5)) for c, p in zip(counts, populations)]
        ws = [1 / ((1 / (c + 0.5)) + (1 / (p + 0.5))) for c, p in zip(counts, populations)]
        fit = statsmodels_api.WLS(ys, xs, ws).fit()
        # we only care about the slope, not the "intercept" of the line.
        score = fit.params[0]
        sigma = fit.bse[0]

    logger.debug("scoring: %s %s %s => %f %f", times, counts, populations, score, sigma)
    return score, sigma


class ScoringPlugin(DuckdbSimplePlugin):
    name = "Scoring"
    description = "Score variants using counts or frequencies"
    version = VERSION

    replicate = ColumnOrNoneChoiceParam("Replicate Column")
    columns = ColumnGroupChoiceParam("Input Columns")
    wildtype = ColumnOrNoneChoiceParam("Wildtype Indicator")
    output = StringParam("Score Column", "score")
    stddev = StringParam("Standard Deviation Column", "sigma")
    drop_input = BooleanParam("Drop Input Columns?", False)

    function_name = "f_" + secrets.token_hex(16)

    def prepare(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> None:
        super().prepare(ddbc, source)

        try:
            ddbc.create_function(  # type: ignore[call-overload]
                self.function_name,
                score_function,
                return_type=DuckDBPyType("DOUBLE[]"),
                null_handling="special",
            )
        except NotImplementedException:
            # trying to create the function which already exists
            pass

    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        view = duckdb_source_to_view(ddbc, source)

        yaxis_prefix = self.columns.get_column_prefix()
        suffix_set = {k.removeprefix(yaxis_prefix) for k in source.columns if k.startswith(yaxis_prefix)}
        suffixes = sorted(suffix_set)

        count_cols = [duckdb_escape_identifier(yaxis_prefix + suffix) for suffix in suffixes]

        x_values = [float(x) for x in suffixes]
        x_max = max(x_values)
        x_cols = [duckdb_escape_literal(x / x_max) for n, x in enumerate(x_values)]

        where_clause = ""
        if self.wildtype.is_not_none():
            wtcol = duckdb_escape_identifier(self.wildtype.value)
            for col, dtype in zip(source.columns, source.dtypes):
                if col == self.wildtype.value:
                    if duckdb_dtype_is_boolean(dtype) or duckdb_dtype_is_numeric(dtype):
                        where_clause = f"WHERE {wtcol}"
                    else:
                        where_clause = f"WHERE {wtcol} IN ('W', 'p.=', 'g.=')"

        if self.drop_input:
            keep_fields = ",".join(
                [
                    f"A.{duckdb_escape_identifier(f)}"
                    for f in view.columns
                    if duckdb_escape_identifier(f) not in count_cols
                ]
            )
        else:
            keep_fields = "A.*"

        if self.replicate.is_none():
            join_op = "CROSS JOIN"
            repl_id = ""
            group_clause = ""
            join_using = ""
        else:
            join_op = "JOIN"
            repl_id = duckdb_escape_identifier(self.replicate.value)
            group_clause = f"GROUP BY {repl_id}"
            join_using = f"USING ({repl_id})"

        view = duckdb_source_to_view(ddbc, source)

        query = f"""
            SELECT * EXCLUDE _SCORE,
                _SCORE[1] AS {duckdb_escape_identifier(self.output.value)},
                _SCORE[2] AS {duckdb_escape_identifier(self.stddev.value)}
            FROM (
                SELECT {keep_fields}, {self.function_name}(
                    [{', '.join(x_cols)}],
                    [{', '.join("A." + c for c in count_cols)}],
                    [{', '.join("B." + c for c in count_cols)}]
                ) AS _SCORE FROM {view.alias} AS A {join_op} (
                    SELECT {repl_id + ", " if repl_id else ""}
                    {', '.join(f"SUM({c}) AS {c}" for c in count_cols)}
                    FROM {view.alias} {where_clause} {group_clause}
                ) AS B {join_using}
            )
        """

        return ddbc.sql(query)
