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
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_source_to_view, duckdb_escape_literal, duckdb_dtype_is_boolean, duckdb_dtype_is_numeric

logger = logging.getLogger(__name__)


def score(times: list[float], counts: list[float], populations: list[float]) -> Optional[tuple[float, float]]:
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

    # add constant element (we later ignore this coefficient)
    xs = [ [ t, 1.0 ] for t in times ]
    ys = [ log((c + 0.5) / (p + 0.5)) for c, p in zip(counts, populations) ]
    ws = [ 1 / ((1 / (c+0.5)) + (1 / (p + 0.5))) for c, p in zip(counts, populations) ]

    fit = statsmodels_api.WLS(ys, xs, ws).fit()

    logger.debug("scoring: %s %s %s => %f %f", xs, ys, ws, fit.params[0], fit.bse[0])

    # return just the slope and stderr of slope.
    return fit.params[0], fit.bse[0]


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

    def execute(self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        view = duckdb_source_to_view(ddbc, source)

        yaxis_prefix = self.columns.get_column_prefix()
        suffix_set = {k.removeprefix(yaxis_prefix) for k in source.columns if k.startswith(yaxis_prefix)}
        suffixes = sorted(suffix_set)

        count_cols = [ duckdb_escape_identifier(yaxis_prefix+suffix) for suffix in suffixes ]

        x_values = [ float(x) for x in suffixes ]
        x_max = max(x_values)
        x_cols = [ duckdb_escape_literal(x / x_max) for n, x in enumerate(x_values) ]

        where_clause = ""
        if self.wildtype.is_not_none():
            wtcol = duckdb_escape_identifier(self.wildtype.value)
            for col, dtype in zip(source.columns, source.dtypes):
                if col == self.wildtype.value:
                    if duckdb_dtype_is_boolean(dtype) or duckdb_dtype_is_numeric(dtype):
                        where_clause = f"WHERE {wtcol}"
                    else:
                        where_clause = f"WHERE {wtcol} IN ('W', 'p.=')"

        if self.drop_input:
            keep_fields = ','.join([
                f"A.{duckdb_escape_identifier(f)}"
                for f in view.columns
                if duckdb_escape_identifier(f) not in count_cols
            ])
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

        return_type = { self.output.value: 'float', self.stddev.value: 'float' }
        function_name = "f_" + secrets.token_hex(16)
        ddbc.create_function(
            function_name,
            score,
            return_type=return_type,
            null_handling="special",  # type: ignore[arg-type]
        )
        view = duckdb_source_to_view(ddbc, source)

        query = f"""
            SELECT {keep_fields}, UNNEST({function_name}(
                [{', '.join(x_cols)}],
                [{', '.join("A." + c for c in count_cols)}],
                [{', '.join("B." + c for c in count_cols)}]
            )) FROM {view.alias} AS A {join_op} (
                SELECT {repl_id + ", " if repl_id else ""}
                {', '.join(f"SUM({c}) AS {c}" for c in count_cols)}
                FROM {view.alias} {where_clause} {group_clause}
            ) AS B {join_using}
        """

        return ddbc.sql(query)
