import functools
import logging
from typing import Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import ChoiceParam, PerColumnArrayParam
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import (
    duckdb_choose_special,
    duckdb_escape_identifier,
    duckdb_escape_literal,
    duckdb_source_to_view,
)

logger = logging.getLogger(__name__)


class PivotPlugin(DuckdbSimplePlugin):
    """Groups a Pandas Dataframe by an arbitrary column and rolls up rows"""

    name = "Pivot Tool"
    description = "Pivots column values into columns."
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#pivot-tool"

    columns = PerColumnArrayParam("Columns", ChoiceParam("Role", "Drop", choices=["Index", "Pivot", "Expand", "Drop"]))
    aggfunc = ChoiceParam("Aggregation Function", "sum", choices=["sum", "mean", "median", "min", "max"])

    def execute(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> Optional[DuckDBPyRelation]:
        if source is None:
            return None
        index_cols = [p.label for p in self.columns if p.value == "Index" and p.label in source.columns]
        pivot_cols = [p.label for p in self.columns if p.value == "Pivot" and p.label in source.columns]
        expand_cols = [p.label for p in self.columns if p.value == "Expand" and p.label in source.columns]

        logger.debug("PivotPlugin.execute index_cols %s", index_cols)
        logger.debug("PivotPlugin.execute pivot_cols %s", pivot_cols)
        logger.debug("PivotPlugin.execute expand_cols %s", expand_cols)
        if not expand_cols or not pivot_cols:
            return source

        # Override the default pivoted column naming convention with
        # our own custom one ... this is what previous CountESS used
        # but it frankly could be better or customizable.
        pivot_str = " || '__' || ".join(
            duckdb_escape_literal(pc + "_") + " || " + duckdb_escape_identifier(pc) for pc in pivot_cols
        )

        # Pick an arbitrary character that isn't in any of the
        # index column or expand column names so we can
        # definitively pick out the pivot output columns later ...
        pivot_char = duckdb_choose_special(index_cols + expand_cols)

        using_str = ", ".join(
            "%s(%s) AS %s"
            % (self.aggfunc.value, duckdb_escape_identifier(ec), duckdb_escape_identifier(pivot_char + ec))
            for ec in expand_cols
        )
        group_str = ", ".join(duckdb_escape_identifier(ic) for ic in index_cols)

        query_str = f"PIVOT {source.alias} ON {pivot_str} USING {using_str}"
        if group_str:
            query_str = f"{query_str} GROUP BY {group_str}"

        logger.debug("PivotPlugin.execute query_str %s", query_str)

        # Rename the columns to match the old CountESS naming,
        # with the original column at the start instead of the end.
        project_str = ", ".join(
            [duckdb_escape_identifier(ic) for ic in index_cols] + [f"COLUMNS('(.*)_{pivot_char}(.*)') AS '\\2__\\1'"]
        )
        logger.debug("PivotPlugin.execute project_str %s", project_str)

        return ddbc.sql(query_str).project(project_str)
