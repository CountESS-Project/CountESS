import logging
from typing import Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import BooleanParam, ChoiceParam, PerColumnArrayParam
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_choose_special, duckdb_escape_identifier, duckdb_escape_literal

logger = logging.getLogger(__name__)


class PivotPlugin(DuckdbSimplePlugin):
    """Pivots columns by other columns."""

    name = "Pivot Tool"
    description = "Pivots column values into columns."
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#pivot-tool"

    columns = PerColumnArrayParam("Columns", ChoiceParam("Role", "Drop", choices=["Index", "Pivot", "Expand", "Drop"]))
    aggfunc = ChoiceParam("Aggregation Function", "sum", choices=["sum", "mean", "median", "min", "max"])
    default_0 = BooleanParam("Default value 0?", False)
    short_names = BooleanParam("Short Output Names?", False)

    def execute(
        self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation], row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        if source is None:
            return None
        index_cols = [p.label for p in self.columns if p.value == "Index" and p.label in source.columns]
        pivot_cols = [p.label for p in self.columns if p.value == "Pivot" and p.label in source.columns]
        expand_cols = [p.label for p in self.columns if p.value == "Expand" and p.label in source.columns]

        logger.debug("PivotPlugin.execute columns %s", source.columns)
        logger.debug("PivotPlugin.execute index_cols %s", index_cols)
        logger.debug("PivotPlugin.execute pivot_cols %s", pivot_cols)
        logger.debug("PivotPlugin.execute expand_cols %s", expand_cols)
        if not expand_cols or not pivot_cols:
            return source

        # Override the default pivoted column naming convention with
        # our own custom one ... this is what previous CountESS used
        # but it frankly could be better or customizable.
        if self.short_names:
            pivot_str = " || '_' || ".join(duckdb_escape_identifier(pc) for pc in pivot_cols)
        else:
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
        rel = ddbc.sql(query_str)

        project_str = f"COLUMNS('(.*)_{pivot_char}(.*)')"
        if self.default_0:
            project_str = f"COALESCE({project_str}, 0)"

        if self.short_names:
            project_str += " AS '\\2_\\1'"
        else:
            project_str += " AS '\\2__\\1'"

        if index_cols:
            project_str = (", ".join([duckdb_escape_identifier(ic) for ic in index_cols])) + ", " + project_str

        logger.debug("PivotPlugin.execute project_str %s", project_str)
        logger.debug("PivotPlugin.execute project columns %s", rel.columns)

        return rel.project(project_str)
