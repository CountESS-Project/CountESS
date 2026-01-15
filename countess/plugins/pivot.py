import logging
from typing import Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import BooleanParam, ChoiceParam, PerColumnArrayParam
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_choose_special, duckdb_escape_identifier, duckdb_escape_literal, duckdb_dtype_is_numeric

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

        column_is_numeric = {
            name: duckdb_dtype_is_numeric(dt)
            for name, dt in zip(source.columns, source.dtypes)
            if name in expand_cols
        }

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

        # produces clauses like `SUM("foo") AS "~foo"` and the pivot will
        # then make columns like "value_~foo" (short names) or "column_value__~foo"
        # (long names)
        using_str = ", ".join(
            "%s(%s) AS %s" % (
                self.aggfunc.value if column_is_numeric[ec] else 'string_agg',
                duckdb_escape_identifier(ec),
                duckdb_escape_identifier(pivot_char + ec)
            )
            for ec in expand_cols
        )
        group_str = ", ".join(duckdb_escape_identifier(ic) for ic in index_cols)

        query_str = f"PIVOT {source.alias} ON {pivot_str} USING {using_str}"
        if group_str:
            query_str = f"{query_str} GROUP BY {group_str}"

        logger.debug("PivotPlugin.execute query_str %s", query_str)
        rel = ddbc.sql(query_str)

        # Rather than keep the duckdb columns we match them and switch them to our 
        # preferred format.
        project_str = ', '.join(
            ("COALESCE(" if self.default_0 and column_is_numeric[ec] else "") +
            "COLUMNS(" + duckdb_escape_literal('(.*)_'+pivot_char+ec) + ")" +
            (", 0)" if self.default_0 and column_is_numeric[ec] else "") +
            " AS " + duckdb_escape_literal(ec + ("_" if self.short_names else "__") + r"\1")
            for ec in expand_cols
        )

        if index_cols:
            project_str = (", ".join([duckdb_escape_identifier(ic) for ic in index_cols])) + ", " + project_str

        logger.debug("PivotPlugin.execute project_str %s", project_str)
        logger.debug("PivotPlugin.execute project columns %s", rel.columns)

        return rel.project(project_str)
