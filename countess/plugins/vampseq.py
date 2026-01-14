import logging
from typing import Iterable, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import (
        FloatParam,
        PerNumericColumnArrayParam,
        TabularMultiParam,
        PerColumnArrayParam,
        BooleanParam,
)
from countess.core.plugins import DuckdbSqlPlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal

logger = logging.getLogger(__name__)


class CountColumnParam(TabularMultiParam):
    weight = FloatParam("Weight")


class VampSeqScorePlugin(DuckdbSqlPlugin):
    name = "VAMP-seq Scoring"
    description = "Calculate scores from weighed bin counts"
    version = VERSION

    columns = PerNumericColumnArrayParam("Columns", CountColumnParam("Column"))
    group_by = PerColumnArrayParam("Group By", BooleanParam("Column", False))

    def prepare(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> None:
        super().prepare(ddbc, source)

        # set default values for weights on "count" columns
        if all(c.weight.value is None for c in self.columns):
            count_cols = [c for c in self.columns if c.label.startswith("count")]
            for n, c in enumerate(count_cols):
                c.weight.value = (n + 1) / len(count_cols)

        weight_cols = set(n for n, p in self.columns.get_column_params() if p.weight.value is not None)
        for n, p in self.group_by.get_column_params():
            if n in weight_cols:
                p.set_value(False)

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        group_cols = {
            duckdb_escape_identifier(name)
            for name, param in self.group_by.get_column_params()
            if param
        }
        weighted_columns = {
            duckdb_escape_identifier(name): duckdb_escape_literal(param.weight.value)
            for name, param in self.columns.get_column_params()
            if param.weight.value is not None
        }

        if not weighted_columns:
            return None

        inner_select = ", ".join(
            [ f"T0.{k}" for k in group_cols ] +
            [ f"sum(T0.{k}) as {k}" for k in weighted_columns.keys() ]
        )
        weighted_counts = " + ".join(
            f"CASE WHEN T2.{k} > 0 THEN T1.{k} * {v} / T2.{k} ELSE 0 END" for k, v in weighted_columns.items()
        )
        total_counts = " + ".join(
            f"CASE WHEN T2.{k} > 0 THEN T1.{k} / T2.{k} ELSE 0 END" for k in weighted_columns.keys()
        )
        group_by = ("GROUP BY " + ", ".join("T0." + c for c in group_cols)) if group_cols else ""
        join_on = (" AND ".join(f"T1.{c} = T2.{c}" for c in group_cols)) if group_cols else "1=1"

        return f"""
            select T1.*, ({weighted_counts}) / ({total_counts}) as score
            from {table_name} T1 join (
                select {inner_select}
                from {table_name} T0
                {group_by}
            ) T2 on {join_on}
        """
