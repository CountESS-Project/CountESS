import logging
from typing import Iterable, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import FloatParam, MultiColumnChoiceParam, PerNumericColumnArrayParam, TabularMultiParam
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
    group_by = MultiColumnChoiceParam("Group By")
    threshold = FloatParam("Total Frequency Threshold", 1.78e-5)

    def prepare(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> None:
        super().prepare(ddbc, source)

        # set default values for weights on "count" columns
        if all(c.weight.value is None for c in self.columns):
            count_cols = [c for c in self.columns if c.label.startswith("count")]
            for n, c in enumerate(count_cols):
                c.weight.value = (n + 1) / len(count_cols)

        weight_cols = set(n for n, p in self.columns.get_column_params() if p.weight.value is not None)
        if source:
            self.group_by.set_choices([c for c in source.columns if c not in weight_cols])

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        group_cols = {duckdb_escape_identifier(name) for name in self.group_by.get_values()}
        weighted_columns = {
            duckdb_escape_identifier(name): duckdb_escape_literal(param.weight.value)
            for name, param in self.columns.get_column_params()
            if param.weight.value is not None
        }

        if not weighted_columns:
            return None

        inner_select = ", ".join(
            [f"T0.{k}" for k in group_cols] + [f"sum(T0.{k}) as {k}" for k in weighted_columns.keys()]
        )

        total_counts = "+".join(f"Var.{k}" for k in weighted_columns.keys())
        total_sum_counts = " + ".join(f"Tot.{k}" for k in weighted_columns.keys())
        total_freq = f"({total_counts})/({total_sum_counts})"

        weighted_freqs = " + ".join(
            f"CASE WHEN Tot.{k} > 0 THEN Var.{k} * {v} / Tot.{k} ELSE 0 END" for k, v in weighted_columns.items()
        )
        unweighted_freqs = " + ".join(
            f"CASE WHEN Tot.{k} > 0 THEN Var.{k} / Tot.{k} ELSE 0 END" for k in weighted_columns.keys()
        )
        group_by = ("GROUP BY " + ",".join(group_cols)) if group_cols else ""
        join_on = (" AND ".join(f"Var.{c} = Tot.{c}" for c in group_cols)) if group_cols else "1=1"

        return f"""
            SELECT Var.*, ({weighted_freqs}) / ({unweighted_freqs}) as score,
                {total_freq} AS total_freq
            FROM {table_name} Var JOIN (
                SELECT {inner_select}
                FROM {table_name} T0
                {group_by}
            ) Tot ON {join_on}
            WHERE total_freq > {duckdb_escape_literal(self.threshold.value)}
        """
