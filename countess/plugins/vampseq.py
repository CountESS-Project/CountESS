import logging
from typing import Iterable, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import ColumnOrNoneChoiceParam, FloatParam, PerNumericColumnArrayParam, TabularMultiParam
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
    group_col = ColumnOrNoneChoiceParam("Group By")

    def prepare(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> None:
        super().prepare(ddbc, source)

        # set default values for weights on "count" columns
        if all(c.weight.value is None for c in self.columns):
            count_cols = [c for c in self.columns if c.label.startswith("count")]
            for n, c in enumerate(count_cols):
                c.weight.value = (n + 1) / len(count_cols)

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        weighted_columns = {
            duckdb_escape_identifier(name): duckdb_escape_literal(param.weight.value)
            for name, param in self.columns.get_column_params()
            if param.weight.value is not None
        }

        if not weighted_columns:
            return None

        if self.group_col.is_not_none():
            group_col_id = "T0." + duckdb_escape_identifier(self.group_col.value)
        else:
            group_col_id = "1"

        sums = ", ".join(f"sum(T0.{k}) as {k}" for k in weighted_columns.keys())
        weighted_counts = " + ".join(
            f"CASE WHEN T1.{k} > 0 THEN T0.{k} * {v} / T1.{k} ELSE 0 END" for k, v in weighted_columns.items()
        )
        total_counts = " + ".join(
            f"CASE WHEN T1.{k} > 0 THEN T0.{k} / T1.{k} ELSE 0 END" for k in weighted_columns.keys()
        )

        return f"""
            select T0.*, ({weighted_counts}) / ({total_counts}) as score
            from {table_name} T0 join (
                select {group_col_id} as score_group, {sums}
                from {table_name} T0
                group by score_group
            ) T1 on ({group_col_id} = T1.score_group)
        """
