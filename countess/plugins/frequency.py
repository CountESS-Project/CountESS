import logging
from typing import Iterable, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import BooleanParam, ColumnOrNoneChoiceParam, PerNumericColumnArrayParam
from countess.core.plugins import DuckdbSqlPlugin
from countess.utils.duckdb import duckdb_escape_identifier

logger = logging.getLogger(__name__)


class FrequencyPlugin(DuckdbSqlPlugin):
    name = "Calculate Frequencies"
    description = "Calculate frequencies from counts"
    version = VERSION

    columns = PerNumericColumnArrayParam("Columns", BooleanParam("Convert?"))
    group_col = ColumnOrNoneChoiceParam("Group By")

    def prepare(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> None:
        super().prepare(ddbc, source)

        # set default values for converting "count" columns
        if not any(cp.value for cp in self.columns):
            for cp in self.columns:
                if "count" in cp.label:
                    cp.value = True

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        sums = ", ".join(
            f"sum(T0.{duckdb_escape_identifier(cp.label)}) as sum_{n}" for n, cp in enumerate(self.columns) if cp.value
        )

        if not sums:
            return None

        freqs = ", ".join(
            f"CASE WHEN T1.sum_{n} > 0 THEN T0.{duckdb_escape_identifier(cp.label)} / T1.sum_{n} ELSE 0 END "
            f"as {duckdb_escape_identifier(cp.label + '_freq')}"
            for n, cp in enumerate(self.columns)
            if cp.value
        )

        group_col_id = duckdb_escape_identifier(self.group_col.value) if self.group_col.is_not_none() else "1"

        return f"""
            select T0.*, {freqs}
            from {table_name} T0 join (
                select {group_col_id} as score_group, {sums}
                from {table_name} T0
                group by score_group
            ) T1 on ({group_col_id} = T1.score_group)
        """
