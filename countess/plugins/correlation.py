from typing import Optional

import pandas as pd
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import ColumnOrNoneChoiceParam, NumericColumnChoiceParam
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_escape_identifier


class CorrelationPlugin(DuckdbSimplePlugin):
    """Correlations"""

    name = "Correlation Tool"
    description = "Measures Pearsons r² correlation of columns"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#correlation-tool"

    group = ColumnOrNoneChoiceParam("Group")
    column1 = NumericColumnChoiceParam("Column X")
    column2 = NumericColumnChoiceParam("Column Y")

    columns: list[str] = []
    dataframes: list[pd.DataFrame] = []

    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        col1 = duckdb_escape_identifier(self.column1.value)
        col2 = duckdb_escape_identifier(self.column2.value)

        aggregate = f"regr_r2({col2},{col1}) AS regr_r2, regr_slope({col2},{col1}) AS regr_slope"

        if self.group.is_not_none():
            aggregate = duckdb_escape_identifier(self.group.value) + ", " + aggregate

        return source.aggregate(aggregate)
