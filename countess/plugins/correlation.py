from typing import Optional
import logging

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import PerNumericColumnArrayParam, BooleanParam, ColumnOrNoneChoiceParam
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal

logger = logging.getLogger(__name__)

class CorrelationPlugin(DuckdbSimplePlugin):
    """Correlations"""

    name = "Correlation Tool"
    description = "Measures correlation coefficient, covariance and Pearsons r² correlation of columns"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#correlation-tool"

    columns = PerNumericColumnArrayParam("Columns", BooleanParam("Correlate?", False))
    group = ColumnOrNoneChoiceParam("Group")

    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:

        grp = duckdb_escape_identifier(self.group.value) if self.group.is_not_none() else None

        if sum(1 for c in self.columns.params if c.value) < 2:
            return None

        sql = " union all ".join(f"""
            select {(grp + ", ") if grp else ""}
            {duckdb_escape_literal(c1.label)} as column_x,
            {duckdb_escape_literal(c2.label)} as column_y,
            corr({duckdb_escape_identifier(c2.label)},{duckdb_escape_identifier(c1.label)}) as correlation_coefficient,
            covar_pop({duckdb_escape_identifier(c2.label)},{duckdb_escape_identifier(c1.label)}) as covariance_population,
            regr_r2({duckdb_escape_identifier(c2.label)},{duckdb_escape_identifier(c1.label)}) as pearsons_r2
            from {source.alias}
            {("group by "+grp) if grp else ""}
        """
            for c1 in self.columns.params
            for c2 in self.columns.params
            if c1.value and c2.value and c1.label < c2.label
        )
        logger.debug("CorrelationPlugin.execute sql %s", sql)
        return ddbc.sql(sql)
