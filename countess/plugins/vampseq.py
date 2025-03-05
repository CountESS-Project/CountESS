import logging
from typing import Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import FloatParam, PerNumericColumnArrayParam, TabularMultiParam
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal

logger = logging.getLogger(__name__)


class CountColumnParam(TabularMultiParam):
    weight = FloatParam("Weight")


class VampSeqScorePlugin(DuckdbSimplePlugin):
    name = "VAMP-seq Scoring"
    description = "Calculate scores from weighed bin counts"
    version = VERSION

    columns = PerNumericColumnArrayParam("Columns", CountColumnParam("Column"))

    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        weighted_columns = {
            duckdb_escape_identifier(name): duckdb_escape_literal(param.weight.value)
            for name, param in self.columns.get_column_params()
            if param.weight.value is not None
        }

        if not weighted_columns:
            return source

        weighted_counts = " + ".join(f"{k} * {v}" for k, v in weighted_columns.items())
        total_counts = " + ".join(k for k in weighted_columns.keys())

        proj = f"*, ({weighted_counts}) / ({total_counts}) as score"

        logger.debug("VampseqScorePlugin proj %s", proj)
        return source.project(proj)
