import logging
from typing import Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    ColumnChoiceParam,
    FloatParam,
    TabularMultiParam,
)
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal, duckdb_dtype_is_numeric

logger = logging.getLogger(__name__)


class CountColumnParam(TabularMultiParam):
    col = ColumnChoiceParam("Column")
    weight = FloatParam("Weight")

class VampSeqScorePlugin(DuckdbSimplePlugin):
    name = "VAMP-seq Scoring"
    description = "Calculate scores from weighed bin counts"
    version = VERSION

    count_columns = ArrayParam("Count Columns", CountColumnParam("Count"), min_size=2)

    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:

        weighted_columns = {
            duckdb_escape_identifier(c.col.value): duckdb_escape_literal(c.weight.value)
            for c in self.count_columns
            if c.col.value in source.columns and duckdb_dtype_is_numeric(source[c.col.value].dtypes[0]) and c.weight.value is not None
        }

        weighted_counts = ' + '.join(f"{k} * {v}" for k, v in weighted_columns.items())
        total_counts = ' + '.join(k for k in weighted_columns.keys())

        if not weighted_counts:
            return source

        proj = f"*, ({weighted_counts}) / ({total_counts}) as score"

        logger.debug("VampseqScorePlugin proj %s", proj)
        return source.project(proj)
