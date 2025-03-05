import logging
from typing import Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    ChoiceParam,
    ColumnChoiceParam,
    ColumnOrNoneChoiceParam,
    FloatParam,
    StringParam,
    TabularMultiParam,
)
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal

logger = logging.getLogger(__name__)


class ScaleClassParam(TabularMultiParam):
    col = ColumnChoiceParam("Column")
    op = ChoiceParam("Operation", "Equals", ["Equals", "Starts With", "Ends With", "Contains"])
    st = StringParam("Value")
    score = FloatParam("Scaled Score")

    def filter(self):
        col = duckdb_escape_identifier(self.col.value)
        val = duckdb_escape_literal(self.st.value)
        if self.op.value == "Equals":
            return f"{col} = {val}"
        elif self.op.value == "Starts With":
            return f"starts_with({col},{val})"
        elif self.op.value == "Ends With":
            return f"ends_with({col},{val})"
        elif self.op.value == "Contains":
            return f"contains({col},{val})"
        else:
            raise NotImplementedError()


class ScoreScalingPlugin(DuckdbSimplePlugin):
    name = "Score Scaling"
    description = "Scaled Scores using variant classification"
    version = VERSION

    score_col = ColumnChoiceParam("Score Column")
    classifiers = ArrayParam("Variant Classifiers", ScaleClassParam("Class"), min_size=2, max_size=2, read_only=True)
    group_col = ColumnOrNoneChoiceParam("Group By")

    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        score_col_id = duckdb_escape_identifier(self.score_col.value)

        all_columns = ",".join(duckdb_escape_identifier(c) for c in source.columns if c != self.score_col.value)

        if self.group_col.is_not_none():
            group_col_id = "T0." + duckdb_escape_identifier(self.group_col.value)
        else:
            group_col_id = "1"  # dummy value for one big group.

        sql = f"""
            select {all_columns}, ({score_col_id} - T1.y) / (T1.z - T1.y) as {score_col_id}
            from {source.alias} T0 join (
                select {group_col_id} as x,
                    median({score_col_id}) filter ({self.classifiers[0].filter()}) as y,
                    median({score_col_id}) filter ({self.classifiers[1].filter()}) as z
                from {source.alias} T0
                group by x
            ) T1 on ({group_col_id} = T1.x)
        """

        logger.debug("ScoreScalingPlugin sql %s", sql)

        return ddbc.sql(sql)
