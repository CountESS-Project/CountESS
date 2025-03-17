import logging
from typing import Iterable, Optional

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    ChoiceParam,
    ColumnChoiceParam,
    ColumnOrNoneChoiceParam,
    NumericColumnChoiceParam,
    StringParam,
    TabularMultiParam,
)
from countess.core.plugins import DuckdbSqlPlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal

logger = logging.getLogger(__name__)


class ScaleClassParam(TabularMultiParam):
    col = ColumnChoiceParam("Column")
    op = ChoiceParam("Operation", "Equals", ["Equals", "Starts With", "Ends With", "Contains", "Matches"])
    st = StringParam("Value")

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
        elif self.op.value == "Matches":
            return f"regexp_full_match({col},{val})"
        else:
            raise NotImplementedError()


class ScoreScalingPlugin(DuckdbSqlPlugin):
    name = "Score Scaling"
    description = "Scaled Scores using variant classification"
    version = VERSION

    score_col = NumericColumnChoiceParam("Score Column")
    scaled_col = StringParam("Scaled Score Column", "scaled_score")
    classifiers = ArrayParam("Variant Classifiers", ScaleClassParam("Class"), min_size=2, max_size=2, read_only=True)
    group_col = ColumnOrNoneChoiceParam("Group By")

    def __init__(self, *a, **k):
        super().__init__(*a, **k)

        # override classifiers labels
        self.classifiers[0].label = "Scale to 0.0"
        self.classifiers[1].label = "Scale to 1.0"

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        score_col_id = duckdb_escape_identifier(self.score_col.value)
        scaled_col_id = duckdb_escape_identifier(self.scaled_col.value)

        all_columns = ",".join("T0." + duckdb_escape_identifier(c) for c in columns if self.scaled_col != c)

        if self.group_col.is_not_none():
            group_col_id = "T0." + duckdb_escape_identifier(self.group_col.value)
        else:
            group_col_id = "1"  # dummy value for one big group.

        c0, c1 = self.classifiers

        return f"""
            select {all_columns}, ({score_col_id} - T1.score_0) / (T1.score_1 - T1.score_0) as {scaled_col_id}
            from {table_name} T0 join (
                select {group_col_id} as score_group,
                    median({score_col_id}) filter ({c0.filter()}) as score_0,
                    median({score_col_id}) filter ({c1.filter()}) as score_1
                from {table_name} T0
                group by score_group
            ) T1 on ({group_col_id} = T1.score_group)
        """
