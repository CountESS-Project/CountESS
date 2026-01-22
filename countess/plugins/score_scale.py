import logging
from typing import Iterable, Optional

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    ChoiceParam,
    ColumnChoiceParam,
    ColumnOrNoneChoiceParam,
    MultiColumnChoiceParam,
    NumericColumnChoiceParam,
    StringParam,
    TabularMultiParam,
)
from countess.core.plugins import DuckdbSqlPlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal

logger = logging.getLogger(__name__)


class ScaleClassParam(TabularMultiParam):
    agg = ChoiceParam("Aggregation", "Median", ["Median", "Avg"])
    col = ColumnChoiceParam("Column")
    op = ChoiceParam("Operation", "Equals", ["Equals", "Starts With", "Ends With",
                                             "Contains", "Matches", "Greater Than", "Less Than"])
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
        elif self.op.value == "Greater Than":
            return f"({col} > {val})"
        elif self.op.value == "Less Than":
            return f"({col} < {val})"
        else:
            raise NotImplementedError()


class ScoreScalingPlugin(DuckdbSqlPlugin):
    name = "Score Scaling"
    description = "Scaled Scores using variant classification"
    version = VERSION

    score_col = NumericColumnChoiceParam("Score Column")
    scaled_col = StringParam("Scaled Score Column", "scaled_score")
    classifiers = ArrayParam("Variant Classifiers", ScaleClassParam("Class"), min_size=2, max_size=2, read_only=True)
    group_col = MultiColumnChoiceParam("Group By")

    def __init__(self, *a, **k):
        super().__init__(*a, **k)

        # override classifiers labels
        self.classifiers[0].label = "Scale to 0.0"
        self.classifiers[1].label = "Scale to 1.0"

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        score_col_id = duckdb_escape_identifier(self.score_col.value)
        scaled_col_id = duckdb_escape_identifier(self.scaled_col.value)

        all_columns = ",".join("T1." + duckdb_escape_identifier(c) for c in columns if self.scaled_col != c)

        if len(self.group_col.get_values()):
            group_cols = ", ".join(
                duckdb_escape_identifier(v)
                for v in self.group_col.get_values()
            )
            group_by = f"GROUP BY {group_cols}"
            join_method = f"USING ({group_cols})"
            group_cols += ", "
        else:
            group_cols = ""
            group_by = ""
            join_method = "ON (1=1)"

        c0, c1 = self.classifiers

        return f"""
            select {all_columns},
                ({score_col_id} - T2.score_0) / (T2.score_1 - T2.score_0) as {scaled_col_id}
            from {table_name} T1 join (
                select {group_cols}
                    coalesce({c0.agg.value}({score_col_id}) filter ({c0.filter()}), 0) as score_0,
                    coalesce({c1.agg.value}({score_col_id}) filter ({c1.filter()}), 1) as score_1
                FROM {table_name}
                {group_by}
            ) T2 {join_method}
        """
