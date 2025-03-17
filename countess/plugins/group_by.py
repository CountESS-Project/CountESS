import logging
from typing import Iterable, Optional

from countess import VERSION
from countess.core.parameters import BooleanParam, PerColumnArrayParam, TabularMultiParam
from countess.core.plugins import DuckdbSqlPlugin
from countess.utils.duckdb import duckdb_escape_identifier

logger = logging.getLogger(__name__)


class ColumnMultiParam(TabularMultiParam):
    index = BooleanParam("Index")
    count = BooleanParam("Count")
    nunique = BooleanParam("Count Distinct")
    min = BooleanParam("Min")
    max = BooleanParam("Max")
    sum = BooleanParam("Sum")
    mean = BooleanParam("Mean")
    median = BooleanParam("Median")
    var_pop = BooleanParam("Var")


def _op(op_name, col_name):
    col_ident = duckdb_escape_identifier(col_name)
    col_output = duckdb_escape_identifier(col_name + "__" + op_name)
    op_call = "COUNT(DISTINCT " if op_name == "nunique" else op_name.upper() + "("
    return f"{op_call}{col_ident}) AS {col_output}"


class GroupByPlugin(DuckdbSqlPlugin):
    """Groups by an arbitrary column and rolls up rows"""

    name = "Group By"
    description = "Group records by column(s) and calculate aggregates"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#group-by"
    additional = "Note: 'Var' is uncorrected population variance."

    columns = PerColumnArrayParam("Columns", ColumnMultiParam("Column"))
    join = BooleanParam("Join Back?")

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        column_params = list(self.columns.get_column_params())
        columns = (
            ", ".join(
                _op(op, col_name)
                for col_name, col_param in column_params
                for op, bp in col_param.params.items()
                if bp.value and op != "index"
            )
            or "count(*) as count"
        )
        group_by = ", ".join(
            duckdb_escape_identifier(col_name)
            for col_name, col_param in column_params
            if col_param.params["index"].value
        )
        if self.join:
            if group_by:
                return (
                    f"SELECT * FROM {table_name} JOIN (SELECT {group_by}, {columns} "
                    "FROM {table_name} GROUP BY {group_by}) USING ({group_by})"
                )
            else:
                return "SELECT * FROM {table_name} CROSS JOIN (SELECT {columns} " "FROM {table_name}"
        else:
            if group_by:
                return f"SELECT {group_by}, {columns} FROM {table_name} GROUP BY {group_by}"
            else:
                return f"SELECT {columns} FROM {table_name}"
