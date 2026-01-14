import logging
from typing import Optional, Iterable

from countess import VERSION
from countess.core.parameters import (
    DataTypeOrNoneChoiceParam,
    PerColumnArrayParam,
    StringParam,
    TabularMultiParam
)
from countess.core.plugins import DuckdbSqlPlugin
from countess.utils.duckdb import duckdb_escape_identifier

logger = logging.getLogger(__name__)


class ColumnMultiParam(TabularMultiParam):
    name = StringParam("Rename")
    ttype = DataTypeOrNoneChoiceParam("Column Type")


class ColumnsPlugin(DuckdbSqlPlugin):
    name = "Columns"
    description = "Rename, remove or cast columns to a different type"
    version = VERSION

    columns = PerColumnArrayParam("Columns", ColumnMultiParam("Column"))

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:

        cols = ', '.join(
            f"TRY_CAST({duckdb_escape_identifier(n)} as {p.ttype.value}) " +
            f"AS {duckdb_escape_identifier(p.name.value or n)}"
            for n, p in self.columns.get_column_params()
            if p.ttype.is_not_none()
        )
        if not cols:
            return None

        return f"SELECT {cols} FROM {table_name}"
