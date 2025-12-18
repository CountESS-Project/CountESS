import logging
from typing import Optional, Iterable

from countess import VERSION
from countess.core.parameters import BooleanParam, PerColumnArrayParam, StringParam
from countess.core.plugins import DuckdbSqlPlugin
from countess.utils.duckdb import duckdb_escape_identifier

logger = logging.getLogger(__name__)


class UnpivotPlugin(DuckdbSqlPlugin):
    """UnPivots (Melts) a Table"""

    name = "Unpivot Tool"
    description = "Unpivots (Melts) Tables"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#unpivot-tool"

    columns = PerColumnArrayParam("Columns", BooleanParam("Melt?", default=False))
    name_column = StringParam("Name Column")
    value_column = StringParam("Value Column")
    omit_blanks = BooleanParam("Omit Blanks?")

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        column_set = set(columns)
        melt_cols = [p.label for p in self.columns if p.value and p.label in column_set]
        melt_str = ", ".join(duckdb_escape_identifier(c) for c in melt_cols)

        name_str = duckdb_escape_identifier(self.name_column.value or 'name')
        value_str = duckdb_escape_identifier(self.value_column.value or 'value')

        query_str = f"""UNPIVOT {table_name} ON {melt_str}
                        INTO NAME {name_str} VALUE {value_str}"""

        if self.omit_blanks:
            query_str = f"SELECT * FROM ({query_str}) WHERE {value_str}::string != ''"

        return query_str
