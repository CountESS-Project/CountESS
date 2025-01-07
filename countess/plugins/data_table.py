import logging

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    DataTypeChoiceParam,
    MultiParam,
    StringParam,
    TabularMultiParam,
)
from countess.core.plugins import DuckdbInputPlugin
from countess.utils.duckdb import duckdb_escape_literal, duckdb_escape_identifier

logger = logging.getLogger(__name__)

class _ColumnsMultiParam(MultiParam):
    name = StringParam("Name")
    type = DataTypeChoiceParam("Type")
    index = BooleanParam("Index?")


class DataTablePlugin(DuckdbInputPlugin):
    """DataTable"""

    name = "DataTable"
    description = "enter small amounts of data directly"
    link = "https://countess-project.github.io/CountESS/included-plugins/#data-table"
    version = VERSION
    num_inputs = 0

    columns = ArrayParam("Columns", _ColumnsMultiParam("Column"))
    rows = ArrayParam("Rows", TabularMultiParam("Row"))

    #show_preview = False

    def fix_columns(self):
        old_rows = self.rows.params

        # XXX should deal with duplicate column names more generally
        for num, col in enumerate(self.columns):
            if col.name == "":
                col.name = f"col_{num+1}"

        self.params["rows"] = self.rows = ArrayParam(
            "Rows",
            TabularMultiParam("Row", {str(col.name): col.type.get_parameter(str(col.name)) for col in self.columns}),
        )

        # fix rows to use the latest columns
        for old_row in old_rows:
            new_row = self.rows.add_row()
            for col in self.columns:
                try:
                    new_row[str(col.name)].value = old_row[str(col.name)].value
                except (KeyError, ValueError):
                    pass

    def set_parameter(self, key: str, *a, **k):
        if key.startswith("rows.0."):
            self.fix_columns()
        super().set_parameter(key, *a, **k)

    def execute(self, ddbc: DuckDBPyConnection, source: None) -> DuckDBPyRelation:
        self.fix_columns()

        if len(self.rows) == 0:
            return None

        sql = ("SELECT * FROM (VALUES " + 
            (", ".join(
                "(" + (", ".join(duckdb_escape_literal(val.value) for val in row.values())) + ")"
                for row in self.rows
            )) +
            ") _(" +
            (", ".join(duckdb_escape_identifier(col.name.value) for col in self.columns)) +
            ")"
        )

        logger.debug("DataTablePlugin.execute sql %s", sql)

        return ddbc.sql(sql)
