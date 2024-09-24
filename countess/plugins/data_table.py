from typing import Iterable

import pandas as pd

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    DataTypeChoiceParam,
    MultiParam,
    StringParam,
    TabularMultiParam,
)
from countess.core.plugins import PandasInputPlugin


class _ColumnsMultiParam(MultiParam):
    name = StringParam("Name")
    type = DataTypeChoiceParam("Type", "string")
    index = BooleanParam("Index?")


class DataTablePlugin(PandasInputPlugin):
    """DataTable"""

    name = "DataTable"
    description = "enter small amounts of data directly"
    link = "https://countess-project.github.io/CountESS/included-plugins/#data-table"
    version = VERSION
    num_inputs = 0

    columns = ArrayParam("Columns", _ColumnsMultiParam("Column"))
    rows = ArrayParam("Rows", TabularMultiParam("Row"))

    show_preview = False

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

    def finalize(self) -> Iterable[pd.DataFrame]:
        self.fix_columns()
        values = []
        for row in self.rows:
            values.append({str(col.name): row[str(col.name)].value for col in self.columns})

        df = pd.DataFrame(values)

        index_cols = [str(col.name) for col in self.columns if col.index]

        if index_cols:
            df = df.set_index(index_cols)

        yield df
