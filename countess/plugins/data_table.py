from typing import Iterable, Optional

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    DataTypeChoiceParam,
    MultiParam,
    StringParam,
    TabularMultiParam,
)
from countess.core.plugins import PandasInputPlugin


class DataTablePlugin(PandasInputPlugin):
    """DataTable"""

    name = "DataTable"
    description = "enter small amounts of data directly"
    link = "https://countess-project.github.io/CountESS/included-plugins/#data-table"
    version = VERSION

    parameters = {
        "columns": ArrayParam(
            "Columns",
            MultiParam(
                "Column",
                {
                    "name": StringParam("Name"),
                    "type": DataTypeChoiceParam("Type", "string"),
                    "index": BooleanParam("Index?"),
                },
            ),
        ),
        "rows": ArrayParam("Rows", TabularMultiParam("Row", {})),
    }

    col_rows: list[MultiParam] = []

    show_preview = False

    def fix_columns(self):
        for num, col_row in enumerate(self.col_rows):
            if col_row not in self.parameters["columns"].params:
                row_params = self.parameters["rows"].param.params
                # XXX a column has been deleted!
                for col_num in range(num, len(self.col_rows) - 1):
                    row_params[str(col_num)] = row_params[str(col_num + 1)]
                    for row in self.parameters["rows"]:
                        row.params[str(col_num)] = row.params[str(col_num + 1)]
                del row_params[str(len(self.col_rows) - 1)]
                for row in self.parameters["rows"]:
                    del row.params[str(len(self.col_rows) - 1)]
        self.col_rows = self.parameters["columns"].params[:]

        for num, col in enumerate(self.parameters["columns"]):
            col_name = col["name"].value
            try:
                self.parameters["rows"].param.params[str(num)].label = col_name
            except KeyError:
                new_param = col["type"].get_parameter(col_name)
                self.parameters["rows"].param.params[str(num)] = new_param
            for row in self.parameters["rows"]:
                try:
                    new_param = col["type"].get_parameter(col_name)
                    cur_param = row.params[str(num)]
                    if type(cur_param) != type(new_param):
                        try:
                            new_param.value = cur_param.value
                        except ValueError:
                            pass
                        row.params[str(num)] = new_param
                    row.params[str(num)].label = col_name
                except KeyError:
                    p = col["type"].get_parameter(col_name)
                    row.params[str(num)] = p

    def set_parameter(self, key: str, *a, **k):
        if key == "rows.0.0":
            self.fix_columns()
        super().set_parameter(key, *a, **k)

    def num_files(self):
        return 1

    def load_file(self, file_number: int, logger: Logger, row_limit: Optional[int] = None) -> Iterable[pd.DataFrame]:
        assert file_number == 0
        assert isinstance(self.parameters["rows"], ArrayParam)
        assert isinstance(self.parameters["columns"], ArrayParam)
        self.fix_columns()
        values = []
        for row in self.parameters["rows"]:
            values.append(
                dict((col["name"].value, row[str(num)].value) for num, col in enumerate(self.parameters["columns"]))
            )

        df = pd.DataFrame(values)

        index_cols = [col["name"].value for col in self.parameters["columns"] if col["index"].value]
        if index_cols:
            df = df.set_index(index_cols)

        yield df
