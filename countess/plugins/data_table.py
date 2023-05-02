from typing import Any, Optional
from itertools import islice

import pandas as pd

from countess import VERSION
from countess.core.parameters import (
    SimpleParam,
    ArrayParam,
    BooleanParam,
    StringParam,
    MultiParam,
    TabularMultiParam,
    DataTypeChoiceParam,
)
from countess.core.plugins import PandasBasePlugin
from countess.core.logger import Logger

def mutagenize(sequence: str, mutate: bool, delete: bool, insert: bool):
    # XXX it'd be faster, but less neat, to include logic for duplicate
    # removal here instead of producing duplicates and then removing them
    # later. 
    for n, b1 in enumerate(sequence):
        for b2 in "ACGT":
            if mutate and b1 != b2:
                yield sequence[0:n] + b2 + sequence[n+1:]
            if insert:
                yield sequence[0:n] + b2 + sequence[n:]
        if delete:
            yield sequence[0:n] + sequence[n+1:]
    if insert:
        for b2 in "ACGT":
            yield sequence + b2


class DataTablePlugin(PandasBasePlugin):
    """DataTable"""

    name = "DataTable"
    description = "enter small amounts of data directly"
    link = "https://countess-project.github.io/CountESS/plugins/#data-table"
    version = VERSION

    parameters = {
        "columns": ArrayParam("Columns", MultiParam("Column", {
            "name": StringParam("Name"),
            "type": DataTypeChoiceParam("Type", "string"),
        })),
        "rows": ArrayParam("Rows", TabularMultiParam("Row", {})),
    }

    col_rows : list[MultiParam] = []

    def fix_columns(self):
        for num, col_row in enumerate(self.col_rows):
            if col_row not in self.parameters["columns"].params:
                row_params = self.parameters["rows"].param.params
                # XXX a column has been deleted!
                for col_num in range(num, len(self.col_rows)-1):
                    row_params[str(col_num)] = row_params[str(col_num+1)]
                    for row in self.parameters["rows"]:
                        row.params[str(col_num)] = row.params[str(col_num+1)]
                del row_params[str(len(self.col_rows)-1)]
                for row in self.parameters["rows"]:
                    del row.params[str(len(self.col_rows)-1)]
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
        if key == 'rows.0.0':
            self.fix_columns()
        super().set_parameter(key, *a, **k)

    def prepare(self, data: Any, logger: Logger) -> bool:
        self.fix_columns()
        if data is not None:
            logger.error("DataTable doesn't take inputs")
            return False
        return True

    def run(self, data: Any, logger: Logger, row_limit: Optional[int] = None):
        self.fix_columns()
        values = []
        for row in self.parameters["rows"]:
            values.append(dict(
                (col["name"].value, row[str(num)].value)
                for num, col in enumerate(self.parameters["columns"])
            ))

        return pd.DataFrame(values)
