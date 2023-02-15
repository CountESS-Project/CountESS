import csv
from typing import Any, Callable, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore

from countess import VERSION
from countess.core.parameters import *
from countess.core.plugins import DaskBasePlugin, DaskInputPlugin
from countess.utils.dask import merge_dask_dataframes

# XXX it would be better to do the same this Regex Tool does and get the user to assign
# data types to each column


def maybe_number(x):
    """CSV is never clear on if something is actually a number so ... try it I guess ..."""
    try:
        return int(x)
    except ValueError:
        pass

    try:
        return float(x)
    except ValueError:
        pass

    return x


def clean_row(row):
    return [maybe_number(x) for x in row]


class LoadCsvPlugin(DaskInputPlugin):
    """Load CSV files"""

    name = "CSV Load"
    title = "Load from CSV"
    description = "Loads columns from CSV files and merges them into the data"
    version = VERSION

    file_types = [("CSV", "*.csv"), ("TSV", "*.tsv")]

    parameters = {
        "columns": ArrayParam(
            "Columns",
            MultiParam(
                "Column",
                {
                    "name": StringParam("Column Name", ""),
                    "type": ChoiceParam(
                        "Column Type",
                        "string",
                        choices=["string", "number", "integer", "none"],
                    ),
                    "index": BooleanParam("Index?", False),
                },
            ),
        ),
        "header": BooleanParam("CSV file has header row?", True),
        "filename_column": StringParam("Filename Column", ""),
    }

    column_type_translate = {
        "string": str,
        "number": float,
        "integer": int,
        "none": None,
    }

    def read_file_to_dataframe(self, file_param, row_limit=None):

        filename = file_param["filename"].value

        options = {
            "header": 0 if self.parameters["header"].value else None,
        }
        if row_limit is not None:
            options["nrows"] = row_limit

        if len(self.parameters["columns"]):
            options["names"] = []
            options["dtype"] = {}
            options["usecols"] = []

            for n, pp in enumerate(self.parameters["columns"]):
                options["names"].append(pp["name"].value or f"column_{n}")
                column_type = self.column_type_translate[pp["type"].value]
                if column_type:
                    options["dtype"][n] = column_type
                    options["usecols"].append(n)

        # XXX dd.read_csv().set_index() is very very slow!
        # XXX pd.read_csv(index_col=) is half the speed of pd.read_csv().set_index()

        df = pd.read_csv(filename, **options)

        while len(df.columns) > len(self.parameters["columns"]):
            self.parameters["columns"].add_row()

        if self.parameters["header"].value:
            for n, col in enumerate(df.columns):
                if not self.parameters["columns"][n]["name"].value:
                    self.parameters["columns"][n]["name"].value = str(col)

        filename_column = self.parameters["filename_column"].value
        if filename_column:
            df[filename_column] = filename

        index_cols = [
            df.columns[n]
            for n, pp in enumerate(self.parameters["columns"])
            if pp["index"].value
        ]
        if index_cols:
            df = df.set_index(index_cols)

        return df


class SaveCsvPlugin(DaskBasePlugin):

    name = "CSV Save"
    title = "Save to CSV"
    description = "CSV CSV CSV"

    file_types = [("CSV", "*.csv")]

    parameters = {
        "header": BooleanParam("CSV header row?", True),
        "filename": FileSaveParam("Filename", file_types=file_types),
    }

    def run(
        self,
        obj: Any,
        callback: Callable[[int, int, Optional[str]], None],
        row_limit: Optional[int] = None,
    ):
        assert isinstance(self.parameters["filename"], StringParam)

        filename = self.parameters["filename"].value

        if row_limit is None:
            if isinstance(obj, dd.DataFrame):
                obj.to_csv(filename, single_file=True, compute=True)
            else:
                obj.to_csv(filename)

        return None
