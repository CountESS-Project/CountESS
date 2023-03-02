import csv
from io import StringIO
from typing import Any, Optional

import dask.dataframe as dd
import pandas as pd  # type: ignore

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ChoiceParam,
    DataTypeOrNoneChoiceParam,
    FileSaveParam,
    MultiParam,
    StringParam,
)
from countess.core.plugins import DaskBasePlugin, DaskInputPlugin

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
    description = "Loads data from CSV or similar delimited text files and assigns types to columns"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/plugins/#csv-reader"

    file_types = [("CSV", "*.csv"), ("TSV", "*.tsv"), ("TXT", "*.txt")]

    parameters = {
        "delimiter": ChoiceParam("Delimiter", ",", choices=[",", ";", "TAB", "|", "WHITESPACE"]),
        "quoting": ChoiceParam(
            "Quoting", "None", choices=["None", "Double-Quote", "Quote with Escape"]
        ),
        "comment": ChoiceParam("Comment", "None", choices=["None", "#", ";"]),
        "columns": ArrayParam(
            "Columns",
            MultiParam(
                "Column",
                {
                    "name": StringParam("Column Name", ""),
                    "type": DataTypeOrNoneChoiceParam("Column Type"),
                    "index": BooleanParam("Index?", False),
                },
            ),
        ),
        "header": BooleanParam("CSV file has header row?", True),
        "filename_column": StringParam("Filename Column", ""),
    }

    def read_file_to_dataframe(self, file_params, logger, row_limit=None):
        filename = file_params["filename"].value

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
                if not pp["type"].is_none():
                    options["dtype"][n] = pp["type"].get_selected_type()
                    options["usecols"].append(n)

        delimiter = self.parameters["delimiter"].value
        if delimiter == "TAB":
            options["delimiter"] = "\t"
        elif delimiter == "WHITESPACE":
            options["delim_whitespace"] = True
        else:
            options["delimiter"] = delimiter

        quoting = self.parameters["quoting"].value
        if quoting == "None":
            options["quoting"] = csv.QUOTE_NONE
        elif quoting == "Double-Quote":
            options["quotechar"] = '"'
            options["doublequote"] = True
        elif quoting == "Quote with Escape":
            options["quotechar"] = '"'
            options["doublequote"] = False
            options["escapechar"] = "\\"

        comment = self.parameters["comment"].value
        if comment != "None":
            options["comment"] = comment

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
            df.columns[n] for n, pp in enumerate(self.parameters["columns"]) if pp["index"].value
        ]
        if index_cols:
            df = df.set_index(index_cols)

        return df


class SaveCsvPlugin(DaskBasePlugin):
    name = "CSV Save"
    title = "Save to CSV"
    description = "Save data as CSV or similar delimited text files"
    link = "https://countess-project.github.io/CountESS/plugins/#csv-writer"

    file_types = [("CSV", "*.csv"), ("TSV", "*.tsv"), ("TXT", "*.txt")]

    parameters = {
        "header": BooleanParam("CSV header row?", True),
        "filename": FileSaveParam("Filename", file_types=file_types),
        "delimiter": ChoiceParam("Delimiter", ",", choices=[",", ";", "TAB", "|", "SPACE"]),
        "quoting": BooleanParam("Quote all Strings", False),
    }

    def run(
        self,
        data: Any,
        logger,
        row_limit: Optional[int] = None,
    ):
        assert isinstance(self.parameters["filename"], StringParam)

        filename = self.parameters["filename"].value
        sep = self.parameters["delimiter"].value
        if sep == "TAB":
            sep = "\t"
        elif sep == "SPACE":
            sep = " "

        options = {
            "sep": sep,
            "quoting": csv.QUOTE_NONNUMERIC
            if self.parameters["quoting"].value
            else csv.QUOTE_MINIMAL,
        }

        if row_limit is None:
            if isinstance(data, dd.DataFrame):
                data.to_csv(filename, single_file=True, compute=True, **options)
            else:
                data.to_csv(filename, **options)
            return None
        else:
            if isinstance(data, dd.DataFrame):
                data = data.compute()

            buf = StringIO()
            data.to_csv(buf, **options)
            return buf.getvalue()
