import csv
import gzip
from io import BufferedWriter, BytesIO
from typing import Optional, Union

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ChoiceParam,
    DataTypeOrNoneChoiceParam,
    FileSaveParam,
    MultiParam,
    StringParam,
)
from countess.core.plugins import PandasInputFilesPlugin, PandasProcessPlugin
from countess.utils.pandas import flatten_columns

class LoadCsvPlugin(PandasInputFilesPlugin):
    """Load CSV files"""

    name = "CSV Load"
    description = "Loads data from CSV or similar delimited text files and assigns types to columns"
    link = "https://countess-project.github.io/CountESS/included-plugins/#csv-reader"
    version = VERSION

    file_types = [("CSV", [".csv", ".gz"]), ("TSV", [".tsv", ".gz"]), ("TXT", ".txt")]

    parameters = {
        "delimiter": ChoiceParam("Delimiter", ",", choices=[",", ";", "TAB", "|", "WHITESPACE"]),
        "quoting": ChoiceParam("Quoting", "None", choices=["None", "Double-Quote", "Quote with Escape"]),
        "comment": ChoiceParam("Comment", "None", choices=["None", "#", ";"]),
        "header": BooleanParam("CSV file has header row?", True),
        "filename_column": StringParam("Filename Column", ""),
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
    }

    def read_file_to_dataframe(self, file_params, logger, row_limit=None):
        filename = file_params["filename"].value

        options = {
            "header": 0 if self.parameters["header"].value else None,
        }
        if row_limit is not None:
            options["nrows"] = row_limit

        index_col_numbers = []

        if len(self.parameters["columns"]):
            options["names"] = []
            options["dtype"] = {}
            options["usecols"] = []

            for n, pp in enumerate(self.parameters["columns"]):
                options["names"].append(pp["name"].value or f"column_{n}")
                if not pp["type"].is_none():
                    if pp["index"].value:
                        index_col_numbers.append(len(options["usecols"]))
                    options["usecols"].append(n)
                    options["dtype"][n] = pp["type"].get_selected_type()

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

        # XXX pd.read_csv(index_col=) is half the speed of pd.read_csv().set_index()

        df = pd.read_csv(filename, **options)

        while len(df.columns) > len(self.parameters["columns"]):
            self.parameters["columns"].add_row()

        if self.parameters["header"].value:
            for n, col in enumerate(df.columns):
                if not self.parameters["columns"][n]["name"].value:
                    self.parameters["columns"][n]["name"].value = str(col)
                    self.parameters["columns"][n]["type"].value = "string"

        filename_column = self.parameters["filename_column"].value
        if filename_column:
            df[filename_column] = filename

        if index_col_numbers:
            df = df.set_index([df.columns[n] for n in index_col_numbers])

        return df


class SaveCsvPlugin(PandasProcessPlugin):
    name = "CSV Save"
    description = "Save data as CSV or similar delimited text files"
    link = "https://countess-project.github.io/CountESS/included-plugins/#csv-writer"
    version = VERSION

    file_types = [("CSV", [".csv", ".gz"]), ("TSV", [".tsv", ".gz"]), ("TXT", ".txt")]

    parameters = {
        "header": BooleanParam("CSV header row?", True),
        "filename": FileSaveParam("Filename", file_types=file_types),
        "delimiter": ChoiceParam("Delimiter", ",", choices=[",", ";", "TAB", "|", "SPACE"]),
        "quoting": BooleanParam("Quote all Strings", False),
    }

    filehandle: Optional[Union[BufferedWriter, BytesIO]] = None
    csv_columns = None

    SEPARATORS = {",": ",", ";": ";", "SPACE": " ", "TAB": "\t"}
    QUOTING = {False: csv.QUOTE_MINIMAL, True: csv.QUOTE_NONNUMERIC}

    def prepare(self, sources: list[str], row_limit: Optional[int] = None):
        if row_limit is None:
            filename = self.parameters["filename"].value
            if filename.endswith(".gz"):
                self.filehandle = gzip.open(filename, "wb")
            else:
                self.filehandle = open(filename, "wb")
        else:
            self.filehandle = BytesIO()

        self.csv_columns = None

    def process(self, data: pd.DataFrame, source: str, logger: Logger):
        # reset indexes so we can treat all columns equally.
        # if there's just a nameless index then we don't care about it, drop it.
        drop_index = data.index.name is None and data.index.names[0] is None
        dataframe = flatten_columns(data.reset_index(drop=drop_index))

        # if this is our first dataframe to write then decide whether to
        # include the header or not.
        if self.csv_columns is None:
            self.csv_columns = list(dataframe.columns)
            emit_header = bool(self.parameters["header"].value)
        else:
            # add in any columns we haven't seen yet in previous dataframes.
            for c in dataframe.columns:
                if c not in self.csv_columns:
                    self.csv_columns.append(c)
                    logger.warning(f"Added CSV Column {repr(c)} with no header")
            # fill in blanks for any columns which are in previous dataframes but not
            # in this one.
            dataframe = dataframe.assign(**{c: None for c in self.csv_columns if c not in dataframe.columns})
            emit_header = False

        dataframe.to_csv(
            self.filehandle,
            header=emit_header,
            columns=self.csv_columns,
            index=False,
            sep=self.SEPARATORS[self.parameters["delimiter"].value],
            quoting=self.QUOTING[self.parameters["quoting"].value],
        )  # type: ignore [call-overload]
        return []

    def finalize(self, logger: Logger):
        if isinstance(self.filehandle, BytesIO):
            yield self.filehandle.getvalue().decode("utf-8")
