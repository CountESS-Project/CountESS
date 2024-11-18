import bz2
import csv
import gzip
import logging
from io import BufferedWriter, BytesIO
from typing import Any, List, Optional, Sequence, Tuple, Union

import pandas as pd

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    BaseParam,
    BooleanParam,
    ChoiceParam,
    DataTypeOrNoneChoiceParam,
    FileSaveParam,
    MultiParam,
    StringParam,
)
from countess.core.plugins import PandasInputFilesPlugin, PandasOutputPlugin
from countess.utils.files import clean_filename
from countess.utils.pandas import flatten_columns

CSV_FILE_TYPES: Sequence[Tuple[str, Union[str, List[str]]]] = [
    ("CSV", [".csv", ".csv.gz"]),
    ("TSV", [".tsv", ".tsv.gz"]),
    ("TXT", [".txt", ".txt.gz"]),
]

logger = logging.getLogger(__name__)


class ColumnsMultiParam(MultiParam):
    name = StringParam("Column Name", "")
    type = DataTypeOrNoneChoiceParam("Column Type")
    index = BooleanParam("Index?", False)


class LoadCsvPlugin(PandasInputFilesPlugin):
    """Load CSV files"""

    name = "CSV Load"
    description = "Loads data from CSV or similar delimited text files and assigns types to columns"
    link = "https://countess-project.github.io/CountESS/included-plugins/#csv-reader"
    version = VERSION
    file_types = CSV_FILE_TYPES

    delimiter = ChoiceParam("Delimiter", ",", choices=[",", ";", "TAB", "|", "WHITESPACE"])
    quoting = ChoiceParam("Quoting", "None", choices=["None", "Double-Quote", "Quote with Escape"])
    comment = ChoiceParam("Comment", "None", choices=["None", "#", ";"])
    header = BooleanParam("CSV file has header row?", True)
    filename_column = StringParam("Filename Column", "")
    columns = ArrayParam("Columns", ColumnsMultiParam("Column"))

    def read_file_to_dataframe(self, filename: str, file_param: BaseParam, row_limit=None):
        options: dict[str, Any] = {
            "header": 0 if self.header else None,
        }
        if row_limit is not None:
            options["nrows"] = row_limit

        index_col_numbers = []

        if len(self.columns):
            options["names"] = []
            options["usecols"] = []
            options["converters"] = {}

            for n, pp in enumerate(self.columns):
                options["names"].append(str(pp.name) or f"column_{n}")
                if pp.type.is_not_none():
                    if pp.index:
                        index_col_numbers.append(len(options["usecols"]))
                    options["usecols"].append(n)
                    options["converters"][n] = pp["type"].cast_value

        if self.delimiter == "TAB":
            options["delimiter"] = "\t"
        elif self.delimiter == "WHITESPACE":
            options["delim_whitespace"] = True
        else:
            options["delimiter"] = str(self.delimiter)

        if self.quoting == "None":
            options["quoting"] = csv.QUOTE_NONE
        elif self.quoting == "Double-Quote":
            options["quotechar"] = '"'
            options["doublequote"] = True
        elif self.quoting == "Quote with Escape":
            options["quotechar"] = '"'
            options["doublequote"] = False
            options["escapechar"] = "\\"

        if self.comment.value != "None":
            options["comment"] = str(self.comment)

        # XXX pd.read_csv(index_col=) is half the speed of pd.read_csv().set_index()

        df = pd.read_csv(filename, **options)

        while len(df.columns) > len(self.columns):
            self.columns.add_row()

        if self.header:
            for n, col in enumerate(df.columns):
                if not self.columns[n].name:
                    self.columns[n].name = str(col)
                    self.columns[n].type = "string"

        if self.filename_column:
            df[str(self.filename_column)] = clean_filename(filename)

        if index_col_numbers:
            df = df.set_index([df.columns[n] for n in index_col_numbers])

        return df


class SaveCsvPlugin(PandasOutputPlugin):
    name = "CSV Save"
    description = "Save data as CSV or similar delimited text files"
    link = "https://countess-project.github.io/CountESS/included-plugins/#csv-writer"
    version = VERSION
    file_types = CSV_FILE_TYPES

    header = BooleanParam("CSV header row?", True)
    filename = FileSaveParam("Filename", file_types=file_types)
    delimiter = ChoiceParam("Delimiter", ",", choices=[",", ";", "TAB", "|", "SPACE"])
    quoting = BooleanParam("Quote all Strings", False)

    filehandle: Optional[Union[BufferedWriter, BytesIO, gzip.GzipFile, bz2.BZ2File]] = None
    csv_columns = None

    SEPARATORS = {",": ",", ";": ";", "SPACE": " ", "TAB": "\t"}
    QUOTING = {False: csv.QUOTE_MINIMAL, True: csv.QUOTE_NONNUMERIC}

    def prepare(self, sources: list[str], row_limit: Optional[int] = None):
        if row_limit is None:
            logger.debug("SaveCsvPlugin.process %s prepare %s", self.name, self.filename)
            filename = str(self.filename)
            if filename.endswith(".gz"):
                self.filehandle = gzip.open(filename, "wb")
            elif filename.endswith(".bz2"):
                self.filehandle = bz2.open(filename, "wb")
            else:
                self.filehandle = open(filename, "wb")
        else:
            logger.debug("SaveCsvPlugin.process %s prepare BytesIO", self.name)
            self.filehandle = BytesIO()

        self.csv_columns = None

    def process(self, data: pd.DataFrame, source: str):
        # reset indexes so we can treat all columns equally.
        # if there's just a nameless index then we don't care about it, drop it.
        drop_index = data.index.name is None and data.index.names[0] is None
        dataframe = flatten_columns(data.reset_index(drop=drop_index))

        # if this is our first dataframe to write then decide whether to
        # include the header or not.
        if self.csv_columns is None:
            self.csv_columns = list(dataframe.columns)
            emit_header = bool(self.header)
        else:
            # add in any columns we haven't seen yet in previous dataframes.
            for c in dataframe.columns:
                if c not in self.csv_columns:
                    self.csv_columns.append(c)
                    logger.warning("Added CSV Column %s with no header", repr(c))
            # fill in blanks for any columns which are in previous dataframes but not
            # in this one.
            dataframe = dataframe.assign(**{c: None for c in self.csv_columns if c not in dataframe.columns})
            emit_header = False

        logger.debug("SaveCsvPlugin.process %s writing rows %d columns %d", self.name, len(dataframe), len(self.csv_columns))

        dataframe.to_csv(
            self.filehandle,
            header=emit_header,
            columns=self.csv_columns,
            index=False,
            sep=self.SEPARATORS[str(self.delimiter)],
            quoting=self.QUOTING[bool(self.quoting)],
        )  # type: ignore [call-overload]
        return []

    def finalize(self):
        logger.debug("SaveCsvPlugin.process %s finalize", self.name)
        if isinstance(self.filehandle, BytesIO):
            yield self.filehandle.getvalue().decode("utf-8")
        else:
            self.filehandle.close()
