import bz2
import csv
import gzip
import logging
from io import BufferedWriter, BytesIO
from typing import Any, List, Optional, Sequence, Tuple, Union

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

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
from countess.core.plugins import DuckdbLoadFilePlugin, DuckdbSaveFilePlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal, duckdb_source_to_view
from countess.utils.files import clean_filename

CSV_FILE_TYPES: Sequence[Tuple[str, Union[str, List[str]]]] = [
    ("CSV", [".csv", ".csv.gz"]),
    ("TSV", [".tsv", ".tsv.gz"]),
    ("TXT", [".txt", ".txt.gz"]),
]

logger = logging.getLogger(__name__)


class ColumnsMultiParam(MultiParam):
    name = StringParam("Column Name", "")
    type = DataTypeOrNoneChoiceParam("Column Type")


CSV_DELIMITER_CHOICES = {",": ",", ";": ";", "|": "|", "TAB": "\t", "SPACE": " ", "NONE": None}


class LoadCsvPlugin(DuckdbLoadFilePlugin):
    """Load CSV files"""

    name = "CSV Load"
    description = "Loads data from CSV or similar delimited text files and assigns types to columns"
    link = "https://countess-project.github.io/CountESS/included-plugins/#csv-reader"
    version = VERSION
    file_types = CSV_FILE_TYPES

    delimiter = ChoiceParam("Delimiter", ",", choices=CSV_DELIMITER_CHOICES.keys())
    header = BooleanParam("CSV file has header row?", True)
    filename_column = StringParam("Filename Column", "")
    columns = ArrayParam("Columns", ColumnsMultiParam("Column"))

    def load_file(
        self, cursor: DuckDBPyConnection, filename: str, file_param: BaseParam, file_number: int
    ) -> duckdb.DuckDBPyRelation:
        if self.header and len(self.columns) == 0:
            table = cursor.read_csv(
                filename,
                header=True,
                delimiter=CSV_DELIMITER_CHOICES[self.delimiter.value],
            )
            for column_name, column_dtype in zip(table.columns, table.dtypes):
                column_param = self.columns.add_row()
                column_param.name.value = column_name
                column_param.type.value = str(column_dtype)
        else:
            table = cursor.read_csv(
                filename,
                header=False,
                skiprows=1 if self.header else 0,
                delimiter=CSV_DELIMITER_CHOICES[self.delimiter.value],
                columns={str(c.name): "VARCHAR" if c.type.is_none() else str(c.type) for c in self.columns}
                if self.columns
                else None,
            )

        if self.filename_column:
            escaped_filename = duckdb_escape_literal(clean_filename(filename))
            escaped_column = duckdb_escape_identifier(self.filename_column.value)
            table = table.project(f"*, {escaped_filename} AS {escaped_column}")

        return table


class SaveCsvPlugin(DuckdbSaveFilePlugin):
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

    def execute(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> Optional[DuckDBPyRelation]:
        pass
