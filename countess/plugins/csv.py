import bz2
import csv
import gzip
import logging
from io import BufferedWriter, BytesIO
from typing import List, Optional, Sequence, Tuple, Union, Iterable

import duckdb
import pyarrow
import pyarrow.csv
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
from countess.utils.pyarrow import python_type_to_arrow_dtype
from countess.utils.duckdb import duckdb_dtype_to_datatype_choice

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
    columns = ArrayParam("Columns", ColumnsMultiParam("Column"))

    def load_file(self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam) -> duckdb.DuckDBPyRelation:
        if len(self.columns):
            column_names = [c.name.value for c in self.columns]
            read_options = pyarrow.csv.ReadOptions(
                column_names=column_names,
                skip_rows=1
            )
            convert_options = pyarrow.csv.ConvertOptions(
                column_types = { c.name.value: python_type_to_arrow_dtype(c.type.get_selected_type()) for c in self.columns }
            )
        else:
            read_options = None
            convert_options = None

        parse_options = pyarrow.csv.ParseOptions(delimiter=CSV_DELIMITER_CHOICES[self.delimiter.value])

        try:
            t = pyarrow.csv.read_csv(filename, read_options, parse_options, convert_options)
        except pyarrow.ArrowInvalid:
            t = pyarrow.csv.read_csv(filename, None, parse_options, None)

        return cursor.from_arrow(t)

    def combine(self, ddbc: duckdb.DuckDBPyConnection, tables: Iterable[duckdb.DuckDBPyRelation]
    ) -> duckdb.DuckDBPyRelation:
        combined = super().combine(ddbc, tables)
        for num, (column, dtype) in enumerate(zip(combined.columns, combined.dtypes)):
            if num >= len(self.columns):
                new_param = self.columns.add_row()
                new_param.name.value = column
                new_param.type.value = duckdb_dtype_to_datatype_choice(dtype)
        return combined




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
