import bz2
import csv
import gzip
import logging
from io import BufferedWriter, BytesIO
from typing import Iterable, List, Optional, Sequence, Tuple, Union

import duckdb
import pyarrow  # type: ignore
import pyarrow.csv  # type: ignore
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    BaseParam,
    BooleanParam,
    ChoiceParam,
    ColumnChoiceParam,
    DataTypeOrNoneChoiceParam,
    FileSaveParam,
    MultiParam,
    StringParam,
    TabularMultiParam,
)
from countess.core.plugins import DuckdbLoadFilePlugin, DuckdbSaveFilePlugin
from countess.utils.duckdb import duckdb_dtype_to_datatype_choice, duckdb_escape_identifier
from countess.utils.pyarrow import python_type_to_arrow_dtype

CSV_FILE_TYPES: Sequence[Tuple[str, Union[str, List[str]]]] = [
    ("CSV", [".csv", ".csv.gz"]),
    ("TSV", [".tsv", ".tsv.gz"]),
    ("TXT", [".txt", ".txt.gz"]),
]

logger = logging.getLogger(__name__)


class ColumnsMultiParam(MultiParam):
    name = StringParam("Column Name", "")
    type = DataTypeOrNoneChoiceParam("Column Type")


CSV_DELIMITER_CHOICES = {",": ",", ";": ";", "|": "|", "TAB": "\t", "SPACE": " "}


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

    def load_file(
        self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam, row_limit: Optional[int] = None
    ) -> duckdb.DuckDBPyRelation:
        if len(self.columns):
            column_names = [c.name.value for c in self.columns]
            read_options = pyarrow.csv.ReadOptions(column_names=column_names, skip_rows=1 if self.header else 0)
            convert_options = pyarrow.csv.ConvertOptions(
                column_types={
                    c.name.value: python_type_to_arrow_dtype(c.type.get_selected_type()) for c in self.columns
                }
            )
        else:
            read_options = None
            convert_options = None

        parse_options = pyarrow.csv.ParseOptions(delimiter=CSV_DELIMITER_CHOICES[self.delimiter.value])

        try:
            t = pyarrow.csv.read_csv(filename, read_options, parse_options, convert_options)
        except pyarrow.ArrowInvalid:
            t = pyarrow.csv.read_csv(filename, None, parse_options, None)

        rel = cursor.from_arrow(t)
        if row_limit:
            rel = rel.limit(row_limit)
        return rel

    def combine(
        self, ddbc: duckdb.DuckDBPyConnection, tables: Iterable[duckdb.DuckDBPyRelation]
    ) -> Optional[duckdb.DuckDBPyRelation]:
        combined = super().combine(ddbc, tables)
        if combined is not None:
            for num, (column, dtype) in enumerate(zip(combined.columns, combined.dtypes)):
                if num >= len(self.columns):
                    new_param = self.columns.add_row()
                    new_param.name.value = column
                    new_param.type.value = duckdb_dtype_to_datatype_choice(dtype)
        return combined


class SaveCsvOrderParameter(TabularMultiParam):
    order_by = ColumnChoiceParam("Order By")
    descending = BooleanParam("Descending")

class SaveCsvPlugin(DuckdbSaveFilePlugin):
    name = "CSV Save"
    description = "Save data as CSV or similar delimited text files"
    link = "https://countess-project.github.io/CountESS/included-plugins/#csv-writer"
    version = VERSION
    file_types = CSV_FILE_TYPES

    header = BooleanParam("CSV header row?", True)
    filename = FileSaveParam("Filename", file_types=file_types)
    delimiter = ChoiceParam("Delimiter", ",", choices=[",", ";", "TAB", "|", "SPACE"])
    sorting = ArrayParam("Sorting", SaveCsvOrderParameter("sort"))

    filehandle: Optional[Union[BufferedWriter, BytesIO, gzip.GzipFile, bz2.BZ2File]] = None
    csv_columns = None

    SEPARATORS = {",": ",", ";": ";", "SPACE": " ", "TAB": "\t"}
    QUOTING = {False: csv.QUOTE_MINIMAL, True: csv.QUOTE_NONNUMERIC}

    show_preview = False

    def execute(
        self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation], row_limit: Optional[int] = None
    ) -> None:
        filename = self.filename.value

        if len(self.sorting):
            order_by = ",".join(
                duckdb_escape_identifier(sp.order_by.value) + (" desc" if sp.descending else "")
                for sp in self.sorting
            )
            logger.debug("SaveCsvPlugin.execute order_by %s", order_by)
            table = source.order(order_by)
        else:
            table = source

        def _write(fh):
            for num, record_batch in enumerate(table.record_batch()):
                write_options = pyarrow.csv.WriteOptions(
                    include_header=self.header.value and num == 0,
                    delimiter=self.delimiter.value,
                )
                pyarrow.csv.write_csv(record_batch, fh, write_options)

        if filename and row_limit is None:
            if filename.endswith(".gz"):
                with gzip.open(filename, "wb") as fh:
                    _write(fh)
            elif filename.endswith(".bz2"):
                with bz2.open(filename, "wb") as fh:
                    _write(fh)
            else:
                with open(filename, "wb") as fh:
                    _write(fh)
