import bz2
import csv
import gzip
import logging
from io import BufferedWriter, BytesIO
from itertools import zip_longest
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
from countess.core.plugins import (
    DuckdbLoadFilePlugin,
    DuckdbSaveFilePlugin,
    LoadFileDeGlobMixin,
    LoadFileWithFilenameMixin,
)
from countess.utils.duckdb import duckdb_dtype_to_datatype_choice, duckdb_escape_identifier, duckdb_source_to_view

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


class LoadCsvPlugin(LoadFileDeGlobMixin, LoadFileWithFilenameMixin, DuckdbLoadFilePlugin):
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
        # DuckDB currently can only read .gz compressed CSV files, which is frustrating:
        # https://github.com/duckdb/duckdb/discussions/12232
        # For now, we use pyarrow as an intermediary.
        # See 36de8150bf for a cleaner, duckdb-only version if that gets fixed.

        if len(self.columns):
            skip_rows = 1 if self.header else 0
            read_options = pyarrow.csv.ReadOptions(column_names=[], autogenerate_column_names=True, skip_rows=skip_rows)
        elif self.header:
            read_options = pyarrow.csv.ReadOptions()
        else:
            read_options = pyarrow.csv.ReadOptions(column_names=[], autogenerate_column_names=True)

        parse_options = pyarrow.csv.ParseOptions(delimiter=CSV_DELIMITER_CHOICES[self.delimiter.value])

        pyarrow_table = pyarrow.csv.read_csv(filename, read_options, parse_options, None)
        if row_limit is not None:
            pyarrow_table = pyarrow_table.slice(length=row_limit)
        rel = cursor.from_arrow(pyarrow_table)

        if len(self.columns):
            # there's three cases here, either we've got both a column
            # name or a column parameter or both, depending on the relative
            # lengths of the column lists.
            proj = ",".join(
                duckdb_escape_identifier(cn)
                if cp is None
                else (
                    (
                        f"TRY_CAST(NULL as {cp.type.value})"
                        if cn is None
                        else f"TRY_CAST({duckdb_escape_identifier(cn)} as {cp.type.value})"
                    )
                    + " AS "
                    + (duckdb_escape_identifier(cp.name.value) if cp.name.value else "column%d" % num)
                )
                for num, (cn, cp) in enumerate(zip_longest(rel.columns, self.columns))
                if cp is None or cp.type.is_not_none()
            )

            logger.debug("LoadCsvPlugin.load_file proj %s", proj)
            rel = rel.project(proj)

        return duckdb_source_to_view(cursor, rel)

    def combine(
        self, ddbc: duckdb.DuckDBPyConnection, tables: Iterable[duckdb.DuckDBPyRelation]
    ) -> Optional[duckdb.DuckDBPyRelation]:
        combined = super().combine(ddbc, tables)
        if combined is None:
            return None

        # filename is always last column if it exists.
        combined_columns_and_dtypes = list(zip(combined.columns, combined.dtypes))
        if self.filename_column:
            combined_columns_and_dtypes.pop()

        for num, (column, dtype) in enumerate(combined_columns_and_dtypes):
            if num >= len(self.columns) and not (self.filename_column and column == "filename"):
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

        if source is None:
            return
        elif len(self.sorting):
            order_by = ",".join(
                duckdb_escape_identifier(sp.order_by.value) + (" desc" if sp.descending else "") for sp in self.sorting
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
