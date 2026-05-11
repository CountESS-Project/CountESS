import bz2
import csv
import gzip
import logging
import lzma
from io import BufferedWriter, BytesIO
from itertools import zip_longest
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple, Union

import duckdb
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
from countess.utils.duckdb import (
    duckdb_dtype_to_datatype_choice,
    duckdb_escape_identifier,
    duckdb_source_to_table,
    duckdb_source_to_view,
)

CSV_FILE_TYPES: Sequence[Tuple[str, Union[str, List[str]]]] = [
    ("CSV", [".csv", ".csv.gz", ".csv.bz2"]),
    ("TSV", [".tsv", ".tsv.gz", ".tsv.bz2"]),
    ("TXT", [".txt", ".text", ".txt.gz", ".text.gz", ".txt.bz2", ".text.bz2"]),
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
        options = {
            "sep": CSV_DELIMITER_CHOICES[self.delimiter.value],
            "filename": self.filename_column.value,
            "null_padding": True,
            "strict_mode": False,
        }

        if len(self.columns):
            options["all_varchar"] = True
            options["skiprows"] = 1 if self.header else 0
            options["header"] = False
        else:
            options["header"] = self.header.value

        if filename.endswith(".xz"):
            logger.debug("Reading file %s with LZMA", filename)
            with lzma.open(filename) as fh:
                rel = duckdb_source_to_table(cursor, cursor.read_csv(fh, **options))
        if filename.endswith(".bz2"):
            logger.debug("Reading file %s with BZ2", filename)
            with bz2.open(filename) as fh:
                rel = duckdb_source_to_table(cursor, cursor.read_csv(fh, **options))
        else:
            rel = duckdb_source_to_view(cursor, cursor.read_csv(filename, **options))

        if row_limit is not None:
            rel = rel.limit(row_limit)

        if len(self.columns):
            # If there's a bonus filename column ignore it for now
            rel_columns = [c for c in rel.columns if not (self.filename_column and c == "filename")]
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
                for num, (cn, cp) in enumerate(zip_longest(rel_columns, self.columns))
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
    quoting = BooleanParam("Quote Non-numerics?", True)
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
            source = source.order(order_by)

        options = {
            "index": False,
            "sep": self.SEPARATORS[self.delimiter.value],
            "quoting": self.QUOTING[self.quoting.value],
        }

        Path(filename).parent.mkdir(parents=True, exist_ok=True)

        def _openfile(filename):
            if filename.endswith(".gz"):
                return gzip.open(filename, "wb")
            elif filename.endswith(".bz2"):
                return bz2.open(filename, "wb")
            else:
                return open(filename, "wb")

        # the type check supression is because the first parameter to
        # to_csv is called path_or_buf and takes either a filename path
        # or a file-like buffer, but is declared as str|None.

        with _openfile(filename) as fh:
            chunk = source.fetch_df_chunk()
            chunk.to_csv(fh, header=True, **options)  # type: ignore
            while len(chunk) > 0:
                chunk = source.fetch_df_chunk()
                chunk.to_csv(fh, header=False, **options)  # type: ignore
