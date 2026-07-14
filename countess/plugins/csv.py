import bz2
import csv
import gzip
import logging
import lzma
from io import BufferedWriter, BytesIO, TextIOBase
from itertools import zip_longest
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple, TypedDict, Union

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
from countess.core.plugins import DuckdbLoadFileWithTheLotPlugin, DuckdbSaveFilePlugin
from countess.utils.duckdb import duckdb_dtype_to_datatype_choice, duckdb_escape_identifier, duckdb_source_to_table

CSV_FILE_TYPES: Sequence[Tuple[str, Union[str, List[str]]]] = [
    ("CSV", [".csv", ".csv.gz", ".csv.bz2"]),
    ("TSV", [".tsv", ".tsv.gz", ".tsv.bz2"]),
    ("TXT", [".txt", ".text", ".txt.gz", ".text.gz", ".txt.bz2", ".text.bz2"]),
]

logger = logging.getLogger(__name__)


class ColumnsMultiParam(MultiParam):
    name = StringParam("Column Name", "")
    type = DataTypeOrNoneChoiceParam("Column Type")


CSV_DELIMITER_CHOICES: dict[str, str] = {",": ",", ";": ";", "|": "|", "TAB": "\t", "SPACE": " "}


class ReadCsvOptions(TypedDict):
    sep: str
    null_padding: bool
    strict_mode: bool
    all_varchar: bool
    skiprows: int
    header: bool


class LoadCsvPlugin(DuckdbLoadFileWithTheLotPlugin):
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
        options: ReadCsvOptions = {
            "sep": CSV_DELIMITER_CHOICES[self.delimiter.value],
            "null_padding": True,
            "strict_mode": False,
            "all_varchar": len(self.columns) > 0,
            "skiprows": 1 if self.header and len(self.columns) > 0 else 0,
            "header": self.header.value and len(self.columns) == 0,
        }
        if filename.endswith(".bz2"):
            with bz2.open(filename) as fh:
                rel = cursor.read_csv(fh, **options)  # typing: disable=arg-type
        elif filename.endswith(".xz"):
            with lzma.open(filename) as fh:
                rel = cursor.read_csv(fh, **options)  # typing: disable=arg-type
        else:
            rel = cursor.read_csv(filename, **options)  # typing: disable=arg-type

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

        # `rel = cursor.read_csv(fh)` returns a `DuckDBPyRelation`
        # with `rel.alias` starting `DUCKDB_INTERNAL_OBJECTSTORE:`
        # but limits / projections / views of that relation stop
        # working once `rel` falls out of scope.
        #
        # `rel = cursor.read_csv(filename)` returns a `DuckDBPyRelation`
        # with `rel.alias` starting `unnamed_relation_` and those
        # seem to work fine to limit / project / view.

        if rel.alias.startswith("DUCKDB_INTERNAL_OBJECTSTORE:"):
            rel = duckdb_source_to_table(cursor, rel)

        return rel

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


def write_to_csv(source: DuckDBPyRelation, filename: str, sep: str, quoting: int) -> None:
    logger.debug("writing file %s", filename)

    def _write(fh: TextIOBase | gzip.GzipFile | bz2.BZ2File | lzma.LZMAFile, **kwargs):
        # the type check supression is because the first parameter to
        # to_csv is:
        #     path_or_buf: str | PathLike[str] | WriteBuffer[bytes] | WriteBuffer[str]
        # but bz2.BZFile|lzma.LZMAFile don't seem to be compatible.
        chunk = source.fetch_df_chunk()
        chunk.to_csv(fh, header=True, index=False, **kwargs)  # type: ignore
        while len(chunk) > 0:
            chunk = source.fetch_df_chunk()
            chunk.to_csv(fh, header=False, index=False, **kwargs)  # type: ignore

    # If it's one of the formats that duckdb doesn't (yet) support, write via pandas
    # See also https://github.com/nickzoic/duckdb-compression/

    if filename.endswith(".bz2"):
        with bz2.open(filename, "w") as fh_bz2:
            _write(fh_bz2, sep=sep, quoting=quoting)
    elif filename.endswith(".xz"):
        with lzma.open(filename, "w") as fh_xz:
            _write(fh_xz, sep=sep, quoting=quoting)
    elif quoting not in (csv.QUOTE_MINIMAL, csv.QUOTE_ALL):
        # duckdb doesn't support this properly
        # https://github.com/duckdb/duckdb-python/discussions/491
        if filename.endswith(".gz"):
            with gzip.open(filename, "w") as fh_gz:
                _write(fh_gz, sep=sep, quoting=quoting)
        else:
            with open(filename, "w", encoding="utf-8") as fh_open:
                _write(fh_open, sep=sep, quoting=quoting)
    elif quoting == csv.QUOTE_ALL:
        # otherwise write it directly
        source.to_csv(filename, sep=sep, quoting=quoting)
    else:
        source.to_csv(filename, sep=sep)


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
        if source is None or row_limit is not None or not self.filename:
            return

        if len(self.sorting):
            order_by = ",".join(
                duckdb_escape_identifier(sp.order_by.value) + (" desc" if sp.descending else "") for sp in self.sorting
            )
            logger.debug("SaveCsvPlugin.execute order_by %s", order_by)
            source = source.order(order_by)

        filename = self.filename.get_file_path()
        Path(filename).parent.mkdir(parents=True, exist_ok=True)
        write_to_csv(
            source, filename, sep=self.SEPARATORS[self.delimiter.value], quoting=self.QUOTING[self.quoting.value]
        )
