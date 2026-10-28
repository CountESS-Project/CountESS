import bz2
import csv
import gzip
import logging
import lzma
from datetime import datetime
from io import BufferedWriter, BytesIO
from itertools import zip_longest
from typing import Iterable, List, Optional, Sequence, Tuple, TypedDict, Union

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION

# write_to_csv was defined here, and is still importable from here
from countess.core.output import write_to_csv  # noqa: F401  # pylint: disable=unused-import
from countess.core.output import count_leading_comment_lines, write_csv
from countess.core.parameters import (
    ArrayParam,
    BaseParam,
    BooleanParam,
    ChoiceParam,
    ColumnChoiceParam,
    DataTypeOrNoneChoiceParam,
    MultiParam,
    OutputFileParam,
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
            # a metadata header, lines starting '#', is skipped, and so is the header row when the
            # columns are named here
            "skiprows": count_leading_comment_lines(filename) + (1 if self.header and len(self.columns) > 0 else 0),
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


METADATA_HEADER_WORKFLOW = "As workflow"
METADATA_HEADER_CHOICES = {METADATA_HEADER_WORKFLOW: None, "Yes": True, "No": False}


class SaveCsvPlugin(DuckdbSaveFilePlugin):
    name = "CSV Save"
    description = "Save data as CSV or similar delimited text files"
    link = "https://countess-project.github.io/CountESS/included-plugins/#csv-writer"
    version = VERSION
    file_types = CSV_FILE_TYPES

    header = BooleanParam("CSV header row?", True)
    filename = OutputFileParam("Filename", "results.csv", file_types=file_types)
    metadata_header = ChoiceParam(
        "Metadata header ('#' lines)?", METADATA_HEADER_WORKFLOW, choices=list(METADATA_HEADER_CHOICES)
    )
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

        context = self.run_context()
        filename = self.output_path()
        logger.info("%s: writing %s", context.node_name, filename)
        header = context.metadata_lines(datetime.now()) if self.writes_metadata_header() else None
        write_csv(
            source,
            filename,
            sep=self.SEPARATORS[self.delimiter.value],
            quoting=self.QUOTING[self.quoting.value],
            header=header,
        )

    def output_path(self) -> str:
        """The file this writes: its filename, in the run's output directory unless absolute."""
        return self.filename.get_output_path(self.run_context().output_dir())

    def writes_metadata_header(self) -> bool:
        choice = METADATA_HEADER_CHOICES.get(self.metadata_header.value)
        return self.run_context().workflow.metadata_header if choice is None else choice
