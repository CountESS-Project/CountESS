import logging
import secrets

import biobear
import duckdb
import pyarrow

from countess import VERSION
from countess.core.parameters import BaseParam, BooleanParam, FloatParam, StringParam
from countess.core.plugins import DuckdbLoadFilePlugin
from countess.utils.duckdb import duckdb_escape_literal

logger = logging.getLogger(__name__)


class LoadFastqPlugin(DuckdbLoadFilePlugin):
    """Load counts from one or more FASTQ files, by first building a dask dataframe of raw sequences
    with count=1 and then grouping by sequence and summing counts.  It supports counting
    in multiple columns."""

    name = "FASTQ Load"
    description = "Loads counts from FASTQ files containing either variant or barcodes"
    link = "https://countess-project.github.io/CountESS/included-plugins/#fastq-load"
    version = VERSION

    file_types = [("FASTQ", [".fastq", ".fastq.gz", ".fastq.bz2"])]

    min_avg_quality = FloatParam("Minimum Average Quality", 10)
    header_column = BooleanParam("Header Column?", False)
    group = BooleanParam("Group by Sequence?", True)

    def load_file(self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam) -> pyarrow.RecordBatchReader:
        reader_name = "t_" + secrets.token_hex(10)
        cursor.register(
            reader_name,
            biobear.connect().read_fastq_file(filename).to_arrow_record_batch_reader()
        )
        rel = cursor.sql(f"SELECT * FROM {reader_name}")
        if self.group:
            rel = rel.aggregate("sequence, count(*) as count")
        elif self.header_column:
            rel = rel.project("sequence, name || ' ' || description as header")
        return rel

    def get_schema(self):
        r = {"sequence": str}
        if self.header_column:
            r["header"] = str
        if self.filename_column:
            r["filename"] = str
        return r

    def x_post_process(self, ddbc, view):
        sql = "SELECT sequence"
        if self.filename_column:
            sql += ", filename"
        if self.group:
            sql += ", COUNT(*) AS count"
        else:
            sql += ", name || ' ' || description AS header"
        sql += " FROM " + view.alias

        if self.min_avg_quality.value:
            # XXX it would be great to do this as a native
            # DuckDB function as calling a Python UDF is quite
            # slow.
            def _avg_quality(quality: str) -> float:
                q_bytes = quality.encode("ascii")
                return sum(q_bytes) / len(q_bytes) - 33

            try:
                ddbc.create_function(
                    "fastq_avg_quality", _avg_quality, exception_handling="return_null", side_effects=False
                )
            except duckdb.NotImplementedException as exc:
                assert "'fastq_avg_quality' is already created" in str(exc)
            sql += " WHERE fastq_avg_quality(quality_scores) >= " + duckdb_escape_literal(self.min_avg_quality.value)

        if self.group:
            sql += " GROUP BY sequence"
            if self.filename_column:
                sql += ", filename"

        logger.debug("LoadFastqPlugin SQL %s", sql)
        return ddbc.sql(sql)


class LoadFastaPlugin(DuckdbLoadFilePlugin):
    name = "FASTA Load"
    description = "Loads sequences from FASTA files"
    link = "https://countess-project.github.io/CountESS/included-plugins/#fasta-load"
    version = VERSION

    file_types = [("FASTA", [".fasta", ".fa", ".fasta.gz", ".fa.gz", ".fasta.bz2", ".fa.bz2"])]

    sequence_column = StringParam("Sequence Column", "sequence")
    header_column = StringParam("Header Column", "header")
    filename_column = StringParam("Filename Column", "filename")

    def load_file(self, filename: str, file_param: BaseParam) -> duckdb.DuckDBPyRelation:
        return biobear.connect().read_fasta_file(filename).to_arrow_record_batch_reader()

    def get_schema(self):
        return {self.sequence_column.value: str, self.header_column.value: str, self.filename_column.value: str}
