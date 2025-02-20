import logging
from typing import Iterable

import biobear
import duckdb

from countess import VERSION
from countess.core.parameters import BaseParam, BooleanParam, FloatParam, StringParam
from countess.core.plugins import DuckdbLoadFilePlugin

logger = logging.getLogger(__name__)


# UDF for calculating average quality so it can be filtered.
# XXX it would be great to do this as a native DuckDB function
# as calling a Python UDF is quite slow.
def _fastq_avg_quality(quality: str) -> float:
    q_bytes = quality.encode("ascii")
    return sum(q_bytes) / len(q_bytes) - 33


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

    def load_file(
        self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam
    ) -> duckdb.DuckDBPyRelation:
        # Open the file, convert it to a RecordBatchReader and then
        # wrap that up as a DuckDBPyRelation so we can filter it.
        reader = biobear.connect().read_fastq_file(filename)
        rel = cursor.from_arrow(reader.to_arrow_record_batch_reader())

        if self.min_avg_quality > 0:
            try:
                cursor.create_function(
                    "fastq_avg_quality", _fastq_avg_quality, exception_handling="return_null", side_effects=False
                )
            except duckdb.CatalogException as exc:
                assert "fastq_avg_quality" in str(exc)
            rel = rel.filter("fastq_avg_quality(quality_scores) >= %f" % self.min_avg_quality.value)

        if self.group:
            rel = rel.aggregate("sequence, count(*) as count")
        elif self.header_column:
            rel = rel.project("sequence, name || ' ' || description as header")
        else:
            rel = rel.project("sequence")
        return rel

    def combine(
        self, ddbc: duckdb.DuckDBPyConnection, tables: Iterable[duckdb.DuckDBPyRelation]
    ) -> duckdb.DuckDBPyRelation:
        combined_view = super().combine(ddbc, tables)
        if self.filename_column or self.header_column:
            return combined_view
        else:
            return combined_view.aggregate("sequence, sum(count) as count")


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
