import itertools
import logging
from typing import Iterable, Optional

import dnaio
import duckdb
import pyarrow

from countess import VERSION
from countess.core.parameters import BaseParam, BooleanParam, FloatParam, StringParam
from countess.core.plugins import DuckdbLoadFileWithTheLotPlugin

logger = logging.getLogger(__name__)


class LoadFastqPlugin(DuckdbLoadFileWithTheLotPlugin):
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
        self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam, row_limit: Optional[int] = None
    ) -> duckdb.DuckDBPyRelation:
        # Open the file, convert it to a RecordBatchReader and then
        # wrap that up as a DuckDBPyRelation so we can filter it.
        fastq_iter = dnaio.open(filename, open_threads=1)
        record_batch_iter = (
            pyarrow.RecordBatch.from_pylist([{'sequence': z.sequence, 'quality_scores': z.qualities} for z in y])
            for y in itertools.batched(fastq_iter, 5000)
        )
        rel = cursor.from_arrow(
            pyarrow.RecordBatchReader.from_batches(
                pyarrow.schema({'sequence': 'str', 'quality_scores': 'str'}),
                record_batch_iter
            )
        )
        if row_limit is not None:
            pass
        #rel = rel.limit(row_limit)

        if self.min_avg_quality > 0:
            rel = rel.filter(
                "list_aggregate(list_transform(string_split(quality_scores, ''), x -> ord(x)), 'avg') - 33 >= %f"
                % self.min_avg_quality.value
            )

        if self.group:
            rel = rel.aggregate("sequence, count(*) as count")
        elif self.header_column:
            rel = rel.project("sequence, name || ' ' || description as header")
        else:
            rel = rel.project("sequence")
        return rel

    def combine(
        self, ddbc: duckdb.DuckDBPyConnection, tables: Iterable[duckdb.DuckDBPyRelation]
    ) -> Optional[duckdb.DuckDBPyRelation]:
        combined_view = super().combine(ddbc, tables)
        if combined_view is None:
            return None
        elif self.filename_column or self.header_column:
            return combined_view
        else:
            return combined_view.aggregate("sequence, sum(count) as count")


class LoadFastaPlugin(DuckdbLoadFileWithTheLotPlugin):
    name = "FASTA Load"
    description = "Loads sequences from FASTA files"
    link = "https://countess-project.github.io/CountESS/included-plugins/#fasta-load"
    version = VERSION

    file_types = [("FASTA", [".fasta", ".fa", ".fasta.gz", ".fa.gz", ".fasta.bz2", ".fa.bz2"])]

    sequence_column = StringParam("Sequence Column", "sequence")
    header_column = StringParam("Header Column", "header")

    def load_file(
        self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam, row_limit: Optional[int] = None
    ) -> duckdb.DuckDBPyRelation:
        fasta_iter = dnaio.open(filename, open_threads=1)
        record_batch_iter = (
            pyarrow.RecordBatch.from_pylist([{'seq': z.sequence, 'qual': z.qualities} for z in y])
            for y in itertools.batched(fasta_iter, 5000)
        )
        rel = cursor.from_arrow(
            pyarrow.RecordBatchReader.from_batches(
                pyarrow.schema({'seq': 'str', 'qual': 'str'}),
                record_batch_iter
            )
        )
        if row_limit is not None:
            rel = rel.limit(row_limit)
        return rel
