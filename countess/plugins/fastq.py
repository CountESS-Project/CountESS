import itertools
import logging
from typing import Iterable, Optional

import dnaio
import duckdb
import pyarrow

from countess import VERSION
from countess.core.parameters import BaseParam, BooleanParam, FloatParam
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
        logger.debug("Loading file %s row_limit %s", filename, row_limit)

        # Take up to row_limit records from this file
        fastq_iter = itertools.islice(dnaio.open(filename, open_threads=1), row_limit)

        def _record_to_dict(record):
            d = {"sequence": record.sequence}
            if self.header_column:
                d["header"] = record.name
            return d

        def _avg_quality(record):
            return sum(ord(c) for c in record.qualities) / len(record.qualities) - 33

        pyarrow_schema = pyarrow.schema([pyarrow.field("sequence", pyarrow.string())])
        if self.header_column:
            pyarrow_schema.append(pyarrow.field("header", pyarrow.string()))

        # Generator which batches records 5000 at a time into RecordBatches
        record_batch_iter = (
            pyarrow.RecordBatch.from_pylist(
                [
                    _record_to_dict(record)
                    for record in batch
                    if self.min_avg_quality <= 0 or self.min_avg_quality <= _avg_quality(record)
                ]
            )
            for batch in itertools.batched(fastq_iter, 5000)
        )

        # We can turn that generator of RecordBatches into a temporary table
        rel = cursor.from_arrow(pyarrow.RecordBatchReader.from_batches(pyarrow_schema, record_batch_iter))

        if self.group:
            rel = rel.aggregate("sequence, count(*) as count")

        logger.debug("Loading file %s row_limit %s done", filename, row_limit)
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

    def load_file(
        self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam, row_limit: Optional[int] = None
    ) -> duckdb.DuckDBPyRelation:
        pyarrow_schema = pyarrow.schema(
            [pyarrow.field("sequence", pyarrow.string()), pyarrow.field("header", pyarrow.string())]
        )

        fasta_iter = itertools.islice(dnaio.open(filename, open_threads=1), row_limit)
        record_batch_iter = (
            pyarrow.RecordBatch.from_pylist([{"sequence": z.sequence, "header": z.name} for z in y])
            for y in itertools.batched(fasta_iter, 5000)
        )
        rel = cursor.from_arrow(pyarrow.RecordBatchReader.from_batches(pyarrow_schema, record_batch_iter))
        return rel
