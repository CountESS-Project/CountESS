import bz2
import gzip
import logging
from itertools import islice
from typing import Iterable, Optional
import secrets

import biobear
import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import BaseParam, BooleanParam, FloatParam, StringParam
from countess.core.plugins import DuckdbLoadFilePlugin
from countess.utils.files import clean_filename

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
    filename_column = BooleanParam("Filename Column?", False)
    group = BooleanParam("Group by Sequence?", True)

    def load_file(self, cursor: DuckDBPyConnection, filename: str, file_param: BaseParam, file_number: int) -> duckdb.DuckDBPyRelation:

        record_batch_reader = biobear.connect().read_fastq_file(filename).to_arrow_record_batch_reader()
        reader_name = f"r_{file_number}"
        logger.debug("LoadFastqPlugin.load_file reader_name %s filename %s", reader_name, filename)
        cursor.register(reader_name, record_batch_reader)
        return cursor.sql(f"select sequence, count(*) from {reader_name} group by sequence")


class LoadFastaPlugin(DuckdbLoadFilePlugin):
    name = "FASTA Load"
    description = "Loads sequences from FASTA files"
    link = "https://countess-project.github.io/CountESS/included-plugins/#fasta-load"
    version = VERSION

    file_types = [("FASTA", [".fasta", ".fa", ".fasta.gz", ".fa.gz", ".fasta.bz2", ".fa.bz2"])]

    sequence_column = StringParam("Sequence Column", "sequence")
    header_column = StringParam("Header Column", "header")
    filename_column = StringParam("Filename Column", "filename")

    def load_file(self, cursor: DuckDBPyConnection, filename: str, file_param: BaseParam, file_number: int) -> duckdb.DuckDBPyRelation:

        record_batch_reader = biobear.connect().read_fasta_file(filename).to_arrow_record_batch_reader()
        reader_name = f"r_{file_number}"
        cursor.register(reader_name, record_batch_reader)
        return cursor.sql(f"select * from {reader_name}")
