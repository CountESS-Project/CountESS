import bz2
import gzip
from itertools import islice
from typing import Iterable, Optional

import pandas as pd
from fqfa.fasta.fasta import parse_fasta_records  # type: ignore
from fqfa.fastq.fastq import parse_fastq_reads  # type: ignore

from countess import VERSION
from countess.core.parameters import BaseParam, BooleanParam, FloatParam, StringParam
from countess.core.plugins import PandasInputFilesPlugin
from countess.utils.files import clean_filename


def _fastq_reader(
    file_handle, min_avg_quality: float, row_limit: Optional[int] = None, filename: str = ""
) -> Iterable[dict[str, str]]:
    for fastq_read in islice(parse_fastq_reads(file_handle), 0, row_limit):
        if fastq_read.average_quality() >= min_avg_quality:
            yield {
                "sequence": fastq_read.sequence,
                "header": fastq_read.header[1:],
                "filename": clean_filename(filename),
            }


class LoadFastqPlugin(PandasInputFilesPlugin):
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

    def read_file_to_dataframe(self, filename: str, file_param: BaseParam, row_limit=None):
        min_avg_quality = float(self.min_avg_quality)

        if filename.endswith(".gz"):
            with gzip.open(filename, mode="rt", encoding="utf-8") as fh:
                dataframe = pd.DataFrame(_fastq_reader(fh, min_avg_quality, row_limit, filename))
        elif filename.endswith(".bz2"):
            with bz2.open(filename, mode="rt", encoding="utf-8") as fh:
                dataframe = pd.DataFrame(_fastq_reader(fh, min_avg_quality, row_limit, filename))
        else:
            with open(filename, "r", encoding="utf-8") as fh:
                dataframe = pd.DataFrame(_fastq_reader(fh, min_avg_quality, row_limit, filename))

        group_columns = ["sequence"]

        if not self.header_column:
            dataframe.drop(columns="header", inplace=True)
        elif self.group:
            # if we've got a header column and we're grouping by sequence,
            # find maximum common length of the 'header' field in this file
            for common_length in range(0, dataframe["header"].str.len().min() - 1):
                if dataframe["header"].str.slice(0, common_length + 1).nunique() > 1:
                    break
            if common_length > 0:
                dataframe["header"] = dataframe["header"].str.slice(0, common_length)
                group_columns.append("header")

        if self.filename_column:
            group_columns.append("filename")
        else:
            dataframe.drop(columns="filename", inplace=True)

        if self.group:
            return dataframe.assign(count=1).groupby(group_columns).count()
        else:
            return dataframe


def _fasta_reader(file_handle, row_limit: Optional[int] = None) -> Iterable[dict[str, str]]:
    for header, sequence in islice(parse_fasta_records(file_handle), 0, row_limit):
        yield {
            "__s": sequence,
            "__h": header,
        }


class LoadFastaPlugin(PandasInputFilesPlugin):
    name = "FASTA Load"
    description = "Loads sequences from FASTA files"
    link = "https://countess-project.github.io/CountESS/included-plugins/#fasta-load"
    version = VERSION

    file_types = [("FASTA", [".fasta", ".fa", ".fasta.gz", ".fa.gz", ".fasta.bz2", ".fa.bz2"])]

    sequence_column = StringParam("Sequence Column", "sequence")
    header_column = StringParam("Header Column", "header")
    filename_column = StringParam("Filename Column", "filename")

    def read_file_to_dataframe(self, filename: str, file_param: BaseParam, row_limit=None):
        if filename.endswith(".gz"):
            with gzip.open(filename, mode="rt", encoding="utf-8") as fh:
                dataframe = pd.DataFrame(_fasta_reader(fh, row_limit))
        elif filename.endswith(".bz2"):
            with bz2.open(filename, mode="rt", encoding="utf-8") as fh:
                dataframe = pd.DataFrame(_fasta_reader(fh, row_limit))
        else:
            with open(filename, "r", encoding="utf-8") as fh:
                dataframe = pd.DataFrame(_fasta_reader(fh, row_limit))

        dataframe.rename(columns={"__s": self.sequence_column.value}, inplace=True)

        if self.header_column:
            dataframe.rename(columns={"__h": self.header_column.value}, inplace=True)
        else:
            dataframe.drop(columns=["__h"], inplace=True)

        if self.filename_column:
            dataframe[self.filename_column.value] = clean_filename(filename)
        return dataframe
