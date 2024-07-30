import bz2
import gzip
from itertools import islice

import pandas as pd
from fqfa.fastq.fastq import parse_fastq_reads  # type: ignore

from countess import VERSION
from countess.core.parameters import BooleanParam, FloatParam
from countess.core.plugins import PandasInputFilesPlugin
from countess.utils.files import clean_filename


def _file_reader(file_handle, min_avg_quality, row_limit=None, filename=""):
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

    parameters = {
        "min_avg_quality": FloatParam("Minimum Average Quality", 10),
        "header_column": BooleanParam("Header Column?", False),
        "filename_column": BooleanParam("Filename Column?", False),
        "group": BooleanParam("Group by Sequence?", True),
    }

    def read_file_to_dataframe(self, file_params, logger, row_limit=None):
        filename = file_params["filename"].value
        min_avg_quality = self.parameters["min_avg_quality"].value

        if filename.endswith(".gz"):
            with gzip.open(filename, mode="rt", encoding="utf-8") as fh:
                dataframe = pd.DataFrame(_file_reader(fh, min_avg_quality, row_limit, filename))
        elif filename.endswith(".bz2"):
            with bz2.open(filename, mode="rt", encoding="utf-8") as fh:
                dataframe = pd.DataFrame(_file_reader(fh, min_avg_quality, row_limit, filename))
        else:
            with open(filename, "r", encoding="utf-8") as fh:
                dataframe = pd.DataFrame(_file_reader(fh, min_avg_quality, row_limit, filename))

        group_columns = ["sequence"]

        if not self.parameters["header_column"].value:
            dataframe.drop(columns="header", inplace=True)
        elif self.parameters["group"].value:
            # if we've got a header column and we're grouping by sequence,
            # find maximum common length of the 'header' field in this file
            for common_length in range(0, dataframe["header"].str.len().min() - 1):
                if dataframe["header"].str.slice(0, common_length + 1).nunique() > 1:
                    break
            if common_length > 0:
                dataframe["header"] = dataframe["header"].str.slice(0, common_length)
                group_columns.append("header")

        if self.parameters["filename_column"].value:
            group_columns.append("filename")
        else:
            dataframe.drop(columns="filename", inplace=True)

        if self.parameters["group"].value:
            return dataframe.assign(count=1).groupby(group_columns).count()
        else:
            return dataframe
