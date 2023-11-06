import gzip
from itertools import islice

import pandas as pd
from fqfa.fastq.fastq import parse_fastq_reads  # type: ignore

from countess import VERSION
from countess.core.parameters import BooleanParam, FloatParam
from countess.core.plugins import PandasInputFilesPlugin


def _file_reader(file_handle, min_avg_quality, row_limit=None):
    for fastq_read in islice(parse_fastq_reads(file_handle), 0, row_limit):
        if fastq_read.average_quality() >= min_avg_quality:
            yield {"sequence": fastq_read.sequence, "header": fastq_read.header[1:]}


class LoadFastqPlugin(PandasInputFilesPlugin):
    """Load counts from one or more FASTQ files, by first building a dask dataframe of raw sequences
    with count=1 and then grouping by sequence and summing counts.  It supports counting
    in multiple columns."""

    name = "FASTQ Load"
    description = "Loads counts from FASTQ files containing either variant or barcodes"
    link = "https://countess-project.github.io/CountESS/included-plugins/#fastq-load"
    version = VERSION

    file_types = [("FASTQ", ".fastq"), ("FASTQ (gzipped)", ".gz")]

    parameters = {
        "min_avg_quality": FloatParam("Minimum Average Quality", 10),
        "group": BooleanParam("Group by Sequence?", True),
    }

    def read_file_to_dataframe(self, file_params, logger, row_limit=None):
        filename = file_params["filename"].value
        min_avg_quality = self.parameters["min_avg_quality"].value

        if filename.endswith(".gz"):
            with gzip.open(filename, mode="rt", encoding="utf-8") as fh:
                dataframe = pd.DataFrame(_file_reader(fh, min_avg_quality, row_limit))
        else:
            with open(filename, "r", encoding="utf-8") as fh:
                dataframe = pd.DataFrame(_file_reader(fh, min_avg_quality, row_limit))

        if self.parameters["group"].value:
            for comm_len in range(0, dataframe["header"].str.len().min() - 1):
                if dataframe["header"].str.slice(0, comm_len + 1).nunique() > 1:
                    break

            if comm_len > 0:
                dataframe["header"] = dataframe["header"].str.slice(0, comm_len)
                dataframe = dataframe.assign(count=1).groupby(["sequence", "header"]).count()
            else:
                dataframe = dataframe.groupby("sequence").count()

        return dataframe
