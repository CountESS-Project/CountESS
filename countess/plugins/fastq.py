from collections.abc import Iterable, Mapping
from itertools import islice
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore
from fqfa.fastq.fastq import parse_fastq_reads  # type: ignore
from more_itertools import ichunked

from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    FileArrayParam,
    FileParam,
    FloatParam,
    MultiParam,
    StringParam,
)
from countess.core.plugins import DaskInputPlugin
from countess.utils.dask import concat_dask_dataframes, merge_dask_dataframes

VERSION = "0.0.1"


class LoadFastqPlugin(DaskInputPlugin):
    """Load counts from one or more FASTQ files, by first building a dask dataframe of raw sequences
    with count=1 and then grouping by sequence and summing counts.  It supports counting
    in multiple columns."""

    name = "FASTQ Load"
    title = "Load from FastQ"
    description = "Loads counts from FASTQ files containing either variant or barcodes"
    version = VERSION

    file_types = [("FASTQ", "*.fastq"), ("FASTQ (gzipped)", "*.fastq.gz")]

    parameters = {
        "group": BooleanParam("Group by Sequence?", True),
        "min_avg_quality": FloatParam("Minimum Average Quality", 10),
    }

    def read_file_to_dataframe(self, file_param, column_suffix="", row_limit=None):
        records = []
        count_column_name = "count"
        if column_suffix:
            count_column_name += "_" + column_suffix

        with open(file_param["filename"].value, "r") as fh:
            for fastq_read in islice(parse_fastq_reads(fh), 0, row_limit):
                if (
                    fastq_read.average_quality()
                    >= self.parameters["min_avg_quality"].value
                ):
                    records.append((fastq_read.sequence, 1))
        return pd.DataFrame.from_records(
            records, columns=("sequence", count_column_name)
        )

    def combine_dfs(self, dfs):
        """first concatenate the count dataframes, then (optionally) group them by sequence"""

        combined_df = concat_dask_dataframes(dfs)

        if len(combined_df) and self.parameters["group"].value:
            combined_df = combined_df.groupby(by=["sequence"]).sum()

        return combined_df
