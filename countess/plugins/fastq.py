from collections.abc import Iterable, Mapping
from itertools import islice
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore
from fqfa.fastq.fastq import parse_fastq_reads  # type: ignore
from more_itertools import ichunked

from countess.core.parameters import BooleanParam, FloatParam, StringParam
from countess.core.plugins import DaskInputPlugin

VERSION = "0.0.1"


class LoadFastqPlugin(DaskInputPlugin):
    """Load counts from a FASTQ file, by first building a dask dataframe of raw sequences
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

    file_params = {"count_column": StringParam("Count Column Name", "count")}

    def read_file_to_dataframe(self, params, row_limit=None):
        records = []
        with open(params["filename"].value, "r") as fh:
            for fastq_read in islice(parse_fastq_reads(fh), 0, row_limit):
                if (
                    fastq_read.average_quality()
                    >= self.parameters["min_avg_quality"].value
                ):
                    records.append((fastq_read.sequence, 1))
        return pd.DataFrame.from_records(
            records, columns=("sequence", params["count_column"].value)
        )

    def combine_dfs(self, dfs):
        ddf = super().combine_dfs(dfs)
        if len(ddf) > 0 and self.parameters["group"].value:
            ddf = ddf.groupby("sequence").sum()
        return ddf
