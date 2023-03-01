from itertools import islice

import pandas as pd  # type: ignore
from fqfa.fastq.fastq import parse_fastq_reads  # type: ignore

from countess import VERSION
from countess.core.parameters import BooleanParam, FloatParam
from countess.core.plugins import DaskInputPlugin
from countess.utils.dask import concat_dataframes


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

    def read_file_to_dataframe(self, file_params, logger, row_limit=None):
        records = []
        count_column_name = "count"

        with open(file_params["filename"].value, "r", encoding="utf-8") as fh:
            for fastq_read in islice(parse_fastq_reads(fh), 0, row_limit):
                if fastq_read.average_quality() >= self.parameters["min_avg_quality"].value:
                    records.append((fastq_read.sequence, 1))
        return pd.DataFrame.from_records(records, columns=("sequence", count_column_name))

    def combine_dfs(self, dfs):
        """first concatenate the count dataframes, then (optionally) group them by sequence"""

        combined_df = concat_dataframes(dfs)

        if len(combined_df) and self.parameters["group"].value:
            combined_df = combined_df.groupby(by=["sequence"]).sum()

        return combined_df
