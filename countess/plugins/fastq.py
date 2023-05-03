from itertools import islice

import pandas as pd
from fqfa.fastq.fastq import parse_fastq_reads  # type: ignore

from countess import VERSION
from countess.core.parameters import BooleanParam, FloatParam
from countess.core.plugins import PandasInputPlugin


class LoadFastqPlugin(PandasInputPlugin):
    """Load counts from one or more FASTQ files, by first building a dask dataframe of raw sequences
    with count=1 and then grouping by sequence and summing counts.  It supports counting
    in multiple columns."""

    name = "FASTQ Load"
    description = "Loads counts from FASTQ files containing either variant or barcodes"
    version = VERSION

    file_types = [("FASTQ", "*.fastq"), ("FASTQ (gzipped)", "*.fastq.gz")]

    parameters = {
        "min_avg_quality": FloatParam("Minimum Average Quality", 10),
        "group": BooleanParam("Group by Sequence?", True),
    }

    def read_file_to_dataframe(self, file_params, logger, row_limit=None):
        # XXX this should be a bit smarter than building up the entire
        # structure in an array ...
        records = []

        filename = file_params["filename"].value
        with open(filename, "r", encoding="utf-8") as fh:
            for fastq_read in islice(parse_fastq_reads(fh), 0, row_limit):
                if fastq_read.average_quality() >= self.parameters["min_avg_quality"].value:
                    records.append((fastq_read.sequence, fastq_read.header, filename))

        return pd.DataFrame.from_records(records, columns=("sequence", "header", "filename"))

    def combine_dfs(self, dfs):
        """first concatenate the count dataframes, then (optionally) group them by sequence"""

        combined_df = pd.concat(dfs)

        if len(combined_df) and self.parameters["group"].value:
            combined_df = (
                combined_df.groupby(by=["sequence"])
                .agg({"sequence": "first", "header": "count"})
                .rename({"header": "count"}, axis=1)
            )

        return combined_df
