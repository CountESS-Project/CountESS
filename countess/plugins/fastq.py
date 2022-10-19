from collections.abc import Iterable, Mapping
from itertools import islice
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore
from fqfa.fastq.fastq import parse_fastq_reads  # type: ignore
from more_itertools import ichunked

from countess.core.parameters import BooleanParam, FloatParam, StringParam, ArrayParam, MultiParam
from countess.core.plugins import DaskInputPlugin
from countess.utils.dask import concat_dask_dataframes

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

    file_params = {
        "count_column": StringParam("Count Column Name", "count"),
        "additional": ArrayParam("Additional Parameters",
            MultiParam("Key/Value", { 'key': StringParam("Key"), 'val': StringParam("Value")})
        ),
    }

    def read_file_to_dataframe(self, params, row_limit=None):
        records = []
        additional_keys = [ p.key.value for p in params["additional"] ]
        additional_vals = [ p.val.value for p in params["additional"] ]
        print(f"{additional_keys} {additional_vals}")
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
        # XXX this API split is a bit daft considering this change
        # Also a lot of this probably should be in the generic input plugin since other
        # file formats will probably want to do the same thing.
        combined_dfs = {}
        for n, df in enumerate(dfs):
            additional_kvs = tuple(sorted([
                (pp.key.value, pp.val.value)
                for pp in self.parameters["files"][n]['additional']
                if pp.key.value
            ]))
            if additional_kvs not in combined_dfs:
                combined_dfs[additional_kvs] = df
            else:
                print(f"<<< {combined_dfs[additional_kvs]} {df}")
                combined_dfs[additional_kvs] = dd.concat([combined_dfs[additional_kvs], df])

        print(f">>COMBINED>> {combined_dfs}")

        for additional_kvs, combined_df in combined_dfs.items():
            if self.parameters["group"].value:
                combined_df = combined_df.groupby(by=["sequence"]).sum()
            for k, v in additional_kvs:
                combined_df[k] = combined_df.apply(lambda _: v, axis=1, meta=pd.Series(v))

            # XXX why do I have to compute() this?  Surely it should be able to 
            # remain uncomputed for the moment.

            combined_dfs[additional_kvs] = combined_df.compute()

        print(f">>GROUPED>> {combined_dfs}")

        df = concat_dask_dataframes(list(combined_dfs.values()))

        print(f">>CONCAT>> {df}")

        return df
