from typing import Generator, Optional
from collections.abc import Iterable, Mapping

import dask.dataframe as dd
import fqfa
import numpy as np
from countess.core.plugins import DaskInputPlugin, DaskOutputPlugin
from fqfa.fastq.fastq import parse_fastq_reads
from more_itertools import ichunked
import pandas as pd

class LoadFastqPlugin(DaskInputPlugin):
    """Load counts from a FASTQ file, by first building a dask dataframe of raw sequences
    with count=1 and then grouping by sequence and summing counts.  It supports counting
    in multiple columns."""

    name = 'FASTQ Load'
    title = 'Load from FastQ'
    description = "Loads counts from FASTQ files containing either variant or barcodes"
    file_types = [('FASTQ', '*.fastq')]

    params = {
        "group": { "label": "Group by Sequence", "type": bool, "default": True },
    }

    file_params = {
        "min_avg_quality": { "label": "Minimum Average Quality", "type": float, "default": 10 },
        "count_column": { "label": "Count Column", "type": str, "default": "count" },
    }

    def __init__(self, params: Mapping[str,bool|int|float|str], file_params: Iterable[Mapping[str,bool|int|float|str]]):
        self.group = params['group']
        self.files = file_params

    def yield_reads(self, count_names: list[str]) -> Generator[tuple, None, None]:
        for n, f in enumerate(self.files):
            count_values = [ 1 if f['count_column'] == cn else 0 for cn in count_names ]
            with open(f['_filename'], "r") as fh:
                for fastq_read in parse_fastq_reads(fh):
                    if fastq_read.average_quality() >= f['min_avg_quality']:
                        yield (fastq_read.sequence, *count_values)

    def run(self, _) -> dd.DataFrame:
        # find list of distinct count column names
        count_names = sorted(list(set((f['count_column'] for f in self.files))))
 
        pandas_dfs = [
                pd.DataFrame.from_records(chunk, columns=["sequence", *count_names])
                for chunk in ichunked(self.yield_reads(count_names),100000)
        ]

        ddf = dd.concat(pandas_dfs)
        if self.params['group']: ddf = ddf.groupby("sequence").sum()
        return ddf
