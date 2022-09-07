from typing import Generator, Optional
from collections.abc import Iterable, Mapping

import dask.dataframe as dd
import fqfa
import numpy as np
from countess.core.plugins import DaskInputPlugin, PluginParam

from fqfa.fastq.fastq import parse_fastq_reads
from itertools import islice
from more_itertools import ichunked
import pandas as pd

class LoadFastqPlugin(DaskInputPlugin):
    """Load counts from a FASTQ file, by first building a dask dataframe of raw sequences
    with count=1 and then grouping by sequence and summing counts.  It supports counting
    in multiple columns."""

    name = 'FASTQ Load'
    title = 'Load from FastQ'
    description = "Loads counts from FASTQ files containing either variant or barcodes"
    file_types = [('FASTQ', '*.fastq'), ('FASTQ (gzipped)', '*.fastq.gz')]

    def __init__(self, params: Mapping[str,bool|int|float|str], file_params: Iterable[Mapping[str,bool|int|float|str]]):
        self.group = params['group']
        self.files = []

    def files(self):
        return sorted(set((v for k, v in self.parameters.items() if k.startswith("filename."))))
    
    def add_file(self, filename):
        # Try loading the first few records just to see
        with open(filename, "r") as fh:
            islice(parse_fastq_reads(fh), 0, 10)
        # If that didn't throw an error this file is probably fine
        self.files.append(filename)
        
    def metadata(self):
        yield PluginParam("group", "Group by Sequence?", bool, True)
        yield PluginParam("min_avg_quality", "Minimum Average Quality", float, 10)
        for n, file in enumerate(self.files, 1):
            yield PluginParam(f"filename.{n}", "Filename", str, file, True)
            yield PluginParam(f"column.{n}", "Count Column", str, "count")
    
    def get_files(self):
        pass
        #for k in self.parameters.keys():
        #    if k.startswith("filename."):
        #        yield (self.parameters.)

    def get_columns(self):
        return set((v for k, v in self.parameters.items() if k.startswith("column.")))
        
    def yield_reads(self, count_names: list[str]) -> Generator[tuple, None, None]:
        for n, f in enumerate(self.files()):
            count_values = [ 1 if f['count_column'] == cn else 0 for cn in count_names ]
            with open(f['_filename'], "r") as fh:
                for fastq_read in parse_fastq_reads(fh):
                    if fastq_read.average_quality() >= f['min_avg_quality']:
                        yield (fastq_read.sequence, *count_values)

    def run(self, _) -> dd.DataFrame:
        # find list of distinct count column names
        files = sorted(set((v for k, v in self.parameters.items() if k.startswith("filename."))) )
        count_names = () # XXX
 
        pandas_dfs = [
                pd.DataFrame.from_records(chunk, columns=["sequence", *count_names])
                for chunk in ichunked(self.yield_reads(count_names),100000)
        ]

        ddf = dd.concat(pandas_dfs)
        if self.params['group']: ddf = ddf.groupby("sequence").sum()
        return ddf
