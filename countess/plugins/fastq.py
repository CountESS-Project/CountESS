from typing import Generator, Optional
from collections.abc import Iterable, Mapping

import dask.dataframe as dd
import fqfa
import numpy as np
from countess.core.parameters import BooleanParam, FloatParam, StringParam
from countess.core.plugins import DaskInputPlugin

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
    parameters = {
            'group': BooleanParam('Group by Sequence?', True),
            'min_avg_quality': FloatParam('Minimum Average Quality', 10)
    }

    file_params = {
            'count_column': StringParam('Count Column Name', 'count')
            }

    def read_file_to_dataframe(self, filename, count_column):
        records = []
        with open(filename.value, "r") as fh:
            for fastq_read in parse_fastq_reads(fh):
                if fastq_read.average_quality() >= self.parameters['min_avg_quality'].value:
                    records.append((fastq_read.sequence, 1))
        return pd.DataFrame.from_records(records, columns=("sequence", count_column.value))

    def run_with_progress_callback(self, ddf, callback):
        ddf = super().run_with_progress_callback(ddf, callback)
        n_files = len(list(self.get_file_params()))

        if self.parameters["group"]:
            callback(n_files, n_files+1, "Grouping")
            return ddf.groupby('sequence').sum()
        
        return ddf

    def output_columns(self):
        return set((
            fp['count_column'].value
            for fp in self.get_file_params()
        ))

