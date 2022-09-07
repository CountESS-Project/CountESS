from typing import Generator, Optional
from collections.abc import Iterable, Mapping

import dask.dataframe as dd
import numpy as np
from countess.core.plugins import DaskScoringPlugin

class LogScorePlugin(DaskScoringPlugin):
    """Load counts from a FASTQ file, by first building a dask dataframe of raw sequences
    with count=1 and then grouping by sequence and summing counts.  It supports counting
    in multiple columns."""

    name = 'Log Scorer'
    title = 'Log Scores from Counts'
    description = "calculates log score from counts"

    def score(self, col1, col0):
        return np.log(col1 / col0)
