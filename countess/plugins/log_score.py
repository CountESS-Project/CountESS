from collections.abc import Iterable, Mapping
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np

from countess.core.plugins import DaskScoringPlugin

VERSION = "0.0.1"


class LogScorePlugin(DaskScoringPlugin):
    """Load counts from a FASTQ file, by first building a dask dataframe of raw sequences
    with count=1 and then grouping by sequence and summing counts.  It supports counting
    in multiple columns."""

    name = "Log Scorer"
    title = "Log Scores from Counts"
    description = "calculates log score from counts"
    version = VERSION

    def score(self, cols):
        return np.log(cols[1] / cols[0])
