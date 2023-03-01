import numpy as np

from countess import VERSION
from countess.core.plugins import DaskScoringPlugin


class LogScorePlugin(DaskScoringPlugin):
    """Log Scorer"""

    name = "Log Scorer"
    title = "Log Scores from Counts"
    description = "calculates log score from counts"
    version = VERSION

    def score(self, columns):
        return np.log(columns[1] / columns[0])
