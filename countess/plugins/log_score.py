import numpy as np

from countess import VERSION
from countess.core.plugins import PandasScoringPlugin


class LogScorePlugin(PandasScoringPlugin):
    """Log Scorer"""

    name = "Log Scorer"
    description = "calculates log score from counts"
    version = VERSION

    def score(self, columns):
        return np.log(columns[1] / columns[0])
