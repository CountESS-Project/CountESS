import logging
from typing import Optional, Any, Mapping

from countess import VERSION
from countess.core.parameters import NumericColumnGroupChoiceParam
from countess.core.plugins import DuckdbParallelTransformPlugin

logger = logging.getLogger(__name__)

def rml_estimate(scores: list[float], sigma2s: list[float], iterations:int=50) -> tuple[float, float, float]:
    weights = [ 1 / x for x in sigma2s ]
    sum_of_weights = sum(weights)
    mean_score = sum(scores) / len(scores)
    sigma2ml = sum((score - mean_score) ** 2 for score in scores) / (len(scores)-1)
    eps = 0

    for _ in range(0, iterations):
        weights = [ 1 / (s2+sigma2ml) for s2 in sigma2s ]
        sum_of_weights = sum(weights)
        sum_of_weights_2 = sum(w ** 2 for w in weights)
        beta = sum( score * weight for score, weight in zip(scores, weights)) / sum_of_weights
        scale = sum_of_weights - (sum_of_weights_2 / sum_of_weights)
        sigma2ml_new = sigma2ml * sum((
            (score - beta) ** 2 * (weight ** 2)
            for score, weight in zip(scores, weights)
        )) / scale
        eps = abs(sigma2ml - sigma2ml_new)
        sigma2ml = sigma2ml_new

    return beta, sigma2ml, eps


class RandomEffectsPlugin(DuckdbParallelTransformPlugin):
    name = "Random Effects"
    description = "Calculate frequencies from counts"
    version = VERSION

    score_cols = NumericColumnGroupChoiceParam("Score Columns")
    stderr_cols = NumericColumnGroupChoiceParam("Standard Error Columns")

    def output_score_column(self):
        return self.score_cols.get_column_prefix().rstrip("_")

    def output_stderr_column(self):
        return self.stderr_cols.get_column_prefix().rstrip("_")

    def output_epsilon_column(self):
        return "epsilon"

    def add_fields(self) -> Mapping[Optional[str], Optional[type]]:
        return {
            self.output_score_column(): float,
            self.output_stderr_column(): float,
            self.output_epsilon_column(): float,
        }

    def transform(self, data: dict[str, Any]) -> Optional[dict[str, Any]]:

        scores = [ v for k, v in sorted(data.items()) if k.startswith(self.score_cols.get_column_prefix()) ]
        sigma2s = [ v for k, v in sorted(data.items()) if k.startswith(self.stderr_cols.get_column_prefix()) ]

        score, sigma2, epsilon = rml_estimate(scores, sigma2s)

        data[self.output_score_column()] = score
        data[self.output_stderr_column()] = sigma2
        data[self.output_epsilon_column()] = epsilon

        return data
