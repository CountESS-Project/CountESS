import logging
from typing import Any, Mapping, Optional

from countess import VERSION
from countess.core.parameters import NumericColumnGroupChoiceParam
from countess.core.plugins import DuckdbParallelTransformPlugin

logger = logging.getLogger(__name__)


def rml_estimate(scores: list[float], sigmas: list[float], iterations: int = 50) -> tuple[float, float]:
    weights = [1 / sigma**2 for sigma in sigmas]
    sum_of_weights = sum(weights)
    mean_score = sum(scores) / len(scores)
    variance = sum((score - mean_score) ** 2 for score in scores) / (len(scores) - 1)

    for _ in range(0, iterations):
        weights = [1 / (sigma**2 + variance) for sigma in sigmas]
        sum_of_weights = sum(weights)
        sum_of_weights_2 = sum(w**2 for w in weights)
        beta = sum(score * weight for score, weight in zip(scores, weights)) / sum_of_weights
        scale = sum_of_weights - (sum_of_weights_2 / sum_of_weights)
        adjust = sum((score - beta) ** 2 * (weight**2) for score, weight in zip(scores, weights)) / scale
        variance *= adjust

    return beta, variance**0.5


class RandomEffectsPlugin(DuckdbParallelTransformPlugin):
    name = "Random Effects"
    description = "Calculate frequencies from counts"
    version = VERSION

    score_cols = NumericColumnGroupChoiceParam("Score Columns")
    sigma_cols = NumericColumnGroupChoiceParam("Stddev Columns")

    def add_fields(self) -> Mapping[Optional[str], Optional[type]]:
        return {
            "score": float,
            "sigma": float,
        }

    def transform(self, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        scores = [v for k, v in sorted(data.items()) if k.startswith(self.score_cols.get_column_prefix())]
        sigmas = [v for k, v in sorted(data.items()) if k.startswith(self.sigma_cols.get_column_prefix())]

        if not scores or None in scores or not sigmas or None in sigmas:
            return None

        score, sigma = rml_estimate(scores, sigmas)

        data["score"] = score
        data["sigma"] = sigma

        return data
