import logging
from typing import Any, Mapping, Optional

from countess import VERSION
from countess.core.parameters import NumericColumnGroupChoiceParam
from countess.core.plugins import DuckdbParallelTransformPlugin

logger = logging.getLogger(__name__)


def rml_estimate(
    scores: list[float], sigmas: list[float], iterations: int = 50, epsilon: float = 1e-7
) -> tuple[float, float]:
    """Implementation of the robust maximum likelihood estimator.
    Iteratively estimates the heterogeneity between score estimates
    and returns the weighted average score and the standard deviation
    of that score."""

    # Based on Eugene Demidenko "Mixed models: theory and applications with R"
    # 2ed (2013), Wiley & Sons, pages 246-253.
    #
    # "it is assumed that besides the variation within the study, there
    # exists a variation between studies, and this variation is represented
    # by the random effect `b_i` with an unknown variance `σ^2` [...]
    # called the heterogeneity (variance) parameter."
    #
    # This code is using the formulae on page 253 for "restricted maximum
    # likelihood" rather than the R code on page 252.
    #
    # `y_i` -> scores[i]
    # `\sigma^2_i -> variances[i]
    # `\hat\beta_s` -> estimate
    # `\hat{\sigma^2}_s -> heterogeneity

    variances = [sigma**2 for sigma in sigmas]
    weights = [1 / variance for variance in variances]
    sum_of_weights = sum(weights)
    estimate = sum(score * weight for score, weight in zip(scores, weights)) / sum_of_weights
    heterogeneity = sum((score - estimate) ** 2 for score in scores) / (len(scores) - 1)

    for _ in range(0, iterations):
        weights = [1 / (variance + heterogeneity) for variance in variances]
        sum_of_weights = sum(weights)
        sum_of_weights_2 = sum(w**2 for w in weights)

        estimate = sum(score * weight for score, weight in zip(scores, weights)) / sum_of_weights

        adjustment = sum((score - estimate) ** 2 * (weight**2) for score, weight in zip(scores, weights)) / (
            sum_of_weights - (sum_of_weights_2 / sum_of_weights)
        )
        heterogeneity *= adjustment
        if 1 - epsilon < adjustment < 1 + epsilon:
            break

    # make a final estimate of overall variance
    return estimate, 1 / sum(weights) ** 0.5


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

        if any(sigma == 0 for sigma in sigmas):
            return None

        score, sigma = rml_estimate(scores, sigmas)

        data["score"] = score
        data["sigma"] = sigma

        return data
