import logging
import secrets
from typing import Any, Iterable, Mapping, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import ColumnChoiceParam, MultiColumnChoiceParam, NumericColumnGroupChoiceParam
from countess.core.plugins import DuckdbParallelTransformPlugin, DuckdbSqlPlugin
from countess.utils.duckdb import duckdb_escape_identifier

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

    assert len(scores) == len(sigmas)

    if len(scores) == 1:
        return scores[0], sigmas[0]

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
    description = "Combine scores using a Random Effects Model"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#random-effects-model"

    score_cols = NumericColumnGroupChoiceParam("Score Colums")
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


class RandomEffectsPlugin2(DuckdbSqlPlugin):
    name = "Random Effects (Group)"
    description = "Combine scores in a group using a Random Effects Model"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#random-effects-model"

    score_col = ColumnChoiceParam("Score Column")
    sigma_col = ColumnChoiceParam("Sigma Column")
    group_cols = MultiColumnChoiceParam("Group By")
    function_name = None

    def prepare(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> None:
        super().prepare(ddbc, source)
        if not self.function_name:
            self.function_name = "f_" + secrets.token_hex(16)
            ddbc.create_function(  # type: ignore[call-overload]
                self.function_name,
                lambda scores, sigmas: rml_estimate(scores, sigmas),  # pylint: disable=unnecessary-lambda
                return_type="DOUBLE[]",
                null_handling="special",
            )

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        if not (self.score_col.value and self.sigma_col.value):
            return f"select * from {table_name}"

        score_col = duckdb_escape_identifier(self.score_col.value)
        sigma_col = duckdb_escape_identifier(self.sigma_col.value)

        if self.group_cols.get_values():
            group_cols = ",".join(duckdb_escape_identifier(c) for c in self.group_cols.get_values())
            return f"""
                SELECT {group_cols}, _ROW[1] AS score, _ROW[2] AS sigma
                FROM (
                    SELECT {group_cols}, {self.function_name}(LIST({score_col}), LIST({sigma_col})) as _ROW
                    FROM {table_name} GROUP BY {group_cols}
                ) X
            """
        else:
            return f"""
                 SELECT _ROW[1] AS score, _ROW[2] AS sigma
                 FROM (
                     SELECT {self.function_name}(LIST({score_col}), LIST({sigma_col})) AS _ROW
                     FROM {table_name}
                 )
            """
