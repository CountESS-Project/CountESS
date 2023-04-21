import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import TextParam
from countess.core.plugins import PandasTransformPlugin


def process(df: pd.DataFrame, codes, logger: Logger):
    for code in codes:
        try:
            result = df.eval(code)
        except Exception as exc:  # pylint: disable=W0718
            logger.error(str(exc))
            continue

        if isinstance(result, pd.Series):
            # this was a filter
            df = df.copy()
            df["__filter"] = result
            df = df.query("__filter != 0").drop(columns="__filter")
        else:
            # this was a column assignment
            df = result

    return df


class ExpressionPlugin(PandasTransformPlugin):
    name = "Expression"
    description = "Apply simple expressions"
    version = VERSION

    parameters = {"code": TextParam("Expressions")}

    def run_df(self, df, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["code"], TextParam)

        codes = [c.replace("\n", " ").strip() for c in self.parameters["code"].value.split("\n\n")]

        return process(df, codes, logger)
