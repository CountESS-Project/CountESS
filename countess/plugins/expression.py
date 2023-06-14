import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ArrayParam, BooleanParam, PerColumnArrayParam, TextParam
from countess.core.plugins import PandasTransformPlugin


def process(df: pd.DataFrame, codes, logger: Logger):
    for code in codes:
        if not code:
            continue
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

    parameters = {
        "code": TextParam("Expressions"),
        "drop": PerColumnArrayParam("Drop Columns", BooleanParam("Drop")),
    }

    def run_df(self, df, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["code"], TextParam)
        assert isinstance(self.parameters["drop"], ArrayParam)

        codes = [c.replace("\n", " ").strip() for c in self.parameters["code"].value.split("\n\n")]

        df = process(df, codes, logger)

        drop_columns = [
            col for col, param in zip(self.input_columns, self.parameters["drop"]) if param.value
        ]

        print(drop_columns)
        return df.drop(columns=drop_columns)
