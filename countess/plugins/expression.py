import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, PerColumnArrayParam, TextParam
from countess.core.plugins import PandasSimplePlugin


def process(df: pd.DataFrame, codes, logger: Logger):
    for code in codes:
        if not code:
            continue

        try:
            result = df.eval(code)
        except Exception as exc:  # pylint: disable=W0718
            logger.exception(exc)
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


class ExpressionPlugin(PandasSimplePlugin):
    name = "Expression"
    description = "Apply simple expressions"
    version = VERSION

    parameters = {
        "code": TextParam("Expressions"),
        "drop": PerColumnArrayParam("Drop Columns", BooleanParam("Drop")),
    }

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["drop"], PerColumnArrayParam)

        codes = [c.replace("\n", " ").strip() for c in self.parameters["code"].value.split("\n\n")]
        df = process(dataframe, codes, logger)

        drop_names = [ label for label, param in self.parameters["drop"].get_column_params() if param.value ]

        drop_indexes = [col for col in drop_names if col in df.index.names]
        if drop_indexes:
            df = df.reset_index(drop_indexes, drop=True)

        drop_columns = [col for col in drop_names if col in df.columns]
        if drop_columns:
            df.drop(columns=drop_columns, inplace=True)

        return df
