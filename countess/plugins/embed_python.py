import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ArrayParam, TextParam
from countess.core.plugins import PandasTransformPlugin


def process(df: pd.DataFrame, codes):
    for code in codes:
        result = df.eval(code)
        if isinstance(result, pd.Series):
            # this was a filter
            df = df.copy()
            df["__filter"] = result
            df = df.query("__filter != 0").drop(columns="__filter")
        else:
            # this was a column assignment
            df = result

    return df


class EmbeddedPythonPlugin(PandasTransformPlugin):
    name = "Embedded Python"
    description = "Embed Python code into CountESS"
    version = VERSION

    parameters = {"code": ArrayParam("Code", TextParam("Code"))}

    def run_df(self, df, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["code"], ArrayParam)

        codes = [
            p.value.replace("\n", " ")
            for p in self.parameters["code"]
            if isinstance(p, TextParam) and p.value.strip()
        ]

        return process(df, codes)
