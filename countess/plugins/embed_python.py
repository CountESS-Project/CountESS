import dask.dataframe as dd
import pandas as pd  # type: ignore

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    StringParam,
    TextParam,
)
from countess.core.plugins import DaskTransformPlugin


def process(df: pd.DataFrame, codes):

    for code in codes:
        result = df.eval(code)
        if isinstance(result, (dd.Series, pd.Series)):
            # this was a filter
            df["_filter"] = result
            df = df.query("_filter").drop(columns="_filter")
        else:
            # this was a column assignment
            df = result

    return df


class EmbeddedPythonPlugin(DaskTransformPlugin):

    name = "Embedded Python"
    title = "Embedded Python"
    description = "Embed Python code into CountESS"
    version = VERSION

    parameters = {"code": ArrayParam("Code", TextParam("Code"))}

    def run_dask(self, df) -> dd.DataFrame:
        assert isinstance(self.parameters["code"], ArrayParam)

        codes = [
            p.value.replace("\n", " ")
            for p in self.parameters["code"]
            if isinstance(p, TextParam) and p.value.strip()
        ]

        if isinstance(df, dd.DataFrame):
            return df.map_partitions(process, codes)
        else:
            return process(df, codes)
