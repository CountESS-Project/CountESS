from functools import partial

import dask.dataframe as dd
import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (
    ColumnChoiceParam,
    StringParam,
)
from countess.core.plugins import DaskTransformPlugin
from countess.utils.variant import find_variant_string


def process(df: pd.DataFrame, column: str, sequence: str, output: str):
    dfo = df.copy()
    dfo[output] = dfo[column].apply(partial(find_variant_string, 'g.', sequence))
    return dfo

class VariantPlugin(DaskTransformPlugin):
    """Groups a Dask Dataframe by an arbitrary column and rolls up rows"""

    name = "Variant Translator"
    description = "Turns a DNA sequence into a HGVS variant code"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/plugins/#variant"

    parameters = {
        "column": ColumnChoiceParam("Input Column"),
        "sequence": StringParam("Reference Sequence"),
        "output": StringParam("Output Column"),
    }

    def run_dask(self, df: dd.DataFrame, logger: Logger) -> dd.DataFrame:
        assert isinstance(self.parameters["column"], ColumnChoiceParam)
        assert isinstance(self.parameters["sequence"], StringParam)
        assert isinstance(self.parameters["output"], StringParam)

        args = (
            self.parameters["column"].value,
            self.parameters["sequence"].value,
            self.parameters["output"].value,
        )

        if isinstance(df, dd.DataFrame):
            return df.map_partitions(process, *args)
        else:
            return process(df, *args)
