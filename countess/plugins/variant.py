import dask.dataframe as dd
import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ColumnChoiceParam, IntegerParam, StringParam
from countess.core.plugins import DaskTransformPlugin
from countess.utils.variant import find_variant_string


def process_row(var_seq: str, ref_seq: str, max_mutations: int, logger: Logger):
    try:
        return find_variant_string("g.", ref_seq, var_seq, max_mutations)
    except ValueError as exc:
        logger.warning(str(exc))
        return None


def process_partition(
    df: pd.DataFrame, column: str, sequence: str, output: str, max_mutations: int, logger: Logger
):
    dfo = df.copy()
    dfo[output] = dfo[column].apply(process_row, args=(sequence, max_mutations, logger))
    return dfo


class VariantPlugin(DaskTransformPlugin):
    """Turns a DNA sequence into a HGVS variant code"""

    name = "Variant Translator"
    description = "Turns a DNA sequence into a HGVS variant code"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/plugins/#variant"

    parameters = {
        "column": ColumnChoiceParam("Input Column"),
        "sequence": StringParam("Reference Sequence"),
        "output": StringParam("Output Column"),
        "max_mutations": IntegerParam("Max Mutations", 10),
    }

    def run_dask(
        self, df: pd.DataFrame | dd.DataFrame, logger: Logger
    ) -> pd.DataFrame | dd.DataFrame:
        assert isinstance(self.parameters["column"], ColumnChoiceParam)
        assert isinstance(self.parameters["sequence"], StringParam)
        assert isinstance(self.parameters["output"], StringParam)
        assert isinstance(self.parameters["max_mutations"], IntegerParam)

        args = (
            self.parameters["column"].value,
            self.parameters["sequence"].value,
            self.parameters["output"].value,
            self.parameters["max_mutations"].value,
            logger,
        )

        meta = dict(df.dtypes)
        meta[self.parameters["output"].value] = str

        if isinstance(df, dd.DataFrame):
            return df.map_partitions(process_partition, *args, meta=meta)
        else:
            return process_partition(df, *args)
