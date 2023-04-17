import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ColumnChoiceParam, IntegerParam, StringParam
from countess.core.plugins import PandasTransformPlugin
from countess.utils.variant import find_variant_string


def process_row(var_seq: str, ref_seq: str, max_mutations: int, logger: Logger):
    try:
        return find_variant_string("g.", ref_seq, var_seq, max_mutations)
    except ValueError as exc:
        logger.warning(str(exc))
        return None


class VariantPlugin(PandasTransformPlugin):
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

    def run_df(self, df: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["column"], ColumnChoiceParam)
        assert isinstance(self.parameters["sequence"], StringParam)
        assert isinstance(self.parameters["output"], StringParam)
        assert isinstance(self.parameters["max_mutations"], IntegerParam)

        output = self.parameters["output"].value
        column = self.parameters["column"].value
        sequence = self.parameters["sequence"].value
        max_mutations = self.parameters["max_mutations"].value

        dfo = df.copy()
        dfo[output] = dfo[column].apply(process_row, args=(sequence, max_mutations, logger))
        return dfo
