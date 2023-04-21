import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ColumnOrIndexChoiceParam, IntegerParam, StringParam
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
        "column": ColumnOrIndexChoiceParam("Input Column"),
        "sequence": StringParam("Reference Sequence"),
        "output": StringParam("Output Column", "variant"),
        "max_mutations": IntegerParam("Max Mutations", 10),
    }

    def run_df(self, df: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["column"], ColumnOrIndexChoiceParam)
        assert isinstance(self.parameters["sequence"], StringParam)
        assert isinstance(self.parameters["output"], StringParam)
        assert isinstance(self.parameters["max_mutations"], IntegerParam)

        dfo = df.copy()

        output = self.parameters["output"].value

        if self.parameters["column"].is_index():
            dfo["__index"] = df.index
            column_name = "__index"
        else:
            column_name = self.parameters["column"].value

        if self.parameters["sequence"].value == "":
            self.parameters["sequence"].value = dfo[column_name][0]

        sequence = self.parameters["sequence"].value
        max_mutations = self.parameters["max_mutations"].value

        dfo[output] = dfo[column_name].apply(process_row, args=(sequence, max_mutations, logger))

        if self.parameters["column"].is_index():
            return dfo.drop(columns=["__index"])
        else:
            return dfo
