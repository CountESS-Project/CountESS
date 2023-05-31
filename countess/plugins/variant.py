from typing import Optional

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, ColumnChoiceParam, IntegerParam, StringParam
from countess.core.plugins import PandasTransformPlugin
from countess.utils.variant import find_variant_string


def process_row(var_seq: str, ref_seq: str, max_mutations: int, logger: Logger) -> Optional[str]:
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
        "column": ColumnChoiceParam("Input Column", "sequence"),
        "sequence": StringParam("Reference Sequence"),
        "auto": BooleanParam("Automatic Reference Sequence?", False),
        "output": StringParam("Output Column", "variant"),
        "max_mutations": IntegerParam("Max Mutations", 10),
        "drop": BooleanParam("Drop unidentified variants", False),
    }

    def run_df(self, df: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["column"], ColumnChoiceParam)
        assert isinstance(self.parameters["sequence"], StringParam)
        assert isinstance(self.parameters["output"], StringParam)
        assert isinstance(self.parameters["max_mutations"], IntegerParam)

        dfo = df.copy()

        column = self.parameters["column"].get_column(df)
        output = self.parameters["output"].value

        if self.parameters["auto"].value:
            value = pd.Series.mode(column)[0]
            self.parameters["sequence"].value = value

        sequence = self.parameters["sequence"].value
        max_mutations = self.parameters["max_mutations"].value

        dfo[output] = column.apply(process_row, args=(sequence, max_mutations, logger))

        if self.parameters["drop"].value:
            dfo = dfo.query("variant.notnull()")

        return dfo
