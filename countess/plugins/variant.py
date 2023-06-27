import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (
    BooleanParam,
    ColumnChoiceParam,
    ColumnOrNoneChoiceParam,
    IntegerParam,
    StringParam,
)
from countess.core.plugins import PandasTransformPlugin
from countess.utils.variant import find_variant_string


class VariantPlugin(PandasTransformPlugin):
    """Turns a DNA sequence into a HGVS variant code"""

    name = "Variant Translator"
    description = "Turns a DNA sequence into a HGVS variant code"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/plugins/#variant"

    parameters = {
        "column": ColumnChoiceParam("Input Column", "sequence"),
        "reference": ColumnOrNoneChoiceParam("Reference Column"),
        "sequence": StringParam("*OR* Reference Sequence"),
        "auto": BooleanParam("Automatic Reference Sequence?", False),
        "output": StringParam("Output Column", "variant"),
        "max_mutations": IntegerParam("Max Mutations", 10),
        "drop": BooleanParam("Drop unidentified variants", False),
    }

    def run_df(self, df: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["column"], ColumnChoiceParam)
        assert isinstance(self.parameters["reference"], ColumnChoiceParam)
        assert isinstance(self.parameters["sequence"], StringParam)
        assert isinstance(self.parameters["output"], StringParam)
        assert isinstance(self.parameters["max_mutations"], IntegerParam)

        dfo = df.copy()

        column = self.parameters["column"].get_column(df)
        reference = self.parameters["reference"].get_column(df)
        output = self.parameters["output"].value

        if self.parameters["auto"].value:
            value = pd.Series.mode(column)[0]
            self.parameters["sequence"].value = value

        max_mutations = self.parameters["max_mutations"].value

        if reference is not None:

            def func(ref_var):
                try:
                    return find_variant_string("g.", ref_var[0], ref_var[1], max_mutations)
                except ValueError as exc:
                    logger.warning(str(exc))
                    return None

            dfo[output] = pd.DataFrame([column, reference]).apply(
                func, raw=True, result_type="reduce"
            )
        else:
            ref_str = self.parameters["sequence"].value

            def func(var_str):
                try:
                    return find_variant_string("g.", ref_str, var_str, max_mutations)
                except ValueError as exc:
                    logger.warning(str(exc))
                    return None

            dfo[output] = column.apply(func)

        if self.parameters["drop"].value:
            dfo = dfo.query("variant.notnull()")

        return dfo
