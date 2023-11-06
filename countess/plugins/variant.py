from typing import Optional

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, ColumnChoiceParam, ColumnOrNoneChoiceParam, IntegerParam, StringParam
from countess.core.plugins import PandasTransformDictToSinglePlugin
from countess.utils.variant import find_variant_string


class VariantPlugin(PandasTransformDictToSinglePlugin):
    """Turns a DNA sequence into a HGVS variant code"""

    name = "Variant Translator"
    description = "Turns a DNA sequence into a HGVS variant code"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#variant-caller"

    parameters = {
        "column": ColumnChoiceParam("Input Column", "sequence"),
        "reference": ColumnOrNoneChoiceParam("Reference Column"),
        "sequence": StringParam("*OR* Reference Sequence"),
        "output": StringParam("Output Column", "variant"),
        "max_mutations": IntegerParam("Max Mutations", 10),
        "drop": BooleanParam("Drop unidentified variants", False),
        "drop_columns": BooleanParam("Drop Input Column(s)", False),
    }

    def process_dict(self, data, logger: Logger) -> Optional[str]:
        assert isinstance(self.parameters["reference"], ColumnOrNoneChoiceParam)
        if not self.parameters["column"].value:
            return None
        try:
            sequence = data[self.parameters["column"].value]
            max_mutations = self.parameters["max_mutations"].value
            if self.parameters["reference"].is_none():
                reference = self.parameters["sequence"].value
            else:
                reference = data[self.parameters["reference"].value]
            return find_variant_string("g.", reference, sequence, max_mutations)
        except ValueError:
            return None
        except (TypeError, KeyError) as exc:
            logger.exception(exc)
            return None

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> Optional[pd.DataFrame]:
        assert isinstance(self.parameters["reference"], ColumnOrNoneChoiceParam)
        df_out = super().process_dataframe(dataframe, logger)

        if df_out is not None:
            if self.parameters["drop"].value:
                df_out.dropna(subset=self.parameters["output"].value, inplace=True)
            if self.parameters["drop_columns"].value:
                try:
                    df_out.drop(columns=self.parameters["column"].value, inplace=True)
                    if not self.parameters["reference"].is_none():
                        df_out.drop(columns=self.parameters["reference"].value, inplace=True)
                except KeyError:
                    pass

        return df_out
