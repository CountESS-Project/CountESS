from typing import Optional

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (
    BooleanParam,
    ColumnChoiceParam,
    ColumnOrIntegerParam,
    ColumnOrStringParam,
    IntegerParam,
    StringParam,
)
from countess.core.plugins import PandasTransformDictToDictPlugin
from countess.utils.variant import find_variant_string


class VariantPlugin(PandasTransformDictToDictPlugin):
    """Turns a DNA sequence into a HGVS variant code"""

    name = "Variant Translator"
    description = "Turns a DNA sequence into a HGVS variant code"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#variant-caller"

    parameters = {
        "column": ColumnChoiceParam("Input Column", "sequence"),
        "reference": ColumnOrStringParam("Reference Sequence"),
        "offset": ColumnOrIntegerParam("Reference Offset", 0),
        "output": StringParam("Output Column", "variant"),
        "max_mutations": IntegerParam("Max Mutations", 10),
        "protein": StringParam("Protein Column", "protein"),
        "max_protein": IntegerParam("Max Protein Variations", 10),
        "drop": BooleanParam("Drop unidentified variants", False),
        "drop_columns": BooleanParam("Drop Input Column(s)", False),
    }

    def process_dict(self, data, logger: Logger) -> dict:
        assert isinstance(self.parameters["reference"], ColumnOrStringParam)
        assert isinstance(self.parameters["offset"], ColumnOrIntegerParam)

        sequence = data[self.parameters["column"].value]
        if not sequence:
            return {}

        reference = self.parameters["reference"].get_value(data)
        offset = int(self.parameters["offset"].get_value(data) or 0)

        r = {}

        if self.parameters["output"].value:
            try:
                max_mutations = self.parameters["max_mutations"].value
                r[self.parameters["output"].value] = find_variant_string(
                    "g.", reference, sequence, max_mutations, offset=offset
                )
            except ValueError:
                pass
            except (TypeError, KeyError, IndexError) as exc:
                logger.exception(exc)

        if self.parameters["protein"].value:
            try:
                max_protein = self.parameters["max_protein"].value
                r[self.parameters["protein"].value] = find_variant_string(
                    "p.", reference, sequence, max_protein, offset=offset
                )
            except ValueError:
                pass
            except (TypeError, KeyError, IndexError) as exc:
                logger.exception(exc)

        return r

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> Optional[pd.DataFrame]:
        assert isinstance(self.parameters["reference"], ColumnOrStringParam)
        df_out = super().process_dataframe(dataframe, logger)

        if df_out is not None:
            if self.parameters["drop"].value:
                if self.parameters["output"].value:
                    df_out.dropna(subset=self.parameters["output"].value, inplace=True)
                if self.parameters["protein"].value:
                    df_out.dropna(subset=self.parameters["protein"].value, inplace=True)
            if self.parameters["drop_columns"].value:
                try:
                    df_out.drop(columns=self.parameters["column"].value, inplace=True)
                    if self.parameters["reference"].get_column_name():
                        df_out.drop(columns=self.parameters["reference"].get_column_name(), inplace=True)
                except KeyError:
                    pass

        return df_out
