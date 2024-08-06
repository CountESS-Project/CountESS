from typing import Optional

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, ColumnChoiceParam, ColumnOrStringParam, IntegerParam, StringParam
from countess.core.plugins import PandasTransformDictToDictPlugin
from countess.utils.variant import find_variant_string


class VariantPlugin(PandasTransformDictToDictPlugin):
    """Turns a DNA sequence into a HGVS variant code"""

    name = "Variant Translator"
    description = "Turns a DNA sequence into a HGVS variant code"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#variant-caller"

    column = ColumnChoiceParam("Input Column", "sequence")
    reference = ColumnOrStringParam("Reference Sequence")
    offset = IntegerParam("Reference Offset", 0)
    output = StringParam("Output Column", "variant")
    max_mutations = IntegerParam("Max Mutations", 10)
    protein = StringParam("Protein Column", "protein")
    max_protein = IntegerParam("Max Protein Variations", 10)
    drop = BooleanParam("Drop unidentified variants", False)
    drop_columns = BooleanParam("Drop Input Column(s)", False)

    def process_dict(self, data, logger: Logger) -> dict:
        sequence = data[str(self.column)]
        if not sequence:
            return {}

        reference = self.reference.get_value_from_dict(data)

        r : dict[str,str] = {}

        if self.output:
            try:
                r[self.output.value] = find_variant_string(
                    "g.", reference, sequence, int(self.max_mutations), offset=int(self.offset)
                )
            except ValueError:
                pass
            except (TypeError, KeyError, IndexError) as exc:
                logger.exception(exc)

        if self.protein:
            try:
                r[str(self.protein)] = find_variant_string(
                    "p.", reference, sequence, int(self.max_protein), offset=int(self.offset)
                )
            except ValueError:
                pass
            except (TypeError, KeyError, IndexError) as exc:
                logger.exception(exc)

        return r

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> Optional[pd.DataFrame]:
        df_out = super().process_dataframe(dataframe, logger)

        if df_out is not None:
            if self.drop:
                if self.output:
                    df_out.dropna(subset=str(self.output), inplace=True)
                if self.protein:
                    df_out.dropna(subset=str(self.protein), inplace=True)
            if self.drop_columns:
                try:
                    df_out.drop(columns=str(self.column), inplace=True)
                    if self.reference.get_column_name():
                        df_out.drop(columns=self.reference.get_column_name(), inplace=True)
                except KeyError:
                    pass

        return df_out
