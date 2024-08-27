import logging
from typing import Optional

import pandas as pd

from countess import VERSION
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

logger = logging.getLogger(__name__)


class VariantPlugin(PandasTransformDictToDictPlugin):
    """Turns a DNA sequence into a HGVS variant code"""

    name = "Variant Translator"
    description = "Turns a DNA sequence into a HGVS variant code"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#variant-caller"

    column = ColumnChoiceParam("Input Column", "sequence")
    reference = ColumnOrStringParam("Reference Sequence")
    offset = ColumnOrIntegerParam("Reference Offset", 0)
    output = StringParam("Output Column", "variant")
    max_mutations = IntegerParam("Max Mutations", 10)
    protein = StringParam("Protein Column", "protein")
    max_protein = IntegerParam("Max Protein Variations", 10)
    drop = BooleanParam("Drop unidentified variants", False)
    drop_columns = BooleanParam("Drop Input Column(s)", False)

    def process_dict(self, data) -> dict:
        sequence = data[str(self.column)]
        if not sequence:
            return {}

        reference = self.reference.get_value_from_dict(data)
        offset = int(self.offset.get_value_from_dict(data) or 0)

        r: dict[str, str] = {}

        if self.output:
            try:
                r[self.output.value] = find_variant_string(
                    "g.", reference, sequence, int(self.max_mutations), offset=offset
                )
            except ValueError:
                pass
            except (TypeError, KeyError, IndexError) as exc:
                logger.warning("Exception", exc_info=exc)

        if self.protein:
            try:
                r[str(self.protein)] = find_variant_string(
                    "p.", reference, sequence, int(self.max_protein), offset=offset
                )
            except ValueError:
                pass
            except (TypeError, KeyError, IndexError) as exc:
                logger.warning("Exception", exc_info=exc)

        return r

    def process_dataframe(self, dataframe: pd.DataFrame) -> Optional[pd.DataFrame]:
        df_out = super().process_dataframe(dataframe)

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
