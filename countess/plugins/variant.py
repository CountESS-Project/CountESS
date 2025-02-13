import logging
import string
from functools import lru_cache
from typing import Any, Optional

from countess import VERSION
from countess.core.parameters import (
    BooleanParam,
    ColumnChoiceParam,
    ColumnOrIntegerParam,
    ColumnOrStringParam,
    DictChoiceParam,
    FramedMultiParam,
    IntegerParam,
    StringCharacterSetParam,
    StringParam,
)
from countess.core.plugins import DuckdbParallelTransformPlugin
from countess.utils.variant import TooManyVariationsException, find_variant_string

logger = logging.getLogger(__name__)

REFERENCE_CHAR_SET = set(string.ascii_uppercase + string.digits + "_")

# XXX Should proabably support these other types as well but I don't
# know what I don't know ...
# XXX Supporting protein calls on mitochondrial (or other organisms)
# DNA will required expansion of the variant caller routine to handle
# different codon tables.  This opens up a can of worms of course.
# XXX There should probably also be a warning generated if you ask for a
# non-MT DNA call with an MT protein call or vice versa.

VARIANT_TYPE_CHOICES = {
    "g": "Genomic (g.)",
    # "o": "Circular Genomic",
    # "m": "Mitochondrial",
    "c": "Coding DNA (c.)",
    "n": "Non-Coding DNA (n.)",
}


class DnaVariantMultiParam(FramedMultiParam):
    prefix = StringCharacterSetParam("Prefix", "", character_set=REFERENCE_CHAR_SET)
    seq_type = DictChoiceParam("Type", choices=VARIANT_TYPE_CHOICES)
    offset = ColumnOrIntegerParam("Offset", 0)
    minus_strand = BooleanParam("Minus Strand", False)
    maxlen = IntegerParam("Max Variations", 10)
    output = StringParam("Output Column", "variant")


class ProteinVariantMultiParam(FramedMultiParam):
    prefix = StringCharacterSetParam("Prefix", "", character_set=REFERENCE_CHAR_SET)
    offset = ColumnOrIntegerParam("Offset", 0)
    # XXX different codon tables go here
    maxlen = IntegerParam("Max Variations", 10)
    output = StringParam("Output Column", "protein")


class VariantPlugin(DuckdbParallelTransformPlugin):
    """Turns a DNA sequence into a HGVS variant code"""

    name = "Variant Caller"
    description = "Turns a DNA sequence into a HGVS variant code"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#variant-caller"

    column = ColumnChoiceParam("Input Column", "sequence")
    reference = ColumnOrStringParam("Reference Sequence")

    variant = DnaVariantMultiParam("DNA Variant")
    protein = ProteinVariantMultiParam("Protein Variant")

    drop = BooleanParam("Drop unmatched rows", False)
    drop_columns = BooleanParam("Drop Sequence / Reference Columns", False)

    @lru_cache(maxsize=10000)
    def find_variant_string_cached(self, *a, **k):
        return find_variant_string(*a, **k)

    def add_fields(self):
        return {
            self.variant.output.value: str,
            self.protein.output.value: str,
        }

    def remove_fields(self, field_names: list[str]) -> list[str]:
        if self.drop_columns:
            return [self.column.value, self.reference.get_column_name()]
        else:
            return []

    def transform(self, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        sequence = data[str(self.column)]
        reference = self.reference.get_value_from_dict(data)
        if not sequence or not reference:
            return None

        # r: dict[str, Any] = data
        if self.variant.output:
            data[self.variant.output.value] = None
            try:
                prefix = self.variant.prefix + ":" if self.variant.prefix else ""
                data[self.variant.output.value] = find_variant_string(
                    f"{prefix}{self.variant.seq_type.get_choice()}.",
                    reference,
                    sequence,
                    max_mutations=self.variant.maxlen.value,
                    offset=int(self.variant.offset.get_value_from_dict(data) or 0),
                    minus_strand=self.variant.minus_strand.value,
                )
            except TooManyVariationsException:
                pass
            except (ValueError, TypeError, KeyError, IndexError) as exc:
                logger.warning("Exception", exc_info=exc)

        if self.protein.output:
            data[self.protein.output.value] = None
            try:
                prefix = self.protein.prefix + ":" if self.protein.prefix else ""
                data[self.protein.output.value] = find_variant_string(
                    f"{prefix}p.",
                    reference,
                    sequence,
                    max_mutations=self.protein.maxlen.value,
                    offset=int(self.protein.offset.get_value_from_dict(data) or 0),
                )
            except TooManyVariationsException:
                pass
            except (ValueError, TypeError, KeyError, IndexError) as exc:
                logger.warning("Exception", exc_info=exc)

        return data
