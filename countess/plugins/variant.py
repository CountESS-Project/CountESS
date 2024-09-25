import logging
import string

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    ColumnChoiceParam,
    ColumnOrIntegerParam,
    ColumnOrStringParam,
    DictChoiceParam,
    IntegerParam,
    MultiParam,
    StringCharacterSetParam,
    StringParam,
)
from countess.core.plugins import PandasTransformDictToDictPlugin
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

SEQUENCE_TYPE_CHOICES = {
    "g": "Genomic",
    "g-": "Genomic (Minus Strand)",
    # "o": "Circular Genomic",
    # "m": "Mitochondrial",
    "c": "Coding DNA",
    # "n": "Non-Coding DNA",
    "p": "Protein",
    # "pm": "Protein (MT)",
}


class VariantOutputMultiParam(MultiParam):
    prefix = StringCharacterSetParam("Prefix", "", character_set=REFERENCE_CHAR_SET)
    seq_type = DictChoiceParam("Type", choices=SEQUENCE_TYPE_CHOICES)
    offset = ColumnOrIntegerParam("Offset", 0)
    maxlen = IntegerParam("Max Variations", 10)
    output = StringParam("Output Column", "variant")


class VariantPlugin(PandasTransformDictToDictPlugin):
    """Turns a DNA sequence into a HGVS variant code"""

    name = "Variant Caller"
    description = "Turns a DNA sequence into a HGVS variant code"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#variant-caller"

    column = ColumnChoiceParam("Input Column", "sequence")
    reference = ColumnOrStringParam("Reference Sequence")
    outputs = ArrayParam("Outputs", VariantOutputMultiParam("Output"), min_size=1)

    def process_dict(self, data) -> dict:
        sequence = data[str(self.column)]
        reference = self.reference.get_value_from_dict(data)
        if not sequence:
            return {}

        r: dict[str, str] = {}
        for output in self.outputs:
            seq_type = output.seq_type.get_choice() or "g"
            prefix = f"{output.prefix + ':' if output.prefix else ''}{seq_type[0]}."
            offset = int(output.offset.get_value_from_dict(data) or 0)

            try:
                r[output.output.value] = find_variant_string(
                    prefix,
                    reference,
                    sequence,
                    max_mutations=output.maxlen.value,
                    offset=offset,
                    minus_strand=seq_type.endswith("-"),
                )

            except TooManyVariationsException:
                pass
            except (ValueError, TypeError, KeyError, IndexError) as exc:
                logger.warning("Exception", exc_info=exc)

        return r
