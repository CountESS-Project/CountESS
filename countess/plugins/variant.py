import logging

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    ColumnChoiceParam,
    ColumnOrIntegerParam,
    ColumnOrStringParam,
    IntegerParam,
    MultiParam,
    StringParam,
)
from countess.core.plugins import PandasTransformDictToDictPlugin
from countess.utils.variant import find_variant_string

logger = logging.getLogger(__name__)


class VariantOutputMultiParam(MultiParam):
    prefix = StringParam("Prefix", "g.")
    offset = ColumnOrIntegerParam("Offset", 0)
    maxlen = IntegerParam("Max Variations", 10)
    output = StringParam("Output Column", "variant")


class VariantPlugin(PandasTransformDictToDictPlugin):
    """Turns a DNA sequence into a HGVS variant code"""

    name = "Variant Caller"
    description = "Turns a DNA sequence into a HGVS variant code"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#variant-caller"
    additional = """Prefixes should end with 'g.', 'c.' or 'p.'.
    Use a negative offset to call genomic references to minus-strand genes."""

    column = ColumnChoiceParam("Input Column", "sequence")
    reference = ColumnOrStringParam("Reference Sequence")
    outputs = ArrayParam("Outputs", VariantOutputMultiParam("Output"))

    def process_dict(self, data) -> dict:
        sequence = data[str(self.column)]
        reference = self.reference.get_value_from_dict(data)
        if not sequence:
            return {}

        r: dict[str, str] = {}
        for output in self.outputs:
            offset = int(output.offset.get_value_from_dict(data) or 0)

            try:
                r[output.output.value] = find_variant_string(
                    str(output.prefix) if output.prefix else "g.",
                    reference,
                    sequence,
                    max_mutations=output.maxlen.value,
                    offset=abs(offset),
                    minus_strand=offset < 0,
                )

            except (TypeError, KeyError, IndexError) as exc:
                logger.warning("Exception", exc_info=exc)

        return r
