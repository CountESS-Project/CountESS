from typing import Optional

from fqfa.util.nucleotide import reverse_complement  # type: ignore

from countess import VERSION
from countess.core.parameters import BooleanParam, ColumnChoiceParam, IntegerParam, StringParam
from countess.core.plugins import PandasTransformSingleToSinglePlugin


class SequencePlugin(PandasTransformSingleToSinglePlugin):
    """Manipulate DNA sequences"""

    name = "Sequence Tool"
    description = "Manipulate DNA Sequences"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#valueuence"

    column = ColumnChoiceParam("Input Column")
    invert = BooleanParam("Invert", False)
    offset = IntegerParam("Offset", 0)
    start = StringParam("Start at ...", "")
    stop = StringParam("Stop at ...", "")
    length = IntegerParam("Max Length", 150)
    output = StringParam("Output Column", "sequence")

    def process_value(self, value: str) -> Optional[str]:
        if value is None:
            return None

        if self.invert:
            value = reverse_complement(value)
        if self.offset > 0:
            value = value[int(self.offset) :]
        if self.start:
            offset = value.find(self.start.value)
            if offset >= 0:
                value = value[offset:]
            else:
                return None
        if self.stop:
            offset = value.find(self.stop.value)
            if offset >= 0:
                value = value[0 : offset + len(self.stop.value)]
        if self.length > 0:
            value = value[0 : int(self.length)]
        return value
