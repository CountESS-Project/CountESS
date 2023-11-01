from typing import Optional

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, ColumnChoiceParam, IntegerParam, StringParam
from countess.core.plugins import PandasTransformSingleToSinglePlugin
from countess.utils.variant import invert_dna_sequence


class SequencePlugin(PandasTransformSingleToSinglePlugin):
    """Manipulate DNA valueuences"""

    name = "Sequence Tool"
    description = "Manipulate DNA Sequences"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#valueuence"

    parameters = {
        "column": ColumnChoiceParam("Input Column"),
        "invert": BooleanParam("Invert", False),
        "offset": IntegerParam("Offset", 0),
        "start": StringParam("Start at ...", ""),
        "stop": StringParam("Stop at ...", ""),
        "length": IntegerParam("Max Length", 150),
        "output": StringParam("Output Column", "sequence"),
    }

    def process_value(self, value: str, logger: Logger) -> Optional[str]:
        if value is None:
            return None

        if self.parameters["invert"].value:
            value = invert_dna_sequence(value)
        if self.parameters["offset"].value:
            offset = self.parameters["offset"].value
            value = value[offset:]
        if self.parameters["start"].value:
            offset = value.find(self.parameters["start"].value)
            if offset >= 0:
                value = value[offset:]
            else:
                return None
        if self.parameters["stop"].value:
            offset = value.find(self.parameters["stop"].value)
            if offset >= 0:
                value = value[0 : offset + len(self.parameters["stop"].value)]
        if self.parameters["length"].value:
            value = value[0 : self.parameters["length"].value]
        return value
