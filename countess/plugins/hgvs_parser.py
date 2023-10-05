import re

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, ColumnChoiceParam, ColumnOrNoneChoiceParam, IntegerParam, StringParam
from countess.core.plugins import PandasTransformDictToDictPlugin


class HgvsParserPlugin(PandasTransformDictToDictPlugin):
    name = "HGVS Parser"
    description = "Parse HGVS strings"

    version = VERSION

    parameters = {
        "column": ColumnChoiceParam("Input Column"),
        "guides_col": ColumnOrNoneChoiceParam("Guide(s) Column"),
        "guides_str": StringParam("Guide(s)"),
        "max_var": IntegerParam("Maximum Variations", 1),
        "split": BooleanParam("Split Output", False),
    }

    def process_dict(self, data: dict, logger: Logger):
        assert isinstance(self.parameters["guides_col"], ColumnOrNoneChoiceParam)
        try:
            value = data[self.parameters["column"].value]
        except KeyError:
            return None

        if value is None:
            return None

        output = {}

        guides = []
        if not self.parameters["guides_col"].is_none():
            guides += str(data[self.parameters["guides_col"].value]).split(";")
        if self.parameters["guides_str"].value:
            guides += self.parameters["guides_str"].value.split(";")

        if m := re.match(r"([\w.]+):([ncg].)(.*)", value):
            output["reference"] = m.group(1)
            output["prefix"] = m.group(2)
            value = m.group(3)

        if value == "=":
            variations = []
        else:
            if value.startswith("[") and value.endswith("]"):
                value = value[1:-1]
            variations = value.split(";")

        for n, g in enumerate(guides, 1):
            output[f"guide_{n}"] = g in variations

        max_variations = self.parameters["max_var"].value
        variations = [v for v in variations if v not in guides]
        if len(variations) > max_variations:
            return None

        for n, v in enumerate(variations, 1):
            if self.parameters["split"].value:
                if m := re.match(r"([\d_]+)(.*)", v):
                    output[f"loc_{n}"] = m.group(1)
                    output[f"var_{n}"] = m.group(2)
                    continue
            output[f"var_{n}"] = v

        return output
