import re

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, StringCharacterSetParam, ColumnChoiceParam, ColumnOrStringParam, ColumnOrIntegerParam, StringParam
from countess.core.plugins import PandasTransformDictToDictPlugin


class HgvsParserPlugin(PandasTransformDictToDictPlugin):

    name = "HGVS Parser"
    description = "Parse HGVS strings"

    version = VERSION

    parameters = {
        "column": ColumnChoiceParam("Input Column", "hgvs"),
        "guides": ColumnOrStringParam("Guide(s)"),
        "target_start": ColumnOrIntegerParam("Target Region Start"),
        "target_end": ColumnOrIntegerParam("Target Region End"),
    }

    def process_dict(self, data: dict, logger: Logger):
        value = data[self.parameters["column"].value]
        output = {}

        if self.parameters["guides"].choice:
            guides = data[self.parameters["guides"].value]
        else:
            guides = self.parameters["guides"].value

        guides = guides.split(';') if guides else []

        print(f"@#### {guides} {value}")

        if m := re.match(r'([\w.]+):([ncg].)(.*)', value):
            output['reference'] = m.group(1)
            output['prefix'] = m.group(2)
            value = m.group(3)


        if value == '=':
            variations = []
        else:
            if value.startswith('[') and value.endswith(']'):
                value = value[1:-1]
            variations = value.split(';')

        print(f">>>>> {variations}")

        for n, g in enumerate(guides, 1):
            output[f"guide_{n}"] = g in variations

        for n, v in enumerate((v for v in variations if v not in guides), 1):
            output[f"var_{n}"] = v

        print(f"##### {output}")

        return output
