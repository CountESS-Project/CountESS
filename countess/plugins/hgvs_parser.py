import re
from typing import Optional

import pandas as pd

from countess import VERSION
from countess.core.parameters import BooleanParam, ColumnChoiceParam, ColumnOrNoneChoiceParam, IntegerParam, StringParam
from countess.core.plugins import PandasTransformDictToDictPlugin


class HgvsParserPlugin(PandasTransformDictToDictPlugin):
    name = "HGVS Parser"
    description = "Parse HGVS strings"

    version = VERSION

    column = ColumnChoiceParam("Input Column")
    guides_col = ColumnOrNoneChoiceParam("Guide(s) Column")
    guides_str = StringParam("Guide(s)")
    max_var = IntegerParam("Maximum Variations", 1)
    split = BooleanParam("Split Output", False)
    multi = BooleanParam("Multiple rows", False)

    def process_dict(self, data: dict):
        value = data.get(str(self.column))
        if type(value) is not str:
            return {}

        output = {}

        guides = []
        if self.guides_col.is_not_none():
            guides += str(data[self.guides_col.value]).split(";")
        if self.guides_str:
            guides += self.guides_str.value.split(";")

        if m := re.match(r"(?:([\w.]+):)?([ncg]\.)(.*)", value):
            output["reference"] = m.group(1) or ""
            output["prefix"] = m.group(2)
            value = m.group(3)

        variations: list[str] = []
        if value != "=":
            if value.startswith("[") and value.endswith("]"):
                value = value[1:-1]
            variations = value.split(";")

            for n, g in enumerate(guides, 1):
                output[f"guide_{n}"] = g in variations

            max_variations = int(self.max_var)
            variations = [v for v in variations if v not in guides]

        output_vars: list[Optional[str]] = [None] * max_variations
        output_locs: list[Optional[str]] = [None] * max_variations

        if len(variations) <= max_variations:
            for n, v in enumerate(variations):
                if self.split:
                    if m := re.match(r"([\d_]+)(.*)", v):
                        output_locs[n] = m.group(1)
                        output_vars[n] = m.group(2)
                        continue
                output_vars[n] = v

        if self.multi:
            output["var"] = output_vars
            if self.split:
                output["loc"] = output_locs
        else:
            for n, (var, loc) in enumerate(zip(output_vars, output_locs), 1):
                output[f"var_{n}"] = var
                if self.split:
                    output[f"loc_{n}"] = loc

        return output

    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        dataframe = super().series_to_dataframe(series)

        if self.multi:
            if self.split:
                dataframe = dataframe.explode(["var", "loc"])
            else:
                dataframe = dataframe.explode(["var"])

        return dataframe
