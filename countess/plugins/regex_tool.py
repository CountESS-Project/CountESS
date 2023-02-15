import math
import re
from typing import Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore

from countess import VERSION
from countess.core.parameters import *
from countess.core.plugins import DaskTransformPlugin


class RegexToolPlugin(DaskTransformPlugin):

    name = "Regex Tool"
    title = "Apply regular expressions to column(s) to make new column(s)"
    description = """..."""
    version = VERSION

    parameters = {
        "regexes": ArrayParam(
            "Regexes",
            MultiParam(
                "Regex",
                {
                    "column": ColumnChoiceParam("Input Column"),
                    "regex": StringParam("Regular Expression", ".*"),
                    "output": ArrayParam(
                        "Output Columns",
                        MultiParam(
                            "Col",
                            {
                                "name": StringParam("Column Name"),
                                "datatype": ChoiceParam(
                                    "Column Type",
                                    "string",
                                    choices=["string", "number", "integer", "boolean"],
                                ),
                            },
                        ),
                    ),
                    "drop_column": BooleanParam("Drop Column", False),
                },
            ),
        ),
    }

    def run_dask(self, df):

        for regex_parameter in self.parameters["regexes"]:
            column_name = regex_parameter["column"].value
            compiled_re = re.compile(regex_parameter["regex"].value)

            while compiled_re.groups > len(regex_parameter["output"].params):
                regex_parameter["output"].add_row()

            output_params = regex_parameter["output"].params
            output_names = [pp["name"].value for pp in output_params]
            output_types = [pp["datatype"].value for pp in output_params]

            def cast(value, datatype):
                if datatype == "string":
                    return str(value) if value is not None else None
                elif datatype == "number":
                    return float(value if value is not None else math.nan)
                elif datatype == "integer":
                    return int(value or 0)
                elif datatype == "boolean":
                    return bool(value) if value is not None else None
                else:
                    return None

            def func(row):
                match = compiled_re.match(row[column_name])
                if match:
                    return [
                        cast(g, output_types[n]) for n, g in enumerate(match.groups())
                    ]
                else:
                    return [None] * compiled_re.groups

            df = df.copy()
            x = df.apply(func, axis=1, result_type="expand")

            for n in range(0, compiled_re.groups):
                df[output_names[n]] = x[n]

        drop_columns = set(
            [
                rp["column"].value
                for rp in self.parameters["regexes"]
                if rp["drop_column"].value
            ]
        )
        df = df.drop(columns=drop_columns)

        return df
