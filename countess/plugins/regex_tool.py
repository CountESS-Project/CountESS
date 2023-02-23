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
                    "column": ColumnOrIndexChoiceParam("Input Column"),
                    "regex": StringParam("Regular Expression", ".*"),
                    "output": ArrayParam(
                        "Output Columns",
                        MultiParam(
                            "Col",
                            {
                                "name": StringParam("Column Name"),
                                "datatype": DataTypeChoiceParam(
                                    "Column Type",
                                    "string",
                                ),
                            },
                        ),
                    ),
                    "drop_column": BooleanParam("Drop Column", False),
                },
            ),
        ),
    }

    def run_dask(self, df, logger):

        # prevent added columns from propagating backwards in 
        # the pipeline!
        df = df.copy()

        # the index doesn't seem to be available from within the applied function,
        # which is annoying, so we copy it into a column here.
        if any(rp["column"].is_index() for rp in self.parameters["regexes"]):
            df['__index'] = df.index

        # XXX this could be made more efficient by running
        # multiple regexs in one 'apply' pass.

        for regex_parameter in self.parameters["regexes"]:

            compiled_re = re.compile(regex_parameter["regex"].value)

            while compiled_re.groups > len(regex_parameter["output"].params):
                regex_parameter["output"].add_row()

            output_params = regex_parameter["output"].params
            output_names = [pp["name"].value for pp in output_params]
            output_types = [pp["datatype"].get_selected_type() for pp in output_params]

            if regex_parameter["column"].is_index():
                column_name = '__index'
            else:
                column_name = regex_parameter["column"].value

            def func(row):
                value = str(row[column_name])
                match = compiled_re.match(value)
                if match:
                    return [
                        output_params[n].datatype.cast_value(g)
                        for n, g in enumerate(match.groups())
                    ]
                else:
                    logger.warning(f"Didn't Match", detail=repr(value))
                    return [None] * compiled_re.groups

            if isinstance(df, dd.DataFrame):
                # dask likes a hint about column types
                meta = dict(zip(output_names, output_types))
                re_groups_df = df.apply(func, axis=1, result_type="expand", meta=meta)
            else:
                # pandas infers the column types
                re_groups_df = df.apply(func, axis=1, result_type="expand")

            for n in range(0, compiled_re.groups):
                df[output_names[n]] = re_groups_df[n]

        drop_columns = set(
            rp["column"].value
            for rp in self.parameters["regexes"]
            if rp["drop_column"].value
        )
        if '__index__' in df: drop_columns.add('__index')

        return df.drop(columns=drop_columns)
