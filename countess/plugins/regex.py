import re
from functools import partial

import pandas as pd

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ColumnOrIndexChoiceParam,
    DataTypeChoiceParam,
    MultiParam,
    StringParam,
)
from countess.core.plugins import PandasInputPlugin, PandasTransformPlugin


class RegexToolPlugin(PandasTransformPlugin):
    name = "Regex Tool"
    description = "Apply regular expressions to column(s) to make new column(s)"
    link = "https://countess-project.github.io/CountESS/plugins/#regex-tool"
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
                    "drop_unmatch": BooleanParam("Drop Unmatched Rows", False),
                },
            ),
        ),
    }

    def apply_func(self, column_name, compiled_re, output_params, logger, row):
        value = str(row[column_name])
        match = compiled_re.match(value)
        if match:
            return [1] + [
                output_params[n].datatype.cast_value(g) for n, g in enumerate(match.groups())
            ]
        else:
            logger.warning("Didn't Match", detail=repr(value))
            return [0] + [None] * compiled_re.groups

    def run_df(self, df, logger):
        # prevent added columns from propagating backwards in
        # the pipeline!
        df = df.copy()

        # the index doesn't seem to be available from within the applied function,
        # which is annoying, so we copy it into a column here.
        if any(rp["column"].is_index() for rp in self.parameters["regexes"]):
            df["__index"] = df.index

        # XXX this could be made more efficient by running
        # multiple regexs in one 'apply' pass.

        for regex_parameter in self.parameters["regexes"]:
            compiled_re = re.compile(regex_parameter["regex"].value)

            while compiled_re.groups > len(regex_parameter["output"].params):
                regex_parameter["output"].add_row()

            output_params = regex_parameter["output"].params
            output_names = [pp["name"].value for pp in output_params]

            if regex_parameter["column"].is_index():
                column_name = "__index"
            else:
                column_name = regex_parameter["column"].value

            # XXX not totally happy with this
            func = partial(self.apply_func, column_name, compiled_re, output_params, logger)

            re_groups_df = df.apply(func, axis=1, result_type="expand")

            for n in range(0, compiled_re.groups):
                df[output_names[n]] = re_groups_df[n + 1]

            if regex_parameter["drop_unmatch"].value:
                df["__filter"] = re_groups_df[0]
                df = df.query("__filter != 0").drop(columns="__filter")

        drop_columns = set(
            rp["column"].value for rp in self.parameters["regexes"] if rp["drop_column"].value
        )
        if "__index" in df:
            drop_columns.add("__index")

        return df.drop(columns=drop_columns) if drop_columns else df


class RegexReaderPlugin(PandasInputPlugin):
    name = "Regex Reader"
    description = """Loads arbitrary data from line-delimited files, applying a regular expression
      to each line to extract fields.  If you're trying to read generic CSV or TSV files, use the CSV
      plugin instead as it handles escaping correctly."""
    link = "https://countess-project.github.io/CountESS/plugins/#regex-reader"
    version = VERSION

    file_types = [("CSV", "*.csv"), ("TXT", "*.txt")]

    parameters = {
        "regex": StringParam("Regular Expression", ".*"),
        "skip": BooleanParam("Skip First Row", False),
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
                    "index": BooleanParam("Index?", False),
                },
            ),
        ),
    }

    def read_file_to_dataframe(self, file_params, logger, row_limit=None):
        pdfs = []

        compiled_re = re.compile(self.parameters["regex"].value)

        while compiled_re.groups > len(self.parameters["output"].params):
            self.parameters["output"].add_row()

        output_parameters = list(self.parameters["output"])[: compiled_re.groups]
        columns = [p.name.value or f"column_{n+1}" for n, p in enumerate(output_parameters)]
        index_columns = [
            p.name.value or f"column_{n+1}"
            for n, p in enumerate(output_parameters)
            if p.index.value
        ] or None

        records = []
        with open(file_params["filename"].value, "r", encoding="utf-8") as fh:
            for num, line in enumerate(fh):
                if num == 0 and self.parameters["skip"].value:
                    continue
                match = compiled_re.match(line)
                if match:
                    records.append(
                        (
                            output_parameters[n].datatype.cast_value(g)
                            for n, g in enumerate(match.groups())
                        )
                    )
                else:
                    logger.warning(f"Row {num+1} did not match", detail=line)
                if row_limit is not None:
                    if len(records) >= row_limit or num > 100 * row_limit:
                        break
                elif len(records) >= 100000:
                    pdfs.append(
                        pd.DataFrame.from_records(records, columns=columns, index=index_columns)
                    )
                    records = []

        if len(records) > 0:
            pdfs.append(pd.DataFrame.from_records(records, columns=columns, index=index_columns))

        return pd.concat(pdfs)
