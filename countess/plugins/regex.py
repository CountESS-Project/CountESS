import re

import pandas as pd

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ColumnChoiceParam,
    DataTypeChoiceParam,
    IntegerParam,
    MultiParam,
    StringParam,
)
from countess.core.plugins import PandasInputPlugin, PandasTransformPlugin


class RegexToolPlugin(PandasTransformPlugin):
    name = "Regex Tool"
    description = "Apply regular expressions to a column to make new column(s)"
    link = "https://countess-project.github.io/CountESS/plugins/#regex-tool"
    version = VERSION

    parameters = {
        "column": ColumnChoiceParam("Input Column"),
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
                    "index": BooleanParam("Index?"),
                },
            ),
        ),
        "multi": BooleanParam("Multi Match", False),
        "drop_column": BooleanParam("Drop Column", False),
        "drop_unmatch": BooleanParam("Drop Unmatched Rows", False),
    }

    def run_df(self, df, logger):
        compiled_re = re.compile(self.parameters["regex"].value)

        while compiled_re.groups > len(self.parameters["output"].params):
            self.parameters["output"].add_row()

        output_params = self.parameters["output"].params
        output_names = [pp["name"].value for pp in output_params]
        index_names = [pp["name"].value for pp in output_params if pp["index"].value]

        column = self.parameters["column"].get_column(df)

        if self.parameters["multi"].value:

            def func(value):
                matches = compiled_re.findall(str(value))
                if matches:
                    return [[m[n] for m in matches] for n in range(0, compiled_re.groups)]
                else:
                    return [[None]] * compiled_re.groups

        else:

            def func(value):
                if value is not None:
                    if match := compiled_re.match(value):
                        return [
                            op.datatype.cast_value(g)
                            for op, g in zip(output_params, match.groups())
                        ]
                logger.info("Didn't Match: " + repr(value))
                return [None] * compiled_re.groups

        # make a series of tuples, then turn those back into a dataframe,
        # then copy those new columns over.
        # XXX this isn't particularly elegant but it does seem to be fairly quick
        # XXX should be a special case for a single output column

        series = column.apply(func)
        output_df = pd.DataFrame(series.tolist(), columns=output_names, index=series.index)
        df = df.assign(**{column_name: output_df[column_name] for column_name in output_names})

        if self.parameters["multi"].value:
            df = df.explode(output_names)

        if self.parameters["drop_unmatch"].value:
            df = df.dropna(subset=output_names, how="all")

        if self.parameters["drop_column"].value:
            column_name = self.parameters["column"].value
            if column_name not in df.columns:
                df = df.reset_index()
            df = df.drop(columns=column_name)

        if index_names:
            df = df.set_index(index_names)

        return df


class RegexReaderPlugin(PandasInputPlugin):
    name = "Regex Reader"
    description = "Loads arbitrary data from line-delimited files"
    additional = """Applies a regular expression to each line to extract fields.
        If you're trying to read generic CSV or TSV files, use the CSV plugin
        instead as it handles escaping correctly."""
    link = "https://countess-project.github.io/CountESS/plugins/#regex-reader"
    version = VERSION

    file_types = [("CSV", "*.csv"), ("TXT", "*.txt")]

    parameters = {
        "regex": StringParam("Regular Expression", "(.*)"),
        "skip": IntegerParam("Skip Lines", 0),
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
                if num < self.parameters["skip"].value:
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

        if len(pdfs) == 0:
            return pd.DataFrame([], columns=columns)

        df = pd.concat(pdfs)
        return df
