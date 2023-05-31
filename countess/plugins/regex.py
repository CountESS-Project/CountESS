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


def _process_row(value: str, compiled_re, output_params, logger) -> pd.Series:
    match = compiled_re.match(value)
    if match:
        return pd.Series(
            dict(
                (output_params[n]["name"].value, output_params[n].datatype.cast_value(g))
                for n, g in enumerate(match.groups())
            )
        )
    else:
        logger.warning("Didn't Match", detail=repr(value))
        return pd.Series({})


def _process_row_multi(value: str, compiled_re, output_params, logger) -> pd.Series:
    matches = compiled_re.findall(value)
    if len(matches) == 0:
        return pd.Series({})
    if compiled_re.groups > 1:
        return pd.Series(
            dict(
                (op["name"].value, [op.datatype.cast_value(match[num]) for match in matches])
                for num, op in enumerate(output_params)
            )
        )
    else:
        op = output_params[0]
        return pd.Series({op["name"].value: [op.datatype.cast_value(match) for match in matches]})


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

        column = self.parameters["column"].get_column(df)

        if self.parameters["multi"].value:
            func = _process_row_multi
        else:
            func = _process_row

        df = df.join(column.apply(func, args=(compiled_re, output_params, logger)))

        if self.parameters["drop_unmatch"].value:
            df = df.dropna(subset=output_names)

        if self.parameters["multi"].value:
            df = df.explode(output_names)

        if self.parameters["drop_column"].value:
            column_name = self.parameters["column"].value
            if column_name not in df.columns:
                df = df.reset_index()
            df = df.drop(columns=column_name)

        return df


class RegexReaderPlugin(PandasInputPlugin):
    name = "Regex Reader"
    description = """Loads arbitrary data from line-delimited files, applying a regular expression
      to each line to extract fields.  If you're trying to read generic CSV or TSV files, use the CSV
      plugin instead as it handles escaping correctly."""
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

        if len(pdfs) > 0:
            return pd.concat(pdfs)

        return pd.DataFrame([], columns=columns)
