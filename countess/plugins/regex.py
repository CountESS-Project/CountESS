import re
from typing import Iterable, Mapping, Optional, Tuple

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ColumnChoiceParam,
    DataTypeChoiceParam,
    IntegerParam,
    MultiParam,
    StringParam,
)
from countess.core.plugins import PandasInputPlugin, PandasTransformSingleToTuplePlugin


class RegexToolPlugin(PandasTransformSingleToTuplePlugin):
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
        "drop_column": BooleanParam("Drop Column", False),
        "drop_unmatch": BooleanParam("Drop Unmatched Rows", False),
    }

    compiled_re = None

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        df = super().process_dataframe(dataframe, logger)

        if self.parameters["drop_unmatch"].value:
            output_names = [pp.name.value for pp in self.parameters["output"]]
            df = df.dropna(subset=output_names, how="all")

        if self.parameters["drop_column"].value:
            column_name = self.parameters["column"].value
            if column_name in df.columns:
                df = df.drop(columns=column_name)
            else:
                df = df.reset_index(column_name, drop=True)

        index_names = [pp.name.value for pp in self.parameters["output"] if pp.index.value]
        if index_names:
            df = df.set_index(index_names)

        return df

    def process_inputs(
        self, inputs: Mapping[str, Iterable[pd.DataFrame]], logger: Logger, row_limit: Optional[int]
    ) -> Iterable[pd.DataFrame]:
        self.compiled_re = re.compile(self.parameters["regex"].value)
        while self.compiled_re.groups > len(self.parameters["output"].params):
            self.parameters["output"].add_row()

        return super().process_inputs(inputs, logger, row_limit)

    def process_value(self, value: str, logger: Logger) -> Tuple[str]:
        if value is not None:
            try:
                if match := self.compiled_re.match(str(value)):
                    return [
                        op.datatype.cast_value(val)
                        for op, val in zip(self.parameters["output"], match.groups())
                    ]
                else:
                    logger.info(f"{repr(value)} didn't match")
            except (TypeError, ValueError) as exc:
                logger.exception(exc)

        return [None] * self.compiled_re.groups


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
        index_columns = [p.name.value or f"column_{n+1}" for n, p in enumerate(output_parameters) if p.index.value] or None

        records = []
        with open(file_params["filename"].value, "r", encoding="utf-8") as fh:
            for num, line in enumerate(fh):
                if num < self.parameters["skip"].value:
                    continue
                match = compiled_re.match(line)
                if match:
                    records.append((output_parameters[n].datatype.cast_value(g) for n, g in enumerate(match.groups())))
                else:
                    logger.warning(f"Row {num+1} did not match", detail=line)
                if row_limit is not None:
                    if len(records) >= row_limit or num > 100 * row_limit:
                        break
                elif len(records) >= 100000:
                    pdfs.append(pd.DataFrame.from_records(records, columns=columns, index=index_columns))
                    records = []

        if len(records) > 0:
            pdfs.append(pd.DataFrame.from_records(records, columns=columns, index=index_columns))

        if len(pdfs) == 0:
            return pd.DataFrame([], columns=columns)

        df = pd.concat(pdfs)
        return df
