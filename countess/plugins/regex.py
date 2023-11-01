import re
from typing import Iterable, Optional

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
from countess.core.plugins import PandasInputFilesPlugin, PandasTransformSingleToTuplePlugin


class RegexToolPlugin(PandasTransformSingleToTuplePlugin):
    name = "Regex Tool"
    description = "Apply regular expressions to a column to make new column(s)"
    link = "https://countess-project.github.io/CountESS/included-plugins/#regex-tool"
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
        "multi": BooleanParam("Multi Match", False),
    }

    compiled_re = None

    def prepare(self, sources: list[str], row_limit: Optional[int] = None):
        super().prepare(sources, row_limit)
        self.compiled_re = re.compile(self.parameters["regex"].value)

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> Optional[pd.DataFrame]:
        assert isinstance(self.parameters["output"], ArrayParam)
        df = super().process_dataframe(dataframe, logger)
        if df is None:
            return None

        if self.parameters["drop_column"].value:
            column_name = self.parameters["column"].value
            if column_name in df.columns:
                df = df.drop(columns=column_name)
            else:
                # XXX maybe set up a helper function for this
                try:
                    df = df.reset_index(column_name, drop=True)
                except KeyError:
                    pass

        index_names = [pp.name.value for pp in self.parameters["output"] if pp.index.value]
        if index_names:
            df = df.set_index(index_names)

        return df

    def process_value(self, value: str, logger: Logger) -> Optional[Iterable]:
        assert self.compiled_re is not None
        assert isinstance(self.parameters["output"], ArrayParam)
        if value is not None:
            try:
                if self.parameters["multi"].value:
                    return self.compiled_re.findall(str(value))
                else:
                    if match := self.compiled_re.match(str(value)):
                        return [
                            op.datatype.cast_value(val) for op, val in zip(self.parameters["output"], match.groups())
                        ]
                    else:
                        pass
                        # logger.info(f"{repr(value)} didn't match")
            except (TypeError, ValueError) as exc:
                logger.exception(exc)

        # If dropping unmatched values, return a simple None which will
        # be filtered out in series_to_dataframe below, otherwise return
        # a tuple of Nones which will fill in the unmatched row.

        if self.parameters["drop_unmatch"].value:
            return None
        else:
            return [None] * self.compiled_re.groups

    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        # Unmatched rows return a single None, so we can easily drop
        # them out before doing further processing
        if self.parameters["drop_unmatch"].value:
            series.dropna(inplace=True)

        if self.parameters["multi"].value:
            series = series.explode()

        return super().series_to_dataframe(series)


class RegexReaderPlugin(PandasInputFilesPlugin):
    name = "Regex Reader"
    description = "Loads arbitrary data from line-delimited files"
    additional = """Applies a regular expression to each line to extract fields.
        If you're trying to read generic CSV or TSV files, use the CSV plugin
        instead as it handles escaping correctly."""
    link = "https://countess-project.github.io/CountESS/included-plugins/#regex-reader"
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
            p.name.value or f"column_{n+1}" for n, p in enumerate(output_parameters) if p.index.value
        ] or None

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

    def load_file(self, file_number: int, logger: Logger, row_limit: Optional[int] = None) -> Iterable:
        assert isinstance(self.parameters["files"], ArrayParam)
        file_params = self.parameters["files"][file_number]
        yield self.read_file_to_dataframe(file_params, logger, row_limit)
