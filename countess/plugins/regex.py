import logging
import re
from typing import Iterable, Optional

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
from countess.core.plugins import PandasInputFilesPlugin, PandasTransformSingleToTuplePlugin

logger = logging.getLogger(__name__)


class OutputColumnsMultiParam(MultiParam):
    name = StringParam("Column Name")
    datatype = DataTypeChoiceParam("Column Type", "string")


class RegexToolPlugin(PandasTransformSingleToTuplePlugin):
    name = "Regex Tool"
    description = "Apply regular expressions to a column to make new column(s)"
    link = "https://countess-project.github.io/CountESS/included-plugins/#regex-tool"
    version = VERSION

    column = ColumnChoiceParam("Input Column")
    regex = StringParam("Regular Expression", ".*")
    output = ArrayParam("Output Columns", OutputColumnsMultiParam("Col"))
    drop_column = BooleanParam("Drop Column", False)
    drop_unmatch = BooleanParam("Drop Unmatched Rows", False)
    multi = BooleanParam("Multi Match", False)

    compiled_re = None

    def prepare(self, sources: list[str], row_limit: Optional[int] = None):
        super().prepare(sources, row_limit)
        self.compiled_re = re.compile(self.regex.value)

    def process_dataframe(self, dataframe: pd.DataFrame) -> Optional[pd.DataFrame]:
        df = super().process_dataframe(dataframe)
        if df is None:
            return None

        if self.drop_column:
            column_name = self.column.value
            if column_name in df.columns:
                df = df.drop(columns=column_name)
            else:
                # XXX maybe set up a helper function for this
                try:
                    df = df.reset_index(column_name, drop=True)
                except KeyError:
                    pass

        return df

    def process_value(self, value: str) -> Optional[Iterable]:
        assert self.compiled_re is not None
        if value is not None:
            try:
                if self.multi:
                    return self.compiled_re.findall(str(value))
                else:
                    if match := self.compiled_re.match(str(value)):
                        return [op.datatype.cast_value(val) for op, val in zip(self.output, match.groups())]
                    else:
                        logger.info("%s didn't match", repr(value))
            except (TypeError, ValueError) as exc:
                logger.warning("Exception", exc_info=exc)

        # If dropping unmatched values, return a simple None which will
        # be filtered out in series_to_dataframe below, otherwise return
        # a tuple of Nones which will fill in the unmatched row.

        if self.drop_unmatch:
            return None
        else:
            return [None] * self.compiled_re.groups

    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        # Unmatched rows return a single None, so we can easily drop
        # them out before doing further processing
        if self.drop_unmatch:
            series.dropna(inplace=True)

        if self.multi:
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

    regex = StringParam("Regular Expression", "(.*)")
    skip = IntegerParam("Skip Lines", 0)
    output = ArrayParam("Output Columns", OutputColumnsMultiParam("Col"))

    def read_file_to_dataframe(self, filename, file_param, row_limit=None):
        pdfs = []

        compiled_re = re.compile(self.regex.value)

        while compiled_re.groups > len(self.output.params):
            self.output.add_row()

        output_parameters = list(self.output)[: compiled_re.groups]
        columns = [p.name.value or f"column_{n+1}" for n, p in enumerate(output_parameters)]

        records = []
        with open(filename, "r", encoding="utf-8") as fh:
            for num, line in enumerate(fh):
                if num < self.skip:
                    continue
                match = compiled_re.match(line)
                if match:
                    records.append((output_parameters[n].datatype.cast_value(g) for n, g in enumerate(match.groups())))
                else:
                    logger.warning("Row %d did not match", num)
                if row_limit is not None:
                    if len(records) >= row_limit or num > 100 * row_limit:
                        break
                elif len(records) >= 100000:
                    pdfs.append(pd.DataFrame.from_records(records, columns=columns))
                    records = []

        if len(records) > 0:
            pdfs.append(pd.DataFrame.from_records(records, columns=columns))

        if len(pdfs) == 0:
            return pd.DataFrame([], columns=columns)

        df = pd.concat(pdfs)
        return df
