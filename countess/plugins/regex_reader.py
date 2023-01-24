from typing import Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore

from countess.core.parameters import BooleanParam, StringParam, IntegerParam
from countess.core.plugins import DaskInputPlugin

import re

VERSION = "0.0.1"


def maybe_number(x):
    """CSV is never clear on if something is actually a number so ... try it I guess ..."""
    try:
        return int(x)
    except ValueError:
        pass

    try:
        return float(x)
    except ValueError:
        pass

    return x


class RegexReaderPlugin(DaskInputPlugin):
    """Load CSV files"""

    name = "Regex Reader"
    title = "Load arbitrary data from line-delimited files"
    description = """Loads arbitrary data from line-delimited files, applying a regular expression
      to each line to extract fields.  If you're trying to read generic CSV or TSV files, use the CSV
      plugin instead as it handles escaping correctly."""
    version = VERSION

    file_types = [("CSV", "*.csv"), ("TXT", "*.txt")]

    parameters = {
        "regex": StringParam("Regular Expression", ".*"),
        "skip": BooleanParam("Skip First Row", False),
        "index": IntegerParam("Index Column", 0),
    }

    # XXX should have an update() method to set the column names instead of using "column number"
    # which is pretty clumsy.
    # XXX if regex isn't setting column names maybe there should be StringParams for those?
    # thus avoiding some of the mess of regex named fields.

    def read_file_to_dataframe(self, file_param, column_suffix='', row_limit=None):
     
        print(f"{file_param} {row_limit}")

        records = []

        line_re = re.compile(self.parameters['regex'].value)
        if line_re.groupindex:
            columns = list(line_re.groupindex.keys())
            column_nums = list(line_re.groupindex.values())
        elif line_re.groups:
            column_nums = list(range(1, line_re.groups+1))
            columns = [ "column_%d" % n for n in column_nums ]
        else:
            columns = [ "column_0" ]

        if 0 < self.parameters['index'].value < len(columns):
            index_column = columns[self.parameters['index'].value - 1]
        else:
            index_column = None

        first_row = 1 if self.parameters['skip'].value else 0
        if row_limit and first_row: row_limit = row_limit + first_row

        # XXX should probably slice into pieces rather than building all in one go.
        # XXX note arbitrary backstop against broken REs reading the whole file.
        # this should be removed once resource-based processing limits are added.

        with open(file_param["filename"].value, "r") as fh:
            for num, line in enumerate(fh):
                match = line_re.match(line)
                if match:
                    if match.groups():
                        records.append([ maybe_number(match.group(n)) for n in column_nums ])
                    elif match.group(0):
                        records.append([maybe_number(match.group(0))])
                if row_limit is not None and (len(records) >= row_limit or num > 100 * row_limit):
                    break
                

        return pd.DataFrame.from_records(records, columns=columns, index=index_column)
