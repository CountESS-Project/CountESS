from typing import Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore
import csv

from countess.core.parameters import BooleanParam, ChoiceParam
from countess.core.plugins import DaskInputPlugin
from countess.utils.dask import merge_dask_dataframes

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

def clean_row(row):
    return [ maybe_number(x) for x in row ]

class LoadCsvPlugin(DaskInputPlugin):
    """Load CSV files"""

    name = "CSV Load"
    title = "Load from CSV"
    description = "Loads columns from CSV files and merges them into the data"
    version = VERSION

    file_types = [("CSV", "*.csv"), ("TSV", "*.tsv")]

    parameters = {
        "header": BooleanParam("CSV file has header row?", True),
    }

    def read_file_to_dataframe(self, file_param, column_suffix='', row_limit=None):
      
        # XXX should probably slice into pieces rather than building all in one go.
        # then build those pandas dataframes into a single Dask dataframe as we go.

        columns = []
        records = []
        with open(file_param["filename"].value, "r") as fh:
            csv_reader = csv.reader(fh)
            for n, row in enumerate(csv_reader):
                if n == 0 and self.parameters['header'].value:
                    columns = row
                else:
                    while len(row) > len(columns):
                        columns.append(f"column_%d" % len(columns))
                    records.append(clean_row(row))
                if row_limit is not None and n > row_limit:
                    break

        if column_suffix:
            columns = [ f"{c}_{column_suffix}" for c in columns ]

        return pd.DataFrame.from_records(records, columns=columns, index=columns[0])
