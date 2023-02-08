from typing import Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore
import csv
from typing import Any, Callable

from countess.core.parameters import BooleanParam, ChoiceParam, FileParam, FileSaveParam, StringParam
from countess.core.plugins import DaskBasePlugin, DaskInputPlugin
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
        "prefix": StringParam("Column Name Prefix", ""),
    }

    file_params = {
        "suffix": StringParam("Column Name Suffix", ""),
    }

    def read_file_to_dataframe(self, file_param, column_suffix='', row_limit=None):
     
        filename = file_param["filename"].value

        options = {
            'blocksize': '32MB',
            'header': 0 if self.parameters['header'].value else None
        }
        
        ddf = dd.read_csv(filename, **options)

        # XXX is this reading the whole file?  Is this bad?
        if row_limit is not None:
            ddf = ddf.head(n=row_limit)

        prefix = self.parameters["prefix"].value
        suffix = file_param["suffix"].value
        if prefix or suffix:
            ddf.columns = [ prefix + str(colname) + suffix for colname in ddf.columns ]
 
        return ddf

class SaveCsvPlugin(DaskBasePlugin):

    name = "CSV Save"
    title = "Save to CSV"
    description = "CSV CSV CSV"

    file_types = [("CSV", "*.csv")]

    parameters = {
        "header": BooleanParam("CSV header row?", True),
        "filename": FileSaveParam("Filename", file_types=file_types),
    }

    def run(self, obj: Any, callback: Callable[[int, int, Optional[str]], None], row_limit: Optional[int] = None):

        filename = self.parameters["filename"].value

        if row_limit is None:
            if isinstance(obj, dd.DataFrame):
                obj.to_csv(filename, single_file=True, compute=True)
            else:
                obj.to_csv(filename)

        return obj



