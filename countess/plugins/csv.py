from typing import Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore
import csv
from typing import Any, Callable

from countess import VERSION
from countess.core.parameters import *
from countess.core.plugins import DaskBasePlugin, DaskInputPlugin
from countess.utils.dask import merge_dask_dataframes

# XXX it would be better to do the same this Regex Tool does and get the user to assign
# data types to each column

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
        "filename_column": StringParam("Filename Column", ""),
        "columns": ArrayParam("Columns", MultiParam("Column", {
            "name": StringParam("Column Name", ""),
            "type": ChoiceParam("Column Type", "string", choices = ["string", "number", "integer", "boolean", "none"]),
        }))
    }

    file_params = { }

    def read_file_to_dataframe(self, file_param, column_suffix='', row_limit=None):
     
        filename = file_param["filename"].value

        def cast(value, datatype):
            if datatype == 'string':
                return str(value) if value is not None else None
            elif datatype == 'number':
                return float(value if value is not None else math.nan)
            elif datatype == 'integer':
                return int(value or 0)
            elif datatype == 'boolean':
                return bool(value) if value is not None else None
            else:
                return None

        options = {
            'blocksize': '32MB',
            'header': 0 if self.parameters['header'].value else None,
            'converters': dict([
                (n, lambda v, t=pp["type"].value: cast(v, t))
                for n, pp in enumerate(self.parameters["columns"])
            ])
        }
        
        ddf = dd.read_csv(filename, **options)

        # XXX is this reading the whole file?  Is this bad?
        # Dask doesn't support nrows, I think.
        if row_limit is not None:
            ddf = ddf.head(n=row_limit)

        while len(ddf.columns) > len(self.parameters['columns']):
            self.parameters['columns'].add_row()

        for n, pp in enumerate(self.parameters["columns"]):
            if pp["type"].value == 'none':
                ddf = ddf.drop(columns=[ddf.columns[n]])
            elif pp["name"].value and pp["name"].value != ddf.columns[n]:
                ddf = ddf.rename(columns={ ddf.columns[n]: pp["name"].value})

        filename_column = self.parameters['filename_column'].value
        if filename_column:
            ddf[filename_column] = filename

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



