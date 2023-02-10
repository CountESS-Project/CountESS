import dask.dataframe as dd
import pandas as pd

from countess import VERSION
from countess.core.plugins import DaskTransformPlugin
from countess.core.parameters import StringParam, TextParam, BooleanParam, ArrayParam

def process(df: pd.DataFrame, codes):

    for code in codes:
        result = df.eval(code)
        if isinstance(result, (dd.Series, pd.Series)):
            # this was a filter
            df['_filter'] = result
            df = df.query('_filter').drop(columns='_filter')
        else:
            # this was a column assignment
            df = result

    return df

class EmbeddedPythonPlugin(DaskTransformPlugin):

    name = "Embedded Python"
    title = "Embedded Python"
    description = "Embed Python code into CountESS"
    version = VERSION

    parameters = {
        "code": TextParam('Code')
    }

    def run_dask(self, df) -> dd.DataFrame:

        codes = [ 
            c.replace('\n', ' ').strip()
            for c in self.parameters['code'].value.split('\n\n')
            if c.strip()
        ]

        if isinstance(df, dd.DataFrame):
            return df.map_partitions(process, codes)
        else:
            return process(df, codes)
