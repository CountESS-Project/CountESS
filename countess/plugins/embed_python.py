import dask.dataframe as dd

import pandas as pd
from countess.core.plugins import DaskTransformPlugin
from countess.core.parameters import StringParam, TextParam, BooleanParam, ArrayParam

VERSION = "0.0.1"

class EmbeddedPythonPlugin(DaskTransformPlugin):

    name = "Embedded Python"
    title = "Embedded Python"
    description = "Embed Python code into CountESS"
    version = VERSION

    parameters = {
        "code": TextParam('Code')
    }

    def run_dask(self, df) -> dd.DataFrame:

        for c in self.parameters['code'].value.split('\n\n'):
            code = c.replace('\n', ' ')
            if not code:
                continue

            try:
                df = df.query(code)
            except ValueError as exc:
                if str(exc) == 'cannot assign without a target object':
                    df = df.eval(code)
                else:
                    raise

        return df

