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
        "code": ArrayParam("Python Expressions", TextParam(""))
    }

    def run_dask(self, df) -> dd.DataFrame:

        for pp in self.parameters['code'].params:
            code = pp.value.replace('\n', '')

            try:
                df = df.query(code)
            except ValueError as exc:
                if str(exc) == 'cannot assign without a target object':
                    df = df.eval(code)

        return df

