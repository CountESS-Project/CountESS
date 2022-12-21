import dask.dataframe as dd
import rpy2
import rpy2.robjects as ro
from rpy2.robjects import pandas2ri
from rpy2.robjects.conversion import localconverter
import pandas as pd

from countess.core.plugins import DaskTransformPlugin
from countess.core.parameters import TextParam

VERSION = "0.0.1"

class EmbeddedRPlugin(DaskTransformPlugin):

    name = "Embedded R"
    title = "Embedded R"
    description = "Embed R code into CountESS"
    version = VERSION

    parameters = {
        "code": TextParam("R Code"),
    }

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        print(f"CODE {self.parameters['code'].value}")

        pdf_in = ddf.compute()

        func = ro.r(self.parameters['code'].value)

        print(func)

        with localconverter(ro.default_converter + pandas2ri.converter):
            #print(base.summary(pdf_in))
            pdf_out = func(pdf_in)

        return dd.from_pandas(pd.DataFrame(pdf_out), npartitions=1)

