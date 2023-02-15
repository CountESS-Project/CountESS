import dask.dataframe as dd
import pandas as pd

from countess.core.parameters import StringParam, TextParam
from countess.core.plugins import DaskTransformPlugin

VERSION = "0.0.1"


class EmbeddedRPlugin(DaskTransformPlugin):

    name = "Embedded R"
    title = "Embedded R"
    description = "Embed R code into CountESS"
    version = VERSION

    parameters = {"column": StringParam("Column"), "code": TextParam("R Function")}

    def prepare(self, ddf: dd.DataFrame):
        try:
            import rpy2
        except ImportError:
            raise NotImplementedError("RPy2 doesn't seem to be installed")

    def run_dask(self, df) -> dd.DataFrame:

        import rpy2.robjects as ro
        from rpy2.robjects import pandas2ri

        pandas2ri.activate()

        r_func = ro.r(self.parameters["code"].value)

        print(df)

        x = df.apply(r_func, raw=True, axis=1)

        # XXX problem: this works great if the R function returns
        # a single column which maps per row of the input, but that's
        # not the only format it can come out in.
        # I'd like to be able to support:
        #  function (z) { aggregate(z$y, list(z$x), FUN=mean) }
        # or even:
        #  function (z) { list(aggregate(z$y, list(z$x), FUN=mean), aggregate(z$y, list(z$x), FUN=median)) }
        # for example, but we end up dealing with a pandas Series
        # of rpy2 DataFrames, which we need to convert back to
        # something we can use.  The autoconversion doesn't seem
        # to catch this at all.

        print(str(x))
        if isinstance(x, dd.DataFrame):
            return x
        else:
            df[self.parameters["column"].value] = x

        return df
