import dask.dataframe as dd

import pandas as pd

from countess.core.plugins import DaskTransformPlugin
from countess.core.parameters import ChoiceParam, TextParam

VERSION = "0.0.1"

class EmbeddedRPlugin(DaskTransformPlugin):

    name = "Embedded R"
    title = "Embedded R"
    description = "Embed R code into CountESS"
    version = VERSION

    parameters = {
        "column": ChoiceParam("Group By", "Index", choices=[]),
        "code_map": TextParam("Map Function"),
        "code_red": TextParam("Reduce Function"),
        "code_fin": TextParam("Finalize Function"),
    }

    def update(self):
        self.parameters["column"].choices = ["Index"] + self.input_columns

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:

        try:
            import rpy2
            import rpy2.robjects as ro
            from rpy2.robjects import pandas2ri
            from rpy2.robjects.conversion import localconverter
        except ImportError:
            raise NotImplementedError("No RPy2 Installed")

        col_name = self.parameters["column"].value
        column = ddf.index if col_name == "Index" else ddf[col_name]

        map_f = ro.r(self.parameters['code_map'].value)
        red_f = ro.r(self.parameters['code_red'].value)
        fin_f = ro.r(self.parameters['code_fin'].value)

        aggregation = dd.Aggregation(f"R_{id(self)}", map_f, red_f, fin_f)
       
        with localconverter(ro.default_converter + pandas2ri.converter):
            result = ddf.groupby(column).agg(aggregation)

        return dd.from_pandas(pd.DataFrame(result), npartitions=1)

