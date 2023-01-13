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
        "mode": ChoiceParam("Mode", "Apply", choices=["Apply", "Map Partitions", "Collect"]),
        "code": TextParam("R Function")
    }

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:

        try:
            import rpy2
        except ImportError:
            raise NotImplementedError("RPy2 doesn't seem to be installed")

        import rpy2.robjects as ro
        from rpy2.robjects import pandas2ri
        pandas2ri.activate()

        #from rpy2.robjects.conversion import localconverter

        r_func = ro.r(self.parameters['code'].value)

        mode = self.parameters['mode'].value
        if mode == 'Apply':
            x = ddf.apply(lambda x: r_func(**x.to_dict())[0], axis=1, meta=pd.Series(dtype='float', name='x'))
            ddf['x'] = x
        elif mode == 'Map Partitions':
            x = ddf.map_partitions(r_func)
            ddf['x'] = x
        elif mode == 'Collect':
            ddf = pd.DataFrame(r_func(ddf.compute()))

        return ddf

