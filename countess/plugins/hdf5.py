from typing import Optional

import pandas as pd  # type: ignore

from countess import VERSION
from countess.core.parameters import (
    ChoiceParam,
    MultiParam,
)
from countess.core.plugins import DaskInputPlugin
from countess.utils.dask import empty_dask_dataframe


class LoadHdfPlugin(DaskInputPlugin):
    name = "HDF5 Load"
    title = "Load from HDF5"
    description = "Loads counts from HDF5 files"
    version = VERSION

    file_types = [("HDF5 File", "*.hdf5")]
    file_params = {
        "key": ChoiceParam("HDF Key"),
    }

    keys: list[str] = []

    def read_file_to_dataframe(
        self, file_params: MultiParam, logger, row_limit: Optional[int] = None
    ) -> pd.DataFrame:
        kp = file_params.key
        filename = file_params.filename.value
        with pd.HDFStore(filename) as hs:
            kp.set_choices(sorted(hs.keys()))

        if kp.value is None:
            return empty_dask_dataframe()

        with pd.HDFStore(filename) as hs:
            df = hs.select(kp.value, start=0, stop=row_limit)

        return df


#class StoreHdfPlugin(DaskBasePlugin):
#    name = "HDF Writer"
#    title = "HDF Writer"
#    description = "Write to HDF5"
#
#    params = {
#        "pattern": {
#            "label": "Filename Pattern",
#            "type": str,
#            "text": "Filename pattern",
#        },
#        "key": {"label": "HDF key", "type": str, "text": "hdf key"},
#    }
#
#    def __init__(self, params, file_params):
#        super.__init__(self)
#        self.pattern = params["pattern"]
#        self.key = params["key"]
#
#    def run(self, ddf):
#        return ddf.to_hdf(self.pattern, self.key, "w")
