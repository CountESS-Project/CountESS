from typing import Optional

import pandas as pd

try:
    import tables  # type: ignore  # pylint: disable=unused-import
except ImportError as exc:
    raise NotImplementedError("HDF5 Plugin needs Pytables") from exc

from countess import VERSION
from countess.core.parameters import ChoiceParam, MultiParam
from countess.core.plugins import PandasInputFilesPlugin


class LoadHdfPlugin(PandasInputFilesPlugin):
    name = "HDF5 Load"
    description = "Loads counts from HDF5 files"
    version = VERSION

    file_types = [("HDF5 File", ".hdf5")]
    file_params = {
        "key": ChoiceParam("HDF Key"),
    }

    keys: list[str] = []

    def read_file_to_dataframe(self, file_params: MultiParam, logger, row_limit: Optional[int] = None) -> pd.DataFrame:
        kp = file_params.key
        filename = file_params.filename.value
        with pd.HDFStore(filename) as hs:
            kp.set_choices(sorted(hs.keys()))

        if kp.value is None:
            return pd.DataFrame([])

        with pd.HDFStore(filename) as hs:
            df = hs.select(kp.value, start=0, stop=row_limit)

        assert isinstance(df, pd.DataFrame)

        return df


# class StoreHdfPlugin(PandasBasePlugin):
#    name = "HDF Writer"
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
