from collections import defaultdict
from typing import Mapping, Optional

import dask.dataframe as dd
import pandas as pd  # type: ignore

from countess.core.parameters import BaseParam, StringParam
from countess.core.plugins import DaskBasePlugin, DaskInputPlugin

VERSION = "0.0.1"


class LoadHdfPlugin(DaskInputPlugin):

    name = "HDF5 Load"
    title = "Load from HDF5"
    description = "Loads counts from HDF5 files"
    version = VERSION

    file_types = [("HDF5 File", "*.hdf5")]

    file_params = {
        "key": StringParam("HDF Key"),
        "prefix": StringParam("Index Prefix"),
        "suffix": StringParam("Column Suffix"),
    }

    keys: list[str] = []

    def add_file_params(self, filename, file_number):
        # Open the file and read out the keys and the columns for each key.
        file_params = super().add_file_params(filename, file_number)

        with pd.HDFStore(filename) as hs:
            self.keys = sorted(hs.keys())

    def read_file_to_dataframe(
        self, file_params: Mapping[str, BaseParam], row_limit: Optional[int] = None
    ) -> pd.DataFrame:

        filename = file_params["filename"].value
        key = file_params["key"].value

        with pd.HDFStore(filename) as hs:
            df = hs.select(key, start=0, stop=row_limit)

        prefix = file_params["prefix"].value
        suffix = file_params["suffix"].value
        if prefix:
            df.set_index((prefix + str(i) for i in df.index))
        if suffix:
            df.columns = (str(c) + suffix for c in df.columns)
        return df


class StoreHdfPlugin(DaskBasePlugin):

    name = "HDF Writer"
    title = "HDF Writer"
    description = "Write to HDF5"

    params = {
        "pattern": {
            "label": "Filename Pattern",
            "type": str,
            "text": "Filename pattern",
        },
        "key": {"label": "HDF key", "type": str, "text": "hdf key"},
    }

    def __init__(self, params, file_params):
        self.pattern = params["pattern"]
        self.key = params["key"]

    def run(self, ddf):
        return ddf.to_hdf(self.pattern, self.key, "w")
