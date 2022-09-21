from collections import defaultdict

import dask.dataframe as dd
import pandas as pd  # type: ignore

from countess.core.parameters import StringParam
from countess.core.plugins import DaskBasePlugin, DaskInputPlugin


class LoadHdfPlugin(DaskInputPlugin):

    name = "HDF5 Load"
    title = "Load from HDF5"
    description = "Loads counts from HDF5 files"

    file_types = [("HDF5 File", "*.hdf5")]

    file_params = {
        "key": StringParam("HDF Key"),
        "prefix": StringParam("Index Prefix"),
        "suffix": StringParam("Column Suffix"),
    }

    key_columns: dict[str, dict[str, list[str]]] = {}

    def add_file_params(self, filename, file_number):
        # Open the file and read out the keys and the columns for each key.
        file_params = super().add_file_params(filename, file_number)

        hs = pd.HDFStore(filename)

        for key in hs.keys():
            self.key_columns[filename][key] = list(
                hs.select(key, start=0, stop=0).columns()
            )

        print(file_params)
        hdf_keys = list(hs.keys())
        file_params["key"].choices = hdf_keys
        if len(hdf_keys) == 1:
            file_params["key"].value = hdf_keys[0]

        print(file_params["key"])
        hs.close()

    def read_file_to_dataframe(self, filename, key, prefix, suffix):
        df = pd.read_hdf(filename.value, key.value)
        if prefix:
            df.set_index((prefix + str(i) for i in df.index))
        if suffix:
            df.columns = (str(c) + suffix for c in df.columns)
        return df

    def get_columns(self):
        columns = set()
        for file_params in self.get_file_params():
            for col in key_columns[file_params["filename"].value][
                file_params["key"].value
            ]:
                columns.add(str(col) + file_parmas["suffix"].value)
        return columns


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
