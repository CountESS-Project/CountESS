from collections import defaultdict
from typing import Mapping, Optional

import dask.dataframe as dd
import pandas as pd  # type: ignore

from countess.core.parameters import (
    BaseParam,
    ChoiceParam,
    FileArrayParam,
    FileParam,
    MultiParam,
    StringParam,
)
from countess.core.plugins import DaskBasePlugin, DaskInputPlugin
from countess.utils.dask import concat_dask_dataframes, empty_dask_dataframe

VERSION = "0.0.1"


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

    def update(self):
        super().update()

        for fp in self.parameters["files"]:
            try:
                with pd.HDFStore(fp.filename.value) as hs:
                    fp.key.choices = sorted(hs.keys())
            except IOError as e:
                # XXX
                pass

    def read_file_to_dataframe(
        self, fp: MultiParam, column_suffix: str = "", row_limit: Optional[int] = None
    ) -> pd.DataFrame:

        if not fp.key.value or fp.key.value not in fp.key.choices:
            with pd.HDFStore(fp.filename.value) as hs:
                fp.key.choices = sorted(hs.keys())
            fp.key.value = fp.key.choices[0] if len(fp.key.choices) == 1 else None
            return empty_dask_dataframe()

        filename = fp["filename"].value
        key = fp["key"].value

        with pd.HDFStore(filename) as hs:
            df = hs.select(key, start=0, stop=row_limit)

        if column_suffix:
            df.columns = (str(c) + "_" + column_suffix for c in df.columns)
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
