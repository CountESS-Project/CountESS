import dask.dataframe as dd

from countess.core.plugins import DaskInputPlugin, DaskOutputPlugin

class LoadHdfPlugin(DaskInputPlugin):

    name = 'HDF5 Load'
    title = 'Load from HDF5'
    description = "Loads counts from HDF5 files"

    file_types = [('HDF5 File', '*.hdf5')]

    params = {}

    file_params = {
        "key": { "label": "HDF key", "type": str },
        "index_prefix": { "label": "Index Prefix", "type": str },
        "column_suffix": { "label": "Column Suffix", "type": str },
    }

    def __init__(self, pattern, key):
        self.pattern = pattern
        self.key = key

    def run(self, _):
        return dd.read_hdf(self.pattern, self.key)


class StoreHdfPlugin(DaskOutputPlugin):

    name = 'HDF Writer'
    title = 'HDF Writer'
    description = 'Write to HDF5'

    params = {
        "pattern": { "label": "Filename Pattern", "type": str, "text": "Filename pattern" },
        "key": { "label": "HDF key", "type": str, "text": "hdf key" },
    }

    def __init__(self, pattern, key):
        self.pattern = pattern
        self.key = key

    def run(self, ddf):
        return ddf.to_hdf(self.pattern, self.key, 'w')

