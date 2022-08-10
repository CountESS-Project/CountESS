import dask.dataframe as dd

from countess.core.plugins import InputPlugin, OutputPlugin

class LoadHdfPlugin(InputPlugin):

    name = 'HDF5 Load'
    title = 'Load from HDF5'
    description = "Loads from HDF5 file"

    params = {
        "pattern": { "label": "Filename Pattern", "type": str, "text": "Filename pattern" },
        "key": { "label": "HDF key", "type": str, "text": "hdf key" },
    }

    def __init__(self, pattern, key):
        self.pattern = pattern
        self.key = key

    def run(self, _):
        return dd.read_hdf(self.pattern, self.key)


class StoreHdfPlugin(OutputPlugin):

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

