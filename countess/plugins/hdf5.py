import dask.dataframe as dd
import pandas as pd
from collections import defaultdict

from countess.core.plugins import DaskInputPlugin, DaskBasePlugin, PluginParam

class LoadHdfPlugin(DaskInputPlugin):

    name = 'HDF5 Load'
    title = 'Load from HDF5'
    description = "Loads counts from HDF5 files"

    file_types = [('HDF5 File', '*.hdf5')]

    file_params = [ 
        PluginParam('key', 'HDF Key', str),
        PluginParam('prefix', 'Index Prefix', str),
        PluginParam('suffix', 'Column Suffix', str),
    ]

    def add_file_params(self, filename, file_number):
        # Open the file and read out the keys and the columns for each key.
        file_params = super().add_file_params(filename, file_number)

        hs = pd.HDFStore(filename)
        
        #for key in hs.keys():            
        #    self.file_keys[filename][key] = list(hs.select(key, start=0, stop=0).columns())

        hdf_keys = list(hs.keys())
        file_params['key'].choices = hdf_keys
        if len(hdf_keys) == 1: file_params['key'].value = hdf_keys[0]
         
    def run(self, _):
        return dd.read_hdf(self.pattern, self.key)


class StoreHdfPlugin(DaskBasePlugin):

    name = 'HDF Writer'
    title = 'HDF Writer'
    description = 'Write to HDF5'

    params = {
        "pattern": { "label": "Filename Pattern", "type": str, "text": "Filename pattern" },
        "key": { "label": "HDF key", "type": str, "text": "hdf key" },
    }

    def __init__(self, params, file_params):
        self.pattern = params['pattern']
        self.key = params['key']

    def run(self, ddf):
        return ddf.to_hdf(self.pattern, self.key, 'w')

