import dask.dataframe as dd

from countess.core.plugins import DaskTransformPlugin

class DoNothingPlugin(DaskTransformPlugin):

    name = 'Do Nothing'
    title = 'Do Nothing Plugin'
    description = "Doesn't do anything, just passes input to output."
    params = {}

    def run(self, ddf=None):
        return ddf
