import dask.dataframe as dd

from countess.core.plugins import DaskTransformPlugin

VERSION = "0.0.0"

class DoNothingPlugin(DaskTransformPlugin):

    name: str = 'Do Nothing'
    description: str = "An example CountESS plugin which does nothing at all"
    version: str = VERSION

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        return ddf
