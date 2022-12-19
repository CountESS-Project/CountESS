from countess.core.plugins import DaskTransformPlugin
import dask.dataframe as dd

VERSION = "0.0.0"

class DoNothingPlugin(DaskTransformPlugin):

    name: str = 'Do Nothing'
    title: str = 'Example Plugin: Do Nothing'
    description: str = "An example CountESS plugin which does nothing at all"
    version: str = VERSION

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        return ddf
