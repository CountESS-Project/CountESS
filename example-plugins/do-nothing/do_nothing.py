import pandas as pd

from countess.core.plugins import PandasTransformPlugin

VERSION = "0.0.0"

class DoNothingPlugin(PandasTransformPlugin):

    name: str = 'Do Nothing'
    description: str = "An example CountESS plugin which does nothing at all"
    version: str = VERSION

    def run_dask(self, ddf: pd.DataFrame) -> pd.DataFrame:
        return ddf
