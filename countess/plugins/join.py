from collections.abc import Iterable, Mapping
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd

import itertools
from collections.abc import Callable
from typing import Optional

from countess.core.parameters import ChoiceParam
from countess.core.plugins import DaskBasePlugin, DaskProgressCallback

VERSION = "0.0.1"


class DaskJoinPlugin(DaskBasePlugin):
    """Groups a Dask Dataframe by an arbitrary column and rolls up rows"""

    name = "Join"
    title = "Join"
    description = "..."
    version = VERSION

    parameters = {
        "join_how": ChoiceParam("Join Direction", "outer", ["outer", "inner", "left", "right", "concat"])
    }

    @classmethod
    def accepts(self, data) -> bool:
        return type(data) is list and all(
                isinstance(d, (dd.DataFrame, pd.DataFrame))
                for d in data
            )

    def merge_dfs(self, prev_ddf: dd.DataFrame, this_ddf: dd.DataFrame) -> dd.DataFrame:
        """Merge the new data into the old data.  Only called
        if there is a previous plugin to merge data from."""
        join_how = self.parameters['join_how'].value
        if join_how == 'concat':
            return dd.concat([prev_ddf, this_ddf])
        else:
            return prev_ddf.merge(this_ddf, how=join_how, left_index=True, right_index=True)
       
    def run(
        self,
        data,
        callback: Callable[[int, int, Optional[str]], None],
        row_limit: Optional[int],
    ):
        with DaskProgressCallback(callback):
            return self.merge_dfs(data[1], data[0])
