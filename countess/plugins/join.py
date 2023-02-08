from collections.abc import Iterable, Mapping
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd

import itertools
from collections.abc import Callable
from typing import Optional

from countess.core.parameters import ChoiceParam, StringParam
from countess.core.plugins import DaskBasePlugin, DaskProgressCallback

VERSION = "0.0.1"


INDEX = '— INDEX —'

class DaskJoinPlugin(DaskBasePlugin):
    """Groups a Dask Dataframe by an arbitrary column and rolls up rows"""

    name = "Join"
    title = "Join"
    description = "..."
    version = VERSION

    parameters = {
        "join_how": ChoiceParam("Join Direction", "outer", ["outer", "inner", "left", "right"]),
        "left_on": ChoiceParam("Left Column", INDEX, choices = [INDEX]),
        "right_on": ChoiceParam("Right Column", INDEX, choices = [INDEX]),
    }

    @classmethod
    def accepts(self, data) -> bool:
        return (
            type(data) is list and
            isinstance(data[0], (dd.DataFrame, pd.DataFrame)) and
            isinstance(data[1], (dd.DataFrame, pd.DataFrame))
        )

    def prepare(self, data):
        self.parameters['left_on'].set_choices([INDEX] + list(data[1].columns))
        self.parameters['right_on'].set_choices([INDEX] + list(data[0].columns))

    def merge_dfs(self, prev_ddf: dd.DataFrame, this_ddf: dd.DataFrame) -> dd.DataFrame:
        """Merge the new data into the old data.  Only called
        if there is a previous plugin to merge data from."""
        join_params = {
            "how": self.parameters['join_how'].value
        }
        if self.parameters['left_on'].value == INDEX:
            join_params['left_index'] = True
        else:
            join_params['left_on'] = self.parameters['left_on'].value 
        if self.parameters['right_on'].value == INDEX:
            join_params['right_index'] = True
        else:
            join_params['right_on'] = self.parameters['right_on'].value 

        return prev_ddf.merge(this_ddf, **join_params)
       
    def run(
        self,
        data,
        callback: Callable[[int, int, Optional[str]], None],
        row_limit: Optional[int],
    ):
        with DaskProgressCallback(callback):
            return self.merge_dfs(data[1], data[0])
