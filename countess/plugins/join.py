from collections.abc import Iterable, Mapping
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd

import itertools
from collections.abc import Callable
from typing import Optional

from countess import VERSION
from countess.core.parameters import ChoiceParam, StringParam, ArrayParam, BooleanParam, MultiParam
from countess.core.plugins import DaskBasePlugin, DaskProgressCallback


INDEX = '— INDEX —'

class DaskJoinPlugin(DaskBasePlugin):
    """Groups a Dask Dataframe by an arbitrary column and rolls up rows"""

    name = "Join"
    title = "Join"
    description = "..."
    version = VERSION

    parameters = {
        "inputs": ArrayParam("Inputs",
            MultiParam("Input", {
                "source": StringParam("Source", read_only=True),
                "join_on": ChoiceParam("Join On", INDEX, choices = [INDEX]),
                "required": BooleanParam("Required", True),
            }), read_only=True
        )
    }

    def prepare(self, data):

        print(data.keys())
        if len(data.keys()) > 2:
            raise NotImplementedError("Only two-way joins supported right now")

        inputs_param = self.parameters["inputs"]

        print(inputs_param.value)

        found = set()
        for n, sp in enumerate(inputs_param):
            source = sp['source'].value
            if isinstance(data.get(source), (pd.DataFrame, dd.DataFrame)):
                print(f"FOUND {source}")
                found.add(source)
                sp['join_on'].choices = [ INDEX ] + list(data[source].columns)
            else:
                print(f"DEL {source}")
                inputs_param.del_row(n)

        for key, df in data.items():
            if isinstance(df, (pd.DataFrame, dd.DataFrame)) and not key in found:
                print(f"ADD {key}")
                #sp = inputs_param.add_row()
                #if sp:
                #    sp['source'].value = key
                #    sp['join_on'].choices = [ INDEX ] + list(df.columns )
                
    def merge_dfs(self, prev_ddf: dd.DataFrame, this_ddf: dd.DataFrame) -> dd.DataFrame:
        """Merge the new data into the old data.  Only called
        if there is a previous plugin to merge data from."""
        ip1 = self.parameters["inputs"][0]
        ip2 = self.parameters["inputs"][1]
        if ip1['required'].value:
            if ip2['required'].value:
                join_how = 'inner'
            else:
                join_how = 'left'
        else:
            if ip2['required'].value:
                join_how = 'right'
            else:
                join_how = 'outer'

        join_params = {
            "how": join_how,
        }
        if ip1['join_on'].value == INDEX:
            join_params['left_index'] = True
        else:
            join_params['left_on'] = ip1['join_on'].value
        if ip2['join_on'].value == INDEX:
            join_params['right_index'] = True
        else:
            join_params['right_on'] = ip2['join_on'].value

        return prev_ddf.merge(this_ddf, **join_params)
       
    def run(
        self,
        data,
        callback: Callable[[int, int, Optional[str]], None],
        row_limit: Optional[int],
    ):
        with DaskProgressCallback(callback):
            if type(data) is dict:
                data = list(data.values())
            return self.merge_dfs(data[1], data[0])
