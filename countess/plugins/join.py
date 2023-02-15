import itertools
from collections.abc import Callable, Iterable, Mapping
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ChoiceParam,
    MultiParam,
    StringParam,
)
from countess.core.plugins import DaskBasePlugin, DaskProgressCallback

INDEX = "— INDEX —"


class DaskJoinPlugin(DaskBasePlugin):
    """Groups a Dask Dataframe by an arbitrary column and rolls up rows"""

    name = "Join"
    title = "Join"
    description = "..."
    version = VERSION

    parameters = {
        "inputs": ArrayParam(
            "Inputs",
            MultiParam(
                "Input",
                {
                    "source": StringParam("Source", read_only=True),
                    "join_on": ChoiceParam("Join On", INDEX, choices=[INDEX]),
                    "required": BooleanParam("Required", True),
                },
            ),
            read_only=True,
            min_size=2,
            max_size=2,
        )
    }

    def prepare(self, data):

        data_items = list(data.items())
        inputs_param = self.parameters["inputs"]

        if len(data_items) != 2 or len(inputs_param) != 2:
            raise NotImplementedError("Only two-way joins supported right now")
        if not all(
            (isinstance(df, (dd.DataFrame, pd.DataFrame)) for _, df in data_items)
        ):
            raise NotImplementedError("Feed me dataframes")

        for input_param, (source_name, source_ddf) in zip(inputs_param, data.items()):
            input_param["source"].value = source_name
            input_param["join_on"].choices = [INDEX] + list(source_ddf.columns)

    def run(
        self,
        data,
        callback: Callable[[int, int, Optional[str]], None],
        row_limit: Optional[int],
    ):

        ip1 = self.parameters["inputs"][0]
        ip2 = self.parameters["inputs"][1]
        if ip1["required"].value:
            if ip2["required"].value:
                join_how = "inner"
            else:
                join_how = "left"
        else:
            if ip2["required"].value:
                join_how = "right"
            else:
                join_how = "outer"

        join_params = {
            "how": join_how,
        }
        if ip1["join_on"].value == INDEX:
            join_params["left_index"] = True
        else:
            join_params["left_on"] = ip1["join_on"].value
        if ip2["join_on"].value == INDEX:
            join_params["right_index"] = True
        else:
            join_params["right_on"] = ip2["join_on"].value

        return data[ip1["source"].value].merge(data[ip2["source"].value], **join_params)
