import itertools
from collections.abc import Callable, Iterable, Mapping, MutableMapping
from typing import Generator, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (ArrayParam, BooleanParam, ChoiceParam,
                                      MultiParam, StringParam)
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
                    "join_on": ChoiceParam("Join On", INDEX, choices=[INDEX]),
                    "required": BooleanParam("Required", True),
                },
            ),
            read_only=True,
            min_size=2,
            max_size=2,
        )
    }

    def prepare(self, data, logger: Logger):
        data_items = list(data.items())
        inputs_param = self.parameters["inputs"]
        assert isinstance(inputs_param, ArrayParam)

        if len(data_items) != 2 or len(inputs_param) != 2:
            raise NotImplementedError("Only two-way joins supported right now")
        if not all((isinstance(df, (dd.DataFrame, pd.DataFrame)) for _, df in data_items)):
            raise NotImplementedError("Feed me dataframes")

        for number, (input_param, (source_name, source_ddf)) in enumerate(zip(inputs_param, data.items())):
            input_param.label = f"Input {number+1}: {source_name}"
            input_param["join_on"].choices = [INDEX] + list(source_ddf.columns)

    def run(
        self,
        data,
        logger: Logger,
        row_limit: Optional[int] = None,
    ):
        inputs_param = self.parameters["inputs"]
        assert isinstance(inputs_param, ArrayParam)
        ip1 = inputs_param[0]
        ip2 = inputs_param[1]
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

        join_params: MutableMapping[str, str | bool] = {
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

        dfs = list(data.values())
        return dfs[0].merge(dfs[1], **join_params)
