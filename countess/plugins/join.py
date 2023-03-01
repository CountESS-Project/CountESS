from collections.abc import Mapping, MutableMapping
from typing import Optional

import dask.dataframe as dd
import pandas as pd  # type: ignore

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ChoiceParam,
    MultiParam,
)
from countess.core.plugins import DaskBasePlugin

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

        enumerate_inputs = enumerate(zip(inputs_param, data.items()))
        for number, (input_param, (source_name, source_ddf)) in enumerate_inputs:
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
        assert len(inputs_param) == 2
        assert isinstance(data, Mapping)
        assert len(data) == 2
        assert all(isinstance(d, (dd.DataFrame, pd.DataFrame)) for d in data.values())

        ip1 = inputs_param[0]
        ip2 = inputs_param[1]

        if ip1.required.value and ip2.required.value:
            join_how = "inner"
        elif ip1.required.value:
            join_how = "left"
        elif ip2.required.value:
            join_how = "right"
        else:
            join_how = "outer"

        join_params: MutableMapping[str, str | bool] = {
            "how": join_how,
        }

        dfs = list(data.values())

        if not ip1.join_on.value or ip1.join_on.value == INDEX:
            join_params["left_index"] = True
        else:
            dfs[0] = dfs[0].reset_index(drop=True)
            join_params["left_on"] = ip1.join_on.value

        if not ip2.join_on.value or ip2.join_on.value == INDEX:
            join_params["right_index"] = True
        else:
            dfs[1] = dfs[1].reset_index(drop=True)
            join_params["right_on"] = ip2["join_on"].value

        return dfs[0].merge(dfs[1], **join_params)
