from collections.abc import Mapping, MutableMapping
from typing import Iterable, Optional, Union

import pandas as pd
from moore_itertools import product

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ArrayParam, BooleanParam, ColumnOrIndexChoiceParam, MultiParam
from countess.core.plugins import PandasBasePlugin
from countess.utils.pandas import get_all_columns


def _join_how(left_required: bool, right_required: bool) -> str:
    if left_required:
        return "inner" if right_required else "left"
    else:
        return "right" if right_required else "outer"

class JoinPlugin(PandasBasePlugin):
    """Joins Pandas Dataframes"""

    name = "Join"
    description = "Joins two Pandas Dataframes by indexes or columns"
    version = VERSION

    parameters = {
        "inputs": ArrayParam(
            "Inputs",
            MultiParam(
                "Input",
                {
                    "join_on": ColumnOrIndexChoiceParam("Join On"),
                    "required": BooleanParam("Required", True),
                },
            ),
            read_only=True,
            min_size=2,
            max_size=2,
        )
    }

    def join_dataframes(self, dataframe1: pd.DataFrame, dataframe2: pd.DataFrame, join_params) -> pd.DataFrame:
        # "left_on" and "right_on" don't seem to mind if the column
        # is an index, but don't seem to work correctly if the column
        # is part of a multiindex: the other multiindex columns go missing.
        join1 = join_params.get("left_on")
        if join1 and join1 not in dataframe1.columns and dataframe1.index.name != join1:
            dataframe1 = dataframe1.reset_index()

        join2 = join_params.get("right_on")
        if join2 and join2 not in dataframe2.columns and dataframe2.index.name != join2:
            dataframe2 = dataframe2.reset_index()
        
        return dataframe1.merge(dataframe2, **join_params)

    def process_inputs(
        self, inputs: Mapping[str, Iterable[pd.DataFrame]], logger: Logger, row_limit: Optional[int]
    ) -> Iterable[pd.DataFrame]:
        try:
            input1, input2 = inputs.values()
        except ValueError:
            raise NotImplementedError("Only two-way joins implemented at this time")  # pylint: disable=raise-missing-from

        inputs_param = self.parameters["inputs"]
        assert isinstance(inputs_param, ArrayParam)
        assert len(inputs_param) == 2
        ip1, ip2 = inputs_param

        join_params = {
            "how": _join_how(ip1.required.value, ip2.required.value),
            "left_index": ip1.join_on.is_index(),
            "right_index": ip2.join_on.is_index(),
            "left_on": None if ip1.join_on.is_index() else ip1.join_on.value,
            "right_on": None if ip2.join_on.is_index() else ip2.join_on.value,
        }

        input_columns_1 = {}
        input_columns_2 = {}

        for df_in1, df_in2 in product(input1, input2):
            input_columns_1.update(get_all_columns(df_in1))
            input_columns_2.update(get_all_columns(df_in2))
            df_out = self.join_dataframes(df_in1, df_in2, join_params)
            if len(df_out):
                yield df_out

        ip1.set_column_choices(input_columns_1.keys())
        ip2.set_column_choices(input_columns_2.keys())
