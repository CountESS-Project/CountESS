from collections.abc import Mapping, MutableMapping
from typing import Iterable, Optional, Union

import pandas as pd
from moore_itertools import product

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ArrayParam, BooleanParam, ChoiceParam, MultiParam
from countess.core.plugins import PandasBasePlugin

INDEX = "— INDEX —"


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
        if not all((isinstance(df, pd.DataFrame) for _, df in data_items)):
            raise NotImplementedError("Feed me dataframes")

        enumerate_inputs = enumerate(zip(inputs_param, data.items()))
        for number, (input_param, (source_name, source_ddf)) in enumerate_inputs:
            input_param.label = f"Input {number+1}: {source_name}"
            if hasattr(source_ddf.index, "names"):
                input_columns = [n for n in source_ddf.index.names if n] + list(source_ddf.columns)
            elif source_ddf.index.name:
                input_columns = [source_ddf.index.name] + list(source_ddf.columns)
            else:
                input_columns = list(source_ddf.columns)
            input_param["join_on"].choices = [INDEX] + input_columns

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
        assert all(isinstance(d, pd.DataFrame) for d in data.values())

        ip1, ip2 = inputs_param
        df1, df2 = data.values()

        if ip1.required.value and ip2.required.value:
            join_how = "inner"
        elif ip1.required.value:
            join_how = "left"
        elif ip2.required.value:
            join_how = "right"
        else:
            join_how = "outer"

        join_params: MutableMapping[str, Union[str, bool]] = {
            "how": join_how,
        }

        # "left_on" and "right_on" don't seem to mind if the column
        # is an index, but don't seem to work correctly if the column
        # is part of a multiindex: the other multiindex columns go missing.

        join1 = ip1.join_on.value
        join2 = ip2.join_on.value

        if join1 and join1 != INDEX:
            if join1 not in df1.columns and df1.index.name != join1:
                df1 = df1.reset_index()
            join_params["left_on"] = join1
        else:
            join_params["left_index"] = True

        if join2 and join2 != INDEX:
            if join2 not in df2.columns and df2.index.name != join2:
                df2 = df2.reset_index()
            join_params["right_on"] = join2
        else:
            join_params["right_index"] = True

        return df1.merge(df2, **join_params)

    source1 = None
    source2 = None
    memo1 = []
    memo2 = []

    def join_dataframes(self, dataframe1: pd.DataFrame, dataframe2: pd.DataFrame) -> pd.DataFrame:
        return dataframe1.join(dataframe2)

    def process_inputs(
        self, inputs: Mapping[str, Iterable[pd.DataFrame]], logger: Logger, row_limit: Optional[int]
    ) -> Iterable[pd.DataFrame]:
        try:
            input1, input2 = inputs.values()
        except ValueError:
            raise NotImplementedError("Only two-way joins implemented at this time")  # pylint: disable=raise-missing-from

        for df_in1, df_in2 in product(input1, input2):
            df_out = self.join_dataframes(df_in1, df_in2)
            if len(df_out):
                yield df_out
