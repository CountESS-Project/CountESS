from typing import Dict, Iterable, Optional

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ArrayParam, BooleanParam, ColumnOrIndexChoiceParam, MultiParam
from countess.core.plugins import PandasProductPlugin
from countess.utils.pandas import get_all_columns


def _join_how(left_required: bool, right_required: bool) -> str:
    if left_required:
        return "inner" if right_required else "left"
    else:
        return "right" if right_required else "outer"


class JoinPlugin(PandasProductPlugin):
    """Joins Pandas Dataframes"""

    name = "Join"
    description = "Joins two Pandas Dataframes by indexes or columns"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#join"

    parameters = {
        "inputs": ArrayParam(
            "Inputs",
            MultiParam(
                "Input",
                {
                    "join_on": ColumnOrIndexChoiceParam("Join On"),
                    "required": BooleanParam("Required", True),
                    "drop": BooleanParam("Drop Column", False),
                },
            ),
            read_only=True,
            min_size=2,
            max_size=2,
        ),
    }
    join_params = None
    input_columns_1: Optional[Dict] = None
    input_columns_2: Optional[Dict] = None

    def prepare(self, sources: list[str], row_limit: Optional[int] = None):
        super().prepare(sources, row_limit)

        assert isinstance(self.parameters["inputs"], ArrayParam)
        assert len(self.parameters["inputs"]) == 2
        ip1, ip2 = self.parameters["inputs"]
        ip1.label = f"Input 1: {sources[0]}"
        ip2.label = f"Input 2: {sources[1]}"

        self.join_params = {
            "how": _join_how(ip1.required.value, ip2.required.value),
            "left_index": ip1.join_on.is_index(),
            "right_index": ip2.join_on.is_index(),
            "left_on": None if ip1.join_on.is_index() else ip1.join_on.value,
            "right_on": None if ip2.join_on.is_index() else ip2.join_on.value,
        }
        self.input_columns_1 = {}
        self.input_columns_2 = {}

    def process_dataframes(self, dataframe1: pd.DataFrame, dataframe2: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        # update columns on inputs, these won't propagate back in the case of multiprocess runs but
        # they will work in preview mode where we only run this in a single thread.

        assert self.input_columns_1 is not None
        assert self.input_columns_2 is not None
        assert self.join_params is not None
        assert isinstance(self.parameters["inputs"], ArrayParam)

        self.input_columns_1.update(get_all_columns(dataframe1))
        self.input_columns_2.update(get_all_columns(dataframe2))

        # "left_on" and "right_on" don't seem to mind if the column
        # is an index, but don't seem to work correctly if the column
        # is part of a multiindex: the other multiindex columns go missing.
        join1 = self.join_params.get("left_on")
        if join1 and dataframe1.index.name != join1:
            drop_index = dataframe1.index.name is None and dataframe1.index.names[0] is None
            dataframe1 = dataframe1.reset_index(drop=drop_index)

        join2 = self.join_params.get("right_on")
        if join2 and dataframe2.index.name != join2:
            drop_index = dataframe2.index.name is None and dataframe2.index.names[0] is None
            dataframe2 = dataframe2.reset_index(drop=drop_index)

        try:
            dataframe = dataframe1.merge(dataframe2, **self.join_params)
        except ValueError as exc:
            logger.exception(exc)
            return pd.DataFrame()

        if self.parameters["inputs"][0]["drop"].value and join1 in dataframe.columns:
            dataframe.drop(columns=join1, inplace=True)
        if self.parameters["inputs"][1]["drop"].value and join2 in dataframe.columns:
            dataframe.drop(columns=join2, inplace=True)

        return dataframe

    def finalize(self, logger: Logger) -> Iterable:
        assert isinstance(self.parameters["inputs"], ArrayParam)
        assert len(self.parameters["inputs"]) == 2
        assert self.input_columns_1 is not None
        assert self.input_columns_2 is not None
        ip1, ip2 = self.parameters["inputs"]

        ip1.set_column_choices(self.input_columns_1.keys())
        ip2.set_column_choices(self.input_columns_2.keys())
        yield from super().finalize(logger)
