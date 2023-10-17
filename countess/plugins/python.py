import builtins
from types import CodeType

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, PerColumnArrayParam, TextParam
from countess.core.plugins import PandasTransformDictToDictPlugin

# XXX pretty sure this is a job for ast.parse rather than just
# running compile() and exec() but that can wait.
# Builtins are restricted but there's still plenty of things which
# could go wrong here.

# These types will get copied to columns, anything else
# (eg: classes, methods, functions) won't.

SIMPLE_TYPES = set((bool, int, float, str, tuple, list))

SAFE_BUILTINS = {
    x: builtins.__dict__[x]
    for x in "abs all any ascii bin bool bytearray bytes chr complex dict divmod "
    "enumerate filter float format frozenset hash hex id int len list map max min "
    "oct ord pow range reversed round set slice sorted str sum tuple type zip".split()
}


class PythonPlugin(PandasTransformDictToDictPlugin):
    name = "Python Code"
    description = "Apply python code to each row."
    additional = """
        Columns are mapped to local variables and back.
        If you assign to a variable called "__filter",
        only rows where that value is true will be kept.
    """

    version = VERSION

    parameters = {
        "columns": PerColumnArrayParam("columns", BooleanParam("keep", True)),
        "code": TextParam("Python Code"),
    }

    code_object = None

    def process_dict(self, data: dict, logger: Logger):
        assert isinstance(self.parameters["code"], TextParam)
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        assert isinstance(self.code_object, CodeType)

        try:
            exec(self.code_object, {"__builtins__": SAFE_BUILTINS}, data)  # pylint: disable=exec-used
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.exception(exc)

        column_parameters = list(zip(self.input_columns, self.parameters["columns"].params))
        columns_to_remove = set(col for col, param in column_parameters if not param.value)

        return dict((k, v) for k, v in data.items() if k not in columns_to_remove and type(v) in SIMPLE_TYPES)

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        """Override parent class because we a) want to reset
        the indexes so we can use their values easily and
        b) we don't need to merge afterwards"""

        # XXX cache this?
        self.code_object = compile(self.parameters["code"].value, "<PythonPlugin>", mode="exec")

        dataframe = dataframe.reset_index(drop=False)
        series = self.dataframe_to_series(dataframe, logger)
        dataframe = self.series_to_dataframe(series)

        if "__filter" in dataframe.columns:
            dataframe = dataframe.query("__filter").drop(columns="__filter")

        return dataframe
