import builtins
import logging
import math
import re
from types import FunctionType, ModuleType, NoneType
from typing import Any

import numpy as np
import pandas as pd

from countess import VERSION
from countess.core.parameters import BooleanParam, TextParam
from countess.core.plugins import PandasTransformDictToDictPlugin

logger = logging.getLogger(__name__)

# XXX pretty sure this is a job for ast.parse rather than just
# running compile() and exec() but that can wait.
# Builtins are restricted but there's still plenty of things which
# could go wrong here.

# These types will get copied to columns, anything else
# (eg: classes, methods, functions) won't.

SIMPLE_TYPES = set((bool, int, float, str, tuple, list, NoneType))


def _module_functions(mod: ModuleType):
    """Extracts just the public functions from a module"""
    return {k: v for k, v in mod.__dict__.items() if not k.startswith("_") and type(v) is FunctionType}


SAFE_BUILTINS = {
    x: builtins.__dict__[x]
    for x in "abs all any ascii bin bool bytearray bytes chr complex dict divmod "
    "enumerate filter float format frozenset hash hex id int len list map max min "
    "oct ord pow range reversed round set slice sorted str sum tuple type zip".split()
}
MATH_FUNCTIONS = _module_functions(math)
RE_FUNCTIONS = _module_functions(re)
NUMPY_IMPORTS = {"nan": np.nan, "inf": np.inf, "isnan": np.isnan, "isinf": np.isinf}


class PythonPlugin(PandasTransformDictToDictPlugin):
    name = "Python Code"
    description = "Apply python code to each row."
    additional = """
        Columns are mapped to local variables and back.
        If you assign to a variable called "__filter",
        only rows where that value is true will be kept.
    """

    version = VERSION

    code = TextParam("Python Code")
    dropna = BooleanParam("Drop Null Columns?")

    code_object = None
    code_globals: dict[str, Any] = {"__builtins__": SAFE_BUILTINS, **MATH_FUNCTIONS, **RE_FUNCTIONS, **NUMPY_IMPORTS}

    def process_dict(self, data: dict):
        assert self.code_object is not None
        try:
            exec(self.code_object, self.code_globals, data)  # pylint: disable=exec-used
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Exception", exc_info=exc)

        return dict((k, v) for k, v in data.items() if type(v) in SIMPLE_TYPES or isinstance(v, np.generic))

    def process_dataframe(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        """Override parent class because we a) want to reset
        the indexes so we can use their values easily and
        b) we don't need to merge afterwards"""

        # XXX cache this?
        self.code_object = compile(self.code.value, "<PythonPlugin>", mode="exec")

        dataframe = dataframe.reset_index(drop=dataframe.index.names == [None])
        series = self.dataframe_to_series(dataframe)
        dataframe = self.series_to_dataframe(series)

        if "__filter" in dataframe.columns:
            dataframe = dataframe.query("not `__filter`.isnull() and `__filter` not in (False, 0, '')").drop(
                columns="__filter"
            )

        if self.dropna:
            dataframe.dropna(axis=1, how="all", inplace=True)

        return dataframe
