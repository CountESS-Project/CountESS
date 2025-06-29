import builtins
import logging
import math
import re
from types import BuiltinFunctionType, FunctionType, ModuleType, NoneType
from typing import Any, Optional

import numpy as np

from countess import VERSION
from countess.core.parameters import BooleanParam, TextParam
from countess.core.plugins import DuckdbThreadedTransformPlugin

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
    return {
        k: v
        for k, v in mod.__dict__.items()
        if not k.startswith("_") and type(v) in (BuiltinFunctionType, FunctionType)
    }


SAFE_BUILTINS = {
    x: builtins.__dict__[x]
    for x in "abs all any ascii bin bool chr "
    "float format frozenset hash hex int len max min "
    "ord range round sorted str sum type zip".split()
}
MATH_FUNCTIONS = _module_functions(math)
RE_FUNCTIONS = _module_functions(re)
NUMPY_IMPORTS = {
    "nan": np.nan,
    "inf": np.inf,
    "isnan": np.isnan,
    "isinf": np.isinf,
    "mean": lambda *x: np.mean(x),
    "std": lambda *x: np.std(x),
    "var": lambda *x: np.var(x),
    "median": lambda *x: np.median(x),
}

CODE_GLOBALS: dict[str, Any] = {"__builtins__": SAFE_BUILTINS, **MATH_FUNCTIONS, **RE_FUNCTIONS, **NUMPY_IMPORTS}

AVAILABLE_FUNCTIONS: list[str] = sorted(
    list(SAFE_BUILTINS.keys()) + list(MATH_FUNCTIONS.keys()) + list(RE_FUNCTIONS.keys()) + list(NUMPY_IMPORTS.keys())
)


class PythonPlugin(DuckdbThreadedTransformPlugin):
    name = "Python Code"
    description = "Apply python code to each row."
    additional = """
        Columns are mapped to local variables and back.
        If you assign to a variable called "__filter",
        only rows where that value is true will be kept.

        Available Functions:
    """ + " ".join(
        AVAILABLE_FUNCTIONS
    )
    link = "https://countess-project.github.io/CountESS/included-plugins/#python-code"

    version = VERSION

    code = TextParam("Python Code")
    dropna = BooleanParam("Drop Null Columns?")

    code_object = None
    code_globals: dict[str, Any] = {"__builtins__": SAFE_BUILTINS, **MATH_FUNCTIONS, **RE_FUNCTIONS, **NUMPY_IMPORTS}

    def prepare(self, *a) -> None:
        super().prepare(*a)
        self.code_object = compile(self.code.value or "", "<PythonPlugin>", mode="exec")

    def transform(self, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        assert self.code_object is not None
        try:
            exec(self.code_object, CODE_GLOBALS, data)  # pylint: disable=exec-used
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Exception", exc_info=exc)

        return dict((k, v) for k, v in data.items() if type(v) in SIMPLE_TYPES or isinstance(v, np.generic))
