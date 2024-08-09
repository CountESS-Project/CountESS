from math import e, log2
from typing import Callable, Optional, SupportsFloat, Union

import numpy as np
from scipy.optimize import curve_fit

from countess import VERSION
from countess.core.parameters import ChoiceParam, ColumnGroupChoiceParam, ColumnGroupOrNoneChoiceParam
from countess.core.plugins import PandasTransformDictToDictPlugin

FUNCTIONS: dict[str, Callable] = {
    "linear": lambda x, a, b: x * a + b,
    "exponential": lambda x, a, b, c: a * e**b + c,
    "log": lambda x, a, b: a * log2(x + 0.001) + b,
}


def _to_float(s: Optional[Union[SupportsFloat, str]]) -> Optional[float]:
    if s is None:
        return None
    try:
        if type(s) is str:
            s = s.lstrip("_ ")
        return float(s)
    except (ValueError, TypeError):
        return None


class CurveFitPlugin(PandasTransformDictToDictPlugin):
    name = "Curve Fit Plugin"
    description = "Fit rows of data to curves"

    version = VERSION

    xaxis = ColumnGroupOrNoneChoiceParam("X Axis", None, [])
    yaxis = ColumnGroupChoiceParam("Y Axis", None, [])
    function = ChoiceParam("Function", list(FUNCTIONS.keys())[0], list(FUNCTIONS.keys()))

    def process_dict(self, data: dict) -> dict:
        xprefix = None
        if not self.xaxis.is_none():
            xprefix = self.xaxis.value
        yprefix = self.yaxis.value

        tvals = {k.removeprefix(yprefix) for k in data.keys() if k.startswith(yprefix)}
        if xprefix:
            tvals.update(k.removeprefix(xprefix) for k in data.keys() if k.startswith(xprefix))

        xvals: list[float] = []
        yvals: list[float] = []

        for t in tvals:
            xval = _to_float(data.get(f"{xprefix}{t}") if xprefix else t)
            yval = _to_float(data.get(f"{yprefix}{t}"))
            if xval is not None and yval is not None:
                xvals.append(xval)
                yvals.append(yval)

        try:
            function = FUNCTIONS[self.function.value]
            popt, pcov, *_ = curve_fit(function, xvals, yvals)

            r = {f"popt_{n}": v for n, v in enumerate(popt)}
            r.update({f"perr_{n}": v for n, v in enumerate(np.sqrt(np.diag(pcov)))})
            return r
        except RuntimeError:
            return {}
