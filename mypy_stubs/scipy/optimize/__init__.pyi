from math import inf
from typing import Callable, List, Optional, Tuple, Union

from numpy import ndarray

# XXX This is only a tiny subset of the curve_fit parameters, which is only
# one function of the scipy.optimize library, but it's what I need for the
# moment ...

def curve_fit(
    f: Callable,
    xdata: Union[ndarray, List],
    ydata: Union[ndarray, List],
    bounds: Tuple[
        Union[ndarray, List[Union[float, int]], float, int],
        Union[ndarray, List[Union[float, int]], float, int],
    ] = (-inf, inf),
) -> Tuple[ndarray, ndarray]: ...
