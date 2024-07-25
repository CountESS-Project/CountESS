from typing import Callable, Tuple, List, Optional
from numpy import ndarray

# XXX This is only a tiny subset of the curve_fit parameters, which is only
# one function of the scipy.optimize library, but it's what I need for the
# moment ...

def curve_fit(f: Callable, xdata: ndarray|List, ydata: ndarray|List, bounds: Optional[Tuple[ndarray|List|float|int, ndarray|List|float|int]] = None) -> Tuple[ndarray, ndarray]: ...
