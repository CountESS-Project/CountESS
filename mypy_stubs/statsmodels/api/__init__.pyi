from math import inf
from typing import Callable, List, Optional, Tuple, Union

from numpy import ndarray

# XXX This is only a tiny subset of the API, but it's what I need for the
# moment ...

class WLS:
    def __init__(self, endog: list[float], exog: list[list[float]], weights: list[float], **kwargs):
        pass
    def fit(self):
        pass
