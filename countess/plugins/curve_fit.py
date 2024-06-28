from scipy.optimize import curve_fit

from countess import VERSION
from countess.core.plugins import PandasTransformDictToDictPlugin
from countess.core.parameters import ChoiceParam, ColumnGroupChoiceParam, ColumnGroupOrNoneChoiceParam
from countess.core.logger import Logger

FUNCTIONS = [ 'foo', 'bar', 'baz' ]

class CurveFitPlugin(PandasTransformDictToDictPlugin):
    name = "Curve Fit Plugin"
    description = "Fit rows of data to curves"

    version = VERSION

    parameters = {
        "xaxis": ColumnGroupOrNoneChoiceParam("X Axis", None, []),
        "yaxis": ColumnGroupChoiceParam("Y Axis", None, []),
        "function": ChoiceParam("Function", FUNCTIONS[0], FUNCTIONS),
    }

    def process_dict(self, data: dict, logger: Logger):
        return data
