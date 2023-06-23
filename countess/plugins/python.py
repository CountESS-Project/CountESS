import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import TextParam
from countess.core.plugins import PandasTransformPlugin

# XXX pretty sure this is a job for ast.parse rather than just
# running compile() and exec() but that can wait.


# These types will get copied to columns, anything else
# (eg: classes, methods, functions) won't.

SIMPLE_TYPES = set((bool, int, float, str, tuple, list))


class PythonPlugin(PandasTransformPlugin):
    name = "Python Code"
    description = "Apply python code to each row."
    additional = """
        Columns are mapped to local variables and back.
        If you assign to a variable called "__filter",
        only rows where that value is true will be kept.
    """

    version = VERSION

    parameters = {"code": TextParam("Python Code")}

    def run_df(self, df, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["code"], TextParam)
        code_object = compile(self.parameters["code"].value, "<PythonPlugin>", mode="exec")

        def _process(row):
            row_dict = dict(row)
            exec(code_object, {}, row_dict)  # pylint: disable=exec-used
            return dict((k, v) for k, v in row_dict.items() if type(v) in SIMPLE_TYPES)

        # XXX It'd be nice to do this without resetting the index
        dfo = df.reset_index(drop=False)
        dfo = dfo.apply(_process, axis=1, result_type="expand")

        if "__filter" in dfo.columns:
            dfo = dfo.query("__filter").drop(columns="__filter")

        return dfo
