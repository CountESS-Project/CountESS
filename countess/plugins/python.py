import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, PerColumnArrayParam, TextParam
from countess.core.plugins import PandasTransformRowToDictPlugin

# XXX pretty sure this is a job for ast.parse rather than just
# running compile() and exec() but that can wait.


# These types will get copied to columns, anything else
# (eg: classes, methods, functions) won't.

SIMPLE_TYPES = set((bool, int, float, str, tuple, list))

# XXX should probably actually be based on
# PandasTransformDictToDictPlugin
# which is a bit more efficient.


class PythonPlugin(PandasTransformRowToDictPlugin):
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

    def process_row(self, row: pd.Series, logger: Logger):
        assert isinstance(self.parameters["code"], TextParam)
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        code_object = compile(self.parameters["code"].value, "<PythonPlugin>", mode="exec")

        row_dict = dict(row)
        exec(code_object, {}, row_dict)  # pylint: disable=exec-used

        column_parameters = list(zip(self.input_columns, self.parameters["columns"].params))
        columns_to_remove = set(col for col, param in column_parameters if not param.value)

        return dict((k, v) for k, v in row_dict.items() if k not in columns_to_remove and type(v) in SIMPLE_TYPES)

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        """Override parent class because we a) want to reset
        the indexes so we can use their values easily and
        b) we don't need to merge afterwards"""

        dataframe = dataframe.reset_index(drop=False)
        series = self.dataframe_to_series(dataframe, logger)
        dataframe = self.series_to_dataframe(series)

        if "__filter" in dataframe.columns:
            dataframe = dataframe.query("__filter").drop(columns="__filter")

        return dataframe
