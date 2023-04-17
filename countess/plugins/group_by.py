import pandas as pd

from countess import VERSION
from countess.core.parameters import ChoiceParam, ColumnOrIndexChoiceParam
from countess.core.plugins import PandasTransformPlugin


class GroupByPlugin(PandasTransformPlugin):
    """Groups a Pandas Dataframe by an arbitrary column and rolls up rows"""

    # XXX should support an operation per column, using
    # dd.Aggregation to supply appropriate chunk/agg/finalize
    # functions which (potentially) work differently per column,
    # as opposed to the built-in aggregations which are the
    # same for every column.

    name = "Group By"
    title = "Groups records by a column"
    description = "Group records by a column"
    version = VERSION

    parameters = {
        "column": ColumnOrIndexChoiceParam("Group By"),
        "operation": ChoiceParam(
            "Operation",
            "sum",
            choices=["sum", "size", "std", "var", "sem", "min", "max"],
        ),
    }

    def run_df(self, df: pd.DataFrame, logger) -> pd.DataFrame:
        assert isinstance(self.parameters["column"], ColumnOrIndexChoiceParam)
        if self.parameters["column"].is_index():
            col = df.index
        else:
            col = df[self.parameters["column"].value]
        oper = self.parameters["operation"].value

        df2 = df.groupby(col).agg(oper)
        assert isinstance(df2, pd.DataFrame)
        return df2
