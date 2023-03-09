import dask.dataframe as dd
import pandas as pd  # type: ignore

from countess import VERSION
from countess.core.parameters import ChoiceParam, ColumnOrIndexChoiceParam, PerColumnArrayParam, MultiParam, BooleanParam, MultipleChoiceParam, TabularMultiParam
from countess.core.plugins import DaskTransformPlugin


class GroupByPlugin(DaskTransformPlugin):
    """Groups a Dask Dataframe by an arbitrary column and rolls up rows"""

    # XXX should support an operation per column, using
    # dd.Aggregation to supply appropriate chunk/agg/finalize
    # functions which (potentially) work differently per column,
    # as opposed to the built-in aggregations which are the
    # same for every column.

    name = "Group By"
    description = "Group records by a column"
    version = VERSION

    parameters = {
        "columns": PerColumnArrayParam("Columns",
             TabularMultiParam("Column", {
                 "index": BooleanParam("Index"),
                 "sum": BooleanParam("Sum"),
                 "count": BooleanParam("Count"),
                 "std": BooleanParam("Std"),
                 "var": BooleanParam("Var"),
                 "sem": BooleanParam("Sem"),
                 "min": BooleanParam("Min"),
                 "max": BooleanParam("Max"),
             })
        )
    }

    def run_dask(self, df: pd.DataFrame | dd.DataFrame, logger) -> dd.DataFrame:

        for p in self.parameters['columns']:
            if p['index'].value:
                for k, pp in p.params.items():
                    pp.value = k == 'index'
        
        return df
