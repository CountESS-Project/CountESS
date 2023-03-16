import dask.dataframe as dd
import pandas as pd  # type: ignore

from countess import VERSION
from countess.core.parameters import BaseParam, BooleanParam, PerColumnArrayParam, TabularMultiParam
from countess.core.plugins import DaskTransformPlugin


class GroupByPlugin(DaskTransformPlugin):
    """Groups a Dask Dataframe by an column(s) and calcualtes aggregates"""

    name = "Group By"
    description = "Group records by column(s) and calculate aggregates"
    version = VERSION

    index_cols: set[BaseParam] = set()

    parameters = {
        "columns": PerColumnArrayParam(
            "Columns",
            TabularMultiParam(
                "Column",
                {
                    "index": BooleanParam("Index"),
                    "sum": BooleanParam("Sum"),
                    "count": BooleanParam("Count"),
                    "std": BooleanParam("Std"),
                    "var": BooleanParam("Var"),
                    "sem": BooleanParam("Sem"),
                    "min": BooleanParam("Min"),
                    "max": BooleanParam("Max"),
                },
            ),
        )
    }

    def update(self):
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        for p in self.parameters["columns"]:
            if p["index"].value and p not in self.index_cols:
                for k, pp in p.params.items():
                    pp.value = k == "index"
                self.index_cols.add(p)
            elif p in self.index_cols and any(
                pp.value != (k == "index") for k, pp in p.params.items()
            ):
                p["index"].value = False
                self.index_cols.discard(p)
        return True

    def run_dask(self, df: pd.DataFrame | dd.DataFrame, logger) -> dd.DataFrame:
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))

        index_cols = [col for col, col_param in column_parameters if col_param["index"].value]
        agg_ops = dict(
            (col, [k for k, pp in col_param.params.items() if k != "index" and pp.value])
            for col, col_param in column_parameters
            if col not in index_cols
        )
        return df.groupby(index_cols or df.index).agg(agg_ops)
