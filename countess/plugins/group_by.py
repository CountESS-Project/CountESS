import dask.dataframe as dd
import pandas as pd

from countess import VERSION
from countess.core.parameters import BaseParam, BooleanParam, PerColumnArrayParam, TabularMultiParam, TextParam
from countess.core.plugins import DaskTransformPlugin
from countess.utils.dask import empty_dask_dataframe


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
                    "count": BooleanParam("Count"),
                    "min": BooleanParam("Min"),
                    "max": BooleanParam("Max"),
                    "sum": BooleanParam("Sum"),
                    "mean": BooleanParam("Mean"),
                    "std": BooleanParam("Std"),
                    "var": BooleanParam("Var"),
                    "sem": BooleanParam("Sem"),
                },
            ),
        ),
        "expr": TextParam("Expression"),
        "join": BooleanParam("Join Back?"),
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

    def run_dask(self, df: pd.DataFrame | dd.DataFrame, logger) -> pd.DataFrame | dd.DataFrame:
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))

        index_cols = [col for col, col_param in column_parameters if col_param["index"].value]
        agg_ops = dict(
            (col, [k for k, pp in col_param.params.items() if k != "index" and pp.value])
            for col, col_param in column_parameters
            if col not in index_cols
        )

        try:
            dfo = df.groupby(index_cols or df.index).agg(agg_ops)
            dfo.columns = [
                "__".join(col) if isinstance(col, tuple) else col for col in dfo.columns.values
            ]
        except ValueError as exc:
            logger.exception(exc)
            return empty_dask_dataframe()

        if self.parameters["join"].value:
            return df.merge(dfo, how="left", left_on=index_cols, right_on=index_cols)
        else:
            return dfo


class GroupByExprPlugin(DaskTransformPlugin):
    """Groups a Dask Dataframe by an column(s) and calcualtes aggregates"""

    name = "Group By Expr"
    description = "Group records by column(s) and apply expression"
    version = VERSION

    parameters = {
        "groupby": PerColumnArrayParam("Group By", BooleanParam("Index")),
        "expr": TextParam("Expression"),
    }

    def run_dask(self, df: pd.DataFrame | dd.DataFrame, logger) -> pd.DataFrame | dd.DataFrame:
        index_cols = [col_name for col_name, col_param in zip(df.columns, self.parameters["groupby"].params) if col_param.value]

        expr = self.parameters['expr'].value

        try:
            dfg = df.groupby(index_cols or df.index)
            return dfg.apply(lambda df: df.eval(expr))
        except ValueError as exc:
            logger.exception(exc)
            return empty_dask_dataframe()
