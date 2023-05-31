import pandas as pd
from pandas.api.types import is_numeric_dtype

from countess import VERSION
from countess.core.parameters import (
    BaseParam,
    BooleanParam,
    PerColumnArrayParam,
    TabularMultiParam,
    TextParam,
)
from countess.core.plugins import PandasTransformPlugin


def _column_renamer(col):
    if isinstance(col, tuple):
        if col[-1] == "first":
            return "__".join(col[:-1])
        else:
            return "__".join(col)
    return col


class GroupByPlugin(PandasTransformPlugin):
    """Groups a Pandas Dataframe by an arbitrary column and rolls up rows"""

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
                },
            ),
        ),
        "join": BooleanParam("Join Back?"),
    }

    def update(self):
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        for p in self.parameters["columns"]:
            if p["index"].value and p not in self.index_cols:
                for k, pp in p.params.items():
                    if k not in ("index", "count"):
                        pp.value = False
                self.index_cols.add(p)
            elif p in self.index_cols and any(
                pp.value for k, pp in p.params.items() if k not in ("index", "count")
            ):
                p["index"].value = False
                self.index_cols.discard(p)
        return True

    def prepare_df(self, df: pd.DataFrame, logger):
        super().prepare_df(df, logger)
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        for num, dtype in enumerate(df.dtypes):
            col_param = self.parameters["columns"][num]
            is_numeric_col = is_numeric_dtype(dtype)
            for key, param in col_param.items():
                if key in ("sum", "mean"):
                    param.hide = not is_numeric_col
                    if not is_numeric_col:
                        param.value = False

    def run_df(self, df: pd.DataFrame, logger) -> pd.DataFrame:
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))

        index_cols = [col for col, col_param in column_parameters if col_param["index"].value]
        agg_ops = dict(
            (
                col,
                [k for k, pp in col_param.params.items() if pp.value and k != "index"],
            )
            for col, col_param in column_parameters
            if any(pp.value for k, pp in col_param.params.items() if k != "index")
        )

        try:
            if agg_ops:
                dfo = df.groupby(index_cols or df.index).agg(agg_ops)
                dfo.columns = [_column_renamer(col) for col in dfo.columns.values]  # type: ignore
            else:
                # defaults to just a 'count' column.
                dfo = df.assign(count=1).groupby(index_cols or df.index).count()
        except ValueError as exc:
            logger.exception(exc)
            return pd.DataFrame()

        if self.parameters["join"].value:
            return df.merge(dfo, how="left", left_on=index_cols, right_on=index_cols)
        else:
            return dfo


class GroupByExprPlugin(PandasTransformPlugin):
    """Groups a Dask Dataframe by an column(s) and calcualtes aggregates"""

    name = "Group By Expr"
    description = "Group records by column(s) and apply expression"
    version = VERSION

    parameters = {
        "groupby": PerColumnArrayParam("Group By", BooleanParam("Index")),
        "expr": TextParam("Expression"),
    }

    # XXX not very useful in its current form.
    # probably better off making this part of Expression
    # plugin, or something.

    def run_df(self, df: pd.DataFrame, logger) -> pd.DataFrame:
        assert isinstance(self.parameters["groupby"], PerColumnArrayParam)
        cols = [
            col for col, param in zip(self.input_columns, self.parameters["groupby"]) if param.value
        ]
        expr = self.parameters["expr"].value

        if not cols:
            return df

        return df.reset_index().groupby(cols).agg(expr.strip())
