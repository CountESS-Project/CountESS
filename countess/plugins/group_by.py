import pandas as pd
from pandas.api.types import is_numeric_dtype

from countess import VERSION
from countess.core.parameters import (
    BaseParam,
    BooleanParam,
    ColumnOrIndexChoiceParam,
    PerColumnArrayParam,
    TabularMultiParam,
    TextParam,
)
from countess.core.plugins import PandasTransformPlugin


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
                    pp.value = k == "index"
                self.index_cols.add(p)
            elif p in self.index_cols and any(
                pp.value != (k == "index") for k, pp in p.params.items()
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
            (col, [k for k, pp in col_param.params.items() if k != "index" and pp.value])
            for col, col_param in column_parameters
            if col not in index_cols
        )

        try:
            dfo = df.groupby(index_cols or df.index).agg(agg_ops)
            dfo.columns = [
                "__".join(col) if isinstance(col, tuple) else col for col in dfo.columns.values
            ]  # type: ignore
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
