import itertools

import numpy as np
import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import ChoiceParam, PerColumnArrayParam
from countess.core.plugins import PandasTransformPlugin

AGG_FUNCTIONS = ["first", "sum", "count", "mean"]


class PivotPlugin(PandasTransformPlugin):
    """Groups a Pandas Dataframe by an arbitrary column and rolls up rows"""

    name = "Pivot Tool"
    description = "Groups a dataframe and pivots column values into columns"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/plugins/#pivot-tool"

    parameters = {
        "columns": PerColumnArrayParam(
            "Columns", ChoiceParam("Role", choices=["Index", "Pivot", "Expand", "Drop"])
        )
    }

    def run_df(self, df: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["columns"], PerColumnArrayParam)
        column_parameters = list(zip(self.input_columns, self.parameters["columns"]))
        index_cols = [col for col, col_param in column_parameters if col_param.value == "Index"]
        pivot_cols = [col for col, col_param in column_parameters if col_param.value == "Pivot"]
        expand_cols = [col for col, col_param in column_parameters if col_param.value == "Expand"]
        drop_cols = [col for col, col_param in column_parameters if col_param.value == "Drop"]

        # XXX friendly error messages please
        assert pivot_cols

        if isinstance(df, pd.DataFrame):
            df = pd.pivot_table(
                df,
                values=expand_cols,
                index=index_cols,
                columns=pivot_cols,
                aggfunc=np.sum,
                fill_value=0,
            )
            if isinstance(df.columns, pd.MultiIndex):
                # Clean up MultiIndex names ... XXX until such time as CountESS supports them
                df.columns = [
                    "__".join([f"{cn}_{cv}" if cn else cv for cn, cv in zip(df.columns.names, cc)])
                    for cc in df.columns
                ]  # type: ignore
            return df

        # dask's built-in "dd.pivot_table" can only handle one index column
        # and one pivot column which is too limiting.  So this is a slightly
        # cheesy replacement.
        #
        # XXX this is maybe not actually necessary any more?

        # zorch the existing indexing
        df = df.reset_index().drop(columns=drop_cols)

        # `pivot_product` is every combination of every pivot column, so eg: if you're
        # pivoting on a `bin` column with values 1..4 and a `rep` column with values
        # 1..3 you'll end up with 12 elements, [ [1,1],[1,2],[1,3],[1,4],[2,1],[2,2] etc ]

        pivot_product = list(itertools.product(*[list(df[c].unique()) for c in pivot_cols]))
        # XXX friendly error messages please
        assert len(pivot_product) <= 100

        # `pivot_groups` then reattaches the labels to those values, eg:
        # [[('bin', 1), ('rep', 1)], [('bin', 1), ('rep', 2)] etc
        pivot_groups = sorted(
            [list(zip(pivot_cols, pivot_values)) for pivot_values in pivot_product]
        )

        def _column_name(col, pg):
            return "`" + col + "".join([f"__{pc}_{pv}" for pc, pv in pg]) + "`"

        def _column_condition(col, pg):
            return " and ".join([f"`{pc}` == {repr(pv)}" for pc, pv in pg])

        # XXX note that even though numexpr supports a where() function
        # https://numexpr.readthedocs.io/en/latest/user_guide.html#supported-functions
        # which would be useful here, pandas can't parse this:
        # https://stackoverflow.com/questions/65427413/pandas-eval-not-supporting-where
        # it'd be an easy patch to pandas, mind you.

        expr = "\n".join(
            [
                f"{_column_name(col, pg)} = `{col}` * ({_column_condition(col, pg)})"
                for col in expand_cols
                for pg in pivot_groups
            ]
        )

        df.eval(expr, inplace=True, engine="numexpr")

        return df.drop(columns=pivot_cols + expand_cols).groupby(index_cols).agg("sum")
