import functools
import logging
from typing import Dict, Iterable, List, Optional

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

from countess import VERSION
from countess.core.parameters import ChoiceParam, PerColumnArrayParam
from countess.core.plugins import PandasProcessPlugin
from countess.utils.pandas import get_all_columns

logger = logging.getLogger(__name__)


def _product(iterable):
    return functools.reduce(lambda x, y: x * y, iterable, 1)


class PivotPlugin(PandasProcessPlugin):
    """Groups a Pandas Dataframe by an arbitrary column and rolls up rows"""

    name = "Pivot Tool"
    description = "Pivots column values into columns."
    additional = """Duplicate indexes on inputs may result in duplicate output rows.  If this is a problem,
    perform an additional Group By before or after pivoting."""
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#pivot-tool"

    columns = PerColumnArrayParam("Columns", ChoiceParam("Role", "Drop", choices=["Index", "Pivot", "Expand", "Drop"]))
    aggfunc = ChoiceParam("Aggregation Function", "sum", choices=["sum", "mean", "median", "min", "max"])

    input_columns: Dict[str, np.dtype] = {}

    dataframes: Optional[List[pd.DataFrame]] = None

    def prepare(self, sources: List[str], row_limit: Optional[int] = None):
        self.input_columns = {}
        self.dataframes = []

    def process(self, data: pd.DataFrame, source: str) -> Iterable[pd.DataFrame]:
        assert self.dataframes is not None
        self.input_columns.update(get_all_columns(data))

        data.reset_index(drop=data.index.names == [None], inplace=True)

        index_cols = [p.label for p in self.columns if p.value == "Index" and p.label in data.columns]
        pivot_cols = [p.label for p in self.columns if p.value == "Pivot" and p.label in data.columns]
        expand_cols = [p.label for p in self.columns if p.value == "Expand" and p.label in data.columns]

        if not index_cols:
            logger.warning("No columns to index!")

        if not expand_cols:
            logger.warning("No columns to expand!")

        if not pivot_cols:
            logger.error("No columns to pivot on!")
            return []

        for ec in expand_cols:
            if not is_numeric_dtype(data[ec]):
                logger.warning("Expanding non-numeric column %s", ec)

        n_pivot = _product(data[pc].nunique() for pc in pivot_cols) * len(expand_cols)
        if n_pivot > 200:
            pivot_cols_str = ", ".join(pivot_cols)
            logger.error("Too many pivot combinations on %s (%d)", pivot_cols_str, n_pivot)
            return []

        # `min`, `max` and `sum` are associative, so we just aggregate here and aggregate again
        # at the end and all is fine.
        # For non-associative functions like `mean` and `median`, the `tuple` function is called
        # as a first aggregator, so we end up with a series like [ 1, 2, (3, 4), 5, (6, 7, 8) ] ...
        # this will then get cleared up in the `finalize` stage by the `aggregate` method ...
        def _aggfunc():
            if self.aggfunc in ("min", "max", "sum"):
                return self.aggfunc.value
            return tuple

        df = pd.pivot_table(
            data,
            values=expand_cols,
            index=index_cols,
            columns=pivot_cols,
            fill_value=np.NAN,
            aggfunc=_aggfunc(),
        )

        if isinstance(df.columns, pd.MultiIndex):
            # Clean up MultiIndex names ... XXX until such time as CountESS supports them
            df.columns = [
                "__".join([f"{cn}_{cv}" if cn else cv for cn, cv in zip(df.columns.names, cc)]) for cc in df.columns
            ]  # type: ignore

        self.dataframes.append(df)
        return []

    def finalize(self) -> Iterable[pd.DataFrame]:
        yield from super().finalize()

        if not self.dataframes:
            return

        # for the associative functions we just aggregate again, finding the sum-of-sums etc.
        # for non-associative functions, `explode()` expands the tuples out to get the full
        # series, and then `.agg()` applies the appropriate aggregation function.
        # XXX `mean` could get streamlined by recording (sum, count) tuples at the first stage,
        # and then adding those up here and dividing (but it's a bit tricky)

        def _aggfunc():
            if self.aggfunc in ("min", "max", "sum"):
                return self.aggfunc.value
            return lambda s: s.explode().agg(self.aggfunc.value)

        try:
            dataframe = pd.concat(self.dataframes)
            columns = get_all_columns(dataframe)
            index_cols = [p.label for p in self.columns if p.value == "Index" and p.label in columns]

            if index_cols:
                dataframe = dataframe.groupby(by=index_cols, group_keys=True).agg(_aggfunc())

            yield dataframe
        except (KeyError, ValueError, TypeError) as exc:
            logger.warning("Exception", exc_info=exc)
