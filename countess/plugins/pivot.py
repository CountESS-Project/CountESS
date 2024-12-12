import functools
import logging
from itertools import product
from typing import Dict, Optional

import numpy as np
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import ChoiceParam, PerColumnArrayParam
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_escape_identifier

logger = logging.getLogger(__name__)


def _product(iterable):
    return functools.reduce(lambda x, y: x * y, iterable, 1)


class PivotPlugin(DuckdbSimplePlugin):
    """Groups a Pandas Dataframe by an arbitrary column and rolls up rows"""

    name = "Pivot Tool"
    description = "Pivots column values into columns."
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#pivot-tool"

    columns = PerColumnArrayParam("Columns", ChoiceParam("Role", "Drop", choices=["Index", "Pivot", "Expand", "Drop"]))
    aggfunc = ChoiceParam("Aggregation Function", "sum", choices=["sum", "mean", "median", "min", "max"])

    def execute(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> Optional[DuckDBPyRelation]:
        if source is None:
            return None
        index_cols = [p.label for p in self.columns if p.value == "Index" and p.label in source.columns]
        pivot_cols = [p.label for p in self.columns if p.value == "Pivot" and p.label in source.columns]
        expand_cols = [p.label for p in self.columns if p.value == "Expand" and p.label in source.columns]

        logger.debug("PivotPlugin.execute index_cols %s", index_cols)
        logger.debug("PivotPlugin.execute pivot_cols %s", pivot_cols)
        logger.debug("PivotPlugin.execute expand_cols %s", expand_cols)
        if not expand_cols or not pivot_cols:
            return source

        pivot_str = ", ".join(duckdb_escape_identifier(pc) for pc in pivot_cols)
        using_str = ", ".join(
            "%s(%s) AS C%d" % (self.aggfunc.value, duckdb_escape_identifier(ec), num)
            for num, ec in enumerate(expand_cols)
        )
        group_str = ", ".join(duckdb_escape_identifier(ic) for ic in index_cols)

        query_str = f"PIVOT {source.alias} ON {pivot_str} USING {using_str}"
        if group_str:
            query_str = f"{query_str} GROUP BY {group_str}"

        logger.debug("PivotPlugin.execute query_str %s", query_str)

        # pivot works nice but the column names aren't what I want.
        # let's rename them with a projection.

        # XXX working this list out is a fair fraction of the
        # work required to do the pivot in the first place, so
        # not sure if it'd be easier to just write our own
        # pivot function

        pivot_values = list(
            product(
                *[
                    sorted(r[0] for r in source.project(duckdb_escape_identifier(pc)).distinct().fetchall())
                    for pc in pivot_cols
                ]
            )
        )

        logger.debug("PivotPlugin.execute pivot_values %s", pivot_values)

        # XXX https://github.com/duckdb/duckdb/issues/15293
        # this corrects column names if there are duplicates.
        # our version is still possible to have duplicates, just
        # less likely.
        names = [ [pv, "_".join(pv), 0] for pv in pivot_values ]
        for n1 in range(1, len(names)):
            for n2 in range(0, n1):
                if names[n1][1] == names[n2][1]:
                    names[n1][2] += 1

        project_str = ", ".join(
            duckdb_escape_identifier(
                f"{pc}_C{num}" +
                ("_%d" % xn if xn > 0 else "")
            ) + " AS "
            + duckdb_escape_identifier("__".join([ec] + ["%s_%s" % (pc, v) for pc, v in zip(pivot_cols, pv)]))
            for num, ec in enumerate(expand_cols)
            for pv, pc, xn in names
        )
        logger.debug("PivotPlugin.execute project_str %s", project_str)
        return ddbc.sql(query_str).project(project_str)
