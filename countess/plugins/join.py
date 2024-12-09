import logging
from typing import Optional, Dict, List

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import ArrayParam, BooleanParam, ColumnOrIndexChoiceParam, MultiParam
from countess.core.plugins import DuckdbPlugin, duckdb_concatenate
from countess.utils.duckdb import duckdb_source_to_view, duckdb_source_to_table

logger = logging.getLogger(__name__)


def _join_how(left_required: bool, right_required: bool) -> str:
    if left_required:
        return "inner" if right_required else "left"
    else:
        return "right" if right_required else "outer"

class JoinPlugin(DuckdbPlugin):
    """Joins DuckDB tables"""

    name = "Join"
    description = "Joins two Pandas Dataframes by indexes or columns"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#join"

    class InputMultiParam(MultiParam):
        join_on = ColumnOrIndexChoiceParam("Join On")
        required = BooleanParam("Required", True)
        drop = BooleanParam("Drop Column", False)

    inputs = ArrayParam("Inputs", InputMultiParam("Input"), min_size=2, max_size=2, read_only=True)

    join_params = None
    input_columns_1: Optional[Dict] = None
    input_columns_2: Optional[Dict] = None

    def execute_multi(self, ddbc: DuckDBPyConnection, sources: List[DuckDBPyRelation]) -> Optional[DuckDBPyRelation]:

        # Can't join relations in different connections, so give them temporary views
        # in our connection.
        views = [
            duckdb_source_to_view(ddbc, source)
            for source in sources
        ]

        result = duckdb_source_to_table(ddbc, duckdb_concatenate(views))

        for view in views:
            ddbc.sql(f"drop view if exists {view.alias}")

        return result
