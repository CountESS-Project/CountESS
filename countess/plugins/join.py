import logging
from typing import Dict, Optional

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import ArrayParam, BooleanParam, ColumnChoiceParam, MultiParam
from countess.core.plugins import DuckdbPlugin
from countess.utils.duckdb import duckdb_escape_identifier

logger = logging.getLogger(__name__)


def _join_how(left_required: bool, right_required: bool) -> str:
    if left_required:
        return "INNER" if right_required else "LEFT OUTER"
    else:
        return "RIGHT OUTER" if right_required else "FULL OUTER"


class JoinPlugin(DuckdbPlugin):
    """Joins DuckDB tables"""

    name = "Join"
    description = "Joins two Pandas Dataframes by indexes or columns"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/included-plugins/#join"

    class InputMultiParam(MultiParam):
        join_on = ColumnChoiceParam("Join On")
        required = BooleanParam("Required", True)
        drop = BooleanParam("Drop Column", False)

    inputs = ArrayParam("Inputs", InputMultiParam("Input"), read_only=True)

    join_params = None
    input_columns_1: Optional[Dict] = None
    input_columns_2: Optional[Dict] = None

    def execute_multi(self, ddbc: DuckDBPyConnection,
        sources: Dict[str, DuckDBPyRelation]) -> Optional[DuckDBPyRelation]:
        # Can't join relations in different connections, so give them temporary views
        # in our connection.
        #views = [duckdb_source_to_view(ddbc, source) for source in sources]

        #result = duckdb_source_to_table(ddbc, duckdb_concatenate(views))

        if len(sources) <= 1:
            return None

        while len(self.inputs) > len(sources):
            self.inputs.del_row(len(self.inputs)-1)

        while len(self.inputs) < len(sources):
            self.inputs.add_row()

        for num, (label, table) in enumerate(sources.items()):
            self.inputs[num].label = f"Input {num+1}: {label}"
            self.inputs[num].join_on.set_choices(table.columns)

        tables = list(sources.values())
        identifiers = [
            duckdb_escape_identifier(input_.join_on.value)
            for input_ in self.inputs
        ]
        required = [ input_.required.value for input_ in self.inputs ]
        query = f"SELECT * FROM {tables[0].alias} AS N_0"
        for num, table in enumerate(tables[1:], 1):
            query += (
                f" {_join_how(required[0], required[num])} JOIN {table.alias} AS N_{num}" +
                f" ON N_0.{identifiers[0]} = N_{num}.{identifiers[num]}"
            )
        logger.debug(query)

        try:
            return ddbc.sql(query)
        except duckdb.ConversionException as exc:
            logger.info(exc)
            return None
