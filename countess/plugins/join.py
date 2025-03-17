import logging
from typing import Dict, Mapping, Optional

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
    description = "Joins by indexes or columns"
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

    def execute_multi(
        self, ddbc: DuckDBPyConnection, sources: Mapping[str, DuckDBPyRelation], row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        if len(sources) <= 1:
            return None

        while len(self.inputs) > len(sources):
            self.inputs.del_row(len(self.inputs) - 1)

        while len(self.inputs) < len(sources):
            self.inputs.add_row()

        for num, (label, table) in enumerate(sources.items()):
            logger.debug("JoinPlugin.execute_multi %d %s %s", num + 1, repr(label), table.alias)
            self.inputs[num].label = f"Input {num+1}: {label}"
            self.inputs[num].join_on.set_choices(table.columns)

        # XXX this isn't quite right for >2 tables where some
        # are required and some aren't.  I think what I need to
        # do is join all the required tables to each other and
        # then join the non-required ones on on top.
        tables = list(sources.values())
        identifiers = [duckdb_escape_identifier(input_.join_on.value) for input_ in self.inputs]
        required = [input_.required.value for input_ in self.inputs]

        select_str = ", ".join(
            [
                f"N_{num}.{duckdb_escape_identifier(input_.join_on.value)}"
                for num, (input_, table) in enumerate(zip(self.inputs, tables))
                if not input_.drop and not (num > 0 and identifiers[num] == identifiers[0])
            ]
            + [
                f"N_{num}.{duckdb_escape_identifier(column)}"
                for num, (input_, table) in enumerate(zip(self.inputs, tables))
                for column in table.columns
                if not input_.join_on == column
            ]
        )

        query = f"SELECT {select_str} FROM {tables[0].alias} AS N_0"
        for num, table in enumerate(tables[1:], 1):
            query += (
                f" {_join_how(required[0], required[num])} JOIN {table.alias} AS N_{num}"
                + f" ON N_0.{identifiers[0]} = N_{num}.{identifiers[num]}"
            )
        if row_limit is not None:
            query += f" LIMIT {row_limit}"

        logger.debug("JoinPlugin.execute_multi tables[0] %s %d", tables[0].alias, len(tables[0]))
        logger.debug("JoinPlugin.execute_multi tables[1] %s %d", tables[1].alias, len(tables[1]))
        logger.debug("JoinPlugin.execute_multi query %s", query)

        try:
            rel = ddbc.sql(query)
            logger.debug("JoinPlugin.execute_multi output %d", len(rel))
            return rel
        except duckdb.ConversionException as exc:
            logger.info(exc)
            return None
