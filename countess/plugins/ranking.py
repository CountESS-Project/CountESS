import logging
from typing import Optional, Iterable

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    ColumnChoiceParam,
    NumericColumnChoiceParam,
)
from countess.core.plugins import DuckdbSqlPlugin
from countess.utils.duckdb import duckdb_escape_identifier

logger = logging.getLogger(__name__)


class RankingPlugin(DuckdbSqlPlugin):
    name = "Ranking"
    description = "Rank rows by column value(s)"
    version = VERSION

    order_by = ArrayParam("Order By", NumericColumnChoiceParam("Column"))
    partition = ArrayParam("Partition By", ColumnChoiceParam("Column"))

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:

        order_by = ', '.join(
            duckdb_escape_identifier(p.value)
            for p in self.order_by.params
        )
        if order_by:
            order_by = f"ORDER BY {order_by}"

        partition = ', '.join(
            duckdb_escape_identifier(p.value)
            for p in self.partition.params
        )
        if partition:
            partition = f"PARTITION BY {partition}"

        return f'SELECT *, percent_rank({order_by}) over ({partition}) as rank FROM {table_name}'
