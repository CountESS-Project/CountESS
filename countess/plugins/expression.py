import logging
from typing import Dict, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import TextParam
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.expression_to_sql import FUNC_OPS, LIST_OPS, Block

logger = logging.getLogger(__name__)


class ExpressionPlugin(DuckdbSimplePlugin):
    name = "Expression"
    description = "Apply simple expressions to each row"
    additional = """
        Expressions are applied to each row.  Syntax is python-like, but to concatenate strings,
        use 'concat' function.  For selection use python-like "a if b else c".  To remove a column,
        set it to constant None.   Set a variable '__filter' to False to remove rows.

        Available operators: + - * ** / and or not < <= > >= != ==
        Available constants: True, False, None, NULL
        
        Available functions:
    """ + " ".join(
        sorted(list(FUNC_OPS) + list(LIST_OPS))
    )

    version = VERSION

    code = TextParam("Expressions")
    projection: Optional[Dict[str, str]] = None
    block: Optional[Block] = None

    def prepare(self, *a) -> None:
        self.block = Block.from_string(self.code.value or "")

    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        if self.block:
            return self.block.project_and_filter(source)
        else:
            return None
