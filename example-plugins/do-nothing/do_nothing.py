from typing import Iterable, Optional

from countess.core.plugins import DuckdbSqlPlugin

VERSION = "0.0.0"

class DoNothingPlugin(DuckdbSqlPlugin):

    name: str = 'Do Nothing'
    description: str = "An example CountESS plugin which does nothing at all"
    version: str = VERSION

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        return f"SELECT * FROM {table_name}"
