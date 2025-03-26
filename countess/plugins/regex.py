import logging
from typing import Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ColumnChoiceParam,
    DataTypeChoiceParam,
    MultiParam,
    StringParam,
)
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal

logger = logging.getLogger(__name__)


class OutputColumnsMultiParam(MultiParam):
    name = StringParam("Column Name")
    datatype = DataTypeChoiceParam("Column Type", "STRING")


class RegexToolPlugin(DuckdbSimplePlugin):
    name = "Regex Tool"
    description = "Apply regular expressions to a column to make new column(s)"
    link = "https://countess-project.github.io/CountESS/included-plugins/#regex-tool"
    version = VERSION

    column = ColumnChoiceParam("Input Column")
    regex = StringParam("Regular Expression", ".*")
    output = ArrayParam("Output Columns", OutputColumnsMultiParam("Col"))
    drop_column = BooleanParam("Drop Column", False)
    drop_unmatch = BooleanParam("Drop Unmatched Rows", False)

    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        column_id = duckdb_escape_identifier(self.column.value)
        regexp_value = duckdb_escape_literal(self.regex.value)
        output_ids = [duckdb_escape_literal(o.name.value) for o in self.output if o.name.value]
        output_types = [
            duckdb_escape_identifier(o.name.value) + " " + o.datatype.value for o in self.output if o.name.value
        ]

        if self.drop_column:
            proj = ",".join(duckdb_escape_identifier(c) for c in source.columns if c != self.column.value)
        else:
            proj = "*"

        if output_ids:
            if proj:
                proj += ","
            proj += f"""
                unnest(try_cast(
                    regexp_extract({column_id}, {regexp_value}, [{','.join(output_ids)}])
                    as struct({','.join(output_types)})
                ))
            """

        logger.debug("RegexToolPlugin proj %s", proj)

        if self.drop_unmatch:
            filt = f"regexp_matches({column_id}, {regexp_value})"
            logger.debug("RegexToolPlugin filt %s", filt)
            return source.filter(filt).project(proj)

        else:
            return source.project(proj)
