import logging
import re
from typing import Any, Optional

from countess import VERSION
from countess.core.parameters import (
    ArrayParam,
    BooleanParam,
    ColumnChoiceParam,
    DataTypeChoiceParam,
    MultiParam,
    StringParam,
)
from countess.core.plugins import DuckdbThreadedTransformPlugin

logger = logging.getLogger(__name__)


class OutputColumnsMultiParam(MultiParam):
    name = StringParam("Column Name")
    datatype = DataTypeChoiceParam("Column Type", "STRING")


class RegexToolPlugin(DuckdbThreadedTransformPlugin):
    name = "Regex Tool"
    description = "Apply regular expressions to a column to make new column(s)"
    link = "https://countess-project.github.io/CountESS/included-plugins/#regex-tool"
    version = VERSION

    column = ColumnChoiceParam("Input Column")
    regex = StringParam("Regular Expression", ".*")
    output = ArrayParam("Output Columns", OutputColumnsMultiParam("Col"))
    drop_column = BooleanParam("Drop Column", False)
    drop_unmatch = BooleanParam("Drop Unmatched Rows", False)

    compiled_re = None

    def prepare(self, *a) -> None:
        super().prepare(*a)
        self.compiled_re = re.compile(self.regex.value)

    def add_fields(self):
        return {op.name.value: op.datatype.get_selected_type() for op in self.output}

    def remove_fields(self, field_names):
        if self.drop_column:
            return [self.column.value]
        else:
            return []

    def transform(self, data: dict[str, Any]) -> Optional[dict[str, Any]]:
        assert self.compiled_re is not None
        value = data[self.column.value]
        if value is not None:
            try:
                if match := self.compiled_re.match(str(value)):
                    data.update(
                        {op.name.value: op.datatype.cast_value(val) for op, val in zip(self.output, match.groups())}
                    )
                    return data
                else:
                    logger.info("%s didn't match", repr(value))
            except (TypeError, ValueError) as exc:
                logger.warning("Exception", exc_info=exc)

        if self.drop_unmatch:
            return None
        else:
            return data
