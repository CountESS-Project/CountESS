import logging
from typing import Iterable, Optional, Mapping

from countess import VERSION
from countess.core.parameters import BooleanParam, MultiColumnChoiceParam, NumericMultiColumnChoiceParam
from countess.core.plugins import DuckdbSqlPlugin
from countess.utils.duckdb import duckdb_escape_identifier

logger = logging.getLogger(__name__)


class FrequencyPlugin(DuckdbSqlPlugin):
    name = "Frequency Calculator"
    description = "Calculate frequencies from counts"
    version = VERSION

    columns = NumericMultiColumnChoiceParam("Count Columns")
    group_cols = MultiColumnChoiceParam("Group By")
    sigma = BooleanParam("Output Sigmas?")

    def set_column_choices(self, choices: Mapping[str, bool]):

        self.columns.set_column_choices(choices)
        count_cols = self.columns.get_values()
        if not count_cols:
            count_cols = set( c for c in choices if c.startswith('count') )
            self.columns.set_values(count_cols)

        self.group_cols.set_column_choices({c: n for c, n in choices.items() if c not in count_cols})

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:

        if not self.columns.get_values():
            return None

        columns = self.columns.get_values()
        # whatever -> freq_whatever; count -> freq_count; count_N -> freq_N; etc
        suffixes = [ cc.removeprefix("count_") for cc in columns ]
        count_cols = [ duckdb_escape_identifier(cc) for cc in columns ]
        freq_cols = [ duckdb_escape_identifier("freq_" + s) for s in suffixes ]
        sigma_cols = [ duckdb_escape_identifier("sigma_" + s) for s in suffixes ]

        group_cols = [
            duckdb_escape_identifier(gc)
            for gc in self.group_cols.get_values()
            if gc not in columns
        ]

        sums = ",".join(f"sum({cc}) as S{n}" for n, cc in enumerate(count_cols))

        outputs = ",".join(
            f"CASE WHEN Y.S{n} > 0 THEN X.{cc} / Y.S{n} ELSE 0 END AS {fc}"
            for n, (cc, fc) in enumerate(zip(count_cols, freq_cols))
        )
        if self.sigma.value:
            outputs += "," + ",".join(
                f"SQRT({cc}+0.5)/Y.S{n} AS {sc}"
                for n, (cc, sc) in enumerate(zip(count_cols, sigma_cols))
            )

        if group_cols:
            group_by = ','.join(group_cols)
            return f"""
                SELECT X.*, {outputs}
                from {table_name} X JOIN (
                    SELECT {group_by}, {sums}
                    FROM {table_name}
                    GROUP BY {group_by}
                ) Y USING ({group_by})
            """
        else:
            return f"""
                SELECT X.*, {outputs}
                from {table_name} X CROSS JOIN (
                    SELECT {sums}
                    FROM {table_name}
                ) Y
            """
