from itertools import islice
from typing import Any, Optional

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, StringCharacterSetParam
from countess.core.plugins import PandasBasePlugin


def mutagenize(sequence: str, mutate: bool, delete: bool, insert: bool):
    # XXX it'd be faster, but less neat, to include logic for duplicate
    # removal here instead of producing duplicates and then removing them
    # later.
    for n, b1 in enumerate(sequence):
        for b2 in "ACGT":
            if mutate and b1 != b2:
                yield sequence[0:n] + b2 + sequence[n + 1 :]
            if insert:
                yield sequence[0:n] + b2 + sequence[n:]
        if delete:
            yield sequence[0:n] + sequence[n + 1 :]
    if insert:
        for b2 in "ACGT":
            yield sequence + b2


class MutagenizePlugin(PandasBasePlugin):
    """Mutagenize"""

    name = "Mutagenize"
    description = "Provides all mutations of a sequence"
    link = "https://countess-project.github.io/CountESS/plugins/#mutagenize"
    version = VERSION

    character_set = set(("A", "C", "G", "T", "N"))

    parameters = {
        "sequence": StringCharacterSetParam("Sequence", "", character_set=character_set),
        "mutate": BooleanParam("All Single Mutations?", True),
        "delete": BooleanParam("All Single Deletes?", False),
        "insert": BooleanParam("All Single Inserts?", False),
        "remove": BooleanParam("Remove Duplicates?", False),
    }

    def prepare(self, data: Any, logger: Logger) -> bool:
        if data is not None:
            logger.error("Mutagenize doesn't take inputs")
            return False
        return True

    def run(self, data: Any, logger: Logger, row_limit: Optional[int] = None):
        df = pd.DataFrame(
            islice(
                mutagenize(
                    self.parameters["sequence"].value,
                    self.parameters["mutate"].value,
                    self.parameters["delete"].value,
                    self.parameters["insert"].value,
                ),
                0,
                row_limit,
            ),
            columns=["sequence"],
        )
        if self.parameters["remove"].value:
            df = df.groupby("sequence").agg("first")
        return df
