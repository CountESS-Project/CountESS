from itertools import islice
from typing import Iterable, Optional

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, StringCharacterSetParam
from countess.core.plugins import PandasInputPlugin


def mutagenize(
    sequence: str, mutate: bool, delete: bool, insert: bool
) -> Iterable[tuple[str, int, Optional[str], Optional[str]]]:
    # XXX it'd be faster, but less neat, to include logic for duplicate
    # removal here instead of producing duplicates and then removing them
    # later.
    for n, b1 in enumerate(sequence):
        for b2 in "ACGT":
            if mutate and b1 != b2:
                yield sequence[0:n] + b2 + sequence[n + 1 :], n + 1, b1, b2
            if insert:
                yield sequence[0:n] + b2 + sequence[n:], n + 1, None, b2
        if delete:
            yield sequence[0:n] + sequence[n + 1 :], n + 1, b1, None
    if insert:
        ll = len(sequence)
        for b2 in "ACGT":
            yield sequence + b2, ll, None, b2


class MutagenizePlugin(PandasInputPlugin):
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

    def num_files(self):
        return 1

    def load_file(self, file_number: int, logger: Logger, row_limit: Optional[int] = None) -> Iterable[pd.DataFrame]:
        assert file_number == 0

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
            columns=["sequence", "position", "reference", "variation"],
        )
        if self.parameters["remove"].value:
            df = df.groupby(["sequence"]).agg({"sequence": "count"}).rename(columns={"sequence": "count"})
        yield df
