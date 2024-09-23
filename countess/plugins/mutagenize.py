from itertools import islice, product
from typing import Iterable, Optional

import pandas as pd

from countess import VERSION
from countess.core.parameters import BooleanParam, StringCharacterSetParam
from countess.core.plugins import PandasInputPlugin


def mutagenize(
    sequence: str, mutate: bool, delete: bool, del3: bool, insert: bool, ins3: bool
) -> Iterable[tuple[str, int, Optional[str], Optional[str]]]:
    # XXX not really happy with how the args are multiplying here!
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
        if del3 and n + 3 <= len(sequence):
            yield sequence[0:n] + sequence[n + 3 :], n + 1, sequence[n : n + 3], None
        if ins3:
            for ins in product("ACGT", "ACGT", "ACGT"):
                ins_str = "".join(ins)
                yield sequence[0:n] + ins_str + sequence[n:], n + 1, None, ins_str

    ll = len(sequence) + 1
    if insert:
        for b2 in "ACGT":
            yield sequence + b2, ll, None, b2
    if ins3:
        for ins in product("ACGT", "ACGT", "ACGT"):
            ins_str = "".join(ins)
            yield sequence + ins_str, ll, None, ins_str


class MutagenizePlugin(PandasInputPlugin):
    """Mutagenize"""

    name = "Mutagenize"
    description = "Provides all mutations of a sequence"
    link = "https://countess-project.github.io/CountESS/included-plugins/#mutagenize"
    version = VERSION

    character_set = set(("A", "C", "G", "T", "N"))
    row_limit: Optional[int] = None

    sequence = StringCharacterSetParam("Sequence", "", character_set=character_set)
    mutate = BooleanParam("All Single Mutations?", True)
    delete = BooleanParam("All Single Deletes?", False)
    del3 = BooleanParam("All Triple Deletes?", False)
    insert = BooleanParam("All Single Inserts?", False)
    ins3 = BooleanParam("All Triple Inserts?", False)
    remove = BooleanParam("Remove Duplicates?", False)

    def prepare(self, sources: list[str], row_limit: Optional[int] = None):
        assert len(sources) == 0
        self.row_limit = row_limit

    def finalize(self) -> Iterable[pd.DataFrame]:
        df = pd.DataFrame(
            islice(
                mutagenize(
                    str(self.sequence),
                    bool(self.mutate),
                    bool(self.delete),
                    bool(self.del3),
                    bool(self.insert),
                    bool(self.ins3),
                ),
                0,
                self.row_limit,
            ),
            columns=["sequence", "position", "reference", "variation"],
        )
        if self.remove.value:
            df = df.groupby(["sequence"]).agg({"sequence": "count"}).rename(columns={"sequence": "count"})
        yield df
