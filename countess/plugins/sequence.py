import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.parameters import BooleanParam, ColumnChoiceParam, IntegerParam, StringParam
from countess.core.plugins import PandasTransformPlugin
from countess.utils.variant import invert_dna_sequence


class SequencePlugin(PandasTransformPlugin):
    """Manipulate DNA sequences"""

    name = "Sequence Tool"
    description = "Manipulate DNA Sequences"
    version = VERSION
    link = "https://countess-project.github.io/CountESS/plugins/#sequence"

    parameters = {
        "column": ColumnChoiceParam("Input Column"),
        "invert": BooleanParam("Invert", False),
        "offset": IntegerParam("Offset", 0),
        "start": StringParam("Start at ...", ""),
        "stop": StringParam("Stop at ...", ""),
        "length": IntegerParam("Max Length", 150),
        "output": StringParam("Output Column", "sequence"),
    }

    def run_df(self, df: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        def _process(seq):
            if self.parameters["invert"].value:
                seq = invert_dna_sequence(seq)
            if self.parameters["offset"].value:
                offset = self.parameters["offset"].value
                seq = seq[offset:]
            if self.parameters["start"].value:
                offset = seq.find(self.parameters["start"].value)
                if offset >= 0:
                    seq = seq[offset:]
                else:
                    return None
            if self.parameters["stop"].value:
                offset = seq.find(self.parameters["stop"].value)
                if offset >= 0:
                    seq = seq[0 : offset + len(self.parameters["stop"].value)]
            if self.parameters["length"].value:
                seq = seq[0 : self.parameters["length"].value]
            return seq

        column_name = self.parameters["column"].value
        output_column_name = self.parameters["output"].value

        if column_name in df.columns:
            column = df[column_name]
        else:
            column = df.index.to_frame()[column_name]

        return df.assign(**{output_column_name: column.apply(_process)})
