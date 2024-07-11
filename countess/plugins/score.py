from typing import Optional

import pandas as pd

from countess import VERSION
from countess.core.logger import Logger
from countess.core.plugins import PandasConcatProcessPlugin
from countess.core.parameters import ChoiceParam, ColumnChoiceParam, ColumnGroupChoiceParam, ColumnGroupOrNoneChoiceParam, StringParam

class ScoringPlugin(PandasConcatProcessPlugin):

    name = "Scoring"
    description = "Score variants using counts or frequencies"
    version = VERSION

    parameters = {
        "variant": ColumnChoiceParam("Variant Column"),
        "replicate": ColumnChoiceParam("Replicate Column"),
        "columns": ColumnGroupChoiceParam("Input Columns"),
        "inputtype": ChoiceParam("Input Type", "counts", ["counts", "fractions"]),
        "xaxis": ColumnGroupOrNoneChoiceParam("X Axis Columns"),
        "output": StringParam("Score Column", "score")
    }

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> Optional[pd.DataFrame]:
        assert isinstance(self.parameters["variant"], ColumnChoiceParam)
        assert isinstance(self.parameters["replicate"], ColumnChoiceParam)
        assert isinstance(self.parameters["columns"], ColumnGroupChoiceParam)

        variant = self.parameters["variant"].value
        replicate = self.parameters["replicate"].value
        columns = self.parameters["columns"].get_column_names(dataframe)
        output = self.parameters["output"].value

        if variant and replicate:
            return dataframe.groupby(by=replicate).agg({ c: 'sum' for c in columns}).assign(**{output: 107})
