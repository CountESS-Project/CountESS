import pandas as pd
import pytest

from countess.core.logger import MultiprocessLogger
from countess.plugins.csv import LoadCsvPlugin

logger = MultiprocessLogger()


def test_load_csv():
    plugin = LoadCsvPlugin()

    plugin.set_parameter("files.0.filename", "tests/input1.csv")

    output_df = next(plugin.load_file(0, logger))

    assert list(output_df.columns) == ["thing", "count"]
    assert len(output_df) == 4
