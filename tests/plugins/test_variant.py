import pandas as pd
import pytest

from countess.core.logger import MultiprocessLogger
from countess.plugins.variant import VariantPlugin

logger = MultiprocessLogger()


def test_variant_ref_column():
    input_df = pd.DataFrame(
        [{"ref": "TACACACAG", "seq": "TACAGACAG"}, {"ref": "ATGGTTGGTTC", "seq": "ATGGTTGGTGGTTCG"}]
    )

    plugin = VariantPlugin()
    plugin.set_parameter("column", "seq")
    plugin.set_parameter("reference", "ref")
    plugin.set_parameter("output", "out")

    plugin.prepare(["test"], None)

    output_df = plugin.process_dataframe(input_df, logger)

    output = output_df.to_records()

    assert output[0]["out"] == "g.5C>G"
    assert output[1]["out"] == "g.[7_9dup;11_12insG]"
