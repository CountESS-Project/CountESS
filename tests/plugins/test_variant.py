import numpy as np
import pandas as pd

from countess.plugins.variant import VariantPlugin


def test_variant_ref_value():
    input_df = pd.DataFrame([{"seq": "TGAAGTAGAGG"}, {"seq": "AGAAGTTGTGG"}, {"seq": "ATAAGAAGAGG"}])
    plugin = VariantPlugin()
    plugin.set_parameter("column", "seq")
    plugin.set_parameter("reference", "AGAAGTAGAGG")
    plugin.set_parameter("outputs.0.seq_type", "g")
    plugin.set_parameter("outputs.0.output", "out")

    plugin.prepare(["test"], None)

    output_df = plugin.process_dataframe(input_df)

    output = output_df.to_records()

    assert output[0]["out"] == "g.1A>T"
    assert output[1]["out"] == "g.[7A>T;9A>T]"
    assert output[2]["out"] == "g.[2G>T;6T>A]"


def test_variant_ref_column():
    input_df = pd.DataFrame(
        [{"ref": "TACACACAG", "seq": "TACAGACAG"}, {"ref": "ATGGTTGGTTC", "seq": "ATGGTTGGTGGTTCG"}]
    )

    plugin = VariantPlugin()
    plugin.set_parameter("column", "seq")
    plugin.set_parameter("reference", "— ref")
    plugin.set_parameter("outputs.0.seq_type", "g")
    plugin.set_parameter("outputs.0.output", "out")

    plugin.prepare(["test"], None)

    output_df = plugin.process_dataframe(input_df)

    output = output_df.to_records()

    assert output[0]["out"] == "g.5C>G"
    assert output[1]["out"] == "g.[7_9dup;11_12insG]"


def test_variant_ref_offset():
    input_df = pd.DataFrame(
        [
            {"seq": "TGAAGTAGAGG", "offs": "0"},
            {"seq": "AGAAGTTGTGG", "offs": "10"},
            {"seq": "ATAAGAAGAGG", "offs": "100"},
        ]
    )

    plugin = VariantPlugin()
    plugin.set_parameter("column", "seq")
    plugin.set_parameter("reference", "AGAAGTAGAGG")
    plugin.set_parameter("outputs.0.offset", "— offs")
    plugin.set_parameter("outputs.0.seq_type", "g")
    plugin.set_parameter("outputs.0.output", "out")

    plugin.prepare(["test"], None)

    output_df = plugin.process_dataframe(input_df)

    output = output_df.to_records()

    assert output[0]["out"] == "g.1A>T"
    assert output[1]["out"] == "g.[17A>T;19A>T]"
    assert output[2]["out"] == "g.[102G>T;106T>A]"


def test_variant_ref_offset_minus():
    """check that the reverse-complement behaviour works on the minus strand."""
    # genes on the minus strand are reverse-complemented, so what we're actually
    # comparing is the reverse-complemented sequences:
    #
    #      00000000011
    # num  12345678901
    # ref  CCTCTACTTCT
    # seq1 CCTCTACTTCA => 11T>A
    # seq2 CCACAACTTCT => 3T>A;5T>A
    # seq3 CCTCTTCTTAT => 6A>T;10C>A
    #
    # plus the offset

    input_df = pd.DataFrame(
        [
            {"seq": "TGAAGTAGAGG"},
            {"seq": "AGAAGTTGTGG"},
            {"seq": "ATAAGAAGAGG"},
        ]
    )

    plugin = VariantPlugin()
    plugin.set_parameter("column", "seq")
    plugin.set_parameter("reference", "AGAAGTAGAGG")
    plugin.set_parameter("outputs.0.offset", "1000")
    plugin.set_parameter("outputs.0.seq_type", "g-")
    plugin.set_parameter("outputs.0.output", "out")

    plugin.prepare(["test"], None)

    output_df = plugin.process_dataframe(input_df)

    output = output_df.to_records()

    assert output[0]["out"] == "g.1011T>A"
    assert output[1]["out"] == "g.[1003T>A;1005T>A]"
    assert output[2]["out"] == "g.[1006A>T;1010C>A]"


def test_variant_too_many():
    input_df = pd.DataFrame(
        [{"seq": "TGAAGTAGAGG"}, {"seq": "AGAAGTTGTGG"}, {"seq": "ATAAGAAGACG"}, {"seq": "AGAATTAGAGG"}]
    )
    plugin = VariantPlugin()
    plugin.set_parameter("column", "seq")
    plugin.set_parameter("reference", "AGAAGTAGAGG")
    plugin.set_parameter("outputs.0.seq_type", "g")
    plugin.set_parameter("outputs.0.output", "out")
    plugin.set_parameter("outputs.0.maxlen", 2)

    plugin.prepare(["test"], None)

    output_df = plugin.process_dataframe(input_df)

    output = output_df.to_records()

    assert output[0]["out"] == "g.1A>T"
    assert output[1]["out"] == "g.[7A>T;9A>T]"
    assert output[2]["out"] is np.NAN
    assert output[3]["out"] == "g.5G>T"
