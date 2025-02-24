import numpy as np
import pandas as pd
import duckdb

from countess.plugins.variant import VariantPlugin


def test_variant_ref_value():
    plugin = VariantPlugin()
    plugin.set_parameter("column", "seq")
    plugin.set_parameter("reference", "AGAAGTAGAGG")
    plugin.set_parameter("variant.seq_type", "g")
    plugin.set_parameter("variant.output", "out")
    plugin.set_column_choices(["seq"])

    assert plugin.transform({"seq": "TGAAGTAGAGG"})["out"] == "g.1A>T"
    assert plugin.transform({"seq": "AGAAGTTGTGG"})["out"] == "g.[7A>T;9A>T]"
    assert plugin.transform({"seq": "ATAAGAAGAGG"})["out"] == "g.[2G>T;6T>A]"


def test_variant_ref_column():
    plugin = VariantPlugin()
    plugin.set_parameter("column", "seq")
    plugin.set_parameter("reference", "— ref")
    plugin.set_parameter("variant.seq_type", "g")
    plugin.set_parameter("variant.output", "out")
    plugin.set_column_choices(["seq", "ref"])

    assert plugin.transform({"ref": "TACACACAG", "seq": "TACAGACAG"})["out"] == "g.5C>G"
    assert plugin.transform({"ref": "ATGGTTGGTTC", "seq": "ATGGTTGGTGGTTCG"})["out"] == "g.[7_9dup;11_12insG]"


def test_variant_ref_offset():
    plugin = VariantPlugin()
    plugin.set_parameter("column", "seq")
    plugin.set_parameter("reference", "AGAAGTAGAGG")
    plugin.set_parameter("variant.offset", "— offs")
    plugin.set_parameter("variant.seq_type", "g")
    plugin.set_parameter("variant.output", "out")
    plugin.set_column_choices(["seq", "offs"])

    assert plugin.transform({"seq": "TGAAGTAGAGG", "offs": "0"})["out"] =="g.1A>T"
    assert plugin.transform({"seq": "AGAAGTTGTGG", "offs": "10"})["out"] == "g.[17A>T;19A>T]"
    assert plugin.transform({"seq": "ATAAGAAGAGG", "offs": "100"})["out"] == "g.[102G>T;106T>A]"

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

    plugin = VariantPlugin()
    plugin.set_parameter("column", "seq")
    plugin.set_parameter("reference", "AGAAGTAGAGG")
    plugin.set_parameter("variant.offset", "1000")
    plugin.set_parameter("variant.seq_type", "g")
    plugin.set_parameter("variant.minus_strand", True)
    plugin.set_parameter("variant.output", "out")
    plugin.set_column_choices(["seq", "offs"])
    
    assert plugin.transform({"seq": "TGAAGTAGAGG"})["out"] == "g.1011T>A"
    assert plugin.transform({"seq": "AGAAGTTGTGG"})["out"] == "g.[1003T>A;1005T>A]"
    assert plugin.transform({"seq": "ATAAGAAGAGG"})["out"] == "g.[1006A>T;1010C>A]"


def test_variant_too_many():
    plugin = VariantPlugin()
    plugin.set_parameter("column", "seq")
    plugin.set_parameter("reference", "AGAAGTAGAGG")
    plugin.set_parameter("variant.seq_type", "g")
    plugin.set_parameter("variant.output", "out")
    plugin.set_parameter("variant.maxlen", 2)
    plugin.set_column_choices(["seq", "offs"])

    assert plugin.transform({"seq": "TGAAGTAGAGG"})["out"] == "g.1A>T"
    assert plugin.transform({"seq": "AGAAGTTGTGG"})["out"] == "g.[7A>T;9A>T]"
    assert plugin.transform({"seq": "ATAAGAAGACG"})["out"] is None
    assert plugin.transform({"seq": "AGAATTAGAGG"})["out"] == "g.5G>T"
