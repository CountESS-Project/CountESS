import numpy as np
import pandas as pd

from countess.plugins.hgvs_parser import HgvsParserPlugin

df1 = pd.DataFrame(
    [{"hgvs": "NC_000017.11:g.[43124022G>C;43124175C>T;43124111A>G]", "guides": "43124022G>C;43124111A>G"}]
)


def test_hgvs_parser():
    plugin = HgvsParserPlugin()
    plugin.set_parameter("column", "hgvs")
    plugin.set_parameter("guides_col", "guides")

    df = plugin.process_dataframe(df1)

    assert df["var_1"].iloc[0] == "43124175C>T"
    assert df["guide_1"].iloc[0] == True
    assert df["guide_2"].iloc[0] == True


def test_hgvs_parser_guides_str():
    plugin = HgvsParserPlugin()
    plugin.set_parameter("column", "hgvs")
    plugin.set_parameter("guides_str", "43124022G>C;43124111A>G")

    df = plugin.process_dataframe(df1)

    assert df["var_1"].iloc[0] == "43124175C>T"
    assert df["guide_1"].iloc[0] == True
    assert df["guide_2"].iloc[0] == True


def test_hgvs_parser_split():
    plugin = HgvsParserPlugin()
    plugin.set_parameter("column", "hgvs")
    plugin.set_parameter("guides_col", "guides")
    plugin.set_parameter("split", True)

    df = plugin.process_dataframe(df1)

    assert df["loc_1"].iloc[0] == "43124175"
    assert df["var_1"].iloc[0] == "C>T"
    assert df["guide_1"].iloc[0] == True
    assert df["guide_2"].iloc[0] == True


def test_hgvs_parser_multi():
    plugin = HgvsParserPlugin()
    plugin.set_parameter("column", "hgvs")
    plugin.set_parameter("guides_str", "43124022G>C")
    plugin.set_parameter("multi", True)
    plugin.set_parameter("max_var", 2)

    df = plugin.process_dataframe(df1)

    assert df["var"].iloc[0] == "43124175C>T"
    assert df["var"].iloc[1] == "43124111A>G"


def test_hgvs_parser_split_and_multi():
    plugin = HgvsParserPlugin()
    plugin.set_parameter("column", "hgvs")
    plugin.set_parameter("guides_str", "43124022G>C")
    plugin.set_parameter("split", True)
    plugin.set_parameter("multi", True)
    plugin.set_parameter("max_var", 2)

    df = plugin.process_dataframe(df1)

    assert df["var"].iloc[0] == "C>T"
    assert df["var"].iloc[1] == "A>G"
    assert df["loc"].iloc[0] == "43124175"
    assert df["loc"].iloc[1] == "43124111"


df2 = pd.DataFrame([{"fnords": "whatever"}, {"hgvs": None}, {"hgvs": "g.="}, {"hgvs": "g.[1A>T;2G>C;3C>T;4A>T;5A>T]"}])


def test_hgvs_parser_bad():
    plugin = HgvsParserPlugin()
    plugin.set_parameter("column", "hgvs")

    df = plugin.process_dataframe(df2)

    print(df)
    assert np.isnan(df["var_1"].iloc[0])
    assert np.isnan(df["var_1"].iloc[1])
    assert df["var_1"].iloc[2] == "g.="
    assert np.isnan(df["var_1"].iloc[3])
