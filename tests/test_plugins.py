import importlib.metadata
from unittest.mock import patch

import pandas as pd
import pytest

import countess
from countess.core.plugins import BasePlugin, FileInputPlugin, PandasProductPlugin, get_plugin_classes

empty_entry_points_dict = {"countess_plugins": []}

invalid_entry_points_dict = {
    "countess_plugins": [importlib.metadata.EntryPoint(name="test", value="mockplugin", group="countess_plugins")]
}


class NoParentPlugin:
    pass


noparent_entry_points_dict = {
    "countess_plugins": [importlib.metadata.EntryPoint(name="test", value="NoParentPlugin", group="countess_plugins")]
}


def test_get_plugin_classes_invalid(caplog):
    with patch("importlib.metadata.entry_points", lambda: invalid_entry_points_dict):
        get_plugin_classes()
        assert "could not be loaded" in caplog.text


def test_get_plugin_classes_wrongparent(caplog):
    with patch("importlib.metadata.entry_points", lambda: noparent_entry_points_dict):
        with patch("importlib.metadata.EntryPoint.load", lambda x: NoParentPlugin):
            get_plugin_classes()
            assert "not a valid CountESS plugin" in caplog.text


class PPP(PandasProductPlugin):
    version = "0"

    def process_dataframes(self, dataframe1, dataframe2):
        return dataframe1 + dataframe2


def test_product_plugin():
    ppp = PPP()

    df1 = pd.DataFrame([{"a": 1}])
    df2 = pd.DataFrame([{"a": 2}])
    df3 = pd.DataFrame([{"a": 4}])
    df4 = pd.DataFrame([{"a": 8}])
    df5 = pd.DataFrame([{"a": 16}])

    ppp.prepare(["source1", "source2"])

    dfs = list(ppp.process(df1, "source1"))
    assert len(dfs) == 0

    dfs = list(ppp.process(df2, "source1"))
    assert len(dfs) == 0

    dfs = list(ppp.process(df3, "source2"))
    assert len(dfs) == 2
    assert dfs[0]["a"][0] == 5
    assert dfs[1]["a"][0] == 6

    dfs = list(ppp.process(df4, "source1"))
    assert len(dfs) == 1
    assert dfs[0]["a"][0] == 12

    dfs = list(ppp.finished("source1"))
    assert len(dfs) == 0

    dfs = list(ppp.process(df5, "source2"))
    assert len(dfs) == 3
    assert dfs[0]["a"][0] == 17
    assert dfs[1]["a"][0] == 18
    assert dfs[2]["a"][0] == 24

    dfs = list(ppp.finished("source2"))
    assert len(dfs) == 0


def test_product_plugin_sources():
    with pytest.raises(ValueError):
        ppp = PPP()
        ppp.prepare(["source1"])

    with pytest.raises(ValueError):
        ppp = PPP()
        ppp.prepare(["source1", "source2", "source3"])

    with pytest.raises(ValueError):
        ppp = PPP()
        ppp.prepare(["source1", "source2"])
        list(ppp.process(pd.DataFrame(), "source3"))

    with pytest.raises(ValueError):
        ppp = PPP()
        ppp.prepare(["source1", "source2"])
        list(ppp.finished("source3"))


class FIP(FileInputPlugin):
    version = "0"

    def num_files(self):
        return 3

    def load_file(self, file_number, row_limit):
        if row_limit is None:
            row_limit = 1000000
        return [f"hello{file_number}"] * row_limit


def test_fileinput(caplog):
    caplog.set_level("INFO")
    fip = FIP("fip")

    fip.prepare([], 1000)
    data = list(fip.finalize())

    assert len(data) >= 999
    assert sorted(set(data)) == ["hello0", "hello1", "hello2"]

    assert "100%" in caplog.text
