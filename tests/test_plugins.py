import importlib.metadata
from unittest.mock import patch

import pandas as pd
import pytest

from countess.core.parameters import ColumnChoiceParam, StringParam
from countess.core.plugins import (
    FileInputPlugin,
    PandasConcatProcessPlugin,
    PandasProductPlugin,
    PandasTransformDictToDictPlugin,
    PandasTransformRowToDictPlugin,
    PandasTransformSingleToSinglePlugin,
    get_plugin_classes,
)

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


class TPCPP(PandasConcatProcessPlugin):
    version = "0"

    def process_dataframe(self, dataframe):
        return dataframe


def test_concat():
    df1 = pd.DataFrame([{"a": 1}])
    df2 = pd.DataFrame([{"a": 2}])
    df3 = pd.DataFrame([{"a": 4}])

    pcpp = TPCPP()
    pcpp.prepare(["a"])
    pcpp.process(df1, "a")
    pcpp.process(df2, "a")
    pcpp.process(df3, "a")

    dfs = list(pcpp.finalize())
    assert len(dfs) == 1
    assert all(dfs[0]["a"] == [1, 2, 4])


class TPTSTSP(PandasTransformSingleToSinglePlugin):
    version = "0"
    column = ColumnChoiceParam("Column", "a")
    output = StringParam("Output", "c")

    def process_value(self, value):
        return value * 3 + 1 if value else None


def test_transform_sts():
    thing = TPTSTSP()
    dfi = pd.DataFrame([[1, 4], [2, 5], [3, 6]], columns=["a", "b"])

    dfo = thing.process_dataframe(dfi)
    assert all(dfo["c"] == [4, 7, 10])

    dfo = thing.process_dataframe(dfi.set_index("a"))
    assert all(dfo["c"] == [4, 7, 10])

    dfo = thing.process_dataframe(dfi.set_index(["a", "b"]))
    assert all(dfo["c"] == [4, 7, 10])

    thing.column = "d"
    dfo = thing.process_dataframe(dfi)
    assert list(dfo["c"]) == [None, None, None]

    dfi = pd.DataFrame([[1, 4], [1, 5], [1, 6]], columns=["i", "d"]).set_index("i")
    dfo = thing.process_dataframe(dfi).reset_index()

    assert list(dfo["i"]) == [1, 1, 1]
    assert list(dfo["d"]) == [4, 5, 6]
    assert list(dfo["c"]) == [13, 16, 19]


class TPTRTDP(PandasTransformRowToDictPlugin):
    version = "0"

    def process_row(self, row):
        return {"c": row["a"] * 3 + 1}


def test_transform_rtd():
    thing = TPTRTDP()
    dfi = pd.DataFrame([[1, 4], [2, 5], [3, 6]], columns=["a", "b"])
    dfo = thing.process_dataframe(dfi)
    assert all(dfo["c"] == [4, 7, 10])


class TPTDTDP(PandasTransformDictToDictPlugin):
    version = "0"

    def process_dict(self, data):
        return {"c": data["a"] * 3 + 1}


def test_transform_dtd():
    thing = TPTDTDP()
    dfi = pd.DataFrame([[1, 4], [2, 5], [3, 6]], columns=["a", "b"])
    dfo = thing.process_dataframe(dfi)
    assert all(dfo["c"] == [4, 7, 10])

    dfo = thing.process_dataframe(dfi.set_index("a"))
    assert all(dfo["c"] == [4, 7, 10])

    dfo = thing.process_dataframe(dfi.set_index(["a", "b"]))
    assert all(dfo["c"] == [4, 7, 10])
