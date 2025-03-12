import duckdb
import pandas as pd

from countess.plugins.regex import RegexToolPlugin

ddbc = duckdb.connect()

ddbc.from_df(
    pd.DataFrame(
        [
            {"stuff": "hello"},
            {"stuff": "backwards"},
            {"stuff": "noaardvark"},
        ]
    )
).create("n_0")

source = ddbc.table("n_0")


def test_tool_1():
    plugin = RegexToolPlugin()
    plugin.set_parameter("regex", ".*?([a]+).*")
    plugin.set_parameter("output.0.name", "foo")
    plugin.prepare(ddbc, source)
    plugin.set_parameter("column", "stuff")

    out = plugin.execute(ddbc, source)

    assert len(out) == 3
    assert out.columns == ["stuff", "foo"]
    assert sorted(out.fetchall()) == [("backwards", "a"), ("hello", ""), ("noaardvark", "aa")]


def test_tool_2():
    plugin = RegexToolPlugin()
    plugin.set_parameter("regex", ".*?([a]+).*")
    plugin.set_parameter("output.0.name", "foo")
    plugin.set_parameter("drop_column", True)
    plugin.prepare(ddbc, source)
    plugin.set_parameter("column", "stuff")

    out = plugin.execute(ddbc, source)

    assert len(out) == 3
    assert out.columns == ["foo"]
    assert sorted(out.fetchall()) == [("",), ("a",), ("aa",)]


def test_tool_3():
    plugin = RegexToolPlugin()
    plugin.set_parameter("regex", ".*?([a]+).*")
    plugin.set_parameter("output.0.name", "foo")
    plugin.set_parameter("drop_unmatch", True)
    plugin.prepare(ddbc, source)
    plugin.set_parameter("column", "stuff")

    out = plugin.execute(ddbc, source)

    assert len(out) == 2
    assert out.columns == ["stuff", "foo"]
    assert sorted(out.fetchall()) == [
        (
            "backwards",
            "a",
        ),
        (
            "noaardvark",
            "aa",
        ),
    ]
