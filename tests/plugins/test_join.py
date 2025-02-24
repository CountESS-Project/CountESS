import duckdb
import numpy as np
import pandas as pd
import pytest

ddbc = duckdb.connect()

from countess.plugins.join import JoinPlugin

ddbc.from_df(
    pd.DataFrame(
        [
            {"foo": 1, "bar": 2, "baz": 3},
            {"foo": 4, "bar": 5, "baz": 6},
            {"foo": 7, "bar": 8, "baz": 9},
        ]
    )
).create("n_1")

ddbc.from_df(
    pd.DataFrame(
        [
            {"foo": 1, "qux": "a"},
            {"foo": 4, "qux": "b"},
            {"foo": 8, "qux": "c"},
        ]
    )
).create("n_2")

sources = {"one": ddbc.table("n_1"), "two": ddbc.table("n_2")}


def test_join_inner():
    plugin = JoinPlugin()

    plugin.set_parameter("inputs.0.join_on", "foo")
    plugin.set_parameter("inputs.0.required", True)
    plugin.set_parameter("inputs.1.join_on", "foo")
    plugin.set_parameter("inputs.1.required", True)
    plugin.prepare_multi(ddbc, sources)

    output = plugin.execute_multi(ddbc, sources)

    assert output.columns == ["foo", "bar", "baz", "qux"]
    assert output.df().equals(
        pd.DataFrame(
            [
                {"foo": 1, "bar": 2, "baz": 3, "qux": "a"},
                {"foo": 4, "bar": 5, "baz": 6, "qux": "b"},
            ]
        )
    )


def test_join_left():
    plugin = JoinPlugin()

    plugin.set_parameter("inputs.0.join_on", "foo")
    plugin.set_parameter("inputs.0.required", True)
    plugin.set_parameter("inputs.1.join_on", "foo")
    plugin.set_parameter("inputs.1.required", False)
    plugin.prepare_multi(ddbc, sources)

    output = plugin.execute_multi(ddbc, sources)

    assert output.columns == ["foo", "bar", "baz", "qux"]
    assert output.df().equals(
        pd.DataFrame(
            [
                {"foo": 1, "bar": 2, "baz": 3, "qux": "a"},
                {"foo": 4, "bar": 5, "baz": 6, "qux": "b"},
                {"foo": 7, "bar": 8, "baz": 9, "qux": None},
            ]
        )
    )


def test_join_right():
    plugin = JoinPlugin()

    plugin.set_parameter("inputs.0.join_on", "foo")
    plugin.set_parameter("inputs.0.required", False)
    plugin.set_parameter("inputs.1.join_on", "foo")
    plugin.set_parameter("inputs.1.required", True)
    plugin.prepare_multi(ddbc, sources)

    output = plugin.execute_multi(ddbc, sources)

    assert output.columns == ["foo", "bar", "baz", "qux"]
    assert output.fetchall() == [(1, 2, 3, "a"), (4, 5, 6, "b"), (None, None, None, "c")]


def test_join_full():
    plugin = JoinPlugin()

    plugin.set_parameter("inputs.0.join_on", "foo")
    plugin.set_parameter("inputs.0.required", False)
    plugin.set_parameter("inputs.1.join_on", "foo")
    plugin.set_parameter("inputs.1.required", False)
    plugin.prepare_multi(ddbc, sources)

    output = plugin.execute_multi(ddbc, sources)
    assert output.columns == ["foo", "bar", "baz", "qux"]
    assert output.fetchall() == [(1, 2, 3, "a"), (4, 5, 6, "b"), (None, None, None, "c"), (7, 8, 9, None)]
