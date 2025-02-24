import time
import duckdb
import pandas as pd


from countess.plugins.expression import ExpressionPlugin

ddbc = duckdb.connect()

ddbc.from_df(pd.DataFrame(
    [
        {"foo": 1, "bar": 2, "baz": 3},
        {"foo": 4, "bar": 5, "baz": 6},
        {"foo": 7, "bar": 8, "baz": 9},
    ],
)).create("n_0")

source = ddbc.table("n_0")

code_1 = "qux = bar + baz\n\nquux = bar * baz\n"

code_2 = "__filter = bar + baz != 11"

code_3 = "qux = foo + bar + baz\n\nfoo = None\n\nbar = None if qux else 0"

def test_expr_1():
    plugin = ExpressionPlugin()
    plugin.set_parameter("code", code_1)
    plugin.prepare(ddbc, source)
    out = plugin.execute(ddbc, source)

    assert len(out) == 3
    assert out.columns == [ "foo", "bar", "baz", "qux", "quux" ]
    assert all(out.df()["qux"] == [ 5, 11, 17 ])


def test_expr_2():
    plugin = ExpressionPlugin()
    plugin.set_parameter("code", code_2)
    plugin.prepare(ddbc, source)
    out = plugin.execute(ddbc, source)

    assert len(out) == 2
    assert out.columns == [ "foo", "bar", "baz" ]


def test_expr_3():
    plugin = ExpressionPlugin()
    plugin.set_parameter("code", code_3)
    plugin.prepare(ddbc, source)
    out = plugin.execute(ddbc, source)

    assert len(out) == 3
    assert sorted(out.columns) == [ "bar", "baz", "qux" ]
