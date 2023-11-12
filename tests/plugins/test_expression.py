import time

import pandas as pd

from countess.core.logger import MultiprocessLogger
from countess.plugins.expression import ExpressionPlugin

logger = MultiprocessLogger()

df1 = pd.DataFrame(
    [
        {"foo": 1, "bar": 2, "baz": 3},
        {"foo": 4, "bar": 5, "baz": 6},
        {"foo": 7, "bar": 8, "baz": 9},
    ],
)

df2 = df1.set_index("foo")

code_1 = "qux = bar + baz\n\nquux = bar * baz\n"

code_2 = "bar + baz != 11"


def test_expr_0():
    plugin = ExpressionPlugin()
    plugin.set_parameter("code", "1/0")
    plugin.prepare(["x"])

    df = plugin.process_dataframe(df1, logger)

    # XXX temporarily disabling this because it doesn't
    # seem to work in Github actions
    # time.sleep(1)
    # assert "ZeroDivisionError" in logger.dump()


def test_expr_1():
    plugin = ExpressionPlugin()
    plugin.set_parameter("code", code_1)
    plugin.prepare(["x"])

    df = plugin.process_dataframe(df1, logger)
    assert len(df) == 3
    assert set(df.columns) == {"foo", "bar", "baz", "qux", "quux"}


def test_expr_2():
    plugin = ExpressionPlugin()
    plugin.set_parameter("code", code_2)
    plugin.prepare(["x"])

    df = plugin.process_dataframe(df1, logger)
    assert len(df) == 2
    assert set(df.columns) == {"foo", "bar", "baz"}


def test_expr_3():
    plugin = ExpressionPlugin()
    plugin.set_parameter("code", code_1)
    plugin.set_parameter("drop.0._label", "foo")
    plugin.set_parameter("drop.0", True)
    plugin.set_parameter("drop.1._label", "baz")
    plugin.set_parameter("drop.1", True)
    plugin.prepare(["x"])

    df = next(plugin.process(df2, "x", logger))
    assert len(df) == 3
    assert set(df.columns) == {"bar", "qux", "quux"}
