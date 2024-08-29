import pandas as pd

from countess.plugins.filter import FilterPlugin

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


def test_filter_0():
    plugin = FilterPlugin()
    # plugin.set_parameter("drop.1", True)
    plugin.prepare(["x"])

    dfs = list(plugin.process(df1, "x"))
    assert len(dfs) == 0
