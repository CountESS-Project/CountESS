import logging
import numpy as np
import pandas as pd
import pytest

from countess.plugins.python import PythonPlugin

dfi = pd.DataFrame(
    [[1, 1, 2, 7, 11], [2, 2, 3, 8, 1], [3, 3, 4, 9, 3], [4, 4, 5, 10, 4], [5, 5, 6, 12, 7]],
    columns=["a", "b", "c", "d", "e"],
)

def test_python_builtins():
    plugin = PythonPlugin()
    plugin.set_parameter(
        "code",
        """
x = mean(a,b,c,d,e)
y = std(a,b,c,d,e)
z = sqrt(pow(a,2) + pow(b,2))
v = var(a,b,c,d,e)
    """,
    )

    plugin.prepare(["test"], None)
    dfo = plugin.process_dataframe(dfi)
    output = dfo.to_records()

    assert output[0]["x"] == 4.4
    assert 2.48 < output[1]["y"] < 2.49
    assert 4.24 < output[2]["z"] < 4.25
    assert 5.43 < output[3]["v"] < 5.45


def test_python_dropna():
    plugin = PythonPlugin()
    plugin.set_parameter(
        "code",
        """
a = None
n = None
if d >= 10: d = None
    """,
    )
    plugin.set_parameter("dropna", True)

    plugin.prepare(["test"], None)
    dfo = plugin.process_dataframe(dfi)

    assert "a" not in dfo.columns
    assert "n" not in dfo.columns
    assert "d" in dfo.columns

    assert any(np.isnan(dfo["d"]))
    assert not any(np.isnan(dfo["b"]))


def test_python_filter():
    plugin = PythonPlugin()
    plugin.set_parameter(
        "code",
        """
__filter = d < 10 and a % 2
    """,
    )

    plugin.prepare(["test"], None)
    dfo = plugin.process_dataframe(dfi)

    assert "__filter" not in dfo.columns
    assert len(dfo) == 2


def test_python_exception(caplog):
    plugin = PythonPlugin()
    plugin.set_parameter(
        "code",
        """
e = 1/0
    """,
    )

    plugin.prepare(["test"], None)
    dfo = plugin.process_dataframe(dfi)
    assert len(dfo) == 5

    assert "Exception" in caplog.text
