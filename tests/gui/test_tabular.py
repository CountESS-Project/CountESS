import time

import numpy as np
import pandas as pd
import pytest

from countess.gui.main import make_root
from countess.gui.tabular import TabularDataFrame

# no columns
df0 = pd.DataFrame([])

# columns, but no rows
df00 = pd.DataFrame([], columns=["a"])

# no index other than a range index
df1 = pd.DataFrame(
    [
        {"foo": 1, "bar": 2.0, "baz": True, "nan": np.nan},
        {"foo": 4, "bar": 5.1, "baz": False, "nan": np.nan},
        {"foo": 7, "bar": 8.3, "baz": None, "nan": np.nan},
    ]
)

# one index
df2 = df1.set_index(keys=["foo"])

# multiple index columns
df3 = df1.set_index(keys=["foo", "bar"])

# multi-level indx
df4 = df1.groupby("foo").agg({"bar": ["sum", "count"]})

dfx = pd.DataFrame([{"foo": n, "bar": n * n} for n in range(0, 10000)])


@pytest.mark.gui
def test_tabular_1():
    root = make_root()
    tt = TabularDataFrame(root)

    tt.reset()

    tt.set_dataframe(df0)
    tt.set_dataframe(df00)
    tt.set_dataframe(df1)
    tt.set_dataframe(df2)
    tt.set_dataframe(df3)
    tt.set_dataframe(df4)


@pytest.mark.gui
def test_tabular_scroll():
    root = make_root()
    tt = TabularDataFrame(root)

    tt.set_dataframe(dfx)

    for offset in (10, 0, 6666, 6660, 3333):
        time.sleep(0.1)
        tt.refresh(offset)
        root.update()


@pytest.mark.gui
def test_tabular_copy():
    root = make_root()
    tt = TabularDataFrame(root)
    tt.set_dataframe(dfx)

    tt.select_rows = (30, 32)
    tt._column_copy(None)

    x = root.selection_get(selection="CLIPBOARD")
    assert x == "\tfoo\tbar\n29\t29\t841\n30\t30\t900\n31\t31\t961\n\n"
