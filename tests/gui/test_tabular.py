import pytest
import tkinter as tk

import pandas as pd

from countess.gui.main import make_root
from countess.gui.tabular import TabularDataFrame

df0 = pd.DataFrame([])

df1 = pd.DataFrame([
    { 'foo': 1, 'bar': 2.0, 'baz': True },
    { 'foo': 4, 'bar': 5.1, 'baz': False },
    { 'foo': 7, 'bar': 8.3, 'baz': None },
])

df2 = df1.set_index(keys=['foo'])

df3 = df1.set_index(keys=['foo', 'bar'])

def test_tabular_1():
    root = make_root()

    tt = TabularDataFrame(root)

    tt.reset()

    tt.set_dataframe(df0)
    tt.set_dataframe(df1)
    tt.set_dataframe(df2)
    tt.set_dataframe(df3)

