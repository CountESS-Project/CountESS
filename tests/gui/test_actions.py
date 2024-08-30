import time
import tkinter as tk
from typing import Iterable, Optional
from unittest.mock import patch

import pytest

from countess.gui.main import MainWindow, make_root, PluginChooserFrame
from countess.gui.config import PluginConfigurator
from countess.gui.tabular import TabularDataFrame
from countess.plugins.csv import LoadCsvPlugin

def _find_buttons(frame: tk.Frame, label: str) -> Iterable[tk.Button]:

    for w in frame.winfo_children():
        if isinstance(w, tk.Button):
            if w['text'] == label:
                yield w
        elif isinstance(w, (tk.Frame, tk.LabelFrame)):
            yield from _find_buttons(w, label)

def _find_button(frame: tk.Frame, label: str) -> Optional[tk.Button]:
    try:
        return next(iter(_find_buttons(frame, label)))
    except StopIteration:
        return None

@pytest.mark.gui
def test_open_new():
    root = make_root()
    mw = MainWindow(root)
    root.update()

    assert isinstance(mw.config_wrapper.config_subframe, PluginChooserFrame)

    button = _find_button(mw.config_wrapper.config_subframe, 'CSV Load')
    button.invoke()
    root.update()

    assert isinstance(mw.config_wrapper.configurator, PluginConfigurator)
    plugin = mw.config_wrapper.configurator.plugin
    assert isinstance(plugin, LoadCsvPlugin)

    with patch('tkinter.filedialog.askopenfilenames', return_value=['tests/input1.csv']):
        button = _find_button(mw.config_wrapper.config_subframe, "")
        button.invoke()
        root.update()
        time.sleep(1)

    root.update()
    time.sleep(1)
    root.update()

    preview_frame = mw.config_wrapper.preview_subframe
    assert isinstance(preview_frame, TabularDataFrame)
    dataframe = preview_frame.dataframe

    assert len(dataframe) == 4
    assert len(dataframe.columns) == 2
