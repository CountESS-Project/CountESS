import time
import tkinter as tk
from unittest.mock import MagicMock

import pytest

from countess.core.config import read_config
from countess.core.pipeline import PipelineNode
from countess.gui.main import ConfiguratorWrapper, PluginChooserFrame, RunWindow, make_root
from countess.plugins.data_table import DataTablePlugin


def descendants(widget):
    for w in widget.winfo_children():
        yield w
        yield from descendants(w)


@pytest.mark.gui
def test_chooser():
    callback = MagicMock()

    root = make_root()
    choose = PluginChooserFrame(root, "X", callback, True, True)

    for x in descendants(choose):
        if isinstance(x, tk.Button):
            x.invoke()
            break

    callback.assert_called_once()

    root.destroy()


@pytest.mark.gui
def test_main():
    root = make_root()
    node = PipelineNode(name="NEW", plugin=DataTablePlugin())
    callback = MagicMock()

    wrap = ConfiguratorWrapper(root, node, callback)
    wrap.on_add_notes()

    root.update()

    root.destroy()


@pytest.mark.gui
def test_run():
    graph = read_config("tests/simple.ini")

    runner = RunWindow(graph)
    for _ in range(0, 20):
        time.sleep(0.1)
        runner.toplevel.update()
    runner.on_button()
    time.sleep(1)

    assert runner.process is None

    runner.on_button()
    time.sleep(0.1)

    assert runner.toplevel is None
