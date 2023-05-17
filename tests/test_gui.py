import time
import pytest

from countess.gui.main import make_root, MainWindow
from countess.core.config import write_config

def test_open_nodes():

    root = make_root()

    mw = MainWindow(root, "tests/simple.ini")

    root.update()

    for node in mw.graph_wrapper.graph.traverse_nodes():
        mw.graph_wrapper.on_mousedown(node, None)
        root.update()
        time.sleep(0.1)
    time.sleep(1)

    write_config(mw.graph_wrapper.graph, "tests/simple.ini.output")
