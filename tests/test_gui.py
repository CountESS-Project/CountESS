import tkinter as tk
import time

from countess.gui.main import make_root, MainWindow

root = make_root()

mw = MainWindow(root, "simple.ini")

root.update()

nodes = mw.graph_wrapper.graph.nodes

for node in nodes:
    mw.graph_wrapper.on_mousedown(node, None)
    root.update()
    time.sleep(0.1)
time.sleep(1)

root.destroy()
