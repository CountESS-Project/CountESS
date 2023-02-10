import tkinter as tk
from tkinter import ttk
import tkinter.dnd
import re
import sys
from dataclasses import dataclass

import pandas as pd
import dask.dataframe as dd
import numpy as np

from countess import VERSION
from countess.core.gui import PluginConfigurator, DataFramePreview
from countess.plugins.pivot import DaskPivotPlugin
from countess.core.pipeline import Pipeline
from countess.core.dataflow import PipelineGraph, PipelineNode
from countess.core.config import read_config

def _limit(value, min_value, max_value):
    return max(min_value, min(max_value, value))

def _geometry(widget):
    return (
        widget.winfo_x(),
        widget.winfo_y(),
        widget.winfo_width(),
        widget.winfo_height()
    )

class DraggableMixin:

    __mousedown = False
    __moving = False
    __ghost = None
    __ghost_line = None

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.bind("<Button-1>", self.__on_start, add="+")
        self.bind("<B1-Motion>", self.__on_motion, add="+")
        self.bind("<ButtonRelease-1>", self.__on_release, add="+")

    def __on_start(self, event):
        print("START")
        self['cursor'] = 'fleur'
        self.__start_x = event.x
        self.__start_y = event.y
        self.after(500, self.__on_timeout)
        self.__mousedown = True

    def __on_timeout(self):
        print("TIMEOUT")
        if self.__mousedown and not self.__moving:
            self['cursor'] = 'plus'
            self.__ghost = tk.Frame(self.master)
            self.__ghost.place(self.place_info())
            self.__ghost_line = ConnectingLine(self.master, self, self.__ghost, 'red', True)

    def __on_motion(self, event):
        if not self.__moving: print("MOVING")
        self.__moving = True
        mw = self.master.winfo_width()
        mh = self.master.winfo_height()
        w = self.winfo_width()
        h = self.winfo_height()
        x = _limit(self.winfo_x() - self.__start_x + event.x, w//2, mw-w//2)
        y = _limit(self.winfo_y() - self.__start_y + event.y, h//2, mh-h//2)

        if self.__ghost:
            self.__ghost.place({'relx': x / mw, 'rely': y / mh})
        elif self.place_info()['relx']:
            self.place({'relx': x / mw, 'rely': y / mh})
        else:
            self.place({'x': x, 'y': y})

    def __on_release(self, event):
        print("RELEASE")
        if self.__ghost is not None:
            print("GHOST")
            self.event_generate("<<GhostRelease>>", x=event.x, y=event.y)
            self.__ghost_line.destroy()
            self.__ghost.destroy()
            self.__ghost = None
        self.__mousedown = False
        self.__moving = False
        self['cursor'] = 'hand1'


class FixedUnbindMixin:

    def unbind(self, seq, funcid):
        # widget.unbind(seq, funcid) doesn't actually work as documented. This is
        # my own take on the horrible hacks found at https://bugs.python.org/issue31485
        # and https://mail.python.org/pipermail/tkinter-discuss/2012-May/003152.html
        # Also quite horrible.  "I'm not proud, but I'm not tired either"

        self.bind(seq, re.sub(
            r'^if {"\[' + funcid + '.*$', '', self.bind(seq), flags=re.M
        ))
        self.deletecommand(funcid)


class ConnectingLine:
    line = None
    def __init__(self, canvas, widget1, widget2, color='black', switch=False):
        self.canvas = canvas
        self.widget1 = widget1
        self.widget2 = widget2
        self.color = color
        self.switch = switch

        self.widget1_bind = self.widget1.bind("<Configure>", self.update_line, add=True)
        self.widget2_bind = self.widget2.bind("<Configure>", self.update_line, add=True)
        self.canvas.bind("<Configure>", self.update_line, add=True)
        self.update_line()

    def update_line(self, event=None):
        x1, y1, w1, h1 = _geometry(self.widget1)
        x2, y2, w2, h2 = _geometry(self.widget2)

        xc, yc, wc, hc = _geometry(self.canvas)
        if wc > hc: 
            if self.switch and x1 > x2:
                x1, y1, w1, h1, x2, y2, w2, h2 = x2, y2, w2, h2, x1, y1, w1, h1
            coords = (
                x1 + w1, y1 + h1 //2,
                x1 + w1 + 50, y1 + h1//2,
                x2 - 50, y2 + h2//2,
                x2, y2 + h2//2
            )
        else:
            if self.switch and y1 > y2:
                x1, y1, w1, h1, x2, y2, w2, h2 = x2, y2, w2, h2, x1, y1, w1, h1
            coords = (
                x1 + w1 // 2, y1 + h1,
                x1 + w1 // 2, y1 + h1 + 50,
                x2 + w2 // 2, y2 - 50,
                x2 + w2 // 2, y2
            )

        if self.line: self.canvas.coords(self.line, *coords)
        else: self.line = self.canvas.create_line(*coords, smooth=True, width=3, arrow='last', arrowshape=(20,20,8), fill=self.color)

    def destroy(self):
        self.canvas.delete(self.line)
        self.widget1.unbind("<Configure>", self.widget1_bind)
        self.widget2.unbind("<Configure>", self.widget2_bind)


def on_button_3(event):
    items = canvas.find_overlapping(event.x-10, event.y-10, event.x+10, event.y+10)
    if len(items) != 1: return
    for node in nodes:
        try:
            idx = [ cl.line for cl in node.lines].index(items[0])
            node.parents.pop(idx)
            node.lines.pop(idx).destroy()
        except ValueError:
            pass


class FlippyCanvas(tk.Canvas):
    """A canvas which flips all its children's X and Y
    coordinates when it goes from portrait to landscape.
    Place children with .place(relx=, rely=) for best results."""

    __flipped = False

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.bind("<Configure>", self.__on_configure)

    def __flip(self, width, height):
        for c in self.winfo_children():
            c.place({
                'x': c.place_info()['y'],
                'y': c.place_info()['x'],
                'relx': c.place_info()['rely'],
                'rely': c.place_info()['relx'],
            })
        # XXX should flip canvas items as well,
        # but I didn't actually need that so I didn't bother.

    def __on_configure(self, event):
        flipped = event.height > event.width
        if flipped != self.__flipped:
            self.__flip(event.width, event.height)
            self.__flipped = flipped

class DraggableMessage(DraggableMixin, FixedUnbindMixin, tk.Message):
    pass

class MainWindow:

    preview_frame = None
   
    def __init__(self, tk_parent, pipeline_graph):

        self.frame = tk.Frame(tk_parent)
        self.frame.grid(sticky=tk.NSEW)
        self.frame.columnconfigure(0, weight=0)

        self.canvas = FlippyCanvas(self.frame, bg='skyblue')
        self.subframe = tk.Frame(self.frame, bg="beige")
        self.subframe.rowconfigure(0, weight=0)
        self.subframe.rowconfigure(1, weight=1)
        self.subframe.columnconfigure(0, weight=1)

        tk.Label(self.subframe, text=f"CountESS {VERSION}", font=('Helvetica', 20, 'bold')).grid(row=1, sticky=tk.NSEW)
        
        self.canvas.grid(row=0, column=0, sticky=tk.NSEW)
        self.subframe.grid(row=0, column=1, sticky=tk.NSEW)

        self.frame.bind('<Configure>', self.on_frame_configure, add=True)

        label_for_node = {}
        for n, node in enumerate(pipeline_graph.nodes):
            label = DraggableMessage(self.canvas, text=node.name, aspect=200, cursor='hand1')
            if not node.position:
                node.position = (n%5)*0.2+0.1, (n//5)*0.2+0.1
            label.place({'relx': node.position[0], 'rely': node.position[1], 'anchor': 'c'})
    
            for pn in node.parent_nodes:
                ConnectingLine(self.canvas, label_for_node[pn], label)
    
            label_for_node[node] = label
            label.bind('<Button-1>', lambda event, node=node: self.node_select(node), add=True)
            label.bind('<Configure>', lambda event, node=node: self.node_configure(event, node), add=True)


    def node_select(self, node):
        for widget in self.subframe.winfo_children():
            widget.destroy()

        self.node_update(node)
        self.selected_node = node

        self.configurator = PluginConfigurator(self.subframe, node.plugin, self.change_callback)
        self.configurator.frame.grid(row=0, column=0, sticky=tk.NSEW)

    def node_configure(self, event, node):
        place_info = event.widget.place_info()
        node.position = ( place_info['relx'], place_info['rely'] )

    def node_update(self, node):
        if self.preview_frame: self.preview_frame.destroy()
        node.prepare()
        node.prerun()

        if isinstance(node.result, (dd.DataFrame, pd.DataFrame)):
            self.preview_frame = DataFramePreview(self.subframe, node.result).frame
        elif node.output:
            self.preview_frame = tk.Text(self.subframe, bg='red')
            self.preview_frame.replace("1.0", tk.END, node.output)
        else:
            self.preview_frame = tk.Frame(self.subframe, bg='orange')
        self.preview_frame.grid(row=1, column=0, sticky=tk.NSEW)

    def change_callback(self, _):
        print(f"CHANGE CALLBACK {_}")
        self.selected_node.mark_dirty()
        self.node_update(self.selected_node)

    def on_frame_configure(self, event):
        """Swaps layout around when the window goes from landscape to portrait"""
        print(event)
        self.frame.columnconfigure(0, minsize=event.width / 5, weight=1)
        self.frame.rowconfigure(0, minsize = event.height / 3, weight=1)
    
        if event.width > event.height:
            self.subframe.grid(row=0, column=1, sticky=tk.NSEW)
            self.frame.rowconfigure(1, weight=0)
            self.frame.columnconfigure(1, minsize=0, weight=4)
        else:
            self.subframe.grid(row=1, column=0, sticky=tk.NSEW)
            self.frame.columnconfigure(1, weight=0)
            self.frame.rowconfigure(1, minsize=0, weight=4)

def main():
    try:
        import ttkthemes  # type:ignore
        root = ttkthemes.ThemedTk()
        themes = set(root.get_themes())
        for t in ["winnative", "aqua", "ubuntu", "clam"]:
            if t in themes:
                root.set_theme(t)
    except ImportError:
        root = tk.root()
        # XXX some kind of ttk style setup goes here

    root.title(f"CountESS {VERSION}")
    root.rowconfigure(0, weight=1)
    root.columnconfigure(0, weight=1)

    pipeline_graph = read_config(sys.argv[1:])
    
    MainWindow(root, pipeline_graph)

    root.mainloop()



if __name__ == "__main__":
    main()
