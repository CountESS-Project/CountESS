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

class ConnectingLine:
    line = None
    def __init__(self, canvas, widget1, widget2, color='black', switch=False):
        self.canvas = canvas
        self.widget1 = widget1
        self.widget2 = widget2
        self.color = color
        self.switch = switch

        self.widget1.bind("<Configure>", self.update_line, add=True)
        self.widget2.bind("<Configure>", self.update_line, add=True)
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


class DataflowNode:

    is_mouse_down = False
    is_moving = False
    ghost_widget = None
    ghost_line = None

    def __init__(self, graph, widget):
        self.graph = graph
        self.widget = widget
        self.widget.bind("<Button-1>", self.on_mousedown, add=True)
        self.widget.bind("<B1-Motion>", self.on_mousemove, add=True)
        self.widget.bind("<B1-Release>", self.on_mouseup, add=True)

    def overlaps(self, x, y):
        wx, wy, ww, wh = _geometry(self.widget)
        return (wx <= x <= wx+ww) and (wy <= y <= wy+wh)

    def on_mousedown(self, event):
        self.start_x, self.start_y = event.x, event.y
        self.widget['cursor'] = 'fleur' # arrows
        self.widget.after(500, self.on_timeout)

    def on_timeout(self):
        if self.is_mouse_down and not self.is_moving:
            self.widget['cursor'] = 'plus'
            self.ghost_widget = tk.Frame(self.widget.master)
            self.ghost_widget.place(self.widget.place_info())
            self.ghost_line = ConnectingLine(self.widget.master, self.widget, self.ghost_widget, 'red', True)

    def on_mousemove(self, event):
        self.is_moving = True
        canvas_w = self.widget.master.winfo_width()
        canvas_h = self.widget.master.winfo_height()
        widget_x, widget_y, widget_w, widget_h = _geometry(self.widget)
        new_x = _limit(widget_x - self.start_x + event.x, widget_w//2, canvas_w - widget_w//2)
        new_y = _limit(widget_y - self.start_y + event.y, widget_h//2, canvas_h - widget_h//2)
        
        if self.ghost_widget:
            self.ghost_widget.place({'relx': new_x / canvas_w, 'rely': new_y / canvas_h})
            self.ghost_line.update_line()
        elif self.widget.place_info()['relx']:
            self.widget.place({'relx': new_x / canvas_w, 'rely': new_y / canvas_h})
            self.graph.update_position(self, new_x, new_y)
        else:
            self.widget.place({'x': new_x, 'y': new_y})
            self.graph.update_position(self, new_x, new_y)

    def on_mouseup(self, event):
        if self.ghost_widget:
            self.ghost_line.destroy()
            #new_position = event.x + self.widget.winfo_x(), event_y + self.widget.winfo_h()
            self.graph.new_connection(self, self.ghost_widget.winfo_x(), self.ghost_widget.winfo_y())
            self.ghost_widget.destroy()
            self.ghost_widget = None

        self.is_mouse_down = False
        self.widget['cursor'] = 'hand1'


class DataflowGraph:
    
    # four signals to outside world: 
    # 1. create a new node
    # 2. delete an node
    # 3. select a node
    # 4. dump list of connections

    def __init__(self, canvas, widget_factory):
        self.canvas = canvas or tk.Canvas()
        self.widget_factory = widget_factory or (lambda: tk.Entry(self.canvas, text="NEW"))
        self.nodes = []
        self.lines = []

    def find_node_at_position(x, y):
        for node in nodes:
            if node.overlaps(x, y): return node
        return None

    def new_node(self):
        new_node = DataflowNode(self, self.widget_factory())
        self.nodes.append(new_node)
        return new_node

    def del_node(self, node):
        for n, line in enumerate(self.lines):
            if line.widget1 == node.widget or line.widget2 == node.widget:
                self.lines.pop(n).destroy()
        node.widget.destroy()
        
    def add_node(self, widget, parents=[]):
        self.nodes.append(DataflowNode(self, widget, parents))
        self.lines

    def update_position(self, node, new_x, new_y):
        for line in self.lines:
            if line.widget1 == node.widget or line.widget2 == node.widget:
                line.update_line()

    def new_connection(self, node, new_x, new_y, above=False):
        other = find_node_at_position(new_x, new_y)

        if other and other.is_ancestor(node):
            node.add_parent(other)
        elif other and node.is_ancestor(other):
            other.add_parent(node)
        else:
            if not other: other = self.new_node()
            if above:
                node.add_parent(other)
            else:
                other.add_parent(node)

    def get_netlist(self):
        nodes_list = []
        while True:
            more_nodes = [ node for node in nodes if all((p in nodes_list for p in node.parents)) ]
            if not more_nodes: break
            nodes_list += more_nodes
        return nodes_list





        self.name = label_text
        self.label = DraggableLabel(canvas, text=label_text, takefocus=True, highlightcolor='red', highlightthickness=2, cursor='hand1')
        self.label.place({'relx': position[0], 'rely': position[1], 'anchor': tk.CENTER})
        self.parents = parents
        self.lines = [ ConnectingLine(canvas, p.label, self.label) for p in parents ]

        self.label.bind("<Button-1>", self.on_click, add=True)
        self.label.bind("<<GhostRelease>>", self.on_ghost_release)

        self.plugin = None

    def on_click(self, event):
        node_clicked(self)

    def add_parent(self, other):
        self.parents.append(other)
        self.lines.append(ConnectingLine(self.canvas, other.label, self.label))

    def del_parent(self, other):
        n = self.parents.index(other)
        self.parents.pop(n)
        self.lines.pop(n).destroy()

    def add_or_del_parent(self, other):
        try:
            self.del_parent(other)
        except ValueError:
            self.add_parent(other)

    def on_ghost_release(self, event):
        xl, yl, wl, hl = _geometry(self.label)
        xc, yc, wc, hc = _geometry(self.canvas)
        node = find_node_at_position(event.x + xl, event.y + yl)

        if node == self:
            return

        if not node:
            xn = (event.x + xl) / wc
            yn = (event.y + yl) / hc
            node = Node(self.canvas, f"NEW {len(nodes)}", (xn, yn), [])
            nodes.append(node)
        
        if self.is_ancestor(node):
            node.add_or_del_parent(self)
        elif node.is_ancestor(self):
            self.add_or_del_parent(node)
        elif (event.x if wc > hc else event.y) > 0:
            node.add_or_del_parent(self)
        else:
            self.add_or_del_parent(node)

        node_clicked(node)

        dump_nodes_by_stratum()

    def stratum(self):
        # XXX not very efficient for a big graph!
        if not self.parents: return 1
        return max(n.stratum() for n in self.parents) + 1

    def is_ancestor(self, other):
        return self in other.parents or any((self.is_ancestor(node) for node in other.parents))

    def is_alone(self):
        if self.parents: return False
        for n in nodes:
            if self in n.parents: 
                return False
        return True

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


class DataflowNode:

    def __init__(self, widget, parents):
        self.widget = widget

@dataclass
class Widgets:
    label: tk.Message
    config: tk.Frame


class PipelineCanvas(tk.Canvas):

    def __init__(self, tk_parent, pipeline_graph, *a, **k):
        super().__init__(tk_parent, *a, **k)
        self.__pipeline_graph = pipeline_graph
        self['bg'] = 'skyblue'
        self.__lines = []

        widgets_for_node = {}

            


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
            label = tk.Message(self.canvas, text=node.name, aspect=200, cursor='hand1')
            if not node.position:
                node.position = (n%5)*0.2+0.1, (n//5)*0.2+0.1
            label.place({'relx': node.position[0], 'rely': node.position[1], 'anchor': 'c'})
    
            for pn in node.parent_nodes:
                ConnectingLine(self.canvas, label_for_node[pn], label)
    
            label_for_node[node] = label
            label.bind('<Button-1>', lambda event, node=node: self.node_select(node), add=True)


    def node_select(self, node):
        for widget in self.subframe.winfo_children():
            widget.destroy()

        self.node_update(node)
        self.selected_node = node

        self.configurator = PluginConfigurator(self.subframe, node.plugin, self.change_callback)
        self.configurator.frame.grid(row=0, column=0, sticky=tk.NSEW)

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
        print("YO")
        self.selected_node.mark_dirty()
        self.node_update(self.selected_node)

    def on_frame_configure(self, event):
        """Swaps layout around when the window goes from landscape to portrait"""
        print(event)
        self.frame.columnconfigure(0, minsize=event.width / 3, weight=1)
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
