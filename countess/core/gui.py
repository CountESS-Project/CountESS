import tkinter as tk
from tkinter import ttk
import tkinter.dnd
import re
import sys
from dataclasses import dataclass
import random

import pandas as pd
import dask.dataframe as dd
import numpy as np

from countess import VERSION
from countess.core.plugins import get_plugin_classes
from countess.core.gui_old import PluginConfigurator, DataFramePreview
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
        self['cursor'] = 'fleur'
        self.__start_x = event.x
        self.__start_y = event.y
        self.after(500, self.__on_timeout)
        self.__mousedown = True

    def __on_timeout(self):
        if self.__mousedown and not self.__moving:
            self['cursor'] = 'plus'
            self.__ghost = tk.Frame(self.master)
            self.__ghost.place(self.place_info())
            self.__ghost_line = ConnectingLine(self.master, self, self.__ghost, 'red', True)

    def __on_motion(self, event):
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
        if self.__ghost is not None:
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

        # XXX I'm not totally in love with all this event
        # binding and unbinding and might replace it with
        # a GraphWrapper-level binding.

        self.widget1_bind = self.widget1.bind("<Configure>", self.update_line, add=True)
        self.widget2_bind = self.widget2.bind("<Configure>", self.update_line, add=True)
        self.canvas_bind = self.canvas.bind("<Configure>", self.update_line, add=True)
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
        self.canvas.unbind("<Configure>", self.canvas_bind)
        self.widget1.unbind("<Configure>", self.widget1_bind)
        self.widget2.unbind("<Configure>", self.widget2_bind)


plugin_classes = get_plugin_classes()

class PluginChooserFrame(tk.Frame):

    def __init__(self, master, title, callback, *a, **k):
        super().__init__(master, *a, **k)

        self.columnconfigure(0, weight=1)

        font = ('Helvetica', 16, 'bold')
        tk.Label(self, text=title, font=font).grid(row=0, column=0, sticky=tk.EW)
        label_frame = tk.LabelFrame(self, text="Choose Plugin", padx=10, pady=10)
        label_frame.grid(row=1, column=0, sticky=tk.EW)

        for n, plugin_class in enumerate(plugin_classes):
            ttk.Button(label_frame, text=plugin_class.name, command=lambda plugin_class=plugin_class: callback(plugin_class)).grid(row=n+1, column=0, sticky=tk.EW)
            ttk.Label(label_frame, text=plugin_class.title).grid(row=n+1, column=1, sticky=tk.W)


class FlippyCanvas(FixedUnbindMixin, tk.Canvas):
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


class GraphWrapper:

    def __init__(self, canvas, graph, node_select_callback):
        self.canvas = canvas
        self.graph = graph
        self.node_select_callback = node_select_callback

        self.labels = dict([ (node, self.label_for_node(node)) for node in graph.nodes ])
        self.lines = dict([ (node, self.lines_for_node(node)) for node in graph.nodes ])

    def label_for_node(self, node):
        label = DraggableMessage(self.canvas, text=node.name, aspect=200, cursor='hand1')
        if not node.position: node.position = (random.random() * 0.8 + 0.1, random.random() * 0.8 + 0.1)
        label.place({'relx': node.position[0], 'rely': node.position[1], 'anchor': 'c'})
        label.bind('<Button-1>', lambda event, node=node: self.on_mousedown(event, node), add=True)
        label.bind('<Configure>', lambda event, node=node: self.on_configure(event, node), add=True)
        label.bind('<<GhostRelease>>', lambda event, node=node: self.on_ghost_release(event, node), add=True)
        return label

    def lines_for_node(self, node):
        return dict([
            (parent_node, ConnectingLine(self.canvas, self.labels[parent_node], self.labels[node]))
            for parent_node in node.parent_nodes
        ])

    def highlight_node(self, node):
        for label in self.labels.values(): label['bg'] = '#DDD'
        self.labels[node]['bg'] = 'orange'

    def on_mousedown(self, event, node):
        self.highlight_node(node)
        self.node_select_callback(node, self.labels[node])

    def on_configure(self, event, node):
        place_info = event.widget.place_info()
        node.position = ( place_info['relx'], place_info['rely'] )

    def find_node_at_position(self, x, y):
        for node, label in self.labels.items():
            nx, ny, nw, nh = _geometry(label)
            if (nx <= x <= nx + nw) and (ny <= y <= ny+nh): 
                return node

    def on_ghost_release(self, event, start_node):
        xl, yl, wl, hl = _geometry(event.widget)
        other_node = self.find_node_at_position(event.x + xl, event.y + yl)
        if other_node is None:
            position = (event.x + xl) / self.canvas.winfo_width(), (event.y + yl) / self.canvas.winfo_height()
            other_node = PipelineNode(name="NEW", position=position)
            self.graph.add_node(other_node)
            self.labels[other_node] = self.label_for_node(other_node)
            self.lines[other_node] = {}

        elif other_node == start_node:
            return
        elif start_node in other_node.parent_nodes:
            other_node.del_parent(start_node)
            self.lines[other_node].pop(start_node).destroy()
            return
        elif other_node in start_node.parent_nodes:
            start_node.del_parent(other_node)
            self.lines[start_node].pop(other_node).destroy()
            self.node_select_callback(start_node, self.labels[start_node])
            return

        if start_node.is_ancestor_of(other_node):
            self.add_parent(start_node, other_node)
        elif other_node.is_ancestor_of(start_node):
            self.add_parent(other_node, start_node)
        elif (event.x > 0) if self.canvas.winfo_width() > self.canvas.winfo_height() else (event.y > 0):
            self.add_parent(start_node, other_node)
        else:
            self.add_parent(other_node, start_node)

        self.highlight_node(other_node)
        self.node_select_callback(other_node, self.labels[other_node])

    def add_parent(self, parent_node, child_node):
        child_node.add_parent(parent_node)
        self.lines[child_node][parent_node] = ConnectingLine(self.canvas, self.labels[parent_node], self.labels[child_node])
            

class ConfiguratorWrapper:

    config_subframe = None
    preview_subframe = None

    def __init__(self, frame, node, label):
        self.frame = frame
        self.node = node
        self.label = label

        self.name_var = tk.StringVar(self.frame, value=node.name)
        tk.Entry(self.frame, textvariable=self.name_var, font=('Helvetica', 16, 'bold')).grid(row=0, sticky=tk.EW)
        self.name_var.trace("w", self.name_changed_callback)

        self.frame.rowconfigure(0, weight=0)
        self.frame.rowconfigure(1, weight=0)
        self.frame.rowconfigure(2, weight=2)

        self.show_config_subframe()
        self.show_preview_subframe()

    def show_config_subframe(self):
        if self.config_subframe: self.config_subframe.destroy()
        if self.node.plugin:
            self.config_subframe = PluginConfigurator(self.frame, self.node.plugin, self.config_change_callback).frame
        else:
            self.config_subframe = PluginChooserFrame(self.frame, "Choose Plugin", self.choose_plugin)
        self.config_subframe.grid(row=1, sticky=tk.NSEW)

    def show_preview_subframe(self):
        if self.preview_subframe: self.preview_subframe.destroy()
        if isinstance(self.node.result, (dd.DataFrame, pd.DataFrame)):
            self.preview_subframe = DataFramePreview(self.frame, self.node.result).frame
        elif self.node.output:
            self.preview_subframe = tk.Text(self.frame, bg='indian red')
            self.preview_subframe.replace("1.0", tk.END, self.node.output)
        else:
            self.preview_frame = tk.Frame(self.frame, bg='orange')

        self.preview_subframe.grid(row=2, column=0, sticky=tk.NSEW)

    def name_changed_callback(self, *_):
        name = self.name_var.get()
        self.node.name = name
        self.label['text'] = name

    def config_change_callback(self, *_):
        self.node.mark_dirty()
        self.node.prerun()
        self.show_preview_subframe()

    def choose_plugin(self, plugin_class):
        self.node.plugin = plugin_class()
        self.node.is_dirty = True
        self.show_config_subframe()


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

        self.graph_wrapper = GraphWrapper(self.canvas, pipeline_graph, self.node_select)
        #node_wrapper_dict = {}
        #for node in pipeline_graph.nodes:
        #    nw = NodeWrapper(self, self.canvas, node)
        #    for parent in node.parent_nodes:
        #        nw.add_parent_wrapper(node_wrapper_dict[parent])
        #    node_wrapper_dict[node] = nw
        #self.node_wrappers = list(node_wrapper_dict.values())

    def node_select(self, node, label):
        for widget in self.subframe.winfo_children():
            widget.destroy()
        node.prepare()
        node.prerun()
        ConfiguratorWrapper(self.subframe, node, label)

    def find_node_at_position(self, x, y):
        for node_wrapper in self.node_wrappers:
            if node_wrapper.overlaps_position(x,y):
                return node_wrapper.node

    def add_or_delete_parent(self, parent_node, child_node):
        if child_node in parent_node.child_nodes:
            child_node.del_parent(parent_node)
            # XXX delete line too!
        else:
            child_node.add_parent(parent_node)
            #ConnectingLine(self.canvas, self.label_for_node[parent_node], self.label_for_node[child_node])

    def node_add(self, event, node):
        xl, yl, wl, hl = _geometry(event.widget)
        other = self.find_node_at_position(event.x + xl, event.y + yl)
        if other == node:
            return
        elif other is None:
            position = (event.x + xl) / self.canvas.winfo_width(), (event.y + yl) / self.canvas.winfo_height()
            other = PipelineNode(name="NEW", position=position)
            self.node_wrappers.append(
                NodeWrapper(self, self.canvas, other)
            )

        if node.is_ancestor_of(other):
            self.add_or_delete_parent(node, other)
        elif other.is_ancestor_of(node):
            self.add_or_delete_parent(other, node)
        elif (event.x > 0) if self.canvas.winfo_width() > self.canvas.winfo_height() else (event.y > 0):
            self.add_or_delete_parent(node, other)
        else:
            self.add_or_delete_parent(other, node)

    def node_update(self, node):
        if self.preview_frame: self.preview_frame.destroy()
        if node.plugin:
            node.prepare()
            node.prerun()

    def change_callback(self, _):
        self.selected_node.mark_dirty()
        self.node_update(self.selected_node)

    def on_frame_configure(self, event):
        """Swaps layout around when the window goes from landscape to portrait"""
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
