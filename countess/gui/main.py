import math
import random
import re
import sys
import tkinter as tk
import webbrowser
from enum import Enum, IntFlag
from functools import partial
from tkinter import filedialog, messagebox, ttk
from typing import Optional

import dask.dataframe as dd
import pandas as pd  # type: ignore

from countess import VERSION
from countess.core.config import (
    export_config_graphviz,
    read_config,
    write_config,
)
from countess.core.logger import ConsoleLogger
from countess.core.pipeline import PipelineGraph, PipelineNode
from countess.core.plugins import get_plugin_classes
from countess.gui.config import DataFramePreview, PluginConfigurator
from countess.gui.logger import LoggerFrame

# import faulthandler
# faulthandler.enable(all_threads=True)


def _limit(value, min_value, max_value):
    return max(min_value, min(max_value, value))


def _snap(value, scale, steps=21):
    step_size = scale / steps
    value = _limit(value, 0, scale)
    return ((value // step_size) + 0.5) * step_size


def _geometry(widget):
    return (
        widget.winfo_x(),
        widget.winfo_y(),
        widget.winfo_width(),
        widget.winfo_height(),
    )


# Weird that these aren't defined anywhere obvious?
# XXX note cross-platform mapping for modifiers and buttons
# https://wiki.tcl-lang.org/page/Modifier+Keys


class TkEventState(IntFlag):
    SHIFT = 1
    CAPS_LOCK = 2
    CONTROL = 4
    MOD1 = 8
    MOD2 = 16
    MOD3 = 32
    MOD4 = 64
    MOD5 = 128
    BUTTON1 = 256
    BUTTON2 = 512
    BUTTON3 = 1024
    BUTTON4 = 2048
    BUTTON5 = 4096


class TkCursors(Enum):
    HAND = "hand1"
    ARROWS = "fleur"
    PLUS = "plus"


UNICODE_INFO = "\u2139"

DraggableMixinState = Enum(
    "DraggableMixinState", ["READY", "DRAG_WAIT", "LINK_WAIT", "DRAGGING", "LINKING"]
)


class DraggableMixin:  # pylint: disable=R0903
    __state = DraggableMixinState.READY

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.bind("<Button-1>", self.__on_button, add=True)
        self.bind("<Button-3>", self.__on_button, add=True)
        self.bind("<B1-Motion>", self.__on_motion, add=True)
        self.bind("<B3-Motion>", self.__on_motion, add=True)
        self.bind("<ButtonRelease-1>", self.__on_release, add=True)
        self.bind("<ButtonRelease-3>", self.__on_release, add=True)
        self["cursor"] = TkCursors.HAND.value

    def __on_button(self, event):
        if event.state & TkEventState.CONTROL or event.num == 3:
            self.__state = self.__state.LINK_WAIT
        else:
            self.__state = self.__state.DRAG_WAIT
        self.after(500, self.__on_timeout)

    def __place(self, event=None):
        return {
            "relx": (self.winfo_x() + (event.x if event else self.winfo_width() // 2))
            / self.master.winfo_width(),
            "rely": (self.winfo_y() + (event.y if event else self.winfo_height() // 2))
            / self.master.winfo_height(),
            "anchor": "c",
        }

    def __on_timeout(self):
        if self.__state == self.__state.DRAG_WAIT:
            self.__state = self.__state.DRAGGING
            self["cursor"] = TkCursors.ARROWS.value
        elif self.__state == self.__state.LINK_WAIT:
            self.__state = self.__state.LINKING
            self.__ghost = tk.Frame(self.master)
            self.__ghost.place(self.__place())
            self.__ghost_line = ConnectingLine(self.master, self, self.__ghost, "red", True)
            self["cursor"] = TkCursors.PLUS.value

    def __on_motion(self, event):
        if self.__state == self.__state.DRAGGING:
            self.place(self.__place(event))
        elif self.__state == self.__state.LINKING:
            self.__ghost.place(self.__place(event))

    def __on_release(self, event):
        if self.__state == self.__state.LINKING:
            self.__ghost_line.destroy()
            self.__ghost.destroy()
            self.event_generate("<<GhostRelease>>", x=event.x, y=event.y)
        self.__state = self.__state.READY
        self["cursor"] = TkCursors.HAND.value


class FixedUnbindMixin:  # pylint: disable=R0903
    def unbind(self, seq, funcid=None):
        # widget.unbind(seq, funcid) doesn't actually work as documented. This is
        # my own take on the horrible hacks found at https://bugs.python.org/issue31485
        # and https://mail.python.org/pipermail/tkinter-discuss/2012-May/003152.html
        # Also quite horrible.  "I'm not proud, but I'm not tired either"
        if not funcid:
            for f in re.findall(r'^if {"\[(\w+).*$', self.bind(seq), flags=re.M):
                self.deletecommand(f)
            super().unbind(seq, None)
        else:
            self.bind(
                seq,
                re.sub(r'^if {"\[' + funcid + ".*$", "", self.bind(seq), flags=re.M),
            )
            self.deletecommand(funcid)


class ConnectingLine:
    line = None

    def __init__(self, canvas, widget1, widget2, color="black", switch=False):
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

        # special case for dragging invisible frames
        # XXX bit of a hack just to get it to look nice
        if w2 == 1 and h2 == 1:
            x2, y2, w2, h2 = x2 - 20, y2 - 20, 40, 40

        # If there's enough room, extra control points set
        # up a nice spline, and a little extra padding
        # on the destination end to allow for the arrow head.
        _xc, _yc, wc, hc = _geometry(self.canvas)

        if wc > hc:
            if self.switch and x1 > x2:
                x1, y1, w1, h1, x2, y2, w2, h2 = x2, y2, w2, h2, x1, y1, w1, h1
            if 0 < x2 - (x1 + w1) < 50:
                coords = (
                    x1 + w1,
                    y1 + h1 // 2,
                    x2 - 20,
                    y2 + h2 // 2,
                    x2,
                    y2 + h2 // 2,
                )
            else:
                coords = (
                    x1 + w1,
                    y1 + h1 // 2,
                    x1 + w1 + 20,
                    y1 + h1 // 2,
                    x2 - 40,
                    y2 + h2 // 2,
                    x2,
                    y2 + h2 // 2,
                )
        else:
            if self.switch and y1 > y2:
                x1, y1, w1, h1, x2, y2, w2, h2 = x2, y2, w2, h2, x1, y1, w1, h1
            if 0 < y2 - (y1 + h1) < 50:
                coords = (
                    x1 + w1 // 2,
                    y1 + h1,
                    x2 + w2 // 2,
                    y2 - 20,
                    x2 + w2 // 2,
                    y2,
                )
            else:
                coords = (
                    x1 + w1 // 2,
                    y1 + h1,
                    x1 + w1 // 2,
                    y1 + h1 + 20,
                    x2 + w2 // 2,
                    y2 - 40,
                    x2 + w2 // 2,
                    y2,
                )

        if self.line:
            self.canvas.coords(self.line, *coords)
            self.canvas.itemconfig(self.line, smooth=len(coords) > 6)
        else:
            self.line = self.canvas.create_line(
                *coords,
                smooth=len(coords) > 6,
                width=3,
                arrow="last",
                arrowshape=(15, 15, 6),
                fill=self.color,
            )

    def destroy(self):
        self.canvas.delete(self.line)
        self.canvas.unbind("<Configure>", self.canvas_bind)
        self.widget1.unbind("<Configure>", self.widget1_bind)
        self.widget2.unbind("<Configure>", self.widget2_bind)


plugin_classes = sorted(get_plugin_classes(), key=lambda x: x.name)


class PluginChooserFrame(tk.Frame):
    def __init__(self, master, title, callback, *a, **k):
        super().__init__(master, *a, **k)

        self.columnconfigure(0, weight=1)

        label_frame = tk.LabelFrame(self, text=title, padx=10, pady=10)
        label_frame.grid(row=1, column=0, sticky=tk.EW, padx=10, pady=10)

        for n, plugin_class in enumerate(plugin_classes):
            ttk.Button(
                label_frame,
                text=plugin_class.name,
                command=lambda plugin_class=plugin_class: callback(plugin_class),
            ).grid(row=n + 1, column=0, sticky=tk.EW)
            ttk.Label(label_frame, text=plugin_class.title).grid(
                row=n + 1, column=1, sticky=tk.W, padx=10
            )


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
            c.place(
                {
                    "x": c.place_info()["y"],
                    "y": c.place_info()["x"],
                    "relx": c.place_info()["rely"],
                    "rely": c.place_info()["relx"],
                }
            )
        # XXX should flip canvas items as well,
        # but I didn't actually need that so I didn't bother.

    def __on_configure(self, event):
        flipped = event.height > event.width
        if flipped != self.__flipped:
            self.__flip(event.width, event.height)
            self.__flipped = flipped


class DraggableLabel(DraggableMixin, FixedUnbindMixin, tk.Label):
    pass


class DraggableMessage(DraggableMixin, FixedUnbindMixin, tk.Message):
    pass


class GraphWrapper:
    selected_node = None
    highlight_rectangle = None

    def __init__(self, canvas, graph, node_select_callback):
        self.canvas = canvas
        self.graph = graph
        self.node_select_callback = node_select_callback

        self.labels = dict((node, self.label_for_node(node)) for node in graph.nodes)
        self.lines = dict((node, self.lines_for_node(node)) for node in graph.nodes)
        self.lines_lookup = dict(
            (connecting_line.line, (connecting_line, child_node, parent_node))
            for child_node, lines_for_child in self.lines.items()
            for parent_node, connecting_line in lines_for_child.items()
        )

        self.canvas.bind("<Button-1>", self.on_canvas_button1)
        self.canvas.bind("<Button-3>", self.on_canvas_button3)
        self.canvas.bind("<Motion>", self.on_canvas_motion)
        self.canvas.bind("<Leave>", self.on_canvas_leave)

    def label_for_node(self, node):
        label = DraggableLabel(
            self.canvas, text=node.name, wraplength=125, cursor="hand1", takefocus=True
        )
        if not node.position:
            node.position = (random.random() * 0.8 + 0.1, random.random() * 0.8 + 0.1)
        # XXX should be more elegant way of answering the question "are we flipped?"
        if self.canvas.winfo_width() >= self.canvas.winfo_height():
            label.place({"relx": node.position[0], "rely": node.position[1], "anchor": "c"})
        else:
            label.place({"relx": node.position[1], "rely": node.position[0], "anchor": "c"})

        label.bind("<Button-1>", partial(self.on_mousedown, node), add=True)
        label.bind("<Configure>", partial(self.on_configure, node), add=True)
        label.bind("<<GhostRelease>>", partial(self.on_ghost_release, node), add=True)
        label.bind("<Key-Delete>", partial(self.on_delete, node), add=True)
        label.bind("<Enter>", partial(self.on_enter, node), add=True)
        label.bind("<Leave>", partial(self.on_leave, node), add=True)

        return label

    def lines_for_node(self, node):
        return dict(
            (
                parent_node,
                ConnectingLine(self.canvas, self.labels[parent_node], self.labels[node]),
            )
            for parent_node in node.parent_nodes
        )

    def highlight_node(self, node):
        """Highlight the current node in orange"""
        self.selected_node = node
        for label in self.labels.values():
            label["bg"] = "#DDD"
        if node:
            self.labels[node]["bg"] = "orange"

    def on_mousedown(self, node, event):
        """Button-1 click selects a node to view"""
        if self.highlight_rectangle is not None:
            self.canvas.delete(self.highlight_rectangle)
            self.highlight_rectangle = None
        self.highlight_node(node)
        self.node_select_callback(node)

    def on_configure(self, node, event):
        """Stores the updated position of the label in node.position"""
        place_info = event.widget.place_info()
        # XXX should be more elegant way of answering the question "are we flipped?"
        width = self.canvas.winfo_width()
        height = self.canvas.winfo_height()
        if width > height:
            node.position = (float(place_info["relx"]), float(place_info["rely"]))
        else:
            node.position = (float(place_info["rely"]), float(place_info["relx"]))

        # Adapt label sizes to suit the window size, as best we can ...
        label_max_width = max(width // 10, 25)
        label_font_size = int(math.sqrt(width) / math.pi)
        for label in self.labels.values():
            label["wraplength"] = label_max_width
            label["font"] = ("TkDefaultFont", label_font_size)

    def on_enter(self, node, event):
        """Mouse has entered a label"""
        # don't outline labels while dragging
        if event.state & TkEventState.BUTTON1:
            return
        self.labels[node].focus()
        # hide any highlighted lines
        self.on_canvas_leave(event)
        x, y, w, h = _geometry(self.labels[node])
        if self.highlight_rectangle:
            self.canvas.delete(self.highlight_rectangle)
        self.highlight_rectangle = self.canvas.create_rectangle(
            x - 3, y - 3, x + w + 3, y + h + 3, fill="red", width=0
        )

    def on_leave(self, node, event):
        if self.highlight_rectangle is not None:
            self.canvas.delete(self.highlight_rectangle)
            self.highlight_rectangle = None

    def on_canvas_motion(self, event):
        """Show a preview of line selection when the cursor is over line(s)"""
        items = self.canvas.find_overlapping(event.x - 10, event.y - 10, event.x + 10, event.y + 10)
        for connecting_line, _, _ in self.lines_lookup.values():
            color = "red" if connecting_line.line in items else "black"
            self.canvas.itemconfig(connecting_line.line, fill=color)

    def on_canvas_leave(self, event):
        """Called when the mouse leaves the canvas *OR* the mouse enters
        any of the labels, to clear the line selection preview."""
        for connecting_line, _, _ in self.lines_lookup.values():
            self.canvas.itemconfig(connecting_line.line, fill="black")

    def on_canvas_button1(self, event):
        """Click to create a new node, if it is created on top of a line
        that line is broken and the node is included."""

        items = self.canvas.find_overlapping(event.x - 10, event.y - 10, event.x + 10, event.y + 10)

        # XXX creating a new node every time you click on the background
        # is proving quite annoying.  Lets see how it is to go without it.
        # (you can still create a new node by button-3-dragging from an
        # existing node, and if you really want a disconnected graph you can
        # delete the link to the new node!)
        if not items:
            return

        position = (
            event.x / self.canvas.winfo_width(),
            event.y / self.canvas.winfo_height(),
        )
        new_node = self.add_new_node(position)

        for item in items:
            _, child_node, parent_node = self.lines_lookup[item]
            self.del_parent(parent_node, child_node)
            self.add_parent(new_node, child_node)
            self.add_parent(parent_node, new_node)

    def on_canvas_button3(self, event):
        """Button3 on canvas: delete line(s)."""
        items = self.canvas.find_overlapping(event.x - 10, event.y - 10, event.x + 10, event.y + 10)

        for item in items:
            _, child_node, parent_node = self.lines_lookup[item]
            self.del_parent(parent_node, child_node)

    def on_delete(self, node, event):
        """<Delete> disconnects a node from the graph, connects it parents to its children,
        and deletes the node.  <Shift-Delete> removes and deletes the node, but doesn't
        reconnect parents and children.  <Ctrl-Delete> disconnects the node but doesn't
        delete it."""

        self.on_leave(node, event)

        parent_nodes = list(node.parent_nodes)
        child_nodes = list(node.child_nodes)
        for line in self.lines[node].values():
            line.destroy()
        for parent_node in parent_nodes:
            node.del_parent(parent_node)
        for child_node in child_nodes:
            child_node.del_parent(node)
            self.lines[child_node].pop(node).destroy()
            if not event.state & TkEventState.SHIFT:
                for parent_node in parent_nodes:
                    self.add_parent(parent_node, child_node)
        if not event.state & TkEventState.CONTROL:
            self.graph.nodes.remove(node)
            del self.labels[node]
            del self.lines[node]
            event.widget.destroy()

        if len(self.graph.nodes) == 0:
            self.add_new_node(select=True)
        elif node == self.selected_node:
            # arbitrarily pick another node to show
            new_node = (
                parent_nodes[0]
                if parent_nodes
                else child_nodes[0]
                if child_nodes
                else list(self.graph.nodes)[0]
            )
            self.highlight_node(new_node)
            self.node_select_callback(new_node)

    def find_node_at_position(self, x, y):
        for node, label in self.labels.items():
            nx, ny, nw, nh = _geometry(label)
            if (nx <= x <= nx + nw) and (ny <= y <= ny + nh):
                return node
        return None

    def add_new_node(self, position=(0.5, 0.5), select=True):
        new_node = PipelineNode(name=f"NEW {len(self.graph.nodes)+1}", position=position)
        self.graph.add_node(new_node)
        self.labels[new_node] = self.label_for_node(new_node)
        self.lines[new_node] = {}
        if select:
            self.highlight_node(new_node)
            self.node_select_callback(new_node)
        return new_node

    def on_ghost_release(self, start_node, event):
        xl, yl, _wl, _hl = _geometry(event.widget)
        other_node = self.find_node_at_position(event.x + xl, event.y + yl)
        if other_node is None:
            position = (event.x + xl) / self.canvas.winfo_width(), (
                event.y + yl
            ) / self.canvas.winfo_height()
            other_node = self.add_new_node(position)
        elif other_node == start_node:
            return
        elif start_node in other_node.parent_nodes:
            other_node.del_parent(start_node)
            return
        elif other_node in start_node.parent_nodes:
            start_node.del_parent(other_node)
            return

        if start_node.is_ancestor_of(other_node):
            self.add_parent(start_node, other_node)
        elif other_node.is_ancestor_of(start_node):
            self.add_parent(other_node, start_node)
        elif (
            (event.x > 0)
            if self.canvas.winfo_width() > self.canvas.winfo_height()
            else (event.y > 0)
        ):
            self.add_parent(start_node, other_node)
        else:
            self.add_parent(other_node, start_node)

        self.highlight_node(other_node)
        self.node_select_callback(other_node)

    def add_parent(self, parent_node, child_node):
        if parent_node not in child_node.parent_nodes:
            child_node.add_parent(parent_node)
            connecting_line = ConnectingLine(
                self.canvas, self.labels[parent_node], self.labels[child_node]
            )
            self.lines[child_node][parent_node] = connecting_line
            self.lines_lookup[connecting_line.line] = (
                connecting_line,
                child_node,
                parent_node,
            )

    def del_parent(self, parent_node, child_node):
        connecting_line = self.lines[child_node].pop(parent_node)
        del self.lines_lookup[connecting_line.line]
        connecting_line.destroy()
        child_node.del_parent(parent_node)

    def node_changed(self, node):
        """Called when something external updates the node's name, status
        or configuration."""
        self.labels[node]["text"] = node.name

    def destroy(self):
        for node_lines in self.lines.values():
            for line in node_lines.values():
                line.destroy()
        for label in self.labels.values():
            label.destroy()


class ConfiguratorWrapper:
    """Wraps up the PluginConfigurator or PluginChooserFrame, plus a
    DataFramePreview, to make up the larger part of the main window"""

    config_subframe = None
    preview_subframe = None
    config_change_task = None

    def __init__(self, frame, node, change_callback):
        self.frame = frame
        self.node = node
        self.change_callback = change_callback

        self.name_var = tk.StringVar(self.frame, value=node.name)
        tk.Entry(self.frame, textvariable=self.name_var, font=("TkHeadingFont", 14, "bold")).grid(
            row=0, columnspan=2, sticky=tk.EW, padx=10, pady=5
        )
        self.name_var.trace("w", self.name_changed_callback)

        self.label = tk.Label(self.frame, justify=tk.LEFT, wraplength=500)
        self.label.grid(sticky=tk.EW, row=1, padx=10, pady=5)
        self.label.bind("<Configure>", self.on_label_configure)

        self.notes_widget = tk.Text(self.frame, height=5)
        self.notes_widget.insert("1.0", node.notes or "")
        self.notes_widget.bind("<<Modified>>", self.notes_modified_callback)

        self.logger_subframe = LoggerFrame(self.frame)
        self.logger = self.logger_subframe.get_logger(node.name)

        self.show_config_subframe()
        if self.node.plugin:
            self.show_preview_subframe()

    def show_config_subframe(self):
        if self.config_subframe:
            self.config_subframe.destroy()
        if self.node.plugin:
            self.label['text'] = "%s %s — %s" % (
                self.node.plugin.name,
                self.node.plugin.version,
                self.node.plugin.description,
            )
            if self.node.plugin.link:
                tk.Button(
                    self.frame, text=UNICODE_INFO, fg="blue", command=self.on_info_button_press
                ).grid(row=1, column=1, sticky=tk.SE, padx=10)
            self.notes_widget.grid(row=2, columnspan=2,
                                   sticky=tk.EW, padx=10, pady=5)
            self.configurator = PluginConfigurator(
                self.frame, self.node.plugin, self.config_change_callback
            )
            self.config_subframe = self.configurator.frame
        else:
            self.config_subframe = PluginChooserFrame(
                self.frame, "Choose Plugin", self.choose_plugin
            )
        self.config_subframe.grid(row=3, columnspan=2, sticky=tk.NSEW)

    def on_label_configure(self, *_):
        self.label['wraplength'] = self.label.winfo_width() - 20

    def on_info_button_press(self, *_):
        webbrowser.open_new_tab(self.node.plugin.link)

    def show_preview_subframe(self):
        if self.preview_subframe:
            self.preview_subframe.destroy()
        if isinstance(self.node.result, (dd.DataFrame, pd.DataFrame)):
            self.preview_subframe = DataFramePreview(self.frame, self.node.result).frame
        elif isinstance(self.node.result, str):
            self.preview_subframe = tk.Frame(self.frame)
            self.preview_subframe.rowconfigure(1, weight=1)
            n_lines = len(self.node.result.splitlines())
            tk.Label(self.preview_subframe, text=f"Text Preview {n_lines} Lines").grid(
                sticky=tk.NSEW
            )
            text = tk.Text(self.preview_subframe)
            text.insert("1.0", self.node.result)
            text["state"] = "disabled"
            text.grid(sticky=tk.NSEW)
        else:
            self.preview_subframe = tk.Frame(self.frame)
            self.preview_subframe.columnconfigure(0, weight=1)
            tk.Label(self.preview_subframe, text="no result").grid(sticky=tk.EW)

        self.preview_subframe.grid(row=4, columnspan=2, sticky=tk.NSEW)

        if self.logger.count > 0:
            self.logger_subframe.grid(row=5, columnspan=2, sticky=tk.NSEW)
        else:
            self.logger_subframe.grid_forget()

    def name_changed_callback(self, *_):
        name = self.name_var.get()
        self.node.name = name
        self.change_callback(self.node)

    def notes_modified_callback(self, *_):
        self.node.notes = self.notes_widget.get("1.0", tk.END)
        self.notes_widget.edit_modified(False)

    def config_change_callback(self, *_):
        self.node.mark_dirty()
        if self.config_change_task:
            self.frame.after_cancel(self.config_change_task)
        self.config_change_task = self.frame.after(500, self.config_change_task_callback)

    def config_change_task_callback(self):
        self.config_change_task = None
        self.logger.clear()
        self.logger_subframe.grid(row=3, sticky=tk.NSEW)
        self.node.prerun(self.logger)
        if self.node.result is not None:
            self.show_preview_subframe()
        self.configurator.update()
        self.logger_subframe.after(5000, self.config_change_task_callback_2)
        self.change_callback(self.node)

    def config_change_task_callback_2(self):
        if self.logger.count == 0:
            self.logger_subframe.grid_forget()
        else:
            self.logger.progress_hide()

    def choose_plugin(self, plugin_class):
        self.node.plugin = plugin_class()
        self.node.is_dirty = True
        self.show_config_subframe()
        if self.node.name.startswith("NEW "):
            self.node.name = self.node.plugin.name + self.node.name.removeprefix("NEW")
            self.name_var.set(self.node.name)
        self.change_callback(self.node)

    def destroy(self):
        if self.config_change_task:
            self.frame.after_cancel(self.config_change_task)
        if self.config_subframe:
            self.config_subframe.destroy()
        if self.preview_subframe:
            self.preview_subframe.destroy()
        if self.logger_subframe:
            self.logger_subframe.destroy()


class ButtonMenu:  # pylint: disable=R0903
    def __init__(self, tk_parent, buttons):
        self.frame = tk.Frame(tk_parent)
        for button_number, (button_label, button_command) in enumerate(buttons):
            tk.Button(self.frame, text=button_label, command=button_command).grid(
                row=0, column=button_number, sticky=tk.EW
            )
        self.frame.grid(sticky=tk.NSEW)


class MainWindow:
    """Arrange the parts of the main window, with the FrameWrapper on the top
    left and the ConfiguratorWrapper either beside it or under it depending on
    the window dimenstions"""

    graph_wrapper = None
    preview_frame = None
    config_wrapper = None
    config_changed = False

    def __init__(self, tk_parent: tk.Widget, config_filename: Optional[str] = None):
        self.tk_parent = tk_parent

        ButtonMenu(
            tk_parent,
            [
                ("New Config", self.config_new),
                ("Load Config", self.config_load),
                ("Save Config", self.config_save),
                ("Export Config", self.config_export),
                ("Run", self.program_run),
                ("Exit", self.program_exit),
            ],
        )

        self.frame = tk.Frame(tk_parent)
        self.frame.grid(sticky=tk.NSEW)

        self.canvas = FlippyCanvas(self.frame, bg="skyblue")
        self.subframe = tk.Frame(self.frame)
        self.subframe.columnconfigure(0, weight=1)
        self.subframe.rowconfigure(0, weight=0)
        self.subframe.rowconfigure(1, weight=0)
        self.subframe.rowconfigure(2, weight=0)
        self.subframe.rowconfigure(3, weight=1)
        self.subframe.rowconfigure(4, weight=2)
        self.subframe.rowconfigure(5, weight=0)

        self.frame.bind("<Configure>", self.on_frame_configure, add=True)

        if config_filename:
            self.config_load(config_filename)
        else:
            self.config_new()

    def config_new(self):
        if self.config_changed:
            if not messagebox.askokcancel("Clear Config", "Clear unsaved config?"):
                return
        self.config_changed = False
        self.config_filename = None
        if self.graph_wrapper:
            self.graph_wrapper.destroy()
        self.graph = PipelineGraph()
        self.graph_wrapper = GraphWrapper(self.canvas, self.graph, self.node_select)
        self.graph_wrapper.add_new_node(select=True)

    def config_load(self, filename=None):
        if not filename:
            filename = filedialog.askopenfilename(filetypes=[(".INI Config File", "*.ini")])
        if not filename:
            return
        self.config_filename = filename
        if self.graph_wrapper:
            self.graph_wrapper.destroy()
        self.graph = read_config(filename)
        self.graph_wrapper = GraphWrapper(self.canvas, self.graph, self.node_select)
        self.node_select(None)

    def config_save(self, filename=None):
        if not filename:
            filename = filedialog.asksaveasfilename(
                initialfile=self.config_filename,
                filetypes=[(".INI Config File", "*.ini")],
            )
        if not filename:
            return
        write_config(self.graph, filename)
        self.config_changed = False

    def config_export(self, filename=None):
        if not filename:
            if self.config_filename:
                initialfile = self.config_filename.removesuffix(".ini") + ".dot"
            else:
                initialfile = None
            filename = filedialog.asksaveasfilename(
                initialfile=initialfile, filetypes=[("Graphviz File", "*.dot")]
            )
        if not filename:
            return
        export_config_graphviz(self.graph, filename)

    def program_run(self):
        # XXX should be handled in a different thread
        logger = ConsoleLogger()
        self.graph.run(logger)

    def program_exit(self):
        if not self.config_changed or messagebox.askokcancel(
            "Exit", "Exit CountESS without saving config?"
        ):
            self.tk_parent.quit()

    def node_select(self, node):
        for widget in self.subframe.winfo_children():
            widget.destroy()
        if node:
            if self.config_wrapper:
                self.config_wrapper.destroy()
            self.config_wrapper = ConfiguratorWrapper(self.subframe, node, self.node_changed)

    def node_changed(self, node):
        self.config_changed = True
        self.graph_wrapper.node_changed(node)

    def on_frame_configure(self, event):
        """Swaps layout around when the window goes from landscape to portrait"""
        if event.width > event.height:
            x = event.width // 4
            self.canvas.place(x=0, y=0, w=x, h=event.height)
            self.subframe.place(x=x, y=0, w=event.width - x, h=event.height)
        else:
            y = event.height // 4
            self.canvas.place(x=0, y=0, w=event.width, h=y)
            self.subframe.place(x=0, y=y, w=event.width, h=event.height - y)


def make_root():
    try:
        import ttkthemes  # pylint: disable=C0415

        root = ttkthemes.ThemedTk()
        themes = set(root.get_themes())
        for t in ["winnative", "aqua", "clam"]:
            if t in themes:
                root.set_theme(t)
    except ImportError:
        root = tk.Tk()
        # XXX some kind of ttk style setup goes here as a fallback

    # Set up treeview font and row heights.
    style = ttk.Style()
    style.configure("Treeview", font=(None, 10), rowheight=25)

    root.title(f"CountESS {VERSION}")
    root.rowconfigure(0, weight=0)
    root.rowconfigure(1, weight=1)
    root.columnconfigure(0, weight=1)

    # Try to start off maximized, but this option doesn't exist
    # in all Tks or all platforms.
    try:
        root.state("zoomed")
    except tk.TclError:
        pass

    return root


def main():
    root = make_root()
    MainWindow(root, sys.argv[1] if len(sys.argv) > 1 else None)

    root.mainloop()


if __name__ == "__main__":
    main()
