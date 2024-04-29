"""countess/gui/tree.py: handle presentation of the datagraph
tree diagram in a canvas."""

# XXX there is much here which should be rewritten. See:
# https://github.com/CountESS-Project/CountESS/issues/19
# for more discussion etc.

import math
import random
import re
import tkinter as tk
from configparser import ConfigParser
from enum import Enum, IntFlag
from functools import partial

from countess.core.config import read_config_dict, write_config_node_string
from countess.core.pipeline import PipelineNode
from countess.gui.widgets import copy_to_clipboard, get_icon


def _limit(value, min_value, max_value):
    return max(min_value, min(max_value, value))


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


DraggableMixinState = Enum("DraggableMixinState", ["READY", "DRAG_WAIT", "LINK_WAIT", "DRAGGING", "LINKING"])


class DraggableMixin:  # pylint: disable=R0903
    __state = DraggableMixinState.READY

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.bind("<Button-1>", self.__on_button, add=True)
        self.bind("<Button-2>", self.__on_button, add=True)
        self.bind("<Button-3>", self.__on_button, add=True)
        self.bind("<B1-Motion>", self.__on_motion, add=True)
        self.bind("<B2-Motion>", self.__on_motion, add=True)
        self.bind("<B3-Motion>", self.__on_motion, add=True)
        self.bind("<ButtonRelease-1>", self.__on_release, add=True)
        self.bind("<ButtonRelease-2>", self.__on_release, add=True)
        self.bind("<ButtonRelease-3>", self.__on_release, add=True)
        self["cursor"] = TkCursors.HAND.value

    def __on_button(self, event):
        if event.state & TkEventState.CONTROL or event.num > 1:
            self.__state = self.__state.LINK_WAIT
        else:
            self.__state = self.__state.DRAG_WAIT
        self.after(100, self.__on_timeout)

    def __place(self, event=None):
        x, y, w, h = _geometry(self)
        x = x + (event.x if event else (w // 2))
        y = y + (event.y if event else (h // 2))
        return {
            "relx": _limit(x / self.master.winfo_width(), 0.05, 0.95),
            "rely": _limit(y / self.master.winfo_height(), 0.05, 0.95),
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
        _xc, _yc, wc, hc = _geometry(self.canvas)

        # size factor to allow for different screen geometries.
        k = (wc + hc) / 100

        x1, y1, w1, h1 = _geometry(self.widget1)
        x2, y2, w2, h2 = _geometry(self.widget2)

        # special case for dragging invisible frames
        # XXX bit of a hack just to get it to look nice
        # based on the cursor size not `k`.
        if w2 == 1 and h2 == 1:
            x2, y2, w2, h2 = x2 - 16, y2 - 16, 32, 32

        # Control points set up a nice spline, and a little extra padding
        # on the destination end to allow for the arrow head.
        if wc > hc:
            if self.switch and x1 > x2:
                x1, y1, w1, h1, x2, y2, w2, h2 = x2, y2, w2, h2, x1, y1, w1, h1
            coords = (
                x1 + w1,
                y1 + h1 // 2,
                x1 + w1 + k,
                y1 + h1 // 2,
                x2 - k * 2,
                y2 + h2 // 2,
                x2,
                y2 + h2 // 2,
            )
        else:
            if self.switch and y1 > y2:
                x1, y1, w1, h1, x2, y2, w2, h2 = x2, y2, w2, h2, x1, y1, w1, h1
            coords = (
                x1 + w1 // 2,
                y1 + h1,
                x1 + w1 // 2,
                y1 + h1 + k,
                x2 + w2 // 2,
                y2 - k * 2,
                x2 + w2 // 2,
                y2,
            )

        arrowshape = (15, 20, 6) if k > 20 else (k * 2 / 3, k, k / 3)
        if self.line:
            self.canvas.coords(self.line, *coords)
            self.canvas.itemconfig(self.line, smooth=True, arrowshape=arrowshape)
        else:
            self.line = self.canvas.create_line(
                *coords,
                smooth=True,
                width=3,
                arrow="last",
                arrowshape=arrowshape,
                fill=self.color,
            )

    def destroy(self):
        self.canvas.delete(self.line)
        self.canvas.unbind("<Configure>", self.canvas_bind)
        self.widget1.unbind("<Configure>", self.widget1_bind)
        self.widget2.unbind("<Configure>", self.widget2_bind)


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


class NodeWrapper(DraggableLabel):
    def update_node(self, node, vertical=False):
        input_bar = node.plugin and node.plugin.num_inputs == 0
        output_bar = node.plugin and node.plugin.num_outputs == 0
        if not input_bar and not output_bar:
            image = None
            compound = tk.NONE
        elif vertical:
            image = get_icon(self, "hbar")
            compound = tk.TOP if input_bar else tk.BOTTOM
        else:
            image = get_icon(self, "vbar")
            compound = tk.LEFT if input_bar else tk.RIGHT
        self.configure(text=node.name, image=image, compound=compound)


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

        self.canvas.bind("<Button-1>", self.on_canvas_button)
        self.canvas.bind("<Button-2>", self.on_canvas_button)
        self.canvas.bind("<Button-3>", self.on_canvas_button)
        self.canvas.bind("<Motion>", self.on_canvas_motion)
        self.canvas.bind("<Leave>", self.on_canvas_leave)
        self.canvas.bind("<Key-Delete>", self.on_canvas_delete)
        self.canvas.bind("<Key-BackSpace>", self.on_canvas_delete)
        self.canvas.bind("<<Paste>>", self.on_paste)

    def label_for_node(self, node):
        label = NodeWrapper(self.canvas, wraplength=125, cursor="hand1", takefocus=True)
        if not node.position:
            node.position = (random.random() * 0.8 + 0.1, random.random() * 0.8 + 0.1)
        # XXX should be more elegant way of answering the question "are we flipped?"
        if self.canvas.winfo_width() >= self.canvas.winfo_height():
            label.place({"relx": node.position[0], "rely": node.position[1], "anchor": "c"})
        else:
            label.place({"relx": node.position[1], "rely": node.position[0], "anchor": "c"})

        label.bind("<Button-1>", partial(self.on_mousedown, node), add=True)
        label.bind("<Configure>", partial(self.on_configure, node, label), add=True)
        label.bind("<<GhostRelease>>", partial(self.on_ghost_release, node), add=True)
        label.bind("<Key-Delete>", partial(self.on_delete, node), add=True)
        label.bind("<Key-BackSpace>", partial(self.on_delete, node), add=True)
        label.bind("<Enter>", partial(self.on_enter, node), add=True)
        label.bind("<Leave>", partial(self.on_leave, node), add=True)
        label.bind("<<Copy>>", partial(self.on_copy, node), add=True)
        label.bind("<<Cut>>", partial(self.on_cut, node), add=True)

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

    def on_configure(self, node, label, event):
        """Stores the updated position of the label in node.position"""
        height = self.canvas.winfo_height()
        width = self.canvas.winfo_width()

        xx = float(label.place_info()["relx"]) * width
        yy = float(label.place_info()["rely"]) * height
        node.position = self.new_node_position(xx, yy)
        label.update_node(node, width < height)

        # Adapt label sizes to suit the window size, as best we can ...
        # XXX very arbitrary and definitely open to tweaking
        if height > width:
            label_max_width = max(width // 6, 25)
            label_font_size = int(math.sqrt(width) / 3)
        else:
            label_max_width = max(width // 12, 16)
            label_font_size = int(math.sqrt(width) / 5)
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
        self.highlight_rectangle = self.canvas.create_rectangle(x - 3, y - 3, x + w + 3, y + h + 3, fill="red", width=0)

    def on_leave(self, node, event):
        if self.highlight_rectangle is not None:
            self.canvas.delete(self.highlight_rectangle)
            self.highlight_rectangle = None
        self.canvas.focus_set()

    def on_copy(self, node, event):
        # Copy the selected node's config to the clipboard
        copy_to_clipboard(write_config_node_string(node))

    def on_cut(self, node, event):
        self.on_copy(node, event)
        self.on_delete(node, event)

    def on_paste(self, event):
        # Try and interpret whatever is in the clipboard as a config
        # file section and create it as a node!  This lets users
        # cut and paste between CountESS instances!
        cp = ConfigParser()
        cp.read_string(self.canvas.clipboard_get())

        for section_name in cp.sections():
            config_dict = cp[section_name]
            node = read_config_dict(section_name, "", config_dict)
            node.position = self.new_node_position(event.x, event.y)
            self.add_node(node)

            # try and reconnect parents if they exist
            nodes_by_name = {n.name: n for n in self.graph.nodes}
            for key, val in config_dict.items():
                if key.startswith("_parent.") and val in nodes_by_name:
                    self.add_parent(nodes_by_name[val], node)

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

    def new_node_position(self, x, y):
        flipped = self.canvas.winfo_height() > self.canvas.winfo_width()
        xp = _limit(x / self.canvas.winfo_width(), 0.05, 0.95)
        yp = _limit(y / self.canvas.winfo_height(), 0.05, 0.95)
        return (yp, xp) if flipped else (xp, yp)

    def on_canvas_button(self, event):
        """Control-Click or Secondary Click to create a new node, if it is created on top of a line
        that line is broken and the node is included."""

        if event.num == 1 and not event.state & TkEventState.CONTROL:
            return

        items = self.canvas.find_overlapping(event.x - 10, event.y - 10, event.x + 10, event.y + 10)

        # XXX creating a new node every time you click on the background
        # is proving quite annoying.  Lets see how it is to go without it.
        # (you can still create a new node by button-3-dragging from an
        # existing node, and if you really want a disconnected graph you can
        # delete the link to the new node!)
        # if not items:
        #    return

        position = self.new_node_position(event.x, event.y)
        new_node = self.add_new_node(position, select=False)

        for item in items:
            _, child_node, parent_node = self.lines_lookup[item]
            self.del_parent(parent_node, child_node)
            self.add_parent(new_node, child_node)
            self.add_parent(parent_node, new_node)

        self.highlight_node(new_node)
        self.node_select_callback(new_node)

    def on_canvas_delete(self, event):
        """Delete key on canvas: delete line(s)."""
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
            new_node = self.add_new_node(select=True)
        elif node == self.selected_node:
            # arbitrarily pick another node to show
            new_node = parent_nodes[0] if parent_nodes else child_nodes[0] if child_nodes else list(self.graph.nodes)[0]
            self.highlight_node(new_node)
            self.node_select_callback(new_node)

    def find_node_at_position(self, x, y):
        for node, label in self.labels.items():
            nx, ny, nw, nh = _geometry(label)
            if (nx <= x <= nx + nw) and (ny <= y <= ny + nh):
                return node
        return None

    def add_node(self, new_node, select: bool = True):
        self.graph.add_node(new_node)
        self.labels[new_node] = self.label_for_node(new_node)
        self.labels[new_node].update()
        self.lines[new_node] = {}
        if select:
            self.highlight_node(new_node)
            self.node_select_callback(new_node)

    def add_new_node(self, position=(0.5, 0.5), select: bool = True):
        new_node = PipelineNode(name="NEW", position=position)
        self.add_node(new_node, select)
        return new_node

    def on_ghost_release(self, start_node, event):
        xl, yl, _wl, _hl = _geometry(event.widget)
        flipped = self.canvas.winfo_height() > self.canvas.winfo_width()
        other_node = self.find_node_at_position(event.x + xl, event.y + yl)
        if other_node is None:
            position = self.new_node_position(event.x + xl, event.y + yl)
            other_node = self.add_new_node(position, select=False)
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
        elif (flipped and event.y > 0) or (not flipped and event.x > 0):
            self.add_parent(start_node, other_node)
        else:
            self.add_parent(other_node, start_node)

        self.highlight_node(other_node)
        self.node_select_callback(other_node)

    def add_parent(self, parent_node, child_node):
        if (
            (not parent_node.plugin or parent_node.plugin.num_outputs)
            and (not child_node.plugin or child_node.plugin.num_inputs)
            and parent_node not in child_node.parent_nodes
        ):
            child_node.add_parent(parent_node)
            connecting_line = ConnectingLine(self.canvas, self.labels[parent_node], self.labels[child_node])
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
        flipped = self.canvas.winfo_width() >= self.canvas.winfo_height()

        if node.plugin and node.name.startswith("NEW"):
            node.name = node.plugin.name
            self.graph.reset_node_name(node)

        self.labels[node].update_node(node, not flipped)

    def destroy(self):
        for node_lines in self.lines.values():
            for line in node_lines.values():
                line.destroy()
        for label in self.labels.values():
            label.destroy()
