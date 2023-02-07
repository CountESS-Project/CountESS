import tkinter as tk
from tkinter import ttk
import tkinter.dnd
import re

from countess.core.gui import PluginConfigurator
from countess.plugins.pivot import DaskPivotPlugin
from countess.core.pipeline import Pipeline

def _limit(value, min_value, max_value):
    return max(min_value, min(max_value, value))

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
        self.__start_x = event.x
        self.__start_y = event.y
        self.after(500, self.__on_timeout)
        self.__mousedown = True

    def __on_timeout(self):
        print("TIMEOUT")
        if self.__mousedown and not self.__moving:
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


def _geometry(widget):
    return (
        widget.winfo_x(),
        widget.winfo_y(),
        widget.winfo_width(),
        widget.winfo_height()
    )

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


class PluginChooserFrame(tk.Frame):

    def __init__(self, master, callback, *a, **k):
        super().__init__(master, *a, **k)

        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=2)
        for n, plugin_class in enumerate(Pipeline().plugin_classes):
            ttk.Button(self, text=plugin_class.name, command=lambda plugin_class=plugin_class: callback(plugin_class)).grid(row=n, column=0, sticky=tk.EW)
            ttk.Label(self, text=plugin_class.title).grid(row=n, column=1, sticky=tk.W)


class DraggableLabel(DraggableMixin, FixedUnbindMixin, tk.Message):
    pass

class ConnectingLine:
    line = None
    def __init__(self, canvas, widget1, widget2, color='black', switch=False):
        self.canvas = canvas
        self.widget1 = widget1
        self.widget2 = widget2
        self.color = color
        self.switch = switch

        self.update_line()
        self.widget1_bind = self.widget1.bind("<Configure>", self.update_line, add="+")
        self.widget2_bind = self.widget2.bind("<Configure>", self.update_line, add="+")
        self.canvas.bind("<Configure>", self.update_line, add="+")

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
        # https://bugs.python.org/issue31485
        self.widget1.unbind("<Configure>", self.widget1_bind)
        self.widget2.unbind("<Configure>", self.widget2_bind)
        self.canvas.delete(self.line)


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


root = tk.Tk()
root.rowconfigure(0, weight=1)
root.rowconfigure(1, weight=2)
root.columnconfigure(0, weight=1)
root.columnconfigure(1, weight=2)

canvas = FlippyCanvas(root)
canvas.configure(bg="skyblue")
canvas.grid(sticky=tk.NSEW, row=0, column=0)

_selected_line = None

frame = tk.Frame(root)
#frame.configure(bg="orange")
frame.grid(sticky=tk.NSEW, row=0, column=1)

def root_configure(event):
    if event.widget == root:
        if event.width > event.height:
            root.rowconfigure(1, minsize=0, weight=0)
            root.columnconfigure(1, weight=3)
            frame.grid(rowspan=2, row=0, column=1)
        else:
            root.rowconfigure(1, weight=3)
            root.columnconfigure(1, minsize=0, weight=0)
            frame.grid(row=1, column=0)

root.bind("<Configure>", root_configure)

nodes = []

subframe = None


def node_changed(configurator):
    for node in nodes:
        if node.plugin == configurator.plugin:
            node.name = configurator.plugin.name
            node.label['text'] = node.name
    
def node_clicked(node):
    global subframe
    if subframe: subframe.destroy()
    if node.plugin:
        subframe = PluginConfigurator(frame, node.plugin, node_changed).frame
    else:
        subframe = PluginChooserFrame(frame, lambda plugin_class, node=node: plugin_chosen(node, plugin_class))
    subframe.grid(sticky=tk.NSEW)

def plugin_chosen(node, plugin_class):
    node.plugin = plugin_class()
    node_clicked(node)

def find_node_at_position(x, y):
    for node in nodes:
        nx, ny, nw, nh = _geometry(node.label)
        if (nx <= x <= nx + nw) and (ny <= y <= ny+nh):
            return node
    return None

def dump_nodes_by_stratum():
    # XXX nasty
    for s in range(1,100):
        nn = [ n for n in nodes if n.stratum() == s ]
        if not nn: break
        for n in nn:
            print(f"{s}: {n.name}")

class Node:

    def __init__(self, canvas, label_text, position, parents=[]):
        self.canvas = canvas
        self.name = label_text
        self.label = DraggableLabel(canvas, text=label_text)
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

canvas.bind("<Button-3>", on_button_3, add=True)

nodes.append(Node(canvas, "ZERO", (0.1, 0.75), []))
nodes.append(Node(canvas, "ONE", (0.1, 0.5), []))
nodes.append(Node(canvas, "TWO", (0.3, 0.25), [nodes[1]]))
nodes.append(Node(canvas, "FOO", (0.5, 0.5), [nodes[0],nodes[1],nodes[2]]))
nodes.append(Node(canvas, "BAR", (0.7, 0.25), [nodes[2],nodes[3]]))
nodes.append(Node(canvas, "BAZ", (0.9, 0.33), [nodes[3], nodes[4]]))
nodes.append(Node(canvas, "QUX", (0.9, 0.75), [nodes[0], nodes[3], nodes[4]]))

root.mainloop()
