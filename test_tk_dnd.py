import tkinter as tk
import tkinter.dnd
import re

def _limit(value, min_value, max_value):
    return max(min_value, min(max_value, value))

class DraggableMixin:

    __moving = False
    __ghost = None
    __ghost_line = None

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.bind("<Button-1>", self.__on_start, add="+")
        self.bind("<B1-Motion>", self.__on_motion, add="+")
        self.bind("<ButtonRelease-1>", self.__on_release, add="+")

    def __on_start(self, event):
        self.__start_x = event.x
        self.__start_y = event.y
        self.after(500, self.__on_timeout)

    def __on_timeout(self):
        if not self.__moving:
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
            self.event_generate("<<GhostRelease>>")
            self.__ghost_line.destroy()
            self.__ghost.destroy()
            self.__ghost = None
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


class DraggableLabel(DraggableMixin, FixedUnbindMixin, tk.Label):
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
        print(event)
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

frame = tk.Frame(root)
frame.configure(bg="orange")
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

class Node:
    def __init__(self, canvas, label_text, position, parents=[]):
        self.label = DraggableLabel(canvas, text=label_text)
        self.label.place({'relx': position[0], 'rely': position[1], 'anchor': tk.CENTER})
        self.lines = [ ConnectingLine(canvas, p.label, self.label) for p in parents ]

        self.label.bind("<<GhostRelease>>", self.on_ghost_release)

    def on_enter(self, event):
        print(event)

    def on_leave(self, event):
        print(event)

    def on_ghost_release(self, event):
        print(event)


nodes = []
nodes.append(Node(canvas, "ZERO", (0.1, 0.75), []))
nodes.append(Node(canvas, "ONE", (0.1, 0.5), []))
nodes.append(Node(canvas, "TWO", (0.3, 0.25), [nodes[1]]))
nodes.append(Node(canvas, "FOO", (0.5, 0.5), [nodes[0],nodes[1],nodes[2]]))
nodes.append(Node(canvas, "BAR", (0.7, 0.25), [nodes[2],nodes[3]]))
nodes.append(Node(canvas, "BAZ", (0.9, 0.33), [nodes[3], nodes[4]]))
nodes.append(Node(canvas, "QUX", (0.9, 0.75), [nodes[0], nodes[3], nodes[4]]))

root.mainloop()
