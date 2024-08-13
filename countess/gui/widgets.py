import platform
import tkinter as tk
from dataclasses import dataclass
from enum import Enum
from functools import cache
from importlib.resources import as_file, files
from tkinter import filedialog, ttk
from typing import List, NamedTuple, Optional, Sequence, Tuple, Union

# To keep the cache of bitmaps smaller, we always associate the image with
# the toplevel, not the individual widget it appears on.


@cache
def get_icon_toplevel(toplevel: tk.Toplevel, name: str) -> tk.Image:
    source = files("countess.gui").joinpath("icons").joinpath(f"{name}.gif")
    with as_file(source) as filepath:
        return tk.PhotoImage(master=toplevel, file=filepath)


def get_icon(widget: tk.Widget, name: str) -> tk.Image:
    return get_icon_toplevel(widget.winfo_toplevel(), name)


def info_button(parent: tk.Widget, *args, **kwargs) -> tk.Button:
    kwargs["image"] = get_icon(parent, "info")
    return tk.Button(parent, *args, **kwargs)


def add_button(parent: tk.Widget, *args, **kwargs) -> tk.Button:
    kwargs["image"] = get_icon(parent, "add")
    return tk.Button(parent, *args, **kwargs)


def delete_button(parent: tk.Widget, *args, **kwargs) -> tk.Button:
    kwargs["image"] = get_icon(parent, "del")
    return tk.Button(parent, *args, **kwargs)


class BooleanCheckbox(tk.Button):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def set_value(self, value: Optional[bool]):
        if value is None:
            self["image"] = None
            self["state"] = tk.DISABLED
            self["bd"] = 0
        elif value:
            self["image"] = get_icon(self, "check")
            self["state"] = tk.NORMAL
            self["bd"] = 1
        else:
            self["image"] = get_icon(self, "uncheck")
            self["state"] = tk.NORMAL
            self["bd"] = 1


def copy_to_clipboard(s: str):
    # XXX very cheesy, but self.clipboard_append() etc didn't
    # seem to work, so this is a terrible workaround ... dump the
    # string into a new tk.Text, select the whole thing and copy it
    # into the clipboard.
    top = tk.Toplevel()
    text = tk.Text(top)
    text.insert(tk.END, s)
    text.tag_add("sel", "1.0", tk.END)
    text.event_generate("<<Copy>>")
    top.destroy()


def _clean_filetype_extension(ext: str):
    # See https://tcl.tk/man/tcl8.6/TkCmd/getOpenFile.htm#M16
    # for the rules of file type extensions

    if ext != "*" and ext.startswith("*"):
        ext = ext[1:]

    if platform.system() == "Darwin":
        # Mac OSX crashes if given a double-dotted extension like ".csv.gz" #27
        try:
            return ext[ext.rindex(".") :]
        except ValueError:
            return "." + ext
    else:
        return ext


def _clean_filetype_extensions(extensions: Union[str, List[str]]):
    if type(extensions) is str:
        extensions = extensions.split()
    return [_clean_filetype_extension(ext) for ext in extensions]


def _clean_filetypes(file_types: Sequence[Tuple[str, Union[str, List[str]]]]):
    return [(label, _clean_filetype_extensions(extensions)) for label, extensions in file_types]


def ask_saveas_filename(initial_file: str, file_types: Sequence[Tuple[str, Union[str, List[str]]]]):
    return filedialog.asksaveasfilename(initialfile=initial_file, filetypes=_clean_filetypes(file_types))


def ask_open_filenames(file_types: Sequence[Tuple[str, Union[str, List[str]]]]):
    return filedialog.askopenfilenames(filetypes=_clean_filetypes(file_types))


def ask_open_filename(file_types: Sequence[Tuple[str, Union[str, List[str]]]]):
    return filedialog.askopenfilename(filetypes=_clean_filetypes(file_types))


if platform.system() == "Darwin":
    _RESIZE_CURSOR_H = "resizeleftright"
    _RESIZE_CURSOR_V = "resizeupdown"
elif platform.system() == "Windows":
    _RESIZE_CURSOR_H = "size_we"
    _RESIZE_CURSOR_V = "size_ns"
else:
    _RESIZE_CURSOR_H = "sb_h_double_arrow"
    _RESIZE_CURSOR_V = "sb_v_double_arrow"


@dataclass
class _ChildInfo:
    widget: tk.Widget
    weight: float


class _DragInfo(NamedTuple):
    origin: int
    widget1: tk.Widget
    widget2: tk.Widget
    widget1_size: int
    widget2_pos: int
    widget2_size: int


class ResizingFrame(tk.Frame):
    BORDER = 5
    MINSIZE = 50

    _width = 1
    _height = 1

    class Orientation(Enum):
        HORIZONTAL = 1
        VERTICAL = 2
        AUTOMATIC = 3

    def __init__(self, tk_parent, *a, orientation: Orientation = Orientation.HORIZONTAL, **k):
        self._orientation = orientation
        self._is_horizontal = orientation != self.Orientation.VERTICAL
        self._children: list[_ChildInfo] = []
        super().__init__(tk_parent, *a, **k)
        self._default_cursor = self.cget("cursor") or "arrow"
        self.configure(cursor=_RESIZE_CURSOR_H if self._is_horizontal else _RESIZE_CURSOR_V)
        self.bind("<Configure>", self.on_configure)
        self.bind("<Button-1>", self.on_click)
        self.bind("<ButtonRelease-1>", self.on_release)
        self.bind("<B1-Motion>", self.on_drag)
        self._dragging: Optional[_DragInfo] = None

    def _resize(self):
        # XXX may end up with a couple of pixels left over due to rounding errors
        total_pixels = (self._width if self._is_horizontal else self._height) - (len(self._children) - 1) * self.BORDER
        total_weight = sum(c.weight for c in self._children)

        pos = 0
        for c in self._children:
            child_size = (c.weight * total_pixels) // total_weight
            if self._is_horizontal:
                c.widget.place(x=pos, w=child_size, y=0, h=self._height)
            else:
                c.widget.place(x=0, w=self._width, y=pos, h=child_size)
            pos += child_size + self.BORDER

    def on_configure(self, event):
        self._width = event.width
        self._height = event.height
        if self._orientation == self.Orientation.AUTOMATIC:
            self._is_horizontal = self._width > self._height
            self.configure(cursor=_RESIZE_CURSOR_H if self._is_horizontal else _RESIZE_CURSOR_V)
        self._resize()

    # XXX this whole DragInfo thing seems overcomplicated, revisit it later.

    def on_click(self, event):
        prev = None
        for c in self._children:
            if self._is_horizontal:
                if c.widget.winfo_x() > event.x:
                    self._dragging = _DragInfo(
                        origin=event.x,
                        widget1=prev,
                        widget2=c.widget,
                        widget1_size=prev.winfo_width(),
                        widget2_pos=c.widget.winfo_x(),
                        widget2_size=c.widget.winfo_width(),
                    )
                    return
            else:
                if c.widget.winfo_y() > event.y:
                    self._dragging = _DragInfo(
                        origin=event.y,
                        widget1=prev,
                        widget2=c.widget,
                        widget1_size=prev.winfo_height(),
                        widget2_pos=c.widget.winfo_y(),
                        widget2_size=c.widget.winfo_height(),
                    )
                    return
            prev = c.widget

    def on_release(self, event):
        self._dragging = None

    def on_drag(self, event):
        if not self._dragging:
            return

        if self._is_horizontal:
            drag = event.x - self._dragging.origin
            if (
                self._dragging.widget1_size + drag >= self.MINSIZE
                and self._dragging.widget2_size - drag >= self.MINSIZE
            ):
                self._dragging.widget1.place(w=self._dragging.widget1_size + drag)
                self._dragging.widget2.place(x=self._dragging.widget2_pos + drag, w=self._dragging.widget2_size - drag)
            sizes = [c.widget.winfo_width() for c in self._children]
        else:
            drag = event.y - self._dragging.origin
            if (
                self._dragging.widget1_size + drag >= self.MINSIZE
                and self._dragging.widget2_size - drag >= self.MINSIZE
            ):
                self._dragging.widget1.place(h=self._dragging.widget1_size + drag)
                self._dragging.widget2.place(y=self._dragging.widget2_pos + drag, h=self._dragging.widget2_size - drag)
            sizes = [c.widget.winfo_height() for c in self._children]

        total_size = sum(sizes)
        for n, c in enumerate(self._children):
            c.weight = sizes[n] / total_size

    def add_child(self, widget: tk.Widget, index: Optional[int] = None, weight: float = 1.0):
        if index is None:
            index = len(self._children)
        if not widget.cget("cursor"):
            widget["cursor"] = self._default_cursor
        self._children.insert(index, _ChildInfo(widget, weight))
        self._resize()

    def add_frame(self, *a, index: Optional[int] = None, weight: float = 1.0, **k) -> tk.Frame:
        frame = tk.Frame(self, *a, **k)
        self.add_child(frame, index, weight)
        return frame

    def remove_child(self, widget: tk.Widget) -> tk.Widget:
        self._children = [c for c in self._children if c.widget != widget]
        self._resize()
        return widget

    def replace_child(self, old_widget: tk.Widget, new_widget: tk.Widget):
        self._children = [
            _ChildInfo(
                widget=new_widget if c.widget == old_widget else c.widget,
                weight=c.weight,
            )
            for c in self._children
        ]
        self._resize()


class LabeledProgressbar(ttk.Progressbar):
    """A progress bar with a label on top of it, the progress bar value can be set in the
    usual way and the label can be set with self.update_label"""

    # see https://stackoverflow.com/a/40348163/90927 for how the styling works.

    style_data = [
        (
            "LabeledProgressbar.trough",
            {
                "children": [
                    ("LabeledProgressbar.pbar", {"side": "left", "sticky": tk.NS}),
                    ("LabeledProgressbar.label", {"sticky": ""}),
                ],
                "sticky": tk.NSEW,
            },
        )
    ]

    def __init__(self, master, *args, **kwargs):
        self.style = ttk.Style(master)
        # make up a new style name so we don't interfere with other LabeledProgressbars
        # and accidentally change their color or label (uses arbitrary object ID)
        self.style_name = f"_id_{id(self)}"
        self.style.layout(self.style_name, self.style_data)
        self.style.configure(self.style_name, background="green")

        kwargs["style"] = self.style_name
        self.done = False
        super().__init__(master, *args, **kwargs)

    def progress_update(self, text: str, percentage: Optional[int] = None):
        if percentage is None:
            self.config(mode="indeterminate")
            self.step(5)
        else:
            self.done = percentage == 100
            self.config(mode="determinate", value=percentage)
        self.style.configure(self.style_name, text=text)
