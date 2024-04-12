import platform
import tkinter as tk
from enum import Enum
from functools import cache
from importlib.resources import as_file, files
from tkinter import filedialog
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


if platform.system() == 'Darwin':
    _RESIZE_CURSOR_H = 'resizeleftright'
    _RESIZE_CURSOR_V = 'resizeupdown'
elif platform.system() == 'Windows':
    _RESIZE_CURSOR_H = 'size_we'
    _RESIZE_CURSOR_V = 'size_ns'
else:
    _RESIZE_CURSOR_H = 'sb_h_double_arrow'
    _RESIZE_CURSOR_V = 'sb_v_double_arrow'


class ResizingFrame(tk.Frame):

    BORDER = 5
    MINSIZE = 50

    _width = 1
    _height = 1

    class Orientation(Enum):
        HORIZONTAL = 1
        VERTICAL = 2
        AUTOMATIC = 3

    class DragInfo(NamedTuple):
        origin: int
        widget1 : tk.Widget
        widget2 : tk.Widget
        widget1_size: int
        widget2_pos: int
        widget2_size: int

    def __init__(self, tk_parent, *a, orientation: Orientation = Orientation.AUTOMATIC, **k):
        self._orientation = orientation
        self._is_horizontal = orientation != self.Orientation.VERTICAL
        self._children : list[tk.Widget] = []
        super().__init__(tk_parent, *a, **k)
        self._default_cursor = self.cget("cursor") or 'arrow'
        self.bind("<Configure>", self.on_configure)
        self.bind("<Button-1>", self.on_click)
        self.bind("<B1-Motion>", self.on_drag)
        self._dragging : tuple(int, tk.Widget, int, tk.Widget, int, int) = None

    def resize_cursor(self):
        if platform.system() == 'Darwin':
            return 'resizeleftright'
        elif platform.system() == 'Windows':
            return 'size_we'
        else:
            return 'sb_h_double_arrow'

    def _resize(self):
        if self._is_horizontal:
            self.configure(cursor = _RESIZE_CURSOR_H)
            width_each = (self._width - (len(self._children) - 1) * self.BORDER) / len(self._children)
            x = 0
            for widget in self._children:
                widget.place(x=x,w=width_each,y=0,h=self._height)
                x += width_each + self.BORDER
        else:
            self.configure(cursor = _RESIZE_CURSOR_V)
            height_each = (self._height - (len(self._children) - 1) * self.BORDER) / len(self._children)
            y = 0
            for widget in self._children:
                widget.place(x=0,w=self._width,y=y,h=height_each)
                y += height_each + self.BORDER

    def on_configure(self, event):
        self._width = event.width
        self._height = event.height
        if self._orientation == self.Orientation.AUTOMATIC:
            self._is_horizontal = self._width > self._height

        self._resize()

    def on_click(self, event):
        prev = None
        for widget in self._children:
            if self._is_horizontal:
                if widget.winfo_x() > event.x:
                    self._dragging = self.DragInfo(
                        origin = event.x,
                        widget1 = prev,
                        widget2 = widget,
                        widget1_size = prev.winfo_width(),
                        widget2_pos = widget.winfo_x(),
                        widget2_size = widget.winfo_width(),
                    )
                    return
            else:
                if widget.winfo_y() > event.y:
                    self._dragging = self.DragInfo(
                        origin = event.y,
                        widget1 = prev,
                        widget2 = widget,
                        widget1_size = prev.winfo_height(),
                        widget2_pos = widget.winfo_y(),
                        widget2_size = widget.winfo_height(),
                    )
                    return
            prev = widget

    def on_drag(self, event):
        if self._is_horizontal:
            drag = event.x - self._dragging.origin
            if self._dragging.widget1_size + drag >= self.MINSIZE and self._dragging.widget2_size - drag >= self.MINSIZE:
                self._dragging.widget1.place(w=self._dragging.widget1_size + drag)
                self._dragging.widget2.place(x=self._dragging.widget2_pos + drag, w = self._dragging.widget2_size - drag)
        else:
            drag = event.y - self._dragging.origin
            if self._dragging.widget1_size + drag >= self.MINSIZE and self._dragging.widget2_size - drag >= self.MINSIZE:
                self._dragging.widget1.place(h=self._dragging.widget1_size + drag)
                self._dragging.widget2.place(y=self._dragging.widget2_pos + drag, h = self._dragging.widget2_size - drag)

    def add_child(self, index: int, widget: tk.Widget):
        if not widget.cget('cursor'):
            widget.configure(cursor=self._default_cursor)
        self._children.insert(index, widget)
        self._resize()

    def add_frame(self, index: int, *a, **k):
        self.add_child(index, tk.Frame(self, *a, **k))
