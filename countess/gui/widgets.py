import tkinter as tk
from functools import cache
from importlib.resources import as_file, files
from typing import Optional

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
