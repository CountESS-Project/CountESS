import tkinter as tk
from functools import cache
from typing import Optional
from importlib.resources import files, as_file


@cache
def get_bitmap_image(parent: tk.Widget, name: str) -> tk.PhotoImage:
    source = files('countess.gui').joinpath('icons').joinpath(f"{name}.gif")
    with as_file(source) as filepath:
        return tk.PhotoImage(master=parent, file=filepath)


def info_button(parent: tk.Widget, *args, **kwargs) -> tk.Button:
    kwargs["image"] = get_bitmap_image(parent.winfo_toplevel(), "info")
    return tk.Button(parent, *args, **kwargs)


def add_button(parent: tk.Widget, *args, **kwargs) -> tk.Button:
    kwargs["image"] = get_bitmap_image(parent.winfo_toplevel(), "add")
    return tk.Button(parent, *args, **kwargs)


def delete_button(parent: tk.Widget, *args, **kwargs) -> tk.Button:
    kwargs["image"] = get_bitmap_image(parent.winfo_toplevel(), "del")
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
            self["image"] = get_bitmap_image(self.winfo_toplevel(), "check")
            self["state"] = tk.NORMAL
            self["bd"] = 1
        else:
            self["image"] = get_bitmap_image(self.winfo_toplevel(), "uncheck")
            self["state"] = tk.NORMAL
            self["bd"] = 1
