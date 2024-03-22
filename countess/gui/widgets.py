import tkinter as tk
from functools import cache
from typing import Optional
from importlib.resources import files, as_file


@cache
def get_bitmap_image(name: str) -> tk.PhotoImage:
    source = files('countess.gui').joinpath(f"icons/{name}.gif")
    with as_file(source) as filepath:
        return tk.PhotoImage(file=filepath)


def info_button(parent: tk.Widget, *args, **kwargs) -> tk.Button:
    kwargs["image"] = get_bitmap_image("info")
    return tk.Button(parent, *args, **kwargs)


def add_button(parent: tk.Widget, *args, **kwargs) -> tk.Button:
    kwargs["image"] = get_bitmap_image("add")
    return tk.Button(parent, *args, **kwargs)


def delete_button(parent: tk.Widget, *args, **kwargs) -> tk.Button:
    kwargs["image"] = get_bitmap_image("del")
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
            self["image"] = get_bitmap_image("check")
            self["state"] = tk.NORMAL
            self["bd"] = 1
        else:
            self["image"] = get_bitmap_image("uncheck")
            self["state"] = tk.NORMAL
            self["bd"] = 1
