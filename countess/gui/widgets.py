from functools import cache

import tkinter as tk
import tkinter.font as tk_font


@cache
def unicode_is_broken():
    font = tk_font.Font()
    return font.measure("\u2795") > 3 * font.measure("m")

def info_button(parent, *args, **kwargs):
    kwargs["text"] = "i" if unicode_is_broken() else "\u2139"
    kwargs["fg"] = "blue"
    return tk.Button(parent, *args, **kwargs)

def add_button(parent, *args, **kwargs):
    kwargs["text"] = "+" if unicode_is_broken() else "\u2795"
    kwargs["width"] = 2
    return tk.Button(parent, *args, **kwargs)

def delete_button(parent, *args, **kwargs):
    kwargs["text"] = "X" if unicode_is_broken() else "\u2715"
    kwargs["width"] = 2
    return tk.Button(parent, *args, **kwargs)


class BooleanCheckbox(tk.Button):

    def __init__(self, *args, **kwargs):
        kwargs["width"] = 2
        super().__init__(*args, **kwargs)

    def set_value(self, value):
        if value is None:
            self["text"] = ""
            self["fg"] = self["bg"]
            self["state"] = tk.DISABLED
            self["bd"] = 0
        elif value:
            self["text"] = "Y" if unicode_is_broken() else "\u2714"
            self["fg"] = "black"
            self["state"] = tk.NORMAL
            self["bd"] = 1
        else:
            self["text"] = "N" if unicode_is_broken() else "\u2717"
            self["fg"] = "grey"
            self["state"] = tk.NORMAL
            self["bd"] = 1
