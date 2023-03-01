import datetime
import sys
import tkinter as tk
from tkinter import ttk
from typing import MutableMapping, Optional

from countess.core.logger import Logger


class LoggerTreeview(ttk.Treeview):
    def __init__(self, tk_parent, *a, **k):
        super().__init__(tk_parent, *a, **k)
        self["columns"] = ["name", "message"]
        self.heading(0, text="name")
        self.heading(1, text="message")


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
        super().__init__(master, *args, **kwargs)

    def update_label(self, s):
        self.style.configure(self.style_name, text=s)


class LoggerFrame(ttk.Frame):
    def __init__(self, tk_parent, *a, **k):
        super().__init__(tk_parent, *a, **k)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.treeview = LoggerTreeview(self)
        self.treeview.grid(row=0, column=0, sticky=tk.NSEW)

        self.scrollbar_x = ttk.Scrollbar(self, orient=tk.HORIZONTAL, command=self.treeview.xview)
        self.scrollbar_x.grid(row=1, column=0, sticky=tk.EW)
        self.treeview.configure(xscrollcommand=self.scrollbar_x.set)

        self.scrollbar_y = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.treeview.yview)
        self.scrollbar_y.grid(row=0, column=1, sticky=tk.NS)
        self.treeview.configure(yscrollcommand=self.scrollbar_y.set)

        self.progress_frame = tk.Frame(self)
        self.progress_frame.grid(row=2, columnspan=2, sticky=tk.EW)
        self.progress_frame.columnconfigure(0, weight=1)

    def get_logger(self, name: str):
        return TreeviewLogger(self.treeview, self.progress_frame, name)


class TreeviewDetailWindow(tk.Toplevel):
    def __init__(self, detail, *a, **k):
        super().__init__(*a, **k)

        text = tk.Text(self)
        text.insert("1.0", detail)
        text["state"] = "disabled"
        text.pack(anchor=tk.CENTER)

        button = tk.Button(self, text="CLOSE", command=self.destroy)
        button.pack()


class TreeviewLogger(Logger):
    def __init__(self, treeview: ttk.Treeview, progress_frame: tk.Frame, name: str):
        self.treeview = treeview
        self.progress_bar = LabeledProgressbar(progress_frame, mode="determinate", value=0)
        self.progress_bar.update_label(name)
        self.name = name
        self.count = 0
        self.treeview["height"] = 0
        self.detail: MutableMapping[str, str] = {}

        self.treeview.bind("<<TreeviewSelect>>", self.on_click)

    def on_click(self, event):
        # XXX display detail more nicely
        TreeviewDetailWindow(self.detail[self.treeview.focus()])

    def log(self, level: str, message: str, detail: Optional[str] = None):
        # XXX temporary
        sys.stderr.write(message + "\n")
        if detail:
            sys.stderr.write(detail + "\n\n")

        self.count += 1
        datetime_now = datetime.datetime.now()
        values = [self.name, message]
        iid = self.treeview.insert("", "end", text=datetime_now.isoformat(), values=values)
        if detail is not None:
            self.detail[iid] = detail
        self.treeview["height"] = min(self.count, 10)

    def progress(self, message: str = "Running", percentage: Optional[int] = None):
        self.progress_bar.grid(sticky=tk.EW)
        if percentage is not None:
            self.progress_bar.config(mode="determinate", value=percentage)
            self.progress_bar.update_label(f"{self.name}: {message} {percentage}%")
        else:
            self.progress_bar.config(mode="indeterminate")
            self.progress_bar.step(5)
            self.progress_bar.update_label(f"{self.name}: {message}")

    def progress_hide(self):
        self.progress_bar.grid_forget()

    def clear(self):
        self.progress_bar.config(mode="determinate", value=0)
        self.progress_bar.update_label("")
        for row in self.treeview.get_children():
            self.treeview.delete(row)
        self.count = 0
        self.detail = {}

    def __del__(self):
        self.progress_bar.after(5000, lambda pbar=self.progress_bar: pbar.destroy())
