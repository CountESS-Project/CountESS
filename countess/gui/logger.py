import datetime
import tkinter as tk
from tkinter import ttk

from countess.core.logger import MultiprocessLogger


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
        self.treeview.bind("<<TreeviewSelect>>", self.on_click)

        self.scrollbar_x = ttk.Scrollbar(self, orient=tk.HORIZONTAL, command=self.treeview.xview)
        self.scrollbar_x.grid(row=1, column=0, sticky=tk.EW)
        self.treeview.configure(xscrollcommand=self.scrollbar_x.set)

        self.scrollbar_y = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.treeview.yview)
        self.scrollbar_y.grid(row=0, column=1, sticky=tk.NS)
        self.treeview.configure(yscrollcommand=self.scrollbar_y.set)

        self.progress_frame = tk.Frame(self)
        self.progress_frame.grid(row=2, columnspan=2, sticky=tk.EW)
        self.progress_frame.columnconfigure(0, weight=1)

        self.logger = MultiprocessLogger()
        self.count = 0
        self.details = {}
        self.progress_bars = {}

        self.poll()

    def poll(self):
        datetime_now = datetime.datetime.now()
        for level, message, detail in self.logger.poll():
            if level == "progress":
                try:
                    pbar = self.progress_bars[message]
                except KeyError:
                    pbar = LabeledProgressbar(self.progress_frame, mode="determinate", value=0)
                    self.progress_bars[message] = pbar
                    pbar.update_label(message)
                    pbar.grid(sticky=tk.EW)

                if detail is not None:
                    progress = int(float(detail))
                    pbar.config(mode="determinate", value=progress)
                    pbar.update_label(f"{message} {detail}%")
                    if progress == 100:
                        self.after(2000, self.remove_pbar, message)
                else:
                    pbar.config(mode="indeterminate")
                    pbar.step(5)
                    pbar.update_label(f"{message}")

            else:
                self.count += 1
                iid = self.treeview.insert("", "end", text=datetime_now.isoformat(), values=(level, message))
                if detail:
                    self.details[iid] = detail
        self.after(100, self.poll)

    def get_logger(self, name: str):
        return self.logger

    def on_click(self, _):
        # XXX display detail more nicely
        TreeviewDetailWindow(self.details[self.treeview.focus()])

    def remove_pbar(self, message):
        if message in self.progress_bars:
            self.progress_bars[message].destroy()
            del self.progress_bars[message]


class TreeviewDetailWindow(tk.Toplevel):
    def __init__(self, detail, *a, **k):
        super().__init__(*a, **k)

        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)

        text = tk.Text(self)
        text.insert("1.0", detail)
        text["state"] = "disabled"
        text.grid(sticky=tk.NSEW)

        button = tk.Button(self, text="CLOSE", command=self.destroy)
        button.grid(sticky=tk.EW)
