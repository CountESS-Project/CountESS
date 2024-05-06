import datetime
import tkinter as tk
from tkinter import ttk
from typing import Optional

import pandas as pd

from countess.core.logger import MultiprocessLogger
from countess.gui.tabular import TabularDataFrame


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


class LoggerFrame(tk.Frame):
    def __init__(self, tk_parent: tk.Widget, *a, **k):
        super().__init__(tk_parent, *a, **k)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(0, weight=1)

        self.messages: list[dict] = []

        self.tabular = TabularDataFrame(self)
        self.tabular.grid(row=0, column=0, sticky=tk.NSEW)
        self.tabular.set_click_callback(self.click_callback)
        self.dataframe = pd.DataFrame()

        self.progress_frame = tk.Frame(self)
        self.progress_frame.grid(row=1, columnspan=2, sticky=tk.EW)
        self.progress_frame.columnconfigure(0, weight=1)

        self.detail_window: Optional[tk.Toplevel] = None

        self.logger = MultiprocessLogger()
        self.progress_bars: dict[str, tk.Widget] = {}
        self.count = 0

        self.poll()

    def poll(self):
        datetime_now = datetime.datetime.now()
        update = False
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
                update = True
                self.messages.append(
                    {
                        "row": {
                            "datetime": datetime_now,
                            "level": level,
                            "message": message,
                        },
                        "detail": detail,
                    }
                )
                self.count += 1
        if update:
            self.tabular.set_dataframe(pd.DataFrame([x["row"] for x in self.messages]))
        self.after(100, self.poll)

    def get_logger(self, name: str):
        return self.logger

    def click_callback(self, col: int, row: int, char: int):
        if self.detail_window:
            self.detail_window.destroy()
        if row in self.messages:
            self.detail_window = TreeviewDetailWindow(self.messages[row]["detail"])

    def remove_pbar(self, message):
        if message in self.progress_bars:
            self.progress_bars[message].destroy()
            del self.progress_bars[message]

    def clear(self):
        self.logger.clear()
        self.messages = []
        self.count = 0
        self.tabular.set_dataframe(pd.DataFrame())
        for message in self.progress_bars:
            self.remove_pbar(message)


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
