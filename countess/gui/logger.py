import datetime
import tkinter as tk
from typing import Optional

import pandas as pd

from countess.core.logger import MultiprocessLogger
from countess.gui.tabular import TabularDataFrame
from countess.gui.widgets import LabeledProgressbar


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

        self.poll()

    @property
    def count(self):
        return len(self.messages)

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
        if update:
            self.tabular.set_dataframe(pd.DataFrame([x["row"] for x in self.messages]))
        self.after(100, self.poll)

    def get_logger(self, name: str):
        return self.logger

    def click_callback(self, col: int, row: int, char: int):
        if self.detail_window:
            self.detail_window.destroy()
        if self.messages[row]["detail"]:
            self.detail_window = LoggerDetailWindow(self.messages[row]["detail"])

    def remove_pbar(self, message):
        if message in self.progress_bars:
            self.progress_bars[message].destroy()
            del self.progress_bars[message]

    def clear(self):
        self.logger.clear()
        self.messages = []
        self.tabular.set_dataframe(pd.DataFrame())
        for message in self.progress_bars:
            self.remove_pbar(message)


class LoggerDetailWindow(tk.Toplevel):
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
