from typing import Optional
import tkinter as tk
import webbrowser
from urllib.parse import urlparse


try:
    from tkinterweb import HtmlFrame  # type: ignore
    no_tkinterweb = False
except ImportError:
    no_tkinterweb = True


MINI_CSS = """
    * { padding: 10px; line-height: 150% }
    th, td { border: 1px solid #AAA; border-collapse: collapse; }
    code { border: 1px solid #AAA; font-family: monospace; }
    th code, td code { border: 0px; }
"""


class MiniBrowserFrame(tk.Frame):
    def __init__(self, tk_parent, start_url, *a, **k):
        super().__init__(tk_parent, *a, **k)
        tk.Label(self, text="Documentation Preview").pack(fill="both")

        self.html_frame = HtmlFrame(self, messages_enabled=False)
        self.html_frame.enable_stylesheets(False)
        self.html_frame.enable_objects(False)
        self.html_frame.enable_forms(False)
        self.html_frame.on_done_loading(self.on_done_loading)
        self.html_frame.on_link_click(self.on_link_click)

        self.html_frame.pack(fill="both", expand=True)

        tk.Button(self, text="Open in Browser", command=self.on_browser_button).pack()

        self.load_url(start_url)

    def load_url(self, link_url):
        self.current_url = link_url
        self.html_frame.load_url(link_url)

    def on_browser_button(self):
        webbrowser.open_new_tab(self.current_url)

    def on_done_loading(self):
        self.html_frame.add_css(MINI_CSS)

    def on_link_click(self, link_url):
        if urlparse(self.current_url)[0:2] == urlparse(link_url)[0:2]:
            self.load_url(link_url)
        else:
            webbrowser.open_new_tab(link_url)

toplevel : Optional[tk.Toplevel] = None
info_frame: Optional[tk.Frame] = None

def mini_browser_close():
    global toplevel  # pylint: disable=global-statement
    if toplevel is not None:
        toplevel.destroy()
        toplevel = None

def mini_browser_open(url: str):
    if no_tkinterweb:
        webbrowser.open_new_tab(url)
        return

    global toplevel, info_frame  # pylint: disable=global-statement
    if toplevel is None:
        toplevel = tk.Toplevel()
        toplevel.protocol("WM_DELETE_WINDOW", mini_browser_close)
        info_frame = MiniBrowserFrame(toplevel, url)
        info_frame.pack(fill="both", expand=True)
    else:
        info_frame.load_url(url)
