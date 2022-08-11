# TK based GUI for CountESS 
import threading
import tkinter as tk
import tkinter.ttk as ttk
import ttkthemes # type:ignore

from typing import Optional
from collections.abc import Iterable, Sequence

from tkinter.scrolledtext import ScrolledText

from .core.plugins import BasePlugin, PluginManager

plugin_manager = PluginManager()

class PluginFrameWrapper:

    def __init__(self, previous_pfw: PluginFrameWrapper, plugin_choices: Sequence[BasePlugin]):
        self.previous_pfw = previous_pfw
        self.plugin_choices = plugin_choices
        self.dataframe = None

        self.frame = ttk.Frame()
        self.subframe = None

        self.selector = ttk.Combobox(self.frame)
        self.selector['values'] = [ pp.name for pp in self.plugin_choices ]
        self.selector['state'] = 'readonly'
        self.selector.set('Select Plugin')
        self.selector.pack(fill=tk.X, padx=5, pady=5)

        self.subframe = ttk.Frame(self.frame)
        
        self.selector.bind('<<ComboboxSelected>>', self.selected)
        self.selector.pack()

    def selected(self, event: tk.Event):
        if self.subframe:
            self.subframe.destroy()

        self.plugin_selected = self.plugin_choices[self.selector.current()]

        self.subframe = ttk.Frame(self.frame)
        ttk.Label(self.subframe, text=self.plugin_selected.title).grid(row=0,columnspan=2)
        
        text = ScrolledText(self.subframe, height=5)
        text.insert('1.0', self.plugin_selected.description)
        text['state'] = 'disabled'
        text.grid(row=1,columnspan=2, sticky="ew")

        self.param_entries = {}
        if self.plugin_selected.params is not None:
            for n, (k, v) in enumerate(self.plugin_selected.params.items()):
                ttk.Label(self.subframe, text=v['label']).grid(row=n+2, column=0)
                ee = tk.Entry(self.subframe)
                self.param_entries[k] = ee
                ee.grid(row=n+2, column=1, sticky="ew")

        self.last_row = len(self.plugin_selected.params)+2
        self.button = tk.Button(self.subframe, text="Run", command=self.run_clicked)
        self.button.grid(row=self.last_row, columnspan=2)

        self.subframe.pack(expand=True, pady=10, padx=10)

    def get_params(self):
        return dict( (k, ee.get()) for k, ee in self.param_entries.items() )

    def run_clicked(self, *args):
        self.button['state'] = 'disabled'
        t = threading.Thread(target=self.run)
        t.start()

    def run(self):
   
        plugin = self.plugin_selected(**(self.get_params()))

        progress_window = ProgressWindow(self.frame, plugin.name)

        if self.previous_pfw is not None:
            if self.previous_pfw.dataframe is None:
                self.previous_pfw.run()
            ddf = self.previous_pfw.dataframe
        else:
            ddf = None

        try:
            self.dataframe = plugin.run_with_progress_callback(ddf, progress_window.progress_cb)
            progress_window.finished()
        except Exception as e:
            print(e)
            import traceback
            progress_window.show_output(str(e), traceback.format_exception(e))

        self.button['state'] = 'normal'


class ProgressWindow:

    def __init__(self, parent, plugin_name: str):
        self.window = tk.Toplevel()
        self.pbar = ttk.Progressbar(self.window, mode='indeterminate', length=500)
        self.pbar.pack()

        self.label = ttk.Label(self.window)
        self.label['text'] = 'Running'
        self.label.pack()

        self.text : Optional[ScrolledText] = None

    def progress_cb(self, a: int, b: int, s: Optional[str]='Running'):
        if b:
            self.pbar.stop()
            self.pbar['mode'] = 'determinate'
            self.pbar['value'] = (100 * a / b)
            self.label['text'] = f"{s} : {a} / {b}"
        else:
            self.pbar['mode'] = 'indeterminate'
            self.pbar.start()
            if a is not None:
                self.label['text'] = f"{s} : {a}"
            else:
                self.label['text'] = s

    def show_output(self, label: str, text: Iterable[str]):
        self.label['text'] = label

        self.text = ScrolledText(self.window, height=20)
        for n, t in enumerate(text, 1):
            self.text.insert(f'{n}.0', t)
        self.text['state'] = 'disabled'

        self.text.pack()

    def finished(self):
        self.label['text'] = 'Finished'
        self.pbar.stop()
        self.pbar['value'] = 100


class MainWindow:
    """The main plugin selection window.  This should be building a "chain" of plugins
    but for now it just has three tabs, for Input, Transform and Output plugins"""

    def __init__(self, parent):
        self.parent = parent
        notebook = ttk.Notebook(parent)

        ifw = PluginFrameWrapper(None, plugin_manager.get_input_plugins())
        tfw = PluginFrameWrapper(ifw, plugin_manager.get_transform_plugins())
        ofw = PluginFrameWrapper(tfw, plugin_manager.get_output_plugins())

        notebook.add(ifw.frame, text="Input Plugin")
        notebook.add(tfw.frame, text="Transform Plugin")
        notebook.add(ofw.frame, text="Output Plugin")
        notebook.pack(expand=True, fill=tk.BOTH)

    def quit(self):
        self.parent.destroy()


def main():
    root = ttkthemes.ThemedTk()
    root.title('CountESS')

    themes = set(root.get_themes())
    for t in ['winnative', 'aqua', 'ubuntu', 'clam']:
        if t in themes:
            root.set_theme(t)
            break

    MainWindow(root)
    root.mainloop()

if __name__ == '__main__':
    main()

