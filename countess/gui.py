# TK based GUI for CountESS 
import threading
import tkinter as tk
import tkinter.ttk as ttk
from tkinter.scrolledtext import ScrolledText

from countess.core.plugins import PluginManager

plugin_manager = PluginManager()

class PluginFrameWrapper:

    def __init__(self, previous_pfw, plugin_choices):
        self.previous_pfw = previous_pfw
        self.plugin_choices = plugin_choices
        self.dataframe = None

        self.frame = ttk.Frame()
        self.subframe = None
        self.label = ttk.Label(self.frame, text="Select Plugin").pack(fill=tk.X, padx=5, pady=5)

        self.selector = ttk.Combobox(self.frame)
        self.selector['values'] = [ pp.name for pp in self.plugin_choices ]
        self.selector['state'] = 'readonly'
        self.selector.pack(fill=tk.X, padx=5, pady=5)

        self.subframe = ttk.Frame(self.frame)
        
        self.selector.bind('<<ComboboxSelected>>', self.selected)
        self.selector.pack()

    def selected(self, event):
        if self.subframe:
            self.subframe.destroy()

        self.plugin_selected = self.plugin_choices[self.selector.current()]

        self.subframe = ttk.Frame(self.frame)
        ttk.Label(self.subframe, text=self.plugin_selected.title).grid(row=0,columnspan=2)
        
        text = ScrolledText(self.subframe, width=40, height=5)
        text.insert('1.0', self.plugin_selected.description)
        text.grid(row=1,columnspan=2)

        self.param_entries = {}
        for n, (k, v) in enumerate(self.plugin_selected.params.items()):
            ttk.Label(self.subframe, text=v['label']).grid(row=n+2, column=0)
            ee = tk.Entry(self.subframe)
            self.param_entries[k] = ee
            ee.grid(row=n+2, column=1)

        self.last_row = len(self.plugin_selected.params)+2
        self.button = tk.Button(self.subframe, text="Run", command=self.run_clicked)
        self.button.grid(row=self.last_row, columnspan=2)

        self.subframe.pack(pady=10, padx=10)

    def get_params(self):
        return dict( (k, ee.get()) for k, ee in self.param_entries.items() )

    def run_clicked(self, *args):
        self.button.destroy()
        self.pbar = ttk.Progressbar(self.subframe, mode='indeterminate', length=500)
        self.pbar.grid(row=self.last_row,columnspan=2)
        self.pbar.start()

        t = threading.Thread(target=self.run)
        t.start()

    def run(self):
    
        def progress_cb(a, b):
            print(f"{plugin.name} running: {a} / {b}")
            self.pbar.stop()
            self.pbar['mode'] = 'determinate'
            self.pbar['value'] = (100 * a / b) if b else 0
       
        plugin = self.plugin_selected(**(self.get_params()))

        if self.previous_pfw is not None:
            if self.previous_pfw.dataframe is None:
                self.previous_pfw.run()
            ddf = self.previous_pfw.dataframe
        else:
            ddf = None

        print(f"{plugin.name} starting")
        self.dataframe = plugin.run_with_progress_callback(ddf, progress_cb)

        self.pbar.stop()
        self.pbar['value'] = 100
        
        print(f"{plugin.name} finished")
    
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
        notebook.pack()

    def quit(self):
        self.parent.destroy()
 
root = tk.Tk()
window = MainWindow(root)
root.mainloop()

