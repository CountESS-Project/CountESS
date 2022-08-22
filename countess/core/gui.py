# TK based GUI for CountESS 
import threading
import tkinter as tk
import tkinter.ttk as ttk
import ttkthemes # type:ignore
from tkinter import filedialog
import pathlib

from typing import Optional
from collections.abc import Iterable, Sequence
from functools import partial

from tkinter.scrolledtext import ScrolledText

from .plugins import BasePlugin, PluginManager
from functools import lru_cache
from itertools import islice

plugin_manager = PluginManager()

import dask.dataframe as dd

# These classes specialize widgets instead of wrapping them wwhich isn't my favourite way
# but it seems to be clearer in this case.
# The Tkinter "master" terminology is also objectionable.

class CancelButton(tk.Button):
    def __init__(self, master, **kwargs):
        super().__init__(master, text="\u274c", width=1, highlightthickness=0, bd=0, fg="red", **kwargs)

class PluginWrapper(ttk.Frame):
    def __init__(self, master, plugin):
        super().__init__(master)
        self.plugin = plugin

        #ttk.Label(self, text=self.plugin.description).pack()
        #ttk.Separator(self, orient="horizontal").pack(fill='x')
        self.header = PluginHeader(self, plugin)
        self.config = PluginConfigurator(self, plugin)
        if hasattr(self.plugin, 'file_params'):
            self.file_config = PluginFileConfigurator(self, plugin)
        else:
            self.file_config = None
        self.footer = PluginFooter(self, plugin)
        for w in self.winfo_children(): w.grid(sticky="ew")
        self.columnconfigure(0,weight=1)
        CancelButton(self, command=self.delete).place(anchor=tk.NE, relx=1, rely=0)

    def delete(self):
        self.master.del_plugin(self)

    def run(self):
        self.footer.set_progress(42,107,"WOO")

    def get_params(self):
        return self.config.get_params()

    def get_file_params(self):
        return self.file_config.get_file_params() if self.file_config else None

class PluginHeader(ttk.Frame):
    def __init__(self, master, plugin):
        super().__init__(master)
        self.plugin = plugin
        ttk.Label(self, text=self.plugin.name).pack()
        ttk.Label(self, text=self.plugin.description).pack()
        ttk.Separator(self, orient="horizontal").pack()
    
class PluginConfigurator(ttk.Frame):
    def __init__(self, master, plugin):
        super().__init__(master)
        self.plugin = plugin

        self.param_entries = []
        if self.plugin.params is not None:
            for n, (k, v) in enumerate(self.plugin.params.items()):
                ttk.Label(self, text=v['label']).grid(row=n, column=0)
                ee = tk.Entry(self)
                self.param_entries.append(k, ee)
                ee.grid(row=n, column=1, sticky="ew")

    def get_params(self):
        return dict([
            (k, v.get()) for k,v in self.param_entries
        ])

class PluginFileConfigurator(ttk.Frame):
    def __init__(self, master, plugin):
        super().__init__(master)
        self.plugin = plugin
        self.labels_shown = False

        # XXX needs some trickery to make this scrollable
        #self.scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.canvas.yview)
        self.subframe = ttk.Frame(self)
        self.subframe.columnconfigure(0,weight=1)
        self.add_file_button = ttk.Button(self, text="+ Add Files", command=self.add_file)

        self.table_rows = []

        self.add_file_button.pack(side="bottom")
        self.subframe.pack(side="left", fill="x", expand=1)
        #self.scrollbar.pack(side="right", fill="y")

    def add_file(self):
        filenames = filedialog.askopenfilenames()

        if not self.labels_shown and self.plugin.file_params is not None:
            for n, fp in enumerate(self.plugin.file_params.values()):
                ttk.Label(self.subframe, text=fp['label']).grid(row=0, column=n+1)
            self.labels_shown = True

        for rn, filename in enumerate(filenames, len(self.table_rows)):
            fn_entry = ttk.Entry(self.subframe)
            fn_entry.insert(0, filename);
            fn_entry.grid(row=rn+1, column=0, sticky="ew")
            fn_entry.state(['readonly'])
            self.table_rows.append([fn_entry])
            if self.plugin.file_params is not None:
                for cn, (k, v) in enumerate(self.plugin.file_params.items()):
                    ee = ttk.Entry(self.subframe)
                    self.table_rows[-1].append(ee)
                    ee.grid(row=rn+1, column=cn+1)
            cb = CancelButton(self.subframe, command=lambda n=rn: self.remove_file(n))
            cb.grid(row=rn+1, column=107)
            self.table_rows[-1].append(cb)

    def remove_file(self, rn):
        for w in self.table_rows[rn]:
            w.destroy()
        self.table_rows[rn] = []

    def get_file_params(self):
        return [ 
            [ e.get() for e in ee ]
            for ee in self.table_rows
        ]



class LabeledProgressbar(ttk.Progressbar):

    style_data = [
        ('LabeledProgressbar.trough', {
            'children': [
                ('LabeledProgressbar.pbar', {'side': 'left', 'sticky': 'ns'}),
                ("LabeledProgressbar.label", {"sticky": ""})
            ],
            'sticky': 'nswe'
        })
    ]

    def __init__(self, master, *args, **kwargs):
        self.style = ttk.Style(master)
        # make up a new style name so we don't interfere with other LabeledProgressbars
        # and accidentally change their color or label
        self.style_name = f"_id_{id(self)}"
        self.style.layout(self.style_name, self.style_data)
        self.style.configure(self.style_name, background="green")

        kwargs['style'] = self.style_name
        super().__init__(master, *args, **kwargs)

    def update_label(self, s):
        self.style.configure(self.style_name, text=s)


class PluginFooter(ttk.Frame):
    def __init__(self, master, plugin):
        super().__init__(master)
        self.plugin = plugin

        ttk.Separator(self, orient="horizontal").pack(fill='x')
        tk.Button(self, text="RUN", fg="green", command=master.run).pack()
        self.pbar = LabeledProgressbar(self, length=500)
        self.pbar.pack()

    def set_progress(self, a,b,s="Running"):
        if b:
            self.pbar.stop()
            self.pbar['mode'] = 'determinate'
            self.pbar['value'] = (100 * a / b)
            self.pbar.update_label(f"{s} : {a} / {b}")
        else:
            self.pbar['mode'] = 'indeterminate'
            self.pbar.start()
            self.pbar.update_label(f"{s} : {a}" if a is not None else s)
                
class PluginChooser(ttk.Frame):
    def __init__(self, master):
        super().__init__(master)
        for n, p in enumerate(plugin_manager.plugins):
            ttk.Button(self, text=p.name, command=partial(self.choose, p)).grid(row=n, column=0)
            ttk.Label(self, text=p.description).grid(row=n, column=1)

    def choose(self, plugin):
        self.master.add_plugin(plugin)

        

class PipelineBuilder(ttk.Notebook):
    def __init__(self, master):
        super().__init__(master)
        self.enable_traversal()

        self.add(PluginChooser(self), text="+ Add Plugin")
        self.pack()

    def add_plugin(self, plugin):
        plugin_wrapper = PluginWrapper(self, plugin)

        index = self.index('end')-1
        self.insert(index, plugin_wrapper, text=plugin.name, sticky="nsew")
        self.select(index)
        plugin_wrapper.tab_id = self.select()

    def del_plugin(self, plugin_wrapper):
        self.forget(plugin_wrapper.tab_id)
        self.select(self.index('end')-1)


class DataFramePreview(ttk.Frame):
    def __init__(self, master, ddf: dd.DataFrame):
        super().__init__(master)
        self.ddf = ddf
        self.offset = 0
        self.height = 20
        self.index_values = list(ddf.index)
        self.preview_length = len(self.index_values)

        self.treeview = ttk.Treeview(self, height=self.height, selectmode=tk.NONE)
        self.treeview['columns'] = list(ddf.columns)
        for n, c in enumerate(ddf.columns):
            self.treeview.heading(n, text=c)
        self.treeview.bind('<MouseWheel>', self.scroll_event)
        self.treeview.bind('<Button-4>', self.scroll_event)
        self.treeview.bind('<Button-5>', self.scroll_event)

        self.scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL, command=self.yview)

        self.treeview.pack(side="left", fill="x", expand=1)
        self.scrollbar.pack(side="right", fill="y")

        self.set_offset(0)
   
    def scroll_event(self, event):
        if event.num == 4 or event.delta == -120:
            self.set_offset(self.offset-self.height//2)
        elif event.num == 5 or event.delta == +120:
            self.set_offset(self.offset+self.height//2)

    def yview(self, action: str, number: str, unit=None):
        # https://tkdocs.com/shipman/scrollbar-callback.html
        number = float(number)
        if action == tk.MOVETO:
            # the useful range is 0..1 but you can scroll beyond that.
            x = max(0,min(1,number))
            self.set_offset(int((self.preview_length-self.height) * x))
        elif action == tk.SCROLL:
            scale = self.height if unit == tk.PAGES else 1
            self.set_offset(self.offset + int(number * scale))

    def set_offset(self, offset: int):
        # XXX horribly inefficient PoC
        offset = max(0,min(self.preview_length-self.height, offset))
        for n in range(0, self.height):
            if self.treeview.exists(n): self.treeview.delete(n)
        indices = self.index_values[offset:offset+self.height]
        for n, (index, *values) in enumerate(self.ddf.loc[indices[0]:indices[-1]].itertuples()):
            self.treeview.insert('', 'end', iid=n, text=index, values=values)
        self.offset = offset
        self.scrollbar.set(offset/self.preview_length, (offset+self.height)/self.preview_length)




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

    PipelineBuilder(root).grid(sticky="nsew")
    root.rowconfigure(0, weight=1)
    root.columnconfigure(0, weight=1)
    root.mainloop()

if __name__ == '__main__':
    main()

