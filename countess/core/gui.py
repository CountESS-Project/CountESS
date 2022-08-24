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
        for w in self.winfo_children(): w.grid(sticky="ew")
        self.columnconfigure(0,weight=1)
        CancelButton(self, command=self.delete).place(anchor=tk.NE, relx=1, rely=0)

    def delete(self):
        self.master.del_plugin(self)

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

def variable_for_param(master, param, entry_row, entry_column):
    if param['type'] == bool:
        var = tk.BooleanVar(master, value=param.get('default', 0))
        ent = tk.Checkbutton(master, variable=var)
    else:
        if param['type'] == float:
            var = tk.DoubleVar(master, value=param.get('default', 0.0))
        elif param['type'] == int:
            var = tk.IntVar(master, value=param.get('default', 0))
        else:
            var = tk.StringVar(master, value=param.get('default', ''))
        ent = tk.Entry(master, textvariable=var)

    ent.grid(row=entry_row, column=entry_column, sticky="ew")

    return var
    
class PluginConfigurator(ttk.Frame):
    def __init__(self, master, plugin):
        super().__init__(master)
        self.plugin = plugin

        self.param_vars = []
        if self.plugin.params is not None:
            for n, (k, v) in enumerate(self.plugin.params.items()):
                ttk.Label(self, text=v['label']).grid(row=n, column=0)
                var = variable_for_param(self, v, n, 1)
                self.param_vars.append((k, var))

    def get_params(self):
        return dict([
            (k, v.get()) for k,v in self.param_vars
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

        # XXX awful stuff
        self.filenames = []
        self.file_param_vars = []

        self.add_file_button.pack(side="bottom")
        self.subframe.pack(side="left", fill="x", expand=1)
        #self.scrollbar.pack(side="right", fill="y")

    def add_file(self):
        if hasattr(self.plugin, 'file_types'):
            filenames = filedialog.askopenfilenames(filetypes=self.plugin.file_types)
        else:
            filenames = filedialog.askopenfilenames()

        if not self.labels_shown and self.plugin.file_params is not None:
            for n, fp in enumerate(self.plugin.file_params.values()):
                ttk.Label(self.subframe, text=fp['label']).grid(row=0, column=n+1)
            self.labels_shown = True

        for rn, filename in enumerate(filenames, len(self.file_param_vars)):
            filename_var = tk.StringVar(self.subframe, value=filename)
            filename_label = str(pathlib.Path(filename).relative_to(pathlib.Path.cwd()))
            tk.Label(self.subframe, text=str(filename_label)).grid(row=rn+1,column=0,sticky="ew")
            file_param_vars = { "_filename": filename_var }
            if self.plugin.file_params is not None:
                for cn, (k, v) in enumerate(self.plugin.file_params.items()):
                    var = variable_for_param(self.subframe, v, rn+1, cn+1)
                    file_param_vars[k] = var
            self.file_param_vars.append(file_param_vars)
            CancelButton(self.subframe, command=lambda n=rn: self.remove_file(n)).grid(row=rn+1,column=1007)

    def remove_file(self, rn):
        # when deleting rows, we just throw away their variables and widgets and their row collapses
        self.file_param_vars[rn] = None
        for w in self.subframe.grid_slaves(row=rn+1):
            w.destroy()

    def get_file_params(self):
        # skips deleted rows
        return [ 
            dict( (k, v.get()) for k, v in fpv.items() )
            for fpv in self.file_param_vars
            if fpv is not None
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


class PluginChooser(ttk.Frame):
    def __init__(self, master, callback):
        super().__init__(master)
        for n, p in enumerate(plugin_manager.plugins):
            ttk.Button(self, text=p.name, command=partial(callback, p)).grid(row=n, column=0)
            ttk.Label(self, text=p.description).grid(row=n, column=1)

        
class PipelineBuilder(ttk.Frame):
    # XXX TODO allow tabs to get dragged and dropped to reorder

    def __init__(self, master):
        super().__init__(master)
        self.notebook = ttk.Notebook(self)
        self.button = tk.Button(self, text="RUN", fg="green", command=self.run)
        self.pbar = LabeledProgressbar(self, length=500)
        self.plugin_wrappers = []

        self.notebook.pack()
        self.button.pack()
        self.pbar.pack()

        self.notebook.add(PluginChooser(self.notebook, self.add_plugin), text="+ Add Plugin")

    def add_plugin(self, plugin):
        plugin_wrapper = PluginWrapper(self, plugin)
        self.plugin_wrappers.append(plugin_wrapper)
        index = self.notebook.index('end')-1
        self.notebook.insert(index, plugin_wrapper, text=plugin.name, sticky="nsew")
        self.notebook.select(index)
        plugin_wrapper.tab_id = self.notebook.select()

    def del_plugin(self, plugin_wrapper):
        self.notebook.forget(plugin_wrapper.tab_id)
        self.notebook.select(self.notebook.index('end')-1)
        self.plugin_wrappers.remove(plugin_wrapper)

    def run(self):
        self.pbar['mode'] = 'indeterminate'
        self.pbar.start()

        value = None
        for pw in self.plugin_wrappers:
            plugin = pw.plugin(pw.get_params(), pw.get_file_params())
            value = plugin.run_with_progress_callback(value, callback=self.progress_callback)

        self.pbar.stop()
        self.pbar['mode'] = 'determinate'
        self.pbar['value'] = 100
        self.pbar.update_label("Finished")
    
        # XXX temporary
        self.preview_window = tk.Toplevel()
        DataFramePreview(self.preview_window, value).pack()


    def progress_callback(self, a, b, s="Running"):
        if b:
            self.pbar.stop()
            self.pbar['mode'] = 'determinate'
            self.pbar['value'] = (100 * a / b)
            self.pbar.update_label(f"{s} : {a} / {b}")
        else:
            self.pbar['mode'] = 'indeterminate'
            self.pbar.start()
            self.pbar.update_label(f"{s} : {a}" if a is not None else s)


class DataFramePreview(ttk.Frame):
    def __init__(self, master, ddf: dd.DataFrame):
        super().__init__(master)
        self.ddf = ddf
        self.offset = 0
        self.height = 20

        self.index_length = len(ddf.index)
        self.index_skip_list_stride = 10000 if self.index_length > 1000000 else 1000
        self.index_skip_list = list(islice(ddf.index,0,None,self.index_skip_list_stride))

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
            self.set_offset(int((self.index_length-self.height) * x))
        elif action == tk.SCROLL:
            scale = self.height if unit == tk.PAGES else 1
            self.set_offset(self.offset + int(number * scale))

    @lru_cache(maxsize=2000)
    def get_index_from_offset(self, offset: int):
        """this doesn't seem like it'd do much, but actually the scrollbar returns a limited number
        of discrete floating point values (one per pixel height of scrollbar) and page up / page 
        down have fixed offsets too so while it's not easy to predict it can be memoized"""
        # The "skip list" is used to fast-forward to a more helpful place in the index before we start
        # iterating through records one at a time. It is a horrible work-around for the lack of a useful
        # ddf.iloc[n:m] but it seems to work. This function just returns the index value to keep
        # the lru cache small, the caller can then use ddf.loc[index:] to make a slice from that
        # index forward.

        stride = self.index_skip_list_stride
        base_index = self.index_skip_list[offset // stride]
        return next(islice(self.ddf.loc[base_index:].index, offset % stride, offset % stride + 1))
 
    def set_offset(self, offset: int):
        # XXX horribly inefficient PoC
        offset = max(0,min(self.index_length-self.height, offset))
        index = self.get_index_from_offset(offset)
        value_tuples = islice(self.ddf.loc[index:].itertuples(),0,self.height)
        for n, (index, *values) in enumerate(value_tuples):
            if self.treeview.exists(n): self.treeview.delete(n)
            self.treeview.insert('', 'end', iid=n, text=index, values=values)
        self.offset = offset
        self.scrollbar.set(offset/self.index_length, (offset+self.height)/self.index_length)


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

