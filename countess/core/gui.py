# TK based GUI for CountESS 
import threading
import tkinter as tk
import tkinter.ttk as ttk
import ttkthemes # type:ignore
from tkinter import filedialog
import pathlib

from typing import Optional, NamedTuple
from collections.abc import Iterable, Sequence
from functools import partial

from tkinter.scrolledtext import ScrolledText

from .plugins import BasePlugin, PluginManager, FileInputMixin, PluginParam
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


class ParameterWidgets(NamedTuple):
    label: tk.Widget
    entry: tk.Widget
    var: tk.Variable

def widgets_for_parameter(tk_parent: tk.Widget, parameter: PluginParam) -> ParameterWidgets:
    label = ttk.Label(tk_parent, text=parameter.label)

    var: tk.Variable
    entry: tk.Widget
    
    if parameter.var_type == bool:
        var = tk.BooleanVar(tk_parent, value=parameter.value)
    elif parameter.var_type == float:
        var = tk.DoubleVar(tk_parent, value=parameter.value)
    elif parameter.var_type == int:
        var = tk.IntVar(tk_parent, value=parameter.value)
    else:
        var = tk.StringVar(tk_parent, value=parameter.value)
    
    if parameter.readonly:
        # XXX gross
        entry = tk.Label(tk_parent, text=parameter.value)
    elif parameter.var_type == bool:
        entry = tk.Checkbutton(tk_parent, variable=var)
    else:
        entry = tk.Entry(tk_parent, textvariable=var)

    return ParameterWidgets(label, entry, var)
    

class PipelineManager:
    # XXX allow some way to drag and drop tabs
    
    def __init__(self, tk_parent):
        self.frame = ttk.Frame(tk_parent)
        self.frame.grid(sticky=tk.NSEW)
        self.notebook = ttk.Notebook(self.frame)
        self.button = tk.Button(self.frame, text="RUN", fg="green", command=self.run)
        #self.pbar = LabeledProgressbar(self.frame, length=500)
        self.plugins = []
        
        self.plugin_chooser_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.plugin_chooser_frame, text="+")
        self.update_plugin_chooser_frame()
        
        self.notebook.pack()
        self.button.pack()
        #self.pbar.pack()
        
        self.parameter_cache = dict()
        
    def update_plugin_chooser_frame(self):
        last_plugin = self.plugins[-1] if len(self.plugins) else None
        for w in self.plugin_chooser_frame.winfo_children(): w.destroy()
        plugin_classes =  plugin_manager.plugin_classes_following(last_plugin)
        for n, plugin_class in enumerate(plugin_classes):
            add_callback = partial(self.add_plugin, plugin_class, last_plugin)
            ttk.Button(self.plugin_chooser_frame, text=plugin_class.name, command=add_callback).grid(row=n, column=0)
            ttk.Label(self.plugin_chooser_frame, text=plugin_class.description).grid(row=n, column=1)

    def add_plugin(self, plugin_class, previous_plugin):
        plugin = plugin_class(previous_plugin)
        self.plugins.append(plugin)
        
        frame = ttk.Frame(self.notebook)
        index = self.notebook.index('end')-1
        self.notebook.insert(index, frame, text=plugin.name, sticky=tk.NSEW)
        self.notebook.select(index)
        tab_id = self.notebook.select()
        
        tk.Label(frame, text=plugin.description).grid(row=0, columnspan=2, sticky=tk.EW)
        
        cancel_command = lambda: self.delete_plugin_frame(plugin, frame)
        CancelButton(frame, command=cancel_command).place(anchor=tk.NE, relx=1, rely=0)
        
        frame.columnconfigure(0,weight=1)
        frame.columnconfigure(1,weight=2)
        
        self.display_plugin_configuration(frame, plugin)
        self.update_plugin_chooser_frame()

    def delete_plugin_frame(self, plugin, frame):
        self.notebook.forget(frame)
        self.notebook.select(self.notebook.index('end')-1)
        self.plugins.remove(plugin)
        
    def display_plugin_configuration(self, frame, plugin):
        
        if plugin.parameters:
            for n, (name, parameter) in enumerate(plugin.parameters.items()):
                # XXX make this cache stuff nicerer
                try:
                    widgets = self.parameter_cache[parameter.name]
                except KeyError:
                    widgets = self.parameter_cache[parameter.name] = widgets_for_parameter(frame, parameter)
                    change_callback = partial(self.plugin_change_parameter, frame, plugin, parameter, widgets.var)
                    widgets.var.trace("w", change_callback)
                
                widgets.label.grid(row=n+1, column=0, sticky=tk.EW)
                widgets.entry.grid(row=n+1, column=1, sticky=tk.EW)
                                    
        # XXX handle disappearing parameters by hiding or destroying label and entry.
                
        if isinstance(plugin, FileInputMixin):
            tk.Button(frame, text="+ Add File", command=partial(self.add_file, frame, plugin)).grid(row=1000,column=0)
            
    def add_file(self, frame, plugin):
        filenames = filedialog.askopenfilenames(filetypes=plugin.file_types)
        for filename in filenames:
            plugin.add_file(filename)
        self.display_plugin_configuration(frame, plugin)    

    def plugin_change_parameter(self, frame: tk.Frame, plugin: BasePlugin, parameter: PluginParam, var: tk.Variable, *_):
        parameter.value = var.get()
        self.display_plugin_configuration(frame, plugin)

    def run(self):
        
        for num, plugin in enumerate(self.plugins):
           if plugin.parameters:
               for name, parameter in plugin.parameters.items():
                   print(f'{num} {plugin.name} {name} {parameter.value}')
           else:
               print(f'{num} {plugin.name}')
        
        run_window = PipelineRunner(self.plugins)
        run_window.run()
        
class PipelineRunner:
    def __init__(self, plugins):
        self.plugins = plugins
        self.pbars = []
        
        self.frame = tk.Frame(tk.Toplevel())
        self.frame.pack()
        
        for num, plugin in enumerate(self.plugins):
            ttk.Label(self.frame, text=plugin.title).pack()
            pbar = LabeledProgressbar(self.frame)
            self.pbars.append(pbar) 
            pbar.pack()
        
    def run(self):
        value = None
        
        for num, plugin in enumerate(self.plugins):
            pbar = self.pbars[num]
            
            def progress_callback(a, b, s="Running"):
                if b:
                    pbar.stop()
                    pbar['mode'] = 'determinate'
                    pbar['value'] = (100 * a / b)
                    pbar.update_label(f"{s} : {a} / {b}" if b > 1 else s)
                else:
                    pbar['mode'] = 'indeterminate'
                    pbar.start()
                    pbar.update_label(f"{s} : {a}" if a is not None else s)
                
            progress_callback(0,0,"Running")
            
            value = plugin.run_with_progress_callback(value, callback=progress_callback)
        
            progress_callback(1,1,"Finished")
            
        if isinstance(value, dd.DataFrame):
            DataFramePreview(self.frame, value)
        

class DataFramePreview:
    """Provides a visual preview of a Dask dataframe arranged as a table."""
    def __init__(self, tk_parent, ddf: dd.DataFrame):
        self.frame = ttk.Frame(tk_parent)
        self.ddf = ddf.compute()
        self.offset = 0
        self.height = 20

        self.index_length = len(ddf.index)
        self.index_skip_list_stride = 10000 if self.index_length > 1000000 else 1000
        self.index_skip_list = list(islice(ddf.index,0,None,self.index_skip_list_stride))

        self.treeview = ttk.Treeview(self.frame, height=self.height, selectmode=tk.NONE)
        self.treeview['columns'] = list(ddf.columns)
        for n, c in enumerate(ddf.columns):
            self.treeview.heading(n, text=c)
        self.treeview.bind('<MouseWheel>', self.scroll_event)
        self.treeview.bind('<Button-4>', self.scroll_event)
        self.treeview.bind('<Button-5>', self.scroll_event)
        
        self.scrollbar = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.scroll_command)

        self.treeview.pack(side="left", fill="x", expand=1)
        self.scrollbar.pack(side="right", fill="y")

        self.set_offset(0)
   
    def scroll_event(self, event):
        """Called when mousewheeling on the treeview"""
        if event.num == 4 or event.delta == -120:
            self.set_offset(self.offset-self.height//2)
        elif event.num == 5 or event.delta == +120:
            self.set_offset(self.offset+self.height//2)

    def scroll_command(self, action: str, number: str, unit=None):
        """Called when scrolling the scrollbar"""
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
        # the lru cache small, the caller can then use `islice(ddf.loc[index:], 0, n)`
        # to efficiently fetch `n` records starting at `index`.

        stride = self.index_skip_list_stride
        base_index = self.index_skip_list[offset // stride]
        return next(islice(self.ddf.loc[base_index:].index, offset % stride, offset % stride + 1))
 
    def set_offset(self, offset: int):
        # XXX horribly inefficient PoC
        offset = max(0,min(self.index_length-self.height, offset))
        index = self.get_index_from_offset(offset)
        value_tuples = islice(self.ddf.loc[index:].itertuples(),0,self.height)
        for n, (index, *values) in enumerate(value_tuples):
            row_id = str(n)
            if self.treeview.exists(row_id): self.treeview.delete(row_id)
            self.treeview.insert('', 'end', iid=row_id, text=index, values=values)
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

    #PipelineBuilder(root).grid(sticky="nsew")
    PipelineManager(root)
    root.rowconfigure(0, weight=1)
    root.columnconfigure(0, weight=1)
    root.mainloop()

if __name__ == '__main__':
    main()

