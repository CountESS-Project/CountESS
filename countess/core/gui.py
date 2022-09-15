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
from threading import Thread

from tkinter.scrolledtext import ScrolledText

from .plugins import BasePlugin, PluginManager, FileInputMixin
from .parameters import BaseParam, BooleanParam, IntegerParam, FloatParam, StringParam, FileParam

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

def widgets_for_parameter(tk_parent: tk.Widget, parameter: BaseParam) -> ParameterWidgets:
    label = ttk.Label(tk_parent, text=parameter.label)

    var: tk.Variable
    entry: tk.Widget
   
    if isinstance(parameter, BooleanParam):
        var = tk.BooleanVar(tk_parent, value=parameter.value)
    elif isinstance(parameter, FloatParam):
        var = tk.DoubleVar(tk_parent, value=parameter.value)
    elif isinstance(parameter, IntegerParam):
        var = tk.IntVar(tk_parent, value=parameter.value)
    elif isinstance(parameter, StringParam):
        var = tk.StringVar(tk_parent, value=parameter.value)

    if isinstance(parameter, FileParam):
        entry = tk.Frame(tk_parent)
        tk.Label(entry, text=parameter.value).grid(row=0,column=0)

        def cancel_command(): var['value'] = ''

        CancelButton(entry, command=cancel_command).grid(row=0,column=1)
        entry.columnconfigure(0, weight=1)

    if parameter.read_only:
        # XXX gross
        entry = tk.Label(tk_parent, text=parameter.value)
    elif isinstance(parameter, BooleanParam):
        entry = tk.Checkbutton(tk_parent, variable=var)
    elif parameter.choices:
        entry = ttk.Combobox(tk_parent, textvariable=var)
        entry['values'] = parameter.choices
        entry.state(["readonly"])
    else:
        entry = tk.Entry(tk_parent, textvariable=var)

    print((parameter, label, entry, var))
    return ParameterWidgets(label, entry, var)
    

class PluginAndFrame(NamedTuple):
    plugin: BasePlugin
    frame: tk.Frame

class PipelineManager:
    # XXX allow some way to drag and drop tabs
    
    def __init__(self, tk_parent):
        self.frame = ttk.Frame(tk_parent)
        self.frame.grid(sticky=tk.NSEW)
        self.notebook = ttk.Notebook(self.frame)
        self.button = tk.Button(self.frame, text="RUN", fg="green", command=self.run)
        self.plugins = []
        self.tabs: Mapping[str,PluginAndFrame] = {}
        
        self.plugin_chooser_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.plugin_chooser_frame, text="+")
        self.update_plugin_chooser_frame()
        self.notebook.bind("<<NotebookTabChanged>>", self.notebook_tab_changed)
        
        self.notebook.grid(row=0, sticky=tk.NSEW)
        self.button.grid(row=1,sticky=tk.EW)
        self.frame.columnconfigure(0,weight=1)
        self.frame.rowconfigure(0,weight=1)
        
        self.parameter_cache = dict()
    
    def notebook_tab_changed(self, event):
        tab = self.tabs.get(self.notebook.select())
        if tab:
            plugin_number = self.plugins.index(tab.plugin)
            previous_plugin = self.plugins[plugin_number-1] if plugin_number>0 else None
            print(f"notebook_tab_changed {tab} {plugin_number} {previous_plugin}")
            self.display_plugin_configuration(tab.frame, tab.plugin, previous_plugin)
        else:
            print(f"notebook_tab_changed to chooser {self.notebook.select()}")
            self.update_plugin_chooser_frame()
            return

    def update_plugin_chooser_frame(self):
        last_plugin = self.plugins[-1] if len(self.plugins) else None
        for w in self.plugin_chooser_frame.winfo_children(): w.destroy()
        plugin_classes =  plugin_manager.plugin_classes_following(last_plugin)
        for n, plugin_class in enumerate(plugin_classes):
            add_callback = partial(self.add_plugin, plugin_class, last_plugin)
            ttk.Button(self.plugin_chooser_frame, text=plugin_class.name, command=add_callback).grid(row=n, column=0)
            ttk.Label(self.plugin_chooser_frame, text=plugin_class.description).grid(row=n, column=1)

    def add_plugin(self, plugin_class, previous_plugin):
        plugin = plugin_class()
        self.plugins.append(plugin)
        
        frame = ttk.Frame(self.notebook)
        index = self.notebook.index('end')-1
        self.notebook.insert(index, frame, text=plugin.name, sticky=tk.NSEW)
        self.notebook.select(index)
        tab_id = self.notebook.select()
        self.tabs[tab_id] = PluginAndFrame(plugin, frame)

        print(f"added {tab_id} {plugin} {frame}")
        
        tk.Label(frame, text=plugin.description).grid(row=0, columnspan=2, sticky=tk.EW)
        
        cancel_command = lambda: self.delete_plugin_frame(tab_id)
        CancelButton(frame, command=cancel_command).place(anchor=tk.NE, relx=1, rely=0)
        
        frame.columnconfigure(0,weight=1)
        frame.columnconfigure(1,weight=2)
        
        self.display_plugin_configuration(frame, plugin, previous_plugin)
        self.update_plugin_chooser_frame()

    def delete_plugin_frame(self, tab_id):
        tab = self.tabs.pop(tab_id)
        self.notebook.forget(tab.frame)
        self.notebook.select(self.notebook.index('end')-1)
        
    def display_plugin_configuration(self, frame, plugin, previous_plugin=None):
        if previous_plugin:
            plugin.set_previous_plugin(previous_plugin)

        print(f"display_plugin_configuration {plugin.parameters}")
        
        if plugin.parameters:
            for n, (key, parameter) in enumerate(plugin.parameters.items()):
                # XXX make this cache stuff nicerer
                try:
                    widgets = self.parameter_cache[key]
                except KeyError:
                    widgets = self.parameter_cache[key] = widgets_for_parameter(frame, parameter)
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

    def plugin_change_parameter(self, frame: tk.Frame, plugin: BasePlugin, parameter: BaseParam, var: tk.Variable, *_):
        parameter.value = var.get()
        self.display_plugin_configuration(frame, plugin)

    def run(self):
        
        run_window = PipelineRunner(self.plugins)
        Thread(target=run_window.run).start()

class PipelineRunner:
    def __init__(self, plugins):
        self.plugins = plugins
        self.pbars = []
       
        toplevel = tk.Toplevel()
        self.frame = tk.Frame(toplevel)
        self.frame.grid(sticky=tk.NSEW)
        toplevel.rowconfigure(0,weight=1)
        toplevel.columnconfigure(0,weight=1)
        
        for num, plugin in enumerate(self.plugins):
            ttk.Label(self.frame, text=plugin.title).grid(row=num*2, sticky=tk.EW)
            pbar = LabeledProgressbar(self.frame, length=500)
            self.pbars.append(pbar) 
            pbar.grid(row=num*2+1, sticky=tk.EW)
           
        self.frame.columnconfigure(0,weight=1)
        
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
            preview = DataFramePreview(self.frame, value)
            preview.frame.grid(row=1000,sticky=tk.NSEW)
            self.frame.rowconfigure(1000,weight=1)
            self.frame.columnconfigure(0,weight=1)
        

class DataFramePreview:
    """Provides a visual preview of a Dask dataframe arranged as a table."""
    # XXX uses a treeview, which seemed like a good match but actually a grid-layout
    # of custom styled labels might work better.

    def __init__(self, tk_parent, ddf: dd.DataFrame):
        self.frame = ttk.Frame(tk_parent)
        self.ddf = ddf.compute()
        self.offset = 0
        self.height = 50

        self.label = ttk.Label(self.frame, text="DataFrame Preview")

        self.index_length = len(ddf.index)
        self.index_skip_list_stride = 10000 if self.index_length > 1000000 else 1000
        self.index_skip_list = list(islice(ddf.index,0,None,self.index_skip_list_stride))

        self.treeview = ttk.Treeview(self.frame, height=self.height, selectmode=tk.NONE)
        self.treeview['columns'] = list(ddf.columns)
        for n, c in enumerate(ddf.columns):
            self.treeview.heading(n, text=c)
            self.treeview.column(n, width=50)
       
        self.scrollbar_x = ttk.Scrollbar(self.frame, orient=tk.HORIZONTAL)
        self.scrollbar_x.configure(command=self.treeview.xview)
        self.treeview.configure(xscrollcommand=self.scrollbar_x.set)

        self.frame.grid(sticky=tk.NSEW)

        self.frame.columnconfigure(0,weight=1)
        self.frame.rowconfigure(1,weight=1)
        self.label.grid(row=0,columnspan=2)
        self.treeview.grid(row=1,column=0,sticky=tk.NSEW)
        self.scrollbar_x.grid(row=2,column=0,sticky=tk.EW)

        if self.index_length > self.height:
            self.scrollbar_y = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.scroll_command)
            self.treeview.bind('<MouseWheel>', self.scroll_event)
            self.treeview.bind('<Button-4>', self.scroll_event)
            self.treeview.bind('<Button-5>', self.scroll_event)
            self.scrollbar_y.grid(row=1,column=1,sticky=tk.NS)


        # XXX check for empty dataframe
        self.set_offset(0)

   
    def scroll_event(self, event):
        """Called when mousewheeling on the treeview"""
        if event.num == 4 or event.delta == -120:
            self.set_offset(self.offset-self.height//2)
        elif event.num == 5 or event.delta == +120:
            self.set_offset(self.offset+self.height//2)

    def scroll_command(self, action: str, number: str, unit=None):
        """Called when scrolling the vertical scrollbar"""
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
        # This could be made a lot more sophisticated but it works.
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
            # XXX display NaNs more nicely
            self.treeview.insert('', 'end', iid=row_id, text=index, values=values)
        self.offset = offset

        if self.index_length > self.height:
            self.scrollbar_y.set(offset/self.index_length, (offset+self.height)/self.index_length)
            self.label['text'] = f'Rows {self.offset} - {self.offset+self.height} / {self.index_length}'


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

