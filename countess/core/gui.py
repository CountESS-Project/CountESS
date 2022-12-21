# TK based GUI for CountESS
import os.path
import pathlib
import threading
import tkinter as tk
import tkinter.ttk as ttk
from collections.abc import Iterable, Sequence
from functools import lru_cache, partial
from itertools import islice
from threading import Thread
from tkinter import filedialog
from tkinter.scrolledtext import ScrolledText
from typing import NamedTuple, Optional, Mapping

import dask.dataframe as dd
import ttkthemes  # type:ignore
import re
import configparser
import math
import sys

from .parameters import (
    ArrayParam,
    BaseParam,
    BooleanParam,
    ChoiceParam,
    FileArrayParam,
    FileParam,
    FloatParam,
    IntegerParam,
    MultiParam,
    SimpleParam,
    StringParam,
    TextParam,
)
from .pipeline import Pipeline
from .plugins import BasePlugin, FileInputMixin
from ..utils.dask import crop_dask_dataframe

import numpy as np

def is_nan(v):
    return v is None or v is np.nan or (type(v) is float and math.isnan(v))

class CancelButton(tk.Button):
    """A button which is a red X for cancelling / removing items"""
    def __init__(self, master, **kwargs):
        super().__init__(
            master,
            text="\u274c",
            width=1,
            highlightthickness=0,
            bd=0,
            fg="red",
            **kwargs,
        )


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


class ParameterWrapper:

    # XXX it would probably be better to have ParameterWrapper classes per Parameter class
    # given the ridiculous amount of if/elif going on in here.

    # XXX actually refactoring this whole mess of nested wrappers into a single TreeviewEditor
    # might make more sense (now I've worked out how to make a TreeviewEditor)

    def __init__(self, tk_parent, parameter, callback=None, delete_callback=None):

        self.parameter = parameter
        self.callback = callback
        self.button = None

        self.subwrappers: Mapping[BaseParam,ParameterWrapper] = {}

        if isinstance(parameter, BooleanParam):
            self.var = tk.BooleanVar(tk_parent, value=parameter.value)
        elif isinstance(parameter, FloatParam):
            self.var = tk.DoubleVar(tk_parent, value=parameter.value)
        elif isinstance(parameter, IntegerParam):
            self.var = tk.IntVar(tk_parent, value=parameter.value)
        elif isinstance(parameter, TextParam):
            # XXX tk.Text version doesn't use a var (see below)
            self.var = None
        elif isinstance(parameter, StringParam) or isinstance(parameter, ChoiceParam):
            self.var = tk.StringVar(tk_parent, value=parameter.value)
        else:
            self.var = None

        if self.var: 
            self.var.trace("w", self.value_changed_callback)

        self.label = ttk.Label(tk_parent, text=parameter.label)

        # XXX hang on, what if it's an array in an array?

        if isinstance(parameter, ArrayParam):
            self.button = ttk.Button(tk_parent, width=1, text="+", command=self.add_row_callback)
        elif delete_callback:
            self.button = ttk.Button(tk_parent, text="X", width=1, command=lambda: delete_callback(self))

        if isinstance(parameter, ChoiceParam):
            self.entry = ttk.Combobox(tk_parent, textvariable=self.var)
            self.entry["values"] = parameter.choices
            self.entry.state(["readonly"])
        elif isinstance(parameter, BooleanParam):
            self.entry = ttk.Checkbutton(tk_parent, variable=self.var)
        elif isinstance(parameter, FileParam):
            self.entry = ttk.Entry(tk_parent, textvariable=self.var)
            self.entry.state(['readonly'])
        elif isinstance(parameter, TextParam):
            # tk.Text widget doesn't have a variable, for whatever reason,
            # so we use a different method!
            # XXX is this a simpler way to handle other kinds of fields too?
            self.entry = tk.Text(tk_parent)
            self.entry.bind("<<Modified>>", self.widget_modified_callback)
        elif isinstance(parameter, SimpleParam):
            self.entry = ttk.Entry(tk_parent, textvariable=self.var)
            if parameter.read_only:
                self.entry.state(['readonly'])
        elif isinstance(parameter, (ArrayParam, MultiParam)):
            self.entry = ttk.Frame(tk_parent)
            self.entry.columnconfigure(0,weight=0)
            self.entry.columnconfigure(1,weight=0)
            self.entry.columnconfigure(2,weight=1)
            if isinstance(parameter, ArrayParam):
                self.update_subwrappers(parameter.params, self.delete_row_callback)
            else:
                self.update_subwrappers(parameter.params.values(), None)
        else:
            raise NotImplementedError(f"Unknown parameter type {parameter}")

        self.entry.grid(sticky=tk.EW)

    def update(self):

        if isinstance(self.parameter, ArrayParam):
            self.update_subwrappers(self.parameter.params, self.delete_row_callback)
            if self.parameter.max_size is not None and len(self.subwrappers) >= self.parameter.max_size:
                self.button['state'] = tk.DISABLED
            else:
                self.button['state'] = tk.NORMAL
        elif isinstance(self.parameter, MultiParam):
            self.update_subwrappers(self.parameter.params.values(), None)

        self.label["text"] = self.parameter.label

        if isinstance(self.parameter, ChoiceParam):
            self.entry["values"] = self.parameter.choices

    def update_subwrappers(self, params, delete_row_callback):

        for n, p in enumerate(params):
            if p in self.subwrappers:
                self.subwrappers[p].update()
            else:
                self.subwrappers[p] = ParameterWrapper(self.entry, p, self.callback, delete_row_callback)
            self.subwrappers[p].set_row(n)

        params_set = set(params)
        for p, pw in self.subwrappers.items():
            if p not in params_set:
                pw.destroy()

    def set_row(self, row):
        if self.button:
            self.label.grid(row=row, column=0)
            self.button.grid(row=row, column=1)
        else:
            self.label.grid(row=row, column=0, columnspan=2)

        self.entry.grid(row=row, column=2, sticky=tk.EW)

    def add_row_callback(self, *_):
        assert isinstance(self.parameter, ArrayParam)

        if isinstance(self.parameter, FileArrayParam):
            file_types = self.parameter.file_types
            filenames = filedialog.askopenfilenames(filetypes=file_types)
            self.parameter.add_files(filenames)
        else:
            pp = self.parameter.add_row()

        if self.callback is not None:
            self.callback(self.parameter)

        self.update()

    def delete_row_callback(self, parameter_wrapper):
        assert isinstance(self.parameter, ArrayParam)

        self.parameter.del_subparam(parameter_wrapper.parameter)
        self.update_subwrappers(self.parameter.params, self.delete_row_callback)

        if self.callback is not None:
            self.callback(self.parameter)

    def set_value(self, value):
        self.parameter.value = value
        if self.callback is not None:
            self.callback(self.parameter)
        return self.parameter.value

    def value_changed_callback(self, *_):
        self.var.set(self.set_value(self.var.get()))

    def widget_modified_callback(self, *_):
        # only gets called the *first* time a modification happens, unless
        # we reset the flag which says a change has happened ...
        value = self.entry.get(1.0, tk.END)
        self.entry.edit_modified(False)
        self.set_value(value)

    def clear_value_callback(self, *_):
        self.set_value("")

    def destroy(self):
        self.label.destroy()
        self.entry.destroy()
        if self.button:
            self.button.destroy()


class PluginConfigurator:

    preview = None

    def __init__(self, tk_parent: tk.Widget, plugin: BasePlugin, change_callback=None):
        self.plugin = plugin
        self.change_callback = change_callback

        self.frame = ttk.Frame(tk_parent)
        self.frame.columnconfigure(0, weight=1)
        self.frame.grid(sticky=tk.NSEW)

        self.wrapper_cache: Mapping[str,ParameterWrapper] = {}

        tk.Label(self.frame, text=plugin.title).grid(row=0, sticky=tk.EW)

        self.subframe = ttk.Frame(self.frame)
        self.subframe.columnconfigure(0, weight=0)
        self.subframe.columnconfigure(1, weight=0)
        self.subframe.columnconfigure(2, weight=1)
        self.subframe.grid(row=1, sticky=tk.NSEW)

        self.update()

    def change_parameter(self, parameter):
        """Called whenever a parameter gets changed"""
        self.plugin.update()
        self.update()

    def update(self):

        # Create any new parameter wrappers needed & update existing ones
        for n, (key, parameter) in enumerate(self.plugin.parameters.items()):
            if key in self.wrapper_cache:
                self.wrapper_cache[key].update()
            else:
                self.wrapper_cache[key] = ParameterWrapper(
                    self.subframe, parameter, self.change_parameter
                )
            self.wrapper_cache[key].set_row(n + 1)

        # Remove any parameter wrappers no longer needed
        for key, wrapper in list(self.wrapper_cache.items()):
            if key not in self.plugin.parameters:
                wrapper.destroy()
                del self.wrapper_cache[key]

        if self.change_callback:
            self.change_callback(self)

        if isinstance(self.plugin.prerun_cache, dd.DataFrame):
            ddf = self.plugin.prerun_cache
            if self.preview:
                self.preview.update(ddf)
            else:
                self.preview = DataFramePreview(self.frame, ddf)
                self.preview.frame.grid(row=3)
                self.frame.rowconfigure(3, weight=1)



class PipelineManager:
    # XXX allow some way to drag and drop tabs

    def __init__(self, tk_parent):
        self.pipeline = Pipeline()
        self.configurators = []

        self.frame = ttk.Frame(tk_parent)
        self.frame.grid(sticky=tk.NSEW)

        menu_frame = ttk.Frame(self.frame)
        config_menu_button = tk.Menubutton(menu_frame, text="Configure")
        config_menu = config_menu_button['menu'] = tk.Menu(config_menu_button, tearoff=False)
        config_menu.add_command(label="Load Config", command=self.load_config_dialog)
        config_menu.add_command(label="Save Config", command=self.save_config_dialog)
        config_menu_button.grid(row=0, column=0, sticky=tk.W)

        plugin_menu_button = tk.Menubutton(menu_frame, text="Plugins")
        self.plugin_menu = plugin_menu_button['menu'] = tk.Menu(plugin_menu_button, tearoff=False, postcommand=self.plugin_menu_open)
        plugin_menu_button.grid(row=0, column=1, sticky=tk.W)

        self.notebook = ttk.Notebook(self.frame)

        self.plugin_chooser_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.plugin_chooser_frame, text="+")
        self.update_plugin_chooser_frame()
        self.notebook.bind("<<NotebookTabChanged>>", self.notebook_tab_changed)

        self.run_button = tk.Button(
            self.frame, text="RUN", fg="green", command=self.run_pipeline
        )

        menu_frame.grid(row=0, sticky=tk.EW)
        self.notebook.grid(row=1, sticky=tk.NSEW)
        self.run_button.grid(row=2, sticky=tk.EW)
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(1, weight=1)

    def clear(self):
        for position in range(0, len(self.configurators)):
            self.pipeline.del_plugin(position)
            self.configurators.pop(position)
            self.notebook.forget(position)

    def load_config(self, filename):
        config = configparser.ConfigParser(strict=False)
        config.read(filename)
        self.clear()
        for section_name in config.sections():
            plugin = self.pipeline.load_plugin_config(section_name, config[section_name])
            self.add_plugin_configurator(plugin)

    def load_config_dialog(self):
        filename = filedialog.askopenfilename(filetypes=[(".INI Config File", "*.ini")])
        if filename:
            self.load_config(filename)

    def save_config(self, filename):
        with open(filename, "w") as fh:
            for section_name, config in self.pipeline.get_plugin_configs():
                fh.write(f'[{section_name}]\n')
                for k, v in config:
                    fh.write(f'{k} = {v}\n')
                fh.write('\n')

    def save_config_dialog(self):
        filename = filedialog.asksaveasfilename(filetypes=[(".INI Config File", "*.ini")])
        if filename:
            self.save_config(filename)

    def plugin_menu_open(self):
        self.plugin_menu.delete(0, self.plugin_menu.index(tk.END))

        for plugin_class in self.pipeline.choose_plugin_classes():
            self.plugin_menu.add_command(label=plugin_class.name, command=partial(self.add_plugin, plugin_class))

    def change_callback(self, plugin_configurator):
        """Triggered whenever `plugin` has a change of parameters"""
        try:
            position = self.configurators.index(plugin_configurator)
        except ValueError:
            return

        self.pipeline.prerun(position)

    def notebook_tab_changed(self, event):
        """Triggered when the notebook tab is changed, refresh the displayed tab to
        make sure parameters are updated"""
        position = self.notebook.index(self.notebook.select())
        if position < len(self.configurators):
            self.pipeline.prerun(position)
            self.configurators[position].update()
        else:
            self.update_plugin_chooser_frame()

    def update_plugin_chooser_frame(self):
        for w in self.plugin_chooser_frame.winfo_children():
            w.destroy()

        position = len(self.configurators)

        plugin_classes = self.pipeline.choose_plugin_classes(position)
        for n, plugin_class in enumerate(plugin_classes):
            add_callback = partial(self.add_plugin, plugin_class, position)
            ttk.Button(
                self.plugin_chooser_frame, text=plugin_class.name, command=add_callback
            ).grid(row=n, column=0)
            ttk.Label(self.plugin_chooser_frame, text=plugin_class.title).grid(
                row=n, column=1
            )

    def add_plugin(self, plugin_class, position=None):
        if position is None:
            position = len(self.configurators)

        plugin = plugin_class()

        self.pipeline.add_plugin(plugin, position)
        self.add_plugin_configurator(plugin, position)

    def add_plugin_configurator(self, plugin, position=None):
        if position is None:
            position = len(self.configurators)

        configurator = PluginConfigurator(self.notebook, plugin, self.change_callback)
        self.configurators.insert(position, configurator)
        self.notebook.insert(
            position, configurator.frame, text=plugin.name, sticky=tk.NSEW
        )

        cancel_command = lambda: self.del_plugin(position)
        CancelButton(configurator.frame, command=cancel_command).place(
            anchor=tk.NE, relx=1, rely=0
        )

        self.pipeline.prerun()
        self.notebook.select(position)

    def del_plugin(self, position):
        assert 0 <= position < len(self.configurators)

        # XXX Possibly can't delete this plugin because the one before and after aren't compatible.
        self.pipeline.del_plugin(position)
        self.configurators.pop(position)
        self.notebook.forget(position)

        self.pipeline.prerun()

    def run_pipeline(self):
        Thread(target=self.run_pipeline_thread).start()

    def run_pipeline_thread(self):
        self.run_button["state"] = tk.DISABLED
        run_window = PipelineRunner(self.pipeline)
        run_window.run()
        self.run_button["state"] = tk.NORMAL


class PipelineRunner:
    def __init__(self, pipeline: Pipeline):
        self.pipeline = pipeline
        self.pbars = []

        toplevel = tk.Toplevel()
        self.frame = tk.Frame(toplevel)
        self.frame.grid(sticky=tk.NSEW)
        toplevel.rowconfigure(0, weight=1)
        toplevel.columnconfigure(0, weight=1)

        for num, plugin in enumerate(self.pipeline.plugins):
            ttk.Label(self.frame, text=plugin.title).grid(row=num * 2, sticky=tk.EW)
            pbar = LabeledProgressbar(self.frame, length=500)
            self.pbars.append(pbar)
            pbar.grid(row=num * 2 + 1, sticky=tk.EW)

        self.frame.columnconfigure(0, weight=1)

    def progress_callback(self, n, a, b, s="Running"):
        if b:
            self.pbars[n].stop()
            self.pbars[n]["mode"] = "determinate"
            self.pbars[n]["value"] = 100 * a / b
            self.pbars[n].update_label(f"{s} : {a} / {b}" if b > 1 else s)
        else:
            self.pbars[n]["mode"] = "indeterminate"
            self.pbars[n].start()
            self.pbars[n].update_label(f"{s} : {a}" if a is not None else s)

    def run(self):
        value = self.pipeline.run(self.progress_callback)

        if isinstance(value, dd.DataFrame):
            preview = DataFramePreview(self.frame, value)
            preview.frame.grid(row=1000, sticky=tk.NSEW)
            self.frame.rowconfigure(1000, weight=1)
            self.frame.columnconfigure(0, weight=1)


class DataFramePreview:
    """Provides a visual preview of a Dask dataframe arranged as a table."""

    # XXX uses a treeview, which seemed like a good match but actually a grid-layout
    # of custom styled labels might work better.

    def __init__(self, tk_parent, ddf: dd.DataFrame):
        self.frame = ttk.Frame(tk_parent)
        self.label = ttk.Label(self.frame, text="DataFrame Preview")
        self.treeview = ttk.Treeview(self.frame, selectmode=tk.NONE)

        self.scrollbar_x = ttk.Scrollbar(self.frame, orient=tk.HORIZONTAL, command=self.treeview.xview)
        self.scrollbar_y = ttk.Scrollbar(self.frame, orient=tk.VERTICAL, command=self.treeview.yview)
        self.treeview.configure(xscrollcommand=self.scrollbar_x.set)
        self.treeview.configure(yscrollcommand=self.scrollbar_y.set)

        self.frame.grid(sticky=tk.NSEW)

        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(1, weight=1)
        self.label.grid(row=0, columnspan=2)
        self.treeview.grid(row=1, column=0, sticky=tk.NSEW)
        self.scrollbar_x.grid(row=2, column=0, sticky=tk.EW)
        self.scrollbar_y.grid(row=1, column=1, stick=tk.NS)

        self.update(ddf)

    def update(self, ddf: dd.DataFrame):

        if len(ddf) > 1000:
            self.label['text'] = f"DataFrame Preview (1000 rows out of {len(ddf)})"
            ddf = crop_dask_dataframe(ddf, 1000)
        else:
            self.label['text'] = f"DataFrame Preview {len(ddf)} rows"

        # XXX could handle multiindex columns more elegantly than this
        # (but maybe not in a ttk.Treeview)
        column_names = [".".join(c) if type(c) is tuple else c for c in ddf.columns]
        column_types = [ dt.kind for dt in ddf.dtypes ]

        self.treeview["columns"] = column_names

        for n, cn in enumerate(column_names):
            self.treeview.heading(n, text=cn)

        for n, ct in enumerate(column_types):
            # XXX it'd be nicer if we could do "real" decimal point alignment
            anchor = tk.E if ct in ('i', 'f') else tk.W
            self.treeview.column(n, anchor=anchor, width=100, minwidth=100, stretch=tk.YES)

        for row in self.treeview.get_children():
            self.treeview.delete(row)

        for n, (index, *values) in enumerate(ddf.itertuples()):
            values = [ '—' if is_nan(v) else str(v) for v in values ]
            self.treeview.insert("", n, text=index, values=values)
       

def main():
    root = ttkthemes.ThemedTk()
    root.title("CountESS")

    themes = set(root.get_themes())
    for t in ["winnative", "aqua", "ubuntu", "clam"]:
        if t in themes:
            root.set_theme(t)
            break
        
    pm = PipelineManager(root)

    for filename in sys.argv[1:]:
        pm.load_config(filename)

    root.rowconfigure(0, weight=1)
    root.columnconfigure(0, weight=1)
    root.mainloop()


if __name__ == "__main__":
    main()
