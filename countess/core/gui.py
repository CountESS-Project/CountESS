# TK based GUI for CountESS
import threading
import tkinter as tk
import tkinter.ttk as ttk
import ttkthemes  # type:ignore
from tkinter import filedialog
import pathlib

from typing import Optional, NamedTuple
from collections.abc import Iterable, Sequence
from functools import partial
from threading import Thread

from tkinter.scrolledtext import ScrolledText

from .plugins import Pipeline, BasePlugin, FileInputMixin
from .parameters import (
    BaseParam,
    BooleanParam,
    IntegerParam,
    FloatParam,
    StringParam,
    FileParam,
)

from functools import lru_cache
from itertools import islice

import dask.dataframe as dd

class CancelButton(tk.Button):
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

    style_data = [
        (
            "LabeledProgressbar.trough",
            {
                "children": [
                    ("LabeledProgressbar.pbar", {"side": "left", "sticky": "ns"}),
                    ("LabeledProgressbar.label", {"sticky": ""}),
                ],
                "sticky": "nswe",
            },
        )
    ]

    def __init__(self, master, *args, **kwargs):
        self.style = ttk.Style(master)
        # make up a new style name so we don't interfere with other LabeledProgressbars
        # and accidentally change their color or label
        self.style_name = f"_id_{id(self)}"
        self.style.layout(self.style_name, self.style_data)
        self.style.configure(self.style_name, background="green")

        kwargs["style"] = self.style_name
        super().__init__(master, *args, **kwargs)

    def update_label(self, s):
        self.style.configure(self.style_name, text=s)


class ParameterWrapper:
    def __init__(self, tk_parent, parameter, callback=None):
        self.parameter = parameter
        self.callback = callback
        self.button = None

        if isinstance(parameter, BooleanParam):
            self.var = tk.BooleanVar(tk_parent, value=parameter.value)
        elif isinstance(parameter, FloatParam):
            self.var = tk.DoubleVar(tk_parent, value=parameter.value)
        elif isinstance(parameter, IntegerParam):
            self.var = tk.IntVar(tk_parent, value=parameter.value)
        elif isinstance(parameter, StringParam):
            self.var = tk.StringVar(tk_parent, value=parameter.value)
        else:
            raise NotImplementedError("Unknown parameter type")

        self.var.trace("w", self.value_changed_callback)

        self.label = ttk.Label(tk_parent, text=parameter.label)

        # XXX filenames and readonly fields probably shouldn't be labels
        # XXX filenames shouldn't be full paths, its ugly :-)

        if isinstance(parameter, FileParam):
            self.entry = tk.Label(tk_parent, text=parameter.value)
            self.button = CancelButton(tk_parent, command=self.clear_value_callback)
        elif parameter.read_only:
            self.entry = tk.Label(tk_parent, text=parameter.value)
        elif isinstance(parameter, BooleanParam):
            self.entry = tk.Checkbutton(tk_parent, variable=self.var)
        elif parameter.choices:
            self.entry = ttk.Combobox(tk_parent, textvariable=self.var)
            self.entry["values"] = parameter.choices
            self.entry.state(["readonly"])
        else:
            self.entry = tk.Entry(tk_parent, textvariable=self.var)

        self.entry.grid(sticky=tk.EW)

    def update(self):
        # XXX what if it didn't have choices before and now it does or vice versa?
        # XXX should ChoiceParam be a difference Parameter class.
        if self.parameter.choices and isinstance(self.entry, ttk.Combobox):
            self.entry["values"] = self.parameter.choices

    def set_row(self, row):
        self.label.grid(row=row, column=0)
        self.entry.grid(row=row, column=1)
        if self.button:
            self.button.grid(row=row, column=2)

    def clear_value_callback(self, *_):
        self.parameter.value = ""
        if self.callback is not None:
            self.callback(self.parameter)

    def value_changed_callback(self, *_):
        self.parameter.value = self.var.get()
        if self.callback is not None:
            self.callback(self.parameter)

    def destroy(self):
        self.label.destroy()
        self.entry.destroy()
        if self.button:
            self.button.destroy()


class PluginConfigurator:
    def __init__(self, tk_parent, plugin):
        self.plugin = plugin

        self.frame = ttk.Frame(tk_parent)
        self.frame.grid(sticky=tk.NSEW)

        self.wrapper_cache = {}

        tk.Label(self.frame, text=plugin.description).grid(
            row=0, columnspan=2, sticky=tk.EW
        )

        self.frame.columnconfigure(0, weight=1)
        self.frame.columnconfigure(1, weight=2)

        if isinstance(self.plugin, FileInputMixin):
            self.add_file_button = tk.Button(
                self.frame, text="+ Add File", command=self.add_file
            )

        self.update()

    def add_file(self):
        filenames = filedialog.askopenfilenames(filetypes=self.plugin.file_types)
        for filename in filenames:
            self.plugin.add_file(filename)
        self.update()

    def update(self):

        # Create any new parameter wrappers needed & update existing ones
        for n, (key, parameter) in enumerate(self.plugin.parameters.items()):
            if key in self.wrapper_cache:
                self.wrapper_cache[key].update()
            else:
                self.wrapper_cache[key] = ParameterWrapper(self.frame, parameter, self.change_parameter)

            self.wrapper_cache[key].set_row(n+1)

        # Remove any parameter wrappers no longer needed
        for key, wrapper in list(self.wrapper_cache.items()):
            if key not in self.plugin.parameters:
                wrapper.destroy()
                del self.wrapper_cache[key]

        if isinstance(self.plugin, FileInputMixin):
            self.add_file_button.grid(row=len(self.plugin.parameters)+1, column=0)

    def change_parameter(self, parameter):
        # called when a parameter gets changed.
        # XXX kinda gross, but the whole "file number" thing just is.

        if isinstance(parameter, FileParam) and parameter.value == '':
            self.plugin.remove_file_by_parameter(parameter)
            self.update()



class PipelineManager:
    # XXX allow some way to drag and drop tabs

    def __init__(self, tk_parent):
        self.pipeline = Pipeline()
        self.configurators = []

        self.frame = ttk.Frame(tk_parent)
        self.frame.grid(sticky=tk.NSEW)
        self.notebook = ttk.Notebook(self.frame)

        self.plugin_chooser_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.plugin_chooser_frame, text="+")
        self.update_plugin_chooser_frame()
        self.notebook.bind("<<NotebookTabChanged>>", self.notebook_tab_changed)

        self.run_button = tk.Button(
            self.frame, text="RUN", fg="green", command=self.run_pipeline
        )

        self.notebook.grid(row=0, sticky=tk.NSEW)
        self.run_button.grid(row=1, sticky=tk.EW)
        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(0, weight=1)

    def notebook_tab_changed(self, event):
        """Triggered when the notebook tab is changed, refresh the displayed tab to
        make sure parameters are updated"""
        position = self.notebook.index(self.notebook.select())
        if position < len(self.configurators):
            self.pipeline.update_plugin(position)
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
            ttk.Label(self.plugin_chooser_frame, text=plugin_class.description).grid(
                row=n, column=1
            )

    def add_plugin(self, plugin_class, position=None):
        if position is None:
            position = len(self.configurators)

        plugin = plugin_class()

        self.pipeline.add_plugin(plugin, position)

        configurator = PluginConfigurator(self.notebook, plugin)
        self.configurators.insert(position, configurator)
        self.notebook.insert(
            position, configurator.frame, text=plugin.name, sticky=tk.NSEW
        )

        cancel_command = lambda: self.del_plugin(position)
        CancelButton(configurator.frame, command=cancel_command).place(
            anchor=tk.NE, relx=1, rely=0
        )

        self.notebook.select(position)

    def del_plugin(self, position):
        assert 0 <= position < len(self.configurators)

        # XXX Possibly can't delete this plugin because the one before and after aren't compatible.
        self.pipeline.del_plugin(position)
        self.configurators.pop(position)
        self.notebook.forget(position)

    def run_pipeline(self):
        Thread(target=self.run_pipeline_thread).start()

    def run_pipeline_thread(self):
        self.run_button['state'] = tk.DISABLED
        run_window = PipelineRunner(self.pipeline)
        run_window.run()
        self.run_button['state'] = tk.NORMAL


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

    def run(self):
        value = None

        for num, plugin in enumerate(self.pipeline.plugins):
            pbar = self.pbars[num]

            def progress_callback(a, b, s="Running"):
                if b:
                    pbar.stop()
                    pbar["mode"] = "determinate"
                    pbar["value"] = 100 * a / b
                    pbar.update_label(f"{s} : {a} / {b}" if b > 1 else s)
                else:
                    pbar["mode"] = "indeterminate"
                    pbar.start()
                    pbar.update_label(f"{s} : {a}" if a is not None else s)

            progress_callback(0, 0, "Running")

            value = plugin.run_with_progress_callback(value, callback=progress_callback)

            progress_callback(1, 1, "Finished")

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
        self.ddf = ddf.compute()
        self.offset = 0
        self.height = 50

        self.label = ttk.Label(self.frame, text="DataFrame Preview")

        self.index_length = len(ddf.index)
        self.index_skip_list_stride = 10000 if self.index_length > 1000000 else 1000
        self.index_skip_list = list(
            islice(ddf.index, 0, None, self.index_skip_list_stride)
        )

        self.treeview = ttk.Treeview(self.frame, height=self.height, selectmode=tk.NONE)
        self.treeview["columns"] = list(ddf.columns)
        for n, c in enumerate(ddf.columns):
            self.treeview.heading(n, text=c)
            self.treeview.column(n, width=50)

        self.scrollbar_x = ttk.Scrollbar(self.frame, orient=tk.HORIZONTAL)
        self.scrollbar_x.configure(command=self.treeview.xview)
        self.treeview.configure(xscrollcommand=self.scrollbar_x.set)

        self.frame.grid(sticky=tk.NSEW)

        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(1, weight=1)
        self.label.grid(row=0, columnspan=2)
        self.treeview.grid(row=1, column=0, sticky=tk.NSEW)
        self.scrollbar_x.grid(row=2, column=0, sticky=tk.EW)

        if self.index_length > self.height:
            self.scrollbar_y = ttk.Scrollbar(
                self.frame, orient=tk.VERTICAL, command=self.scroll_command
            )
            self.treeview.bind("<MouseWheel>", self.scroll_event)
            self.treeview.bind("<Button-4>", self.scroll_event)
            self.treeview.bind("<Button-5>", self.scroll_event)
            self.scrollbar_y.grid(row=1, column=1, sticky=tk.NS)

        # XXX check for empty dataframe
        self.set_offset(0)

    def scroll_event(self, event):
        """Called when mousewheeling on the treeview"""
        if event.num == 4 or event.delta == -120:
            self.set_offset(self.offset - self.height // 2)
        elif event.num == 5 or event.delta == +120:
            self.set_offset(self.offset + self.height // 2)

    def scroll_command(self, action: str, number: str, unit=None):
        """Called when scrolling the vertical scrollbar"""
        # https://tkdocs.com/shipman/scrollbar-callback.html
        number = float(number)
        if action == tk.MOVETO:
            # the useful range is 0..1 but you can scroll beyond that.
            x = max(0, min(1, number))
            self.set_offset(int((self.index_length - self.height) * x))
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
        return next(
            islice(
                self.ddf.loc[base_index:].index, offset % stride, offset % stride + 1
            )
        )

    def set_offset(self, offset: int):
        # XXX horribly inefficient PoC
        offset = max(0, min(self.index_length - self.height, offset))
        index = self.get_index_from_offset(offset)
        value_tuples = islice(self.ddf.loc[index:].itertuples(), 0, self.height)
        for n, (index, *values) in enumerate(value_tuples):
            row_id = str(n)
            if self.treeview.exists(row_id):
                self.treeview.delete(row_id)
            # XXX display NaNs more nicely
            self.treeview.insert("", "end", iid=row_id, text=index, values=values)
        self.offset = offset

        if self.index_length > self.height:
            self.scrollbar_y.set(
                offset / self.index_length, (offset + self.height) / self.index_length
            )
            self.label[
                "text"
            ] = f"Rows {self.offset} - {self.offset+self.height} / {self.index_length}"


def main():
    root = ttkthemes.ThemedTk()
    root.title("CountESS")

    themes = set(root.get_themes())
    for t in ["winnative", "aqua", "ubuntu", "clam"]:
        if t in themes:
            root.set_theme(t)
            break

    # PipelineBuilder(root).grid(sticky="nsew")
    PipelineManager(root)
    root.rowconfigure(0, weight=1)
    root.columnconfigure(0, weight=1)
    root.mainloop()


if __name__ == "__main__":
    main()
