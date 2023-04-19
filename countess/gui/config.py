# TK based GUI for CountESS
import math
import tkinter as tk
from functools import partial
from tkinter import filedialog, ttk
from typing import Mapping, MutableMapping, Optional

import dask.dataframe as dd
import numpy as np
from pandas.api.types import is_numeric_dtype  # type: ignore

from ..core.parameters import (
    ArrayParam,
    BaseParam,
    BooleanParam,
    ChoiceParam,
    FileArrayParam,
    FileParam,
    FileSaveParam,
    FloatParam,
    IntegerParam,
    MultiParam,
    SimpleParam,
    TabularMultiParam,
    TextParam,
)
from ..core.plugins import BasePlugin
from ..utils.dask import crop_dataframe

UNICODE_CHECK = "\u2714"
UNICODE_UNCHECK = "\u2717"
UNICODE_CROSS = "\u2715"
UNICODE_PLUS = "\u2795"


def is_nan(v):
    return v is None or v is np.nan or (isinstance(v, float) and math.isnan(v))


class ParameterWrapper:
    # XXX it would probably be better to have ParameterWrapper classes per Parameter class
    # given the ridiculous amount of if/elif going on in here.

    # XXX actually refactoring this whole mess of nested wrappers into a single TreeviewEditor
    # might make more sense (now I've worked out how to make a TreeviewEditor)

    def __init__(  # pylint: disable=R0912,R0915
        self,
        tk_parent: tk.Widget,
        parameter: BaseParam,
        callback=None,
        delete_callback=None,
        level=0,
    ):
        self.parameter = parameter
        self.callback = callback
        self.button = None
        self.level = level
        self.subwrapper_buttons: list[tk.Button] = []

        self.subwrappers: Mapping[BaseParam, ParameterWrapper] = {}

        self.var: Optional[tk.Variable] = None
        self.entry: Optional[tk.Widget] = None
        self.label: Optional[tk.Widget] = None
        self.row_labels: list[tk.Widget] = []

        if isinstance(parameter, ArrayParam):
            self.label = None
        else:
            self.label = ttk.Label(tk_parent, text=parameter.label)

        if isinstance(parameter, ChoiceParam):
            self.var = tk.StringVar(tk_parent, value=parameter.value)
            self.entry = ttk.Combobox(tk_parent, textvariable=self.var)
            self.entry["values"] = parameter.choices
            self.entry.state(["readonly"])  # don't allow other options
        elif isinstance(parameter, BooleanParam):
            self.entry = tk.Button(tk_parent, width=2, command=self.toggle_checkbox_callback)
            self.set_checkbox_value()
        elif isinstance(parameter, FileParam):
            self.var = tk.StringVar(tk_parent, value=parameter.value)
            self.entry = ttk.Entry(tk_parent, textvariable=self.var)
            self.entry.state(["readonly"])
            self.button = tk.Button(
                tk_parent, text="Select", width=3, command=self.change_file_callback
            )
        elif isinstance(parameter, TextParam):
            # tk.Text widget doesn't have a variable, for whatever reason,
            # so we use a different method!
            # XXX is this a simpler way to handle other kinds of fields too?
            self.entry = tk.Text(tk_parent, height=10)
            self.entry.insert("1.0", parameter.value)
            if parameter.read_only:
                self.entry["state"] = "disabled"
            else:
                self.entry.bind("<<Modified>>", self.widget_modified_callback)
        elif isinstance(parameter, SimpleParam):
            if isinstance(parameter, FloatParam):
                self.var = tk.DoubleVar(tk_parent, value=parameter.value)
            elif isinstance(parameter, IntegerParam):
                self.var = tk.IntVar(tk_parent, value=parameter.value)
            else:
                self.var = tk.StringVar(tk_parent, value=parameter.value)

            self.entry = ttk.Entry(tk_parent, textvariable=self.var)
            if parameter.read_only:
                self.entry.state(["readonly"])

        elif (
            isinstance(parameter, ArrayParam)
            and self.level == 0
            and not isinstance(parameter.param, TabularMultiParam)
        ):
            self.entry = ttk.Frame(tk_parent)
            self.entry.columnconfigure(0, weight=1)
            drc = self.delete_row_callback if not parameter.read_only else None
            self.update_subwrappers_framed(parameter.params, drc)
            if not parameter.read_only:
                self.button = tk.Button(
                    self.entry,
                    text=f"Add {parameter.param.label}",
                    command=self.add_row_callback,
                )
                self.button.grid(row=len(parameter.params) + 1)

        elif isinstance(parameter, ArrayParam) and (
            self.level < 3 or isinstance(parameter.param, TabularMultiParam)
        ):
            label_frame_label = tk.Frame(tk_parent)
            tk.Label(label_frame_label, text=parameter.label).grid(row=0, column=0, padx=5)
            self.entry = tk.LabelFrame(tk_parent, labelwidget=label_frame_label, padx=10, pady=5)
            if not parameter.read_only:
                self.button = tk.Button(
                    label_frame_label,
                    text=UNICODE_PLUS,
                    width=2,
                    command=self.add_row_callback,
                )
                self.button.grid(row=0, column=1, padx=10)

            drc = self.delete_row_callback if not parameter.read_only else None
            if isinstance(parameter.param, MultiParam):
                for n, pp in enumerate(parameter.param.values()):
                    tk.Label(self.entry, text=pp.label).grid(
                        row=0, column=n + 1, sticky=tk.EW, padx=10
                    )
                    self.entry.columnconfigure(n + 1, weight=1)

                self.update_subwrappers_tabular(parameter.params, drc)
            else:
                self.entry.columnconfigure(0, weight=0)
                self.entry.columnconfigure(1, weight=0)
                self.entry.columnconfigure(2, weight=1)

                # XXX hack because the empty labelframe collapses
                # for some reason.
                tk.Label(self.entry, text="").grid()

                self.update_subwrappers(parameter.params, drc)

        elif isinstance(parameter, (ArrayParam, MultiParam)):
            self.entry = ttk.Frame(tk_parent)
            self.entry.columnconfigure(0, weight=0)
            self.entry.columnconfigure(1, weight=0)
            self.entry.columnconfigure(2, weight=1)
            if isinstance(parameter, ArrayParam):
                drc = self.delete_row_callback if not parameter.read_only else None
                self.update_subwrappers(parameter.params, drc)
                self.button = tk.Button(
                    tk_parent, text=UNICODE_PLUS, width=2, command=self.add_row_callback
                )
            else:
                self.update_subwrappers(parameter.params.values(), None)
        else:
            raise NotImplementedError(f"Unknown parameter type {parameter}")

        if self.var:
            self.var.trace("w", self.value_changed_callback)

        # XXX hang on, what if it's an array in an array?
        if delete_callback and not self.button:
            self.button = tk.Button(
                tk_parent,
                text=UNICODE_CROSS,
                width=2,
                command=lambda: delete_callback(self),
            )

        if not isinstance(parameter, BooleanParam):
            self.entry.grid(sticky=tk.EW, padx=10, pady=5)

    def update(self):
        print(f"UPDATE {self} {self.parameter.label} {self.parameter.hide}")
        if self.parameter.hide:
            self.entry['fg'] = self.entry['bg']
        else:
            self.entry['fg'] = None

        if (
            isinstance(self.parameter, ArrayParam)
            and self.level == 0
            and not isinstance(self.parameter.param, TabularMultiParam)
        ):
            self.update_subwrappers_framed(self.parameter.params, self.delete_row_callback)
            if self.button:
                self.button.grid(row=len(self.parameter.params) + 1, padx=10)
        elif (
            isinstance(self.parameter, ArrayParam)
            and isinstance(self.parameter.param, MultiParam)
            and self.level < 3
        ):
            self.update_subwrappers_tabular(
                self.parameter.params,
                self.delete_row_callback if not self.parameter.read_only else None,
            )
        elif isinstance(self.parameter, ArrayParam):
            self.update_subwrappers(
                self.parameter.params,
                self.delete_row_callback if not self.parameter.read_only else None,
            )
            if self.button:
                self.button["state"] = (
                    tk.DISABLED
                    if self.parameter.max_size is not None
                    and len(self.subwrappers) >= self.parameter.max_size
                    else tk.NORMAL
                )
        elif isinstance(self.parameter, MultiParam):
            self.update_subwrappers(self.parameter.params.values(), None)
        elif isinstance(self.parameter, ChoiceParam):
            self.entry["values"] = self.parameter.choices
        elif isinstance(self.parameter, BooleanParam):
            self.set_checkbox_value()
        elif isinstance(self.parameter, TextParam):
            if self.parameter.read_only:
                self.entry["state"] = "normal"
            self.entry.replace("1.0", tk.END, self.parameter.value)
            if self.parameter.read_only:
                self.entry["state"] = "disabled"
        else:
            self.var.set(self.parameter.value)

        if self.label:
            self.label["text"] = self.parameter.label

    def cull_subwrappers(self, params):
        params_set = set(params)
        for p, pw in self.subwrappers.items():
            if p not in params_set:
                pw.destroy()

    def update_subwrappers(self, params, delete_row_callback):
        for n, p in enumerate(params):
            if p in self.subwrappers:
                self.subwrappers[p].update()
            else:
                self.subwrappers[p] = ParameterWrapper(
                    self.entry,
                    p,
                    self.callback,
                    delete_row_callback,
                    level=self.level + 1,
                )
            self.subwrappers[p].set_row(n)

        self.cull_subwrappers(params)

    def update_subwrappers_tabular(self, params, delete_row_callback):
        while self.subwrapper_buttons:
            self.subwrapper_buttons.pop().destroy()

        for rl in self.row_labels:
            rl.destroy()
        self.row_labels = []

        for n, p in enumerate(params):
            row_label = tk.Label(self.entry, text=p.label)
            row_label.grid(row=n + 1, column=0, padx=10)
            self.row_labels.append(row_label)

            subparams = p.params.values()
            for m, pp in enumerate(subparams):
                if pp in self.subwrappers:
                    self.subwrappers[pp].update()
                else:
                    self.subwrappers[pp] = ParameterWrapper(
                        self.entry,
                        pp,
                        self.callback,
                        delete_row_callback,
                        level=self.level + 1,
                    )
                self.subwrappers[pp].entry.grid(row=n + 1, column=m + 1, padx=10)
            if delete_row_callback:
                button = tk.Button(
                    self.entry,
                    text=UNICODE_CROSS,
                    width=2,
                    command=partial(delete_row_callback, self, n),
                )
                button.grid(row=n + 1, column=len(subparams) + 1, padx=10)
                self.subwrapper_buttons.append(button)

        self.cull_subwrappers([pp for p in params for pp in p.params.values()])

    def update_subwrappers_framed(self, params, delete_row_callback):
        for n, p in enumerate(params):
            if p in self.subwrappers:
                self.subwrappers[p].update()
            else:
                label_frame_label = tk.Frame(self.entry)
                tk.Label(label_frame_label, text=p.label).grid(row=0, column=0, padx=10)
                label_frame = tk.LabelFrame(self.entry, labelwidget=label_frame_label, padx=10)
                label_frame.grid(sticky=tk.NSEW, padx=10)
                label_frame.columnconfigure(0, weight=1)

                def _command_drc(param, label_frame):
                    delete_row_callback(self.subwrappers[param])
                    label_frame.destroy()

                self.subwrappers[p] = ParameterWrapper(
                    label_frame,
                    p,
                    self.callback,
                    delete_row_callback,
                    level=self.level + 1,
                )
                if delete_row_callback:
                    button = ttk.Button(
                        label_frame_label,
                        text=UNICODE_CROSS,
                        width=2,
                        command=partial(_command_drc, p, label_frame),
                    )
                    button.grid(row=0, column=1, padx=10)

            self.subwrappers[p].entry.grid(row=n, column=0, padx=10)

        self.cull_subwrappers(params)

    def set_row(self, row):
        if not self.label:
            self.entry.grid(row=row, column=0, columnspan=3)
        elif self.button:
            self.label.grid(row=row, column=0)
            self.button.grid(row=row, column=1)
            self.entry.grid(row=row, column=2)
        else:
            self.label.grid(row=row, column=0, columnspan=2)
            self.entry.grid(row=row, column=2)

    def add_row_callback(self, *_):
        assert isinstance(self.parameter, ArrayParam)

        if isinstance(self.parameter, FileSaveParam):
            file_types = self.parameter.file_types
            filename = filedialog.asksaveasfilename(filetypes=file_types)
            self.parameter.value = filename
        if isinstance(self.parameter, FileArrayParam):
            file_types = self.parameter.file_types
            filenames = filedialog.askopenfilenames(filetypes=file_types)
            self.parameter.add_files(filenames)
        else:
            self.parameter.add_row()

        if self.callback is not None:
            self.callback(self.parameter)

        self.update()

    def delete_row_callback(self, parameter_wrapper, row=None):
        assert isinstance(self.parameter, ArrayParam)

        if row is not None:
            self.parameter.del_row(row)
        else:
            self.parameter.del_subparam(parameter_wrapper.parameter)

        self.update()
        if self.callback is not None:
            self.callback(self.parameter)

    def change_file_callback(self, *_):
        file_types = self.parameter.file_types
        filename = filedialog.askopenfilename(filetypes=file_types)
        self.parameter.value = filename
        self.var.set(self.parameter.value)
        self.callback(self.parameter)

    def set_value(self, value):
        # self.parameter.value is a property, and some cleaning may occur, so we just
        # check before and after to see if it has changed.
        old_parameter_value = self.parameter.value
        self.parameter.value = value
        new_parameter_value = self.parameter.value
        if old_parameter_value != new_parameter_value and self.callback is not None:
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

    def set_checkbox_value(self):
        if self.parameter.hide:
            self.entry["text"] = ""
            self.entry["fg"] = self.entry["bg"]
            self.entry["state"] = tk.DISABLED
            self.entry["bd"] = 0
        elif self.parameter.value:
            self.entry["text"] = UNICODE_CHECK
            self.entry["fg"] = "black"
            self.entry["state"] = tk.NORMAL
            self.entry["bd"] = 1
        else:
            self.entry["text"] = UNICODE_UNCHECK
            self.entry["fg"] = "grey"
            self.entry["state"] = tk.NORMAL
            self.entry["bd"] = 1

    def toggle_checkbox_callback(self, *_):
        if self.parameter.read_only or self.parameter.hide:
            # XXX warn?
            return
        value = not self.parameter.value
        self.parameter.value = value
        self.set_checkbox_value()
        if self.callback is not None:
            self.callback(self.parameter)

    def clear_value_callback(self, *_):
        self.set_value("")

    def destroy(self):
        self.label.destroy()
        self.entry.destroy()
        if self.button:
            self.button.destroy()


class PluginConfigurator:
    output_text = None
    preview = None

    def __init__(self, tk_parent: tk.Widget, plugin: BasePlugin, change_callback=None):
        self.plugin = plugin
        self.change_callback = change_callback

        self.frame = ttk.Frame(tk_parent)
        self.frame.columnconfigure(0, weight=1)
        self.frame.grid(sticky=tk.NSEW)

        self.wrapper_cache: MutableMapping[str, ParameterWrapper] = {}

        self.subframe = ttk.Frame(self.frame)
        self.subframe.columnconfigure(0, weight=0)
        self.subframe.columnconfigure(1, weight=0)
        self.subframe.columnconfigure(2, weight=1)
        self.subframe.grid(row=1, sticky=tk.NSEW)

        self.update()

    def change_parameter(self, parameter):
        """Called whenever a parameter gets changed"""
        if self.plugin.update():
            self.update()
        if self.change_callback:
            self.change_callback(self)

    def update(self) -> None:
        # If there's only a single parameter it is presented a little differently.
        top_level = 0 if len(self.plugin.parameters) == 1 else 1

        # Create any new parameter wrappers needed & update existing ones
        for n, (key, parameter) in enumerate(self.plugin.parameters.items()):
            if key in self.wrapper_cache:
                self.wrapper_cache[key].update()
            else:
                self.wrapper_cache[key] = ParameterWrapper(
                    self.subframe, parameter, self.change_parameter, level=top_level
                )
            self.wrapper_cache[key].set_row(n + 1)

        # Remove any parameter wrappers no longer needed
        for key, wrapper in list(self.wrapper_cache.items()):
            if key not in self.plugin.parameters:
                wrapper.destroy()
                del self.wrapper_cache[key]


class DataFramePreview:
    """Provides a visual preview of a Dask dataframe arranged as a table."""

    # XXX uses a treeview, which seemed like a good match but actually a grid-layout
    # of custom styled labels might work better.

    def __init__(self, tk_parent, ddf: Optional[dd.DataFrame] = None, max_rows: int = 10000):
        self.frame = ttk.Frame(tk_parent)
        self.label = ttk.Label(self.frame, text="DataFrame Preview")
        self.max_rows = max_rows

        self.treeview = ttk.Treeview(self.frame, selectmode=tk.NONE)

        self.scrollbar_x = ttk.Scrollbar(
            self.frame, orient=tk.HORIZONTAL, command=self.treeview.xview
        )
        self.scrollbar_y = ttk.Scrollbar(
            self.frame, orient=tk.VERTICAL, command=self.treeview.yview
        )
        self.treeview.configure(xscrollcommand=self.scrollbar_x.set)
        self.treeview.configure(yscrollcommand=self.scrollbar_y.set)

        self.frame.grid(sticky=tk.NSEW)

        self.frame.columnconfigure(0, weight=1)
        self.frame.rowconfigure(1, weight=1)
        self.label.grid(row=0, columnspan=2)
        self.treeview.grid(row=1, column=0, sticky=tk.NSEW)
        self.scrollbar_x.grid(row=2, column=0, sticky=tk.EW)
        self.scrollbar_y.grid(row=1, column=1, stick=tk.NS)

        if ddf is not None:
            self.update(ddf)

    def update(self, ddf: dd.DataFrame):
        if len(ddf) > self.max_rows:
            self.label["text"] = f"DataFrame Preview ({self.max_rows} rows out of {len(ddf)})"
            ddfx = crop_dataframe(ddf, self.max_rows)
            assert isinstance(ddfx, dd.DataFrame)
            ddf = ddfx
        else:
            self.label["text"] = f"DataFrame Preview {len(ddf)} rows"

        # XXX could handle multiindex columns more elegantly than this
        # (but maybe not in a ttk.Treeview)
        column_names = ["__".join(c) if isinstance(c, tuple) else c for c in ddf.columns]

        self.treeview["columns"] = column_names

        for n, (column_name, column_dtype) in enumerate(zip(column_names, ddf.dtypes)):
            self.treeview.heading(n, text=f"{column_name} ({column_dtype.name})")
            # XXX it'd be nicer if we could do "real" decimal point alignment
            anchor = tk.E if is_numeric_dtype(column_dtype) else tk.W
            # XXX type signature appears to be wrong, or at least overly restrictive.
            # I think I'm going to replace treeview anyway so I'm ignoring it for now.
            self.treeview.column(
                n, anchor=anchor, width=100, minwidth=100, stretch=tk.YES
            )  # type: ignore

        for row in self.treeview.get_children():
            self.treeview.delete(row)

        for n, (index, *values) in enumerate(ddf.itertuples()):
            values = ["—" if is_nan(v) else str(v) for v in values]
            self.treeview.insert("", n, text=index, values=values)

    def destroy(self):
        self.frame.destroy()
