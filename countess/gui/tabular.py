import io
import tkinter as tk
from functools import partial
from math import ceil, floor, isinf, isnan
from tkinter import ttk
from typing import Callable, Optional, Union

import pandas as pd
from pandas.api.types import is_integer_dtype, is_numeric_dtype

from countess.gui.widgets import get_icon

# XXX columns should automatically resize based on information
# from _column_xscrollcommand which can tell if they're
# overflowing.  Or maybe use
#    df['seq'].str.len().max() to get max length of a string column
# etc etc.


def column_format_for(df_column: Union[pd.Index, pd.Series]) -> str:
    if is_numeric_dtype(df_column.dtype):
        # Work out the maximum width required to represent the integer part in this
        # column, so we can pad values to that width.
        column_min = df_column.min()
        if isnan(column_min) or isinf(column_min):
            column_min = -100
        column_max = df_column.max()
        if isnan(column_max) or isinf(column_max):
            column_max = 100
        width = max(len(str(floor(column_min))), len(str(ceil(column_max))))
        if is_integer_dtype(df_column.dtype):
            return f"%{width}d"
        else:
            # leave room for the point and 12 decimals.
            # format_value will remove trailing 0s.
            return f"%{width+13}.12f"
    else:
        return "%s"


def format_value(value: Optional[Union[int, float, str]], column_format: str) -> str:
    """Format value for display in a table:
    >>> format_value(None, "%s")
    '—'
    >>> format_value(107, "%4d")
    ' 107'
    >>> format_value(1.23, "%15.12f")
    ' 1.23'
    >>> format_value("foo", "%15.12f")
    'foo'

    """

    if value is None or (type(value) is float and isnan(value)):
        return "—"
    if value is True:
        return "—T"
    if value is False:
        return "—F"

    # remove trailing 0's from floats (%g doesn't align correctly)
    try:
        if column_format.endswith("f"):
            return (column_format % value).rstrip("0")
        else:
            return column_format % value
    except TypeError:
        return str(value)


class TabularDataFrame(tk.Frame):
    """A frame for displaying a pandas (or similar) dataframe.
    Columns are displayed as individual tk.Text widgets which seems to be relatively efficient
    as they only hold the currently displayed rows.  Tested up to a million or so rows."""

    subframe: Optional[tk.Frame] = None
    dataframe: Optional[pd.DataFrame] = None
    offset = 0
    height = 1000
    length = 0
    select_rows = None
    labels: list[tk.Label] = []
    columns: list[tk.Text] = []
    column_formats: list[str] = []
    scrollbar = None
    index_cols = 0
    sort_by_col = None
    sort_ascending = True
    callback: Optional[Callable[[int, int, bool], None]] = None

    def reset(self):
        if self.subframe:
            self.subframe.destroy()
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.subframe = tk.Frame(self)
        self.subframe.rowconfigure(0, weight=0)
        self.subframe.rowconfigure(1, weight=0)
        self.subframe.rowconfigure(2, weight=1)
        self.subframe.grid(sticky=tk.NSEW)

    def set_dataframe(self, dataframe: pd.DataFrame, offset: Optional[int] = 0):
        self.reset()
        assert self.subframe

        self.dataframe = dataframe
        self.length = len(dataframe)

        if hasattr(self.dataframe.index, "names") and hasattr(self.dataframe.index, "dtypes"):
            # MultiIndex case
            column_names = list(self.dataframe.index.names) + list(self.dataframe.columns)
            column_dtypes = list(self.dataframe.index.dtypes) + list(self.dataframe.dtypes)
            index_frame = self.dataframe.index.to_frame()
            self.column_formats = [column_format_for(index_frame[name]) for name in dataframe.index.names] + [
                column_format_for(dataframe[name]) for name in dataframe.columns
            ]
            self.index_cols = len(self.dataframe.index.names)
        elif self.dataframe.index.name:
            # a simple Index, with a name
            column_names = [self.dataframe.index.name] + list(self.dataframe.columns)
            column_dtypes = [self.dataframe.index.dtype] + list(self.dataframe.dtypes)
            self.column_formats = [column_format_for(dataframe.index)] + [
                column_format_for(dataframe[name]) for name in dataframe.columns
            ]
            self.index_cols = 1
        else:
            # if it doesn't have a name, don't bother displaying it
            # XXX it's probably just a RangeIndex, should we display it anyway?
            column_names = list(self.dataframe.columns)
            column_dtypes = list(self.dataframe.dtypes)
            self.column_formats = [column_format_for(dataframe[name]) for name in dataframe.columns]
            self.index_cols = 0

        if len(column_names) == 0:
            label = tk.Label(self.subframe, text="Dataframe Preview\n\nno data")
            label.grid(row=0, column=0, sticky=tk.NSEW)
            return

        title = tk.Label(self.subframe, text=f"Dataframe Preview {len(self.dataframe)} rows")
        title.grid(row=0, column=0, columnspan=len(column_names) * 2 + 1, sticky=tk.NSEW, pady=5)

        ### XXX add in proper handling for MultiIndexes here

        # Even-numbered columns are the data columns

        self.labels = []
        for num, (name, dtype) in enumerate(zip(column_names, column_dtypes)):
            if type(name) is tuple:
                name = "\n".join([str(n) for n in name])
            else:
                name = str(name)
            is_index = " (index)" if num < self.index_cols else ""
            label = tk.Label(
                self.subframe,
                text=f"{name}\n{dtype}{is_index}",
                image=get_icon(self, "sort_un"),
                compound=tk.RIGHT,
            )
            label.grid(row=1, column=num * 2, sticky=tk.EW)
            label.bind("<Button-1>", partial(self._label_button_1, num))
            self.subframe.columnconfigure(num * 2, minsize=10, weight=1)
            self.labels.append(label)

        # Between them are blank columns which provide a handle for adjusting the column
        # widths left and right

        for num in range(0, len(column_names) - 1):
            adjuster = tk.Frame(self.subframe, width=3, cursor="sb_h_double_arrow")
            adjuster.grid(row=1, rowspan=2, column=num * 2 + 1, sticky=tk.NSEW)
            adjuster.bind("<B1-Motion>", partial(self._column_adjust, num))

        if len(self.dataframe) == 0:
            label = tk.Label(self.subframe, text="no data")
            label.grid(row=2, column=0, columnspan=len(column_names), sticky=tk.NSEW)
            return

        self.columns = [tk.Text(self.subframe) for _ in column_names]
        for num, column in enumerate(self.columns):
            column.grid(sticky=tk.NSEW, row=2, column=num * 2)
            column["wrap"] = tk.NONE
            column["xscrollcommand"] = partial(self._column_xscrollcommand, num)
            column["yscrollcommand"] = self._column_yscrollcommand
            column.bind("<Button-4>", self._column_scroll)
            column.bind("<Button-5>", self._column_scroll)
            column.bind("<<Selection>>", partial(self._column_selection, num))
            column.bind("<Control-C>", self._column_copy)
            column.bind("<<Copy>>", self._column_copy)
        if self.columns:
            self.columns[0].bind("<Configure>", self._column_configure)

        self.scrollbar = ttk.Scrollbar(self.subframe, orient=tk.VERTICAL)
        self.scrollbar.grid(sticky=tk.NS, row=2, column=len(self.columns) * 2 - 1)
        self.scrollbar["command"] = self._scrollbar_command
        self.refresh(offset)

    def refresh(self, new_offset=0):
        # Refreshes the column widgets.
        # XXX should handle new_height as well, as this changes a fair bit
        # with some window managers. Needs refactoring.

        new_offset = max(0, min(self.length - self.height, int(new_offset)))
        offset_diff = new_offset - self.offset

        # get the new rows as an iterator
        if 1 <= offset_diff < self.height:
            df = self.dataframe.iloc[self.offset + self.height : self.offset + self.height + offset_diff]
            insert_at = tk.END
        elif 1 <= -offset_diff < self.height:
            # Get rows in reverse order so they can be inserted at the start
            # note offset_diff is negative!
            # XXX check this isn't horribly inefficient with pandas indexes
            if new_offset:
                df = self.dataframe.iloc[self.offset - 1 : new_offset - 1 : -1]
            else:
                df = self.dataframe.iloc[self.offset - 1 :: -1]
            insert_at = "1.0"
        else:
            df = self.dataframe.iloc[new_offset : new_offset + self.height + 1]
            insert_at = tk.END

        # then unlock the columns and delete unnecessary rows
        for cw in self.columns:
            cw["state"] = tk.NORMAL
            if 1 <= offset_diff < self.height:
                # delete rows at start, add new rows on the end
                cw.delete("1.0", f"{offset_diff+1}.0")
            elif 1 <= -offset_diff < self.height:
                # delete rows at end, insert new rows at start
                # note offset_diff is negative!  Note we have to
                # restore the deleted final "\n".  Sigh.
                cw.delete(f"{self.height+offset_diff+2}.0", tk.END)
                cw.insert(tk.END, "\n")
            else:
                # delete everything & add all new rows
                cw.delete("1.0", tk.END)

        for idx, row in df.iterrows():
            if type(idx) is tuple:
                values = list(idx) + list(row)
            elif self.dataframe.index.name:
                values = [idx] + list(row)
            else:
                values = list(row)

            for value, column, column_format in zip(values, self.columns, self.column_formats):
                column.insert(insert_at, format_value(value, column_format) + "\n")

        for cw in self.columns:
            cw["state"] = tk.DISABLED

        self.offset = new_offset
        if self.length:
            self.scrollbar.set(self.offset / self.length, (self.offset + self.height) / self.length)

        if self.callback:
            self.callback(self.offset, self.sort_by_col, not self.sort_ascending)

    def set_callback(self, callback) -> None:
        self.callback = callback

    def set_sort_order(self, column_num: int, descending: Optional[bool] = None):
        assert self.dataframe is not None

        if descending is None and column_num == self.sort_by_col:
            self.sort_ascending = not self.sort_ascending
        else:
            self.sort_by_col = column_num
            self.sort_ascending = not descending
        if column_num < self.index_cols:
            self.dataframe = self.dataframe.sort_index(level=column_num, ascending=self.sort_ascending)
        else:
            self.dataframe = self.dataframe.sort_values(
                self.dataframe.columns[column_num - self.index_cols], ascending=self.sort_ascending
            )

        for n, label in enumerate(self.labels):
            icon = "sort_un" if n != column_num else "sort_up" if self.sort_ascending else "sort_dn"
            label.configure(image=get_icon(self, icon))

        self.refresh()

    def _label_button_1(self, num, event):
        """Click on column labels to set sort order"""
        self.set_sort_order(num)
        if self.callback:
            self.callback(self.offset, self.sort_by_col, not self.sort_ascending)

    def _column_adjust(self, num, event):
        """Adjust column widths left and right by dragging the dummy columns"""
        w0 = self.labels[num].winfo_width()
        w1 = self.labels[num + 1].winfo_width()
        self.subframe.columnconfigure(num * 2, minsize=w0 + event.x)
        self.subframe.columnconfigure(num * 2 + 2, minsize=w1 - event.x)

    def _scrollbar_command(self, command, *parameters):
        # Detect scrollbar movement and move self.offset
        # to compensate.
        if command == "moveto":
            self.refresh(float(parameters[0]) * (self.length - self.height))
        elif command == "scroll":
            self.refresh(self.offset + int(parameters[0]))
        else:
            self.refresh()

    def _column_xscrollcommand(self, num, x1, x2):
        # XXX this gets called as the table is displayed
        # if x2 < 1.0 this column is partially hidden.
        pass

    # XXX the following two functions are clever but not
    # very efficient!  There's got to be a nicer way of
    # detecting this surely?

    def _column_yscrollcommand(self, y1, y2):
        # All this actually does is to detect if there's
        # too many rows for the window, in which case it
        # trims them off.  Once there's the right number
        # of rows, this won't get called any more.
        span = int((float(y2) - float(y1)) * self.height)
        if span > 0 and span != self.height:
            self.height = span
            self.refresh()

    def _column_configure(self, *_):
        # If we've resized the window, start with a huge
        # number of rows and let _cw_yscrollcommand trim
        # it back down again.  Probably could be more
        # sensible.
        self.height = min(self.length, 1000)
        self.refresh()

    def _column_scroll(self, event):
        # Detect scrollwheel motion on any of the columns
        if event.num == 4:
            self.refresh(self.offset - 5)
        elif event.num == 5:
            self.refresh(self.offset + 5)

    def _column_selection(self, num, _):
        # If there's a multi-row selection, then mark the
        # whole rows and set self.select_rows so we know
        # to copy the whole rows if <<Copy>> occurs.
        try:
            i1, i2 = self.columns[num].tag_ranges("sel")
            r1 = int(float(str(i1)))
            r2 = int(float(str(i2)))
        except ValueError:
            r1 = 0
            r2 = 0

        for cw in self.columns:
            cw.tag_delete("xsel")

        if r1 != r2:
            self.select_rows = (r1, r2)
            for cw in self.columns:
                cw.tag_add("xsel", f"{r1}.0", f"{r2+1}.0")
                cw.tag_config("xsel", background="lightgrey")
        else:
            self.select_rows = None

    def _column_copy(self, _):
        # If this was a multi-row selection, then replace
        # the copy buffer with a copy of those whole rows.
        if not self.select_rows:
            return  # not multi-row, keep it.

        r1, r2 = self.select_rows
        df = self.dataframe.iloc[self.offset + r1 - 1 : self.offset + r2]
        buf = io.StringIO()
        df.to_csv(buf, sep="\t")

        # XXX very cheesy, but self.clipboard_append() etc didn't
        # seem to work, so this is a terrible workaround ... dump the
        # TSV into a new tk.Text, select the whole thing and copy it
        # into the clipboard.
        top = tk.Toplevel()
        text = tk.Text(top)
        text.insert(tk.END, buf.getvalue())
        text.tag_add("sel", "1.0", tk.END)
        text.event_generate("<<Copy>>")
        top.destroy()
