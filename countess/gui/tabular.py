import io
import logging
import tkinter as tk
from functools import partial
from math import ceil, floor, isinf, isnan
from tkinter import ttk
from typing import Callable, Optional, Union

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess.gui.widgets import ResizingFrame, copy_to_clipboard, get_icon
from countess.utils.duckdb import (
    duckdb_dtype_is_integer,
    duckdb_dtype_is_numeric,
    duckdb_dtype_to_datatype_choice,
    duckdb_escape_identifier,
)

logger = logging.getLogger(__name__)

# XXX columns should automatically resize based on information
# from _column_xscrollcommand which can tell if they're
# overflowing.  Or maybe use
#    df['seq'].str.len().max() to get max length of a string column
# etc etc.


def column_format_for(table: DuckDBPyRelation, column: str) -> str:
    # logger.debug("column_format_for column %s %s", column, table.columns)

    # XXX https://github.com/duckdb/duckdb/issues/15267
    # dtype = table[column].dtypes[0]
    dtype = table.project(duckdb_escape_identifier(column)).dtypes[0]

    if duckdb_dtype_is_numeric(dtype):
        # Work out the maximum width required to represent the integer part in this
        # column, so we can pad values to that width.
        column_esc = duckdb_escape_identifier(column)
        column_min_max = table.aggregate(f"min({column_esc}), max({column_esc})").fetchone()
        if column_min_max is None:
            return "%s"
        column_min, column_max = column_min_max
        if column_min is None or isnan(column_min) or isinf(column_min):
            column_min = -100
        if column_max is None or isnan(column_max) or isinf(column_max):
            column_max = 100
        width = max(len(str(floor(column_min))), len(str(ceil(column_max))))
        if duckdb_dtype_is_integer(dtype):
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
    #     100.0 => "100.000000000000" => "100."
    try:
        if column_format.endswith("f"):
            return (column_format % value).rstrip("0")
        else:
            return column_format % value
    except TypeError:
        return str(value)


class TabularDataFrame(tk.Frame):
    """A frame for displaying a duckdb relation
    Columns are displayed as individual tk.Text widgets which seems to be relatively efficient
    as they only hold the currently displayed rows.  Tested up to a million or so rows."""

    subframe: Optional[tk.Frame] = None
    ddbc: Optional[DuckDBPyConnection] = None
    table: Optional[DuckDBPyRelation] = None
    table_order: str = ""
    offset = 0
    height = 1000
    length = 0
    select_rows = None
    labels: list[tk.Label] = []
    columns: list[tk.Text] = []
    column_formats: list[str] = []
    scrollbar = None
    sort_by_col: Optional[int] = None
    sort_ascending = True
    callback: Optional[Callable[[int, int, bool], None]] = None
    click_callback: Optional[Callable[[int, int, int], None]] = None

    def __init__(self, *a, **k):
        super().__init__(*a, **k)

        self.rowconfigure(0, weight=0)
        self.rowconfigure(1, weight=0)
        self.rowconfigure(2, weight=1)
        self.columnconfigure(0, weight=1)
        self.columnconfigure(1, weight=0)

        self.label = tk.Label(self, text="Dataframe Preview")
        self.label.pack(fill="x")

        self.label_frame = tk.Frame(self, height=60)
        self.label_frame.pack(fill="x")

        self.scrollbar = ttk.Scrollbar(self, orient=tk.VERTICAL)
        self.scrollbar.pack(side="right", fill="y")
        self.scrollbar["command"] = self._scrollbar_command

        self.bind("<Configure>", self._configure)

    def set_table(self, ddbc: DuckDBPyConnection, table: DuckDBPyRelation, offset: Optional[int] = 0):
        self.ddbc = ddbc
        self.table = table
        self.length = len(table)

        column_names = table.columns
        column_dtypes = table.dtypes
        self.column_formats = [column_format_for(table, name) for name in column_names]

        n_columns = len(column_names)
        if n_columns == 0:
            self.label["text"] = "Dataframe Preview\n\nno data"
            return

        # make new labels for all the columns
        for label in self.labels:
            label.destroy()
        self.labels = []
        for num, (name, dtype) in enumerate(zip(column_names, column_dtypes)):
            label_text = f"{name}\n{duckdb_dtype_to_datatype_choice(dtype)}"
            column_label = tk.Label(
                self.label_frame,
                text=label_text,
                image=get_icon(self, "sort_un"),
                compound=tk.RIGHT,
            )
            column_label.bind("<Button-1>", partial(self._label_button_1, num))
            self.labels.append(column_label)

        # make a subframe to hold the column texts

        if self.subframe:
            self.subframe.destroy()
        self.subframe = ResizingFrame(self, orientation=ResizingFrame.Orientation.HORIZONTAL, bg="darkgrey")
        self.subframe.pack(side="left", fill="both", expand=True)

        self.label["text"] = f"Dataframe Preview {len(self.table)} rows"

        self.columns = []
        for num, (name, dtype) in enumerate(zip(column_names, column_dtypes)):
            column_text = tk.Text(self.subframe)
            self.subframe.add_child(column_text)
            column_text["wrap"] = tk.NONE
            column_text["yscrollcommand"] = self._column_yscrollcommand
            column_text.bind("<Button-1>", partial(self._column_click, num))
            column_text.bind("<Button-4>", self._column_scroll)
            column_text.bind("<Button-5>", self._column_scroll)
            column_text.bind("<<Selection>>", partial(self._column_selection, num))
            column_text.bind("<Control-C>", self._column_copy)
            column_text.bind("<<Copy>>", self._column_copy)
            column_text.bind("<Configure>", partial(self._column_configure, num))
            self.columns.append(column_text)

    def _column_configure(self, num: int, ev):
        # when the column changes position, move the labels around
        # to match
        self.labels[num].place(x=ev.x, width=ev.width)

    def _column_click(self, col: int, ev):
        if self.click_callback:
            line, char = ev.widget.index("current").split(".")
            row = min(self.offset + int(line), self.length) - 1
            self.click_callback(col, row, int(char))

    def refresh(self, new_offset: float = 0):
        # Refreshes the column widgets.
        assert self.table is not None
        assert self.scrollbar

        self.offset = max(0, min(self.length - self.height, int(new_offset)))

        table = self.table.order(self.table_order) if self.table_order else self.table
        rows = table.limit(self.height, offset=self.offset).fetchall()

        for column_num, (column_widget, column_format) in enumerate(zip(self.columns, self.column_formats)):
            column_widget["state"] = tk.NORMAL
            column_widget.delete("1.0", tk.END)
            for row in rows:
                column_widget.insert(tk.END, format_value(row[column_num], column_format) + "\n")
            column_widget["state"] = tk.DISABLED

        if self.length:
            self.scrollbar.set(self.offset / self.length, (self.offset + self.height) / self.length)

        if self.callback:
            self.callback(self.offset, self.sort_by_col, not self.sort_ascending)

    def set_callback(self, callback) -> None:
        self.callback = callback

    def set_click_callback(self, click_callback) -> None:
        self.click_callback = click_callback

    _set_sort_order_thread = None

    def set_sort_order(self, column_num: int, descending: Optional[bool] = None):
        assert self.ddbc is not None
        assert self.table is not None

        if not 0 <= column_num < len(self.table.columns):
            column_num = 0

        if descending is None and column_num == self.sort_by_col:
            self.sort_ascending = not self.sort_ascending
        else:
            self.sort_ascending = not descending
        self.sort_by_col = column_num

        # Create an index for whatever column we're sorting by, followed by
        # all other column names for a stable sort order.

        column_name = self.table.columns[column_num]
        escaped_column_names = [
            duckdb_escape_identifier(c) for c in [column_name] + [c for c in self.table.columns if c != column_name]
        ]

        index_identifier = f"{self.table.alias}_{column_num}_idx"

        # XXX I did have some code here which built the index in a separate thread / cursor but it
        # was mad with race conditions so I've scrapped it for now and the GUI will just have to
        # awkwardly pause.

        sql = (
            f"CREATE INDEX IF NOT EXISTS {index_identifier} ON {self.table.alias} ("
            + ",".join(escaped_column_names)
            + ")"
        )
        self.ddbc.sql(sql)

        direction = "ASC" if self.sort_ascending else "DESC"
        self.table_order = ",".join(f"{c} {direction}" for c in escaped_column_names)

        logger.debug("TabularDataFrame set_sort_order %s %s", column_name, self.table_order)

        for n, label in enumerate(self.labels):
            icon = "sort_un" if n != column_num else "sort_up" if self.sort_ascending else "sort_dn"
            label.configure(image=get_icon(self, icon))

        self.refresh()

    def _label_button_1(self, num, event):
        """Click on column labels to set sort order"""
        self.set_sort_order(num)
        if self.callback:
            self.callback(self.offset, self.sort_by_col, not self.sort_ascending)

    def _scrollbar_command(self, command, *parameters):
        # Detect scrollbar movement and move self.offset
        # to compensate.
        if command == "moveto":
            self.refresh(float(parameters[0]) * (self.length - self.height))
        elif command == "scroll":
            self.refresh(self.offset + int(parameters[0]))
        else:
            self.refresh()

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

    def _configure(self, *_):
        # the delay lets the sub-elements configure
        # themselves before we try to measure them.
        self.after(10, self._reset_height)

    def _reset_height(self):
        # Start with a huge number of rows and let
        # _cw_yscrollcommand trim it back down again.
        # Probably could be more efficient.
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

        # pick the selected rows
        table = self.table.order(self.table_order) if self.table_order else self.table
        r1, r2 = self.select_rows
        limit = r2 - r1 + 1
        offset = self.offset + r1 - 1
        logger.debug("TabularDataFrame._column_copy r1 %d r2 %d limit %d offset %d", r1, r2, limit, offset)
        table = table.limit(limit, offset=offset)

        # Dump TSV into a StringIO and push it onto the clipboard
        buf = io.StringIO()
        table.to_df().to_csv(buf, sep="\t", index=False)
        copy_to_clipboard(buf.getvalue())
