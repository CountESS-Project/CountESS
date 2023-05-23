import tkinter as tk
from tkinter import ttk
import io
from functools import partial


class TabularDataFrame(tk.Frame):

    subframe = None
    dataframe = None
    offset = 0
    height = 1000
    length = 0
    select_rows = None
    labels = []
    columns = []
    scrollbar = None

    def reset(self):
        if self.subframe:
            self.subframe.destroy()
        self.rowconfigure(0, weight=1)
        self.columnconfigure(0, weight=1)
        self.subframe = tk.Frame(self)
        self.subframe.rowconfigure(0, weight=0)
        self.subframe.rowconfigure(1, weight=1)
        self.subframe.grid(sticky=tk.NSEW)
        self.subframe.bind('<Configure>', self.__frame_configure)

    def set_dataframe(self, dataframe):
        self.reset()
        self.dataframe = dataframe
        self.length = len(dataframe)

        self.labels = [ tk.Label(self.subframe, text=str(col)) for col in dataframe.columns ]
        for num, label in enumerate(self.labels):
            label.grid(row=0, column=num, sticky=tk.EW)
            #label.bind("<Button-1>", partial(self.__label_button_1, num))
            label.bind("<B1-Motion>", partial(self.__label_b1_motion, num))
            self.subframe.columnconfigure(num, minsize=10, weight=1)

        self.columns = [ tk.Text(self.subframe) for _ in dataframe.columns ]
        for num, column in enumerate(self.columns):
            column.grid(sticky=tk.NSEW, row=1, column=num)
            column['wrap'] = tk.NONE
            column['xscrollcommand'] = partial(self.__column_xscrollcommand, num)
            column['yscrollcommand'] = self.__column_yscrollcommand
            column.bind('<Button-4>', self.__column_scroll)
            column.bind('<Button-5>', self.__column_scroll)
            column.bind('<<Selection>>', partial(self.__column_selection, num))
            column.bind('<Control-C>', self.__column_copy)
            column.bind('<<Copy>>', self.__column_copy)

        self.scrollbar = ttk.Scrollbar(self.subframe, orient=tk.VERTICAL)
        self.scrollbar.grid(sticky=tk.NS, row=1, column=len(self.columns))
        self.scrollbar['command'] = self.__scrollbar_command
        self.refresh()

    def refresh(self):
        # Refreshes the column widgets.
        # XXX not in a clever way.
        for num, col in enumerate(self.dataframe.columns):
            print("REFRESH {num}")
            cw = self.columns[num]
            cw['state'] = tk.NORMAL
            cw.delete("1.0", tk.END)
            for _, row in self.dataframe.iloc[self.offset:self.offset+self.height+1].iterrows():
                cw.insert(tk.END, str(row[col]) + "\n")

            cw['state'] = tk.DISABLED

        if self.length:
            self.scrollbar.set(self.offset/self.length, (self.offset+self.height)/self.length)
        else:
            self.scrollbar.set(0, 1)

    def scrollto(self, new_offset):
        self.offset = min(max(int(new_offset), 0), self.length - self.height)
        self.refresh()

    def __label_b1_motion(self, num, event):
        # Detect label drags left and right.
        # XXX not quite right yet
        label = self.labels[num]
        label_width = label.winfo_width()

        if num < len(self.labels) - 1 and event.x > label_width:
            next_label = self.labels[num+1]
            next_label_width = next_label.winfo_width()
            self.subframe.columnconfigure(num, minsize=event.x)
            self.subframe.columnconfigure(num+1, minsize=next_label_width+label_width-event.x)
        elif num != 0 and event.x < 0:
            prev_label = self.labels[num-1]
            prev_label_width = prev_label.winfo_width()
            self.subframe.columnconfigure(num-1, minsize=prev_label_width+event.x)
            self.subframe.columnconfigure(num, minsize=label_width+event.x)

    def __scrollbar_command(self, command, *parameters):
        # Detect scrollbar movement and move self.offset
        # to compensate.
        if command == 'moveto':
            self.scrollto(float(parameters[0])*(self.length-self.height))
        elif command == 'scroll':
            self.scrollto(self.offset + int(parameters[0]))
        else:
            self.scrollto(0)

    def __column_xscrollcommand(self, num, x1, x2):
        # XXX if x2 < 1.0 this column is partly hidden.
        pass

    # XXX the following two functions are clever but not
    # efficient!

    def __column_yscrollcommand(self, y1, y2):
        # All this actually does is to detect if there's
        # too many rows for the window, in which case it
        # trims them off.  Once there's the right number
        # of rows, this won't get called any more.
        span = int((float(y2) - float(y1)) * self.height)
        if span > 0 and span != self.height:
            self.height = span
            self.refresh()

    def __frame_configure(self, *_):
        # If we've resized the window, start with a huge
        # number of rows and let __cw_yscrollcommand trim
        # it back down again.  Probably could be more
        # sensible.
        self.height = 1000
        self.refresh()

    def __column_scroll(self, event):
        # Detect scrollwheel motion on any of the columns
        if event.num == 4:
            self.scrollto(self.offset - 5)
        elif event.num == 5:
            self.scrollto(self.offset + 5)

    def __column_selection(self, num, _):
        # If there's a multi-row selection, then mark the
        # whole rows and set self.select_rows so we know
        # to copy the whole rows if <<Copy>> occurs.
        try:
            i1, i2 = self.columns[num].tag_ranges('sel')
            r1 = int(float(str(i1)))
            r2 = int(float(str(i2)))
        except ValueError:
            r1 = 0
            r2 = 0

        for cw in self.columns:
            cw.tag_delete('xsel')

        if r1 != r2:
            self.select_rows = (r1, r2)
            for cw in self.columns:
                cw.tag_add('xsel', f"{r1}.0", f"{r2}.end")
                cw.tag_config('xsel', background='lightgrey')
        else:
            self.select_rows = None

    def __column_copy(self, event):
        # If this was a multi-row selection, then replace
        # the copy buffer with a copy of those whole rows.
        if not self.select_rows:
            return # not multi-row, keep it.

        r1, r2 = self.select_rows
        df = self.dataframe.iloc[self.offset+r1-1 : self.offset + r2]
        buf = io.StringIO()
        df.to_csv(buf, sep='\t', newline='\n')

        # XXX very cheesy, but self.clipboard_append() etc didn't
        # seem to work, so this is a terrible workaround ... dump the
        # TSV into a new tk.Text, select the whole thing and copy it
        # into the clipboard.
        top = tk.Toplevel()
        text = tk.Text(top)
        text.insert(tk.END, buf)
        text.tag_add('sel', '1.0', tk.END)
        text.event_generate('<<Copy>>')
        top.destroy()
