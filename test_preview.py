import tkinter as tk
import tkinter.ttk as ttk

import dask.dataframe as dd
import pandas as pd

from countess.core.gui import DataFramePreview

index = [
    "B_%04d" % x for x in range(0,10000000)
]

columns = ['count_0', 'count_1a', 'count_1b', 'count_2a', 'count_2b']
data = [
    [x*(c+1) for c in range(0,len(columns))]
    for x in range(0,len(index))
    ]

ddf = dd.from_pandas(pd.DataFrame(data, columns=columns, index=index), npartitions=1)

root = tk.Tk()

z = DataFramePreview(root, ddf)

root.rowconfigure(0,weight=1)
root.columnconfigure(0,weight=1)
z.grid(sticky="nsew")


root.mainloop()



