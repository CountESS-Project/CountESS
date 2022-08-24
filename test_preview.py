import tkinter as tk
import tkinter.ttk as ttk

import dask.dataframe as dd
import pandas as pd
import gc
import random

from countess.core.gui import DataFramePreview

index = [
    ''.join(random.choices("AGTC", k=150))
    for x in range(0,200000)
]

columns = ['count_0', 'count_1a', 'count_1b', 'count_2a', 'count_2b']
data = [
    random.choices([1,2,3,4,5],k=len(columns))
    for x in range(0,len(index))
]

pdf = pd.DataFrame(data, columns=columns, index=index)

data=None
index=None
gc.collect()

ddf = dd.from_pandas(pdf, npartitions=50)

pdf = None
gc.collect()

root = tk.Tk()

z = DataFramePreview(root, ddf)

root.rowconfigure(0,weight=1)
root.columnconfigure(0,weight=1)
z.grid(sticky="nsew")


root.mainloop()



