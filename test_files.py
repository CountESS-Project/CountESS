import tkinter as tk
import os

from countess.gui.files import FileDialog, MultiFileDialog

root = tk.Tk()

fd = MultiFileDialog(root)
fd.grid(sticky=tk.NSEW)

fd.set_file_dir(os.curdir)
fd.set_file_types([
    ( 'CSV', [ "*.csv", "*.csv.gz" ]),
    ( 'TSV', [ "*.tsv", "*.tsv.gz" ]),
    ( 'TXT', [ "*.txt" ])
])

root.columnconfigure(0, weight=1)
root.rowconfigure(0, weight=1)

root.mainloop()
