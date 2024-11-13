import itertools
import os
import tkinter as tk
from tkinter import ttk

class FileDialog(tk.Frame):

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        tk.Label(self, text="Directory:").grid(row=0, column=0, sticky=tk.EW)

        self.dir_var = tk.StringVar(self, value='')
        self.dir_box = ttk.Combobox(self, textvariable=self.dir_var)
        self.dir_box["state"] = "readonly"
        self.dir_box.grid(row=0, column=1, sticky=tk.EW)

        tk.Button(self, text="Up", command=self.dir_button).grid(row=0, column=2, sticky=tk.EW)

        self.file_list = tk.Frame(self)
        self.file_list.grid(row=1, columnspan=3, sticky=tk.NSEW)

        tk.Label(self, text="File name:").grid(row=2, column=0, sticky=tk.EW)

        self.file_var = tk.StringVar(self, value='')
        self.file_entry = tk.Entry(self, textvariable=self.file_var)
        self.file_entry.grid(row=2, column=1, sticky=tk.EW)
        self.file_var.trace("w", self.file_var_callback)

        tk.Button(self, text="Open", command=self.open_button).grid(row=2, column=2, sticky=tk.EW)

        tk.Label(self, text="Files of type:").grid(row=3, column=0, sticky=tk.EW)

        self.type_var = tk.StringVar(self, value='')
        self.type_box = ttk.Combobox(self, textvariable=self.type_var)
        self.type_box["state"] = "readonly"
        self.type_box.grid(row=3, column=1, sticky=tk.EW)
        self.type_var.trace("w", self.type_var_callback)

        tk.Button(self, text="Cancel", command=self.cancel_button).grid(row=3, column=2, sticky=tk.EW)

        self.columnconfigure(1, weight=1)
        self.rowconfigure(1, weight=1)

        self.file_types = [
            ("All files", [ "*" ])
        ]
        self.file_type_mask = [ '' ]

        self.file_labels = []
        self.files_selected = set()

    def set_file_types(self, file_types):
        self.file_types = file_types
        self.type_box["values"] = [ x + " (" + ", ".join(y) + ")" for x, y in file_types ]
        self.type_box.current(0)

    def type_var_callback(self, *_):
        choice = self.type_box.current()
        self.file_type_mask = [ x.removeprefix('*') for x in self.file_types[choice][1] ]
        self.update_files()

    def file_var_callback(self, *_):
        file_names = self.file_var.get().split(' ')
        self.files_selected = set()

        for fn in file_names:
            for fl in self.file_labels:
                fln = fl['text']
                if '*' in fn:
                    if fln.startswith(fn[:fn.index('*')]) and fln.endswith(fn[fn.index('*')+1:]):
                        self.files_selected.add(fln)
                elif fln == fn:
                    self.files_selected.add(fln)
        self.update_labels()

    def set_file_dir(self, file_dir):
        d = os.path.realpath(file_dir)
        dirs = [ '/' ]
        while d != '/':
            dirs.insert(1, d)
            d, _ = os.path.split(d)

        self.dir_box["values"] = dirs
        self.dir_var.set(dirs[-1])
        self.file_var.set('')

        self.update_files()

    def set_file_name(self, file_name):
        self.file_var.set(file_name)

    def dir_button(self):
        dirs = self.dir_box["values"][0:-1]
        if dirs:
            self.dir_box["values"] = dirs
            self.dir_var.set(dirs[-1])
            self.file_var.set('')
            self.update_files()

    def open_button(self):
        pass

    def cancel_button(self):
        pass

    def update_files(self):
        for label in self.file_labels:
            label.destroy()
        self.file_labels = []

        this_dir = self.dir_var.get()
        files = sorted(os.listdir(this_dir), key=lambda f: (not os.path.isdir(f), f))
        num = 0
        for file in files:
            abs_file = os.path.join(this_dir, file)
            if os.path.isdir(file):
                label = tk.Label(self.file_list, text=file, compound=tk.LEFT, takefocus=1)
                label["bitmap"] = "gray75"
                label.bind("<Double-Button-1>", lambda ev, d=abs_file: self.set_file_dir(d))
            elif any(file.endswith(x) for x in self.file_type_mask):
                label = tk.Label(self.file_list, text=file, compound=tk.LEFT, takefocus=1)
                label["bitmap"] = "gray12"
                label.bind("<Button-1>", lambda ev, num=num: self.on_click_label(num))
                label.bind("<Control-Button-1>", lambda ev, num=num: self.on_click_label(num, ctrl=True))
                label.bind("<Shift-Button-1>", lambda ev, num=num: self.on_click_label(num, shift=True))
            else:
                continue

            self.file_labels.append(label)
            num += 1
        self.update_labels()

    def on_click_label(self, num, ctrl=False, shift=False):
        # XXX this shouldn't really rely on the filename being in the label
        file_name = self.file_labels[num]["text"]
        self.set_file_name(file_name)
        self.update_labels()

    def update_labels(self):
        for num, label in enumerate(self.file_labels):
            label.grid(column=num // 10, row=num % 10, sticky=tk.W)
            if label["text"] in self.files_selected:
                label.configure(bg="darkblue", fg="white")
            else:
                label.configure(bg=self.cget('bg'), fg='black')


class MultiFileDialog(FileDialog):

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.last_selected = 0

    def find_file_prefix_suffix(self):
        def __all_same(xs):
            return len(set(xs)) == 1

        # to see if this is globbable, first find common prefixes and suffixes ...
        prefix = ''.join(z[0] for z in itertools.takewhile(__all_same, zip(*self.files_selected)))
        reversed_suffixes = [list(reversed(f.removeprefix(prefix))) for f in self.files_selected]
        suffix = ''.join(z[0] for z in reversed(list(itertools.takewhile(__all_same, zip(*reversed_suffixes)))))

        return prefix, suffix

    def check_file_prefix_suffix(self, prefix, suffix):
        # XXX this shouldn't really rely on the filename being in the label
        for file_name in [ fl["text"] for fl in self.file_labels ]:
            if file_name.startswith(prefix) and file_name.endswith(suffix) and file_name not in self.files_selected:
                return False
        return True

    def on_click_label(self, num, ctrl=False, shift=False):
        # XXX this shouldn't really rely on the filename being in the label
        file_name = self.file_labels[num]["text"]
        if ctrl:
            self.files_selected ^= set([file_name])
        elif shift:
            if self.last_selected > num:
                for x in range(num, self.last_selected):
                    self.files_selected.add(self.file_labels[x]["text"])
            else:
                for x in range(self.last_selected, num+1):
                    self.files_selected.add(self.file_labels[x]["text"])
        else:
            self.files_selected = set([file_name])

        self.last_selected = num

        if len(self.files_selected) > 1:
            prefix, suffix = self.find_file_prefix_suffix()
            if self.check_file_prefix_suffix(prefix, suffix):
                self.set_file_name(prefix + '*' + suffix)
            else:
                self.set_file_name(' '.join(sorted(self.files_selected)))
        elif len(self.files_selected) == 1:
            self.set_file_name(list(self.files_selected)[0])
        else:
            self.set_file_name('')

        self.update_labels()
