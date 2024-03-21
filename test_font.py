import tkinter as tk
import tkinter.font as tk_font

root = tk.Tk()
font = tk_font.Font()

print(font.measure("\u2795"))
print(font.measure("m"))
