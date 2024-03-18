import tkinter as tk

# Tcl support for unicode starts in version 8.1, April 1999.

if tk.TclVersion < 8.1:
    UNICODE_CHECK = "Y"
    UNICODE_UNCHECK = "N"
    UNICODE_CROSS = "X"
    UNICODE_PLUS = "+"
    UNICODE_INFO = "i"

else:
    UNICODE_CHECK = "\u2714"
    UNICODE_UNCHECK = "\u2717"
    UNICODE_CROSS = "\u2715"
    UNICODE_PLUS = "\u2795"
    UNICODE_INFO = "\u2139"
