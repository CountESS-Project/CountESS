import tkinter as tk
import tkinter.font as tk_font

# We found a shared remote system where these unicode icons are
# displayed as escaped strings, literally "\u2795" on what should
# be the "+" button.  This is very confusing, so as a workaround
# check if the string "\u2795" (one unicode codepoint) renders over
# three em wide, in which case the Unicode support is fonts are broken.


def unicode_is_broken():
    root = tk.Tk()
    font = tk_font.Font(root)
    is_broken = font.measure("\u2795") > 3 * font.measure("m")
    root.destroy()
    return is_broken


if unicode_is_broken():
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
