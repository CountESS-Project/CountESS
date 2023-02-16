from typing import Optional

import sys

class Logger:

    def __init__(self):
        pass

    def progress(self, a: int, b: int, s: Optional[str] = None):
        pass
   
    def log(self, level: str, message: str, row: Optional[int] = None, col: Optional[int] = None, detail: Optional[str] = None):
        pass

    def info(self, message: str, row: Optional[int] = None, col: Optional[int] = None, detail: Optional[str] = None):
        self.log('info', message, row, col, detail)

    def warning(self, message: str, row: Optional[int] = None, col: Optional[int] = None, detail: Optional[str] = None):
        self.log('warning', message, row, col, detail)

    def error(self, message: str, row: Optional[int] = None, col: Optional[int] = None, detail: Optional[str] = None):
        self.log('error', message, row, col, detail)


class ConsoleLogger:

    def __init__(self, stdout=sys.stdout, stderr=sys.stderr, prefix: Optional[str] = None):
        self.stdout = stdout
        self.stderr = stderr

    def progress(self, a: int, b: int, s: Optional[str] = None):

        self.stdout.write(f"{a:4d}/{b:4d} {s}\n")

    def log(self, level: str, message: str, row: Optional[int] = None, col: Optional[int] = None, detail: Optional[str] = None):
        if prefix: message = prefix + ": " + message
        if row: message = f"{message} [row={row}]"
        if col: message = f"{message} [col={col}]"
        if detail: message += " " + repr(detail)

        self.stderr.write(message + "\n")

