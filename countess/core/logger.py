from typing import Optional

import sys

class Logger:

    def __init__(self):
        pass

    def progress(self, message: str = 'Running', percentage: Optional[int] = None):
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
        self.prefix = prefix

    def progress(self, message: str = 'Running', percentage: Optional[int] = None):
        if self.prefix: message = self.prefix + ": " + message
        if percentage: message += " [{int(percentage):2d}%]"
        self.stdout.write(f"{message}\n")

    def log(self, level: str, message: str, row: Optional[int] = None, col: Optional[int] = None, detail: Optional[str] = None):
        if self.prefix: message = self.prefix + ": " + message
        if row: message = f"{message} [row={row}]"
        if col: message = f"{message} [col={col}]"
        if detail: message += " " + repr(detail)

        self.stderr.write(message + "\n")

