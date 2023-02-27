"""Logger base classes"""

import sys
from typing import Optional


class Logger:
    """Logger Base Class"""

    def __init__(self):
        pass

    def progress(self, message: str = "Running", percentage: Optional[int] = None):
        """Record progress"""
        return None

    def log(self, level: str, message: str, detail: Optional[str] = None):
        """Log a message"""
        return None

    def info(self, message: str, detail: Optional[str] = None):
        """Log a message at level info"""
        self.log("info", message, detail)

    def warning(self, message: str, detail: Optional[str] = None):
        """Log a message at level warning"""
        self.log("warning", message, detail)

    def error(self, message: str, detail: Optional[str] = None):
        """Log a message at level error"""
        self.log("error", message, detail)

    def clear(self):
        """Clear logs (if possible)"""
        return None


class ConsoleLogger(Logger):
    """A simple Logger which sends output to stdout and stderr"""

    def __init__(self, stdout=sys.stdout, stderr=sys.stderr, prefix: Optional[str] = None):
        self.stdout = stdout
        self.stderr = stderr
        self.prefix = prefix

    def progress(self, message: str = "Running", percentage: Optional[int] = None):
        if self.prefix:
            message = self.prefix + ": " + message
        if percentage:
            message += f" [{int(percentage):2d}%]"
        self.stdout.write(f"{message}\n")

    def log(self, level: str, message: str, detail: Optional[str] = None):
        if self.prefix:
            message = self.prefix + ": " + message
        if detail:
            message += " " + repr(detail)

        self.stderr.write(message + "\n")
