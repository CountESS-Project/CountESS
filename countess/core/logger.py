"""Logger base classes"""

import multiprocessing
import queue
import sys
import traceback
from typing import Iterable, Optional, Tuple


class Logger:
    """Logger Base Class"""

    def __init__(self):
        pass

    def log(self, level: str, message: str, detail: Optional[str] = None):
        """Log a message"""
        return None

    def progress(self, message: str = "Running", percentage: Optional[int] = None):
        """Record progress"""
        self.log("progress", message, str(percentage) if percentage else None)

    def info(self, message: str, detail: Optional[str] = None):
        """Log a message at level info"""
        self.log("info", message, detail)

    def warning(self, message: str, detail: Optional[str] = None):
        """Log a message at level warning"""
        self.log("warning", message, detail)

    def error(self, message: str, detail: Optional[str] = None):
        """Log a message at level error"""
        self.log("error", message, detail)

    def exception(self, exception: Exception):
        # Slightly odd calling to maintain compatibility with 3.9 and 3.10
        # XXX format more nicely
        message = traceback.format_exception(None, value=exception, tb=None)
        message += "\n\n" + "".join(traceback.format_tb(exception.__traceback__))
        self.error(str(exception), detail="".join(message))

    def clear(self):
        """Clear logs (if possible)"""
        return None


class ConsoleLogger(Logger):
    """A simple Logger which sends output to stdout and stderr"""

    def __init__(self, stdout=sys.stdout, stderr=sys.stderr, prefix: Optional[str] = None):
        self.stdout = stdout
        self.stderr = stderr
        self.prefix = prefix

    def log(self, level: str, message: str, detail: Optional[str] = None):
        if self.prefix:
            message = self.prefix + ": " + message
        if detail:
            message += " " + repr(detail)

        self.stderr.write(message + "\n")

    def progress(self, message: str = "Running", percentage: Optional[int] = None):
        if percentage is not None:
            self.log("progress", "%-20s [%-10s] %d%%" % (message, "*" * (percentage // 10), percentage))
        else:
            self.log("progress", "%-20s [ Running! ]" % message)


class MultiprocessLogger(Logger):
    def __init__(self):
        self.queue = multiprocessing.Queue()

    def log(self, level: str, message: str, detail: Optional[str] = None):
        self.queue.put((level, message, detail))

    def poll(self) -> Iterable[Tuple[str, str, str]]:
        try:
            while True:
                yield self.queue.get_nowait()
        except queue.Empty:
            pass

    def dump(self):
        return "\n".join("%s\t%s\t%s" % (a, b, c) for a, b, c in self.poll())
