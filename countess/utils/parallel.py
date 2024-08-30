import gc
import logging
import threading
import time
from multiprocessing import Process, Queue, Value
from os import cpu_count, getpid
from queue import Empty
from typing import Callable, Iterable

try:
    from typing import Concatenate, ParamSpec, TypeVar
except ImportError:  # pragma: no cover
    # for Python 3.9 compatibility
    from typing_extensions import Concatenate, ParamSpec, TypeVar  #  type: ignore

import psutil

D = TypeVar("D")
V = TypeVar("V")
P = ParamSpec("P")

logger = logging.getLogger(__name__)


class IterableMultiprocessQueue:
    """This connects a multiprocessing.Queue with a multiprocessing.Value
    and gives us a queue that multiple reader processes can iterate over and
    they'll each get a StopIteration when the Queue is both finished *and*
    empty."""

    def __init__(self, maxsize=3):
        self.queue = Queue(maxsize=maxsize)
        self.finished = Value("b", False)

    def put(self, value, timeout=None):
        if self.finished.value:
            raise ValueError("IterableMultiprocessQueue Stopped")
        self.queue.put(value, timeout=timeout)

    def finish(self):
        self.finished.value = True

    def close(self):
        self.queue.close()

    def __iter__(self):
        return self

    def __next__(self):
        while True:
            try:
                return self.queue.get(timeout=0.1)
            except Empty as exc:
                if self.finished.value:
                    raise StopIteration from exc


def multiprocess_map(
    function: Callable[Concatenate[V, P], Iterable[D]], values: Iterable[V], *args: P.args, **kwargs: P.kwargs
) -> Iterable[D]:
    """Pretty much equivalent to:

        def multiprocess_map(function, values, *args, **kwargs):
            yield from interleave_longest(function(v, *args, **kwargs) for v in values)

    but runs in multiple processes using an input_queue
    to organize `values` and an output_queue to organize the
    returned values.  Or to look at it from a different direction, because
    `multiprocessing.Pool.imap_unordered` doesn't take `args`."""

    # Start up several workers.
    nproc = ((cpu_count() or 1) + 1) // 2
    input_queue = IterableMultiprocessQueue(maxsize=nproc)
    output_queue: Queue = Queue(maxsize=3)

    def __process():  # pragma: no cover
        # this is run in a pool of `nproc` processes to handle resource-intensive
        # processes which don't play nicely with the GIL.
        # XXX Coverage doesn't seem to understand this so we exclude it from coverage.

        for data_in in input_queue:
            for data_out in function(data_in, *args, **(kwargs or {})):
                output_queue.put(data_out)

                # Make sure large data is disposed of before we
                # go around for the next loop
                del data_out
            del data_in
            gc.collect()

            # Prevent processes from using up all available memory while waiting
            # XXX this is probably a bad idea
            while psutil.virtual_memory().percent > 90:
                logger.warning("PID %d LOW MEMORY %f%%", getpid(), psutil.virtual_memory().percent)
                time.sleep(1)

    processes = [Process(target=__process, name=f"worker {n}") for n in range(0, nproc)]
    for p in processes:
        p.start()

    # separate thread is in charge of pushing items into the input_queue
    def __enqueue():
        for v in values:
            input_queue.put(v)
        input_queue.finish()

    thread = threading.Thread(target=__enqueue)
    thread.start()

    # wait for all processes to finish and yield any data which appears
    # on the output_queue as soon as it is available.
    while any(p.is_alive() for p in processes):
        try:
            while True:
                yield output_queue.get(timeout=0.1)
        except Empty:
            # Waiting for the next output, might as well tidy up
            gc.collect()

    # once all processes have finished, we can clean up the queue.
    thread.join()
    for p in processes:
        p.join()
    input_queue.close()
    output_queue.close()
