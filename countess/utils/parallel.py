import gc
import logging
from multiprocessing import Process, Queue, Value
from os import cpu_count, getpid
from queue import Empty, Full
import time
from typing import Callable, Iterable

try:
    from typing import Concatenate, ParamSpec, TypeVar
except ImportError:
    # for Python 3.9 compatibility
    from typing_extensions import Concatenate, ParamSpec, TypeVar  # type: ignore

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

    def close(self):
        self.finished.value = True
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

    def __process():
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

    # push each of the input values onto the input_queue, if it gets full
    # then also try to drain the output_queue.
    for v in values:
        while True:
            try:
                input_queue.put(v, timeout=0.1)
                break
            except Full:
                try:
                    yield output_queue.get(timeout=0.1)
                except Empty:
                    # Waiting for the next output, might as well tidy up
                    gc.collect()

    # we're finished with input values, so close the input_queue to
    # signal to all the processes that there will be no new entries
    # and once the queue is empty they can finish.
    input_queue.close()

    # wait for all processes to finish and yield any data which appears
    # on the output_queue
    while any(p.is_alive() for p in processes):
        try:
            while True:
                yield output_queue.get(timeout=0.1)
        except Empty:
            # Waiting for the next output, might as well tidy up
            gc.collect()

    # once all processes have finished, we can clean up the queue.
    for p in processes:
        p.join()
    output_queue.close()
