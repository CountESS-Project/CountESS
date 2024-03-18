import gc
import threading
import time
from multiprocessing import Process, Queue, Value
from os import cpu_count, getpid
from queue import Empty
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
    input_queue: Queue = Queue()
    output_queue: Queue = Queue(maxsize=3)

    # XXX is it actually necessary to have this in a separate thread or
    # would it be sufficient to add items to input_queue alternately with
    # removing items from output_queue?

    enqueue_running = Value("b", True)

    def __enqueue():
        for v in values:
            input_queue.put(v)
        enqueue_running.value = False

    thread = threading.Thread(target=__enqueue)
    thread.start()

    # XXX is this necessary?
    time.sleep(1)

    def __process():
        while True:
            try:
                while True:
                    # Prevent processes from using up all
                    # available memory while waiting
                    # XXX this is probably a bad idea
                    while psutil.virtual_memory().percent > 75:
                        print(f"{getpid()} LOW MEMORY {psutil.virtual_memory().percent}")
                        time.sleep(1)

                    data_in = input_queue.get(timeout=1)
                    for data_out in function(data_in, *args, **(kwargs or {})):
                        output_queue.put(data_out)

                        # Make sure large data is disposed of before we
                        # go around for the next loop
                        del data_out
                    del data_in
                    gc.collect()

            except Empty:
                if not enqueue_running.value:
                    break

    processes = [Process(target=__process, name=f"worker {n}") for n in range(0, nproc)]
    for p in processes:
        p.start()

    while thread.is_alive() or any(p.is_alive() for p in processes):
        try:
            yield output_queue.get(timeout=0.1)
        except Empty:
            # Waiting for the next input, might as well tidy up
            gc.collect()
