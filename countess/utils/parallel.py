import threading
import time
from multiprocessing import Process, Queue, Value
from os import cpu_count, getpid
from queue import Empty
from typing import Iterable

import psutil



def multi_iterator_map(function, values, args, progress_cb=None) -> Iterable:
    """Pretty much equivalent to:
        interleave_longest(function(v, *args) for v in values)
    but runs in multiple processes using a queue
    to organize `values` and another queue to organize the
    returned values."""

    nproc = ((cpu_count() or 1) + 1) // 2
    queue1: Queue = Queue()
    queue2: Queue = Queue(maxsize=3)

    # XXX do this a better way
    len_values = [0]
    yield_count = 0

    if progress_cb:
        progress_cb(0)

    enqueue_running = Value("b", True)

    def __enqueue():
        for v in values:
            len_values[0] += 1
            queue1.put(v)
        enqueue_running.value = False

    thread = threading.Thread(target=__enqueue)
    thread.start()
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

                    data_in = queue1.get(timeout=1)
                    for data_out in function(data_in, *args):
                        queue2.put(data_out)

                        # Make sure large data is disposed of before we
                        # go around for the next loop
                        del data_out
                    del data_in

            except Empty:
                if not enqueue_running.value:
                    break

    processes = [Process(target=__process, name=f"worker {n}") for n in range(0, nproc)]
    for p in processes:
        p.start()

    while thread.is_alive() or any(p.is_alive() for p in processes):
        try:
            yield queue2.get(timeout=1)
            yield_count += 1
        except Empty:
            pass
        if progress_cb and len_values[0]:
            progress_cb((100 * yield_count) // len_values[0])

    if progress_cb:
        progress_cb(100)
