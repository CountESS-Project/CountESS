import threading
import time
from dataclasses import dataclass, field
from itertools import chain
from multiprocessing import Process, Queue, Value
from os import cpu_count, getpid
from queue import Empty
from typing import Any, Iterable, Optional

import psutil
from more_itertools import interleave_longest

from countess.core.logger import Logger
from countess.core.plugins import BasePlugin, FileInputPlugin, ProcessPlugin, SimplePlugin, get_plugin_classes

PRERUN_ROW_LIMIT = 100000


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


@dataclass
class PipelineNode:
    name: str
    plugin: Optional[BasePlugin] = None
    position: Optional[tuple[float, float]] = None
    notes: Optional[str] = None
    parent_nodes: set["PipelineNode"] = field(default_factory=set)
    child_nodes: set["PipelineNode"] = field(default_factory=set)
    config: Optional[list[tuple[str, str, str]]] = None
    result: Any = None
    is_dirty: bool = True

    # XXX config is a cache for config loaded from the file
    # at config load time, if it is present it is loaded the
    # first time the plugin is prerun.

    def __hash__(self):
        return id(self)

    def is_ancestor_of(self, node):
        return (self in node.parent_nodes) or any((self.is_ancestor_of(n) for n in node.parent_nodes))

    def is_descendant_of(self, node):
        return (self in node.child_nodes) or any((self.is_descendant_of(n) for n in node.child_nodes))

    def process_parent_iterables(self, logger):
        """Combines the values from all the input interables and processes
        them"""
        # XXX this really should be multiprocess and asynchronous.

        iters_dict = {p.name: iter(p.result) for p in self.parent_nodes}
        while iters_dict:
            logger.progress(self.name, None)

            for name, it in list(iters_dict.items()):
                try:
                    yield from self.plugin.process(next(it), name, logger)
                except StopIteration:
                    del iters_dict[name]
                    yield from self.plugin.finished(name, logger)
        yield from self.plugin.finalize(logger)

        logger.progress(self.name, 100)

    def plugin_process(self, x):
        self.plugin.process(*x)

    def execute(self, logger: Logger, row_limit: Optional[int] = None):
        assert row_limit is None or isinstance(row_limit, int)

        if self.plugin is None:
            self.result = []
            return

        elif row_limit is not None and self.result and not self.is_dirty:
            return

        elif isinstance(self.plugin, FileInputPlugin):
            num_files = self.plugin.num_files()
            if not num_files:
                self.result = []
                return
            if row_limit is not None:
                # for preview mode, just do everything in the one process.
                # XXX consider threads for this though
                row_limit_each_file = row_limit // num_files
                self.result = []
                for file_number in range(0, num_files):
                    logger.progress(self.name, file_number * 100 // num_files)
                    self.result += list(self.plugin.load_file(file_number, logger, row_limit_each_file))
                logger.progress(self.name, 100)
            else:
                self.result = multi_iterator_map(
                    self.plugin.load_file,
                    range(0, num_files),
                    args=(logger, None),
                    progress_cb=lambda p: logger.progress(self.name, p),
                )

        elif isinstance(self.plugin, ProcessPlugin) and row_limit is not None:
            # for preview mode, just do everything in the one process.
            self.plugin.prepare([p.name for p in self.parent_nodes], row_limit)
            self.result = []
            for pn in self.parent_nodes:
                for data_in in pn.result:
                    logger.progress(self.name, None)
                    try:
                        self.result += list(self.plugin.process(data_in, pn.name, logger))
                    except Exception as exc:  # pylint: disable=broad-exception-caught
                        logger.exception(exc)
            logger.progress(self.name, 100)
            self.result += list(self.plugin.finalize(logger))

        elif isinstance(self.plugin, SimplePlugin):
            self.plugin.prepare([p.name for p in self.parent_nodes], row_limit)

            input_data = interleave_longest(*[parent_node.result for parent_node in self.parent_nodes])

            self.result = chain(
                multi_iterator_map(
                    self.plugin.process,
                    input_data,
                    args=("", logger),
                    progress_cb=lambda p: logger.progress(self.name, p),
                ),
                self.plugin.finalize(logger),
            )

        elif isinstance(self.plugin, ProcessPlugin):
            self.plugin.prepare([p.name for p in self.parent_nodes], row_limit)
            self.result = self.process_parent_iterables(logger)

        # XXX at the moment, we freeze the results into an array
        # if we have multiple children, as *both children* will be
        # drawing items from the array.  This isn't the most efficient
        # strategy.

        if len(self.child_nodes) != 1:
            self.result = list(self.result)

        self.is_dirty = False

    def load_config(self, logger: Logger):
        assert isinstance(self.plugin, BasePlugin)
        if self.config:
            for key, val, base_dir in self.config:
                try:
                    self.plugin.set_parameter(key, val, base_dir)
                except (KeyError, ValueError):
                    logger.warning(f"Parameter {key}={val} Not Found")
            self.config = None

    def prerun(self, logger: Logger, row_limit=PRERUN_ROW_LIMIT):
        assert isinstance(logger, Logger)

        if self.is_dirty and self.plugin:
            for parent_node in self.parent_nodes:
                parent_node.prerun(logger, row_limit)
            self.load_config(logger)
            self.execute(logger, row_limit)
            self.is_dirty = False

    def mark_dirty(self):
        self.is_dirty = True
        for child_node in self.child_nodes:
            if not child_node.is_dirty:
                child_node.mark_dirty()

    def add_parent(self, parent):
        self.parent_nodes.add(parent)
        parent.child_nodes.add(self)
        self.mark_dirty()

    def del_parent(self, parent):
        self.parent_nodes.discard(parent)
        parent.child_nodes.discard(self)
        self.mark_dirty()

    def configure_plugin(self, key, value, base_dir="."):
        self.plugin.set_parameter(key, value, base_dir)
        self.mark_dirty()

    def final_descendants(self):
        if self.child_nodes:
            return set(n2 for n1 in self.child_nodes for n2 in n1.final_descendants())
        else:
            return set(self)

    def detatch(self):
        for parent_node in self.parent_nodes:
            parent_node.child_nodes.discard(self)
        for child_node in self.child_nodes:
            child_node.parent_nodes.discard(self)

    @classmethod
    def get_ancestor_list(cls, nodes):
        """Given a bunch of nodes, find the list of all the ancestors in a
        sensible order"""
        parents = set((p for n in nodes for p in n.parent_nodes))
        if not parents:
            return list(nodes)
        return cls.get_ancestor_list(parents) + list(nodes)


class PipelineGraph:
    # XXX doesn't actually do much except hold a bag of nodes

    def __init__(self):
        self.plugin_classes = get_plugin_classes()
        self.nodes = []

    def add_node(self, node):
        self.nodes.append(node)

    def del_node(self, node):
        node.detatch()
        self.nodes.remove(node)

    def traverse_nodes(self):
        found_nodes = set(node for node in self.nodes if not node.parent_nodes)
        yield from found_nodes

        while len(found_nodes) < len(self.nodes):
            for node in self.nodes:
                if node not in found_nodes and node.parent_nodes.issubset(found_nodes):
                    yield node
                    found_nodes.add(node)

    def run(self, logger):
        # XXX this is the last thing PipelineGraph actually does!
        # might be easier to just keep a set of nodes and sort through
        # them for output nodes, or something.

        for node in self.traverse_nodes():
            # XXX TODO there's some opportunity for easy parallelization here,
            # by pushing each node into a pool as soon as its parents are
            # complete.
            node.load_config(logger)
            node.execute(logger)

    def reset(self):
        for node in self.nodes:
            node.result = None
            node.is_dirty = True

    def tidy(self):
        """Tidies the graph (sets all the node positions)"""

        # XXX This is very arbitrary and not particularly efficient.
        # Some kind of FDP-like algorithm might be nice.
        # Especially if it could include node/line collisions.
        # See #24

        nodes = list(self.traverse_nodes())

        # first calculate a stratum for each node.

        stratum = {}
        for node in nodes:
            if not node.parent_nodes:
                stratum[node] = 0
            else:
                stratum[node] = max(stratum[n] for n in node.parent_nodes) + 1

        # shufffle nodes back down to avoid really long connections.

        for node in nodes[::-1]:
            if node.child_nodes:
                if len(node.parent_nodes) == 0:
                    stratum[node] = min(stratum[n] for n in node.child_nodes) - 1
                else:
                    stratum[node] = (
                        min(stratum[n] for n in node.child_nodes) + max(stratum[n] for n in node.parent_nodes)
                    ) // 2

        max_stratum = max(stratum.values())

        position = {}
        for s in range(0, max_stratum + 1):
            # now sort all the nodes by the average position of their parents,
            # to try and stop them forming a big tangle.  The current position
            # is included as a "tie breaker" and to keep some memory of the user's
            # preference for position (eg: ordering of branches)

            def avg_pos_parents(node):
                return sum(position[p] for p in node.parent_nodes) / len(node.parent_nodes)

            snodes = [
                (
                    avg_pos_parents(node) if node.parent_nodes else 0.5,
                    node.position[1],
                    n,
                )
                for n, node in enumerate(nodes)
                if stratum[node] == s
            ]
            snodes.sort()

            # Assign node positions with the stratums placed
            # evenly and the nodes spaced evenly per stratum.

            y = (s + 0.5) / (max_stratum + 1)
            for p, (_, _, n) in enumerate(snodes):
                x = (p + 0.5) / len(snodes)
                nodes[n].position = (y, x)
                position[nodes[n]] = x
