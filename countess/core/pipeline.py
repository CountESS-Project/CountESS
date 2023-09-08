from dataclasses import dataclass, field
from multiprocessing import Process, Queue
from os import cpu_count
from queue import Empty
from typing import Any, Iterable, Optional

from countess.core.logger import Logger
from countess.core.plugins import BasePlugin, FileInputPlugin, ProcessPlugin, get_plugin_classes

PRERUN_ROW_LIMIT = 100000


def multi_iterator_map(function, values, args) -> Iterable:
    """Pretty much equivalent to:
        interleave_longest(function(v, *args) for v in values)
    but runs in multiple processes using a queue
    to organize `values` and another queue to organize the
    returned values."""

    nproc = min(((cpu_count() or 1) + 1, len(values)))
    queue1: Queue = Queue()
    queue2: Queue = Queue(maxsize=nproc)

    for v in values:
        queue1.put(v)

    def __target():
        try:
            while True:
                v = queue1.get(timeout=1)
                for x in function(v, *args):
                    queue2.put(x)
        except Empty:
            pass

    processes = [Process(target=__target) for _ in range(0, nproc)]
    for p in processes:
        p.start()

    while any(p.is_alive() for p in processes):
        try:
            yield queue2.get(timeout=1)
        except Empty:
            pass


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
            for name, it in list(iters_dict.items()):
                try:
                    yield from self.plugin.process(next(it), name, logger)
                except StopIteration:
                    del iters_dict[name]
                    yield from self.plugin.finished(name, logger)
        yield from self.plugin.finalize(logger)

    def execute(self, logger: Logger, row_limit: Optional[int] = None):
        assert row_limit is None or isinstance(row_limit, int)

        if self.plugin is None:
            self.result = []
            return
        elif self.result and not self.is_dirty:
            return
        elif isinstance(self.plugin, FileInputPlugin):
            num_files = self.plugin.num_files()
            if not num_files:
                self.result = []
                return

            row_limit_each_file = row_limit // num_files if row_limit is not None else None
            self.result = multi_iterator_map(
                self.plugin.load_file, range(0, num_files), args=(logger, row_limit_each_file)
            )
        elif isinstance(self.plugin, ProcessPlugin):
            self.plugin.prepare([p.name for p in self.parent_nodes], row_limit)
            self.result = self.process_parent_iterables(logger)

        if row_limit is not None or len(self.child_nodes) != 1:
            self.result = list(self.result)

        self.is_dirty = False

    def load_config(self, logger: Logger):
        assert isinstance(self.plugin, BasePlugin)
        if self.config:
            for key, val, base_dir in self.config:
                try:
                    self.plugin.set_parameter(key, val, base_dir)
                except (KeyError, ValueError) as exc:
                    logger.warning(f"Parameter {key}={val} Not Found")
                    print(exc)
            self.config = None

    def prerun(self, logger: Logger, row_limit=PRERUN_ROW_LIMIT):
        assert isinstance(logger, Logger)

        if self.is_dirty and self.plugin:
            logger.progress("Start")
            for parent_node in self.parent_nodes:
                parent_node.prerun(logger, row_limit)
            self.load_config(logger)
            self.execute(logger, row_limit)
            self.is_dirty = False
            logger.progress("Done")

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
