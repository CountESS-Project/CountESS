import re
import time
from queue import Empty, Queue
from threading import Thread
from typing import Any, Optional

from countess.core.logger import Logger
from countess.core.plugins import BasePlugin, FileInputPlugin, ProcessPlugin, get_plugin_classes

PRERUN_ROW_LIMIT = 100000


class SentinelQueue(Queue):

    """This is an easy and a bit lazy way of making a queue iterable.
    The writer is expected to call `queue.finish()` when it is done and
    the reader can treat the queue like an iterable."""

    # XXX this is an attempt to handle multiple threads reading from the
    # queue in parallel: they should all get StopIterations.
    stopped = False

    class SENTINEL:
        pass

    def finish(self):
        self.put(self.SENTINEL)

    def __iter__(self):
        return self

    def get_or_raise_on_stopped(self):
        while True:
            if self.stopped:
                raise StopIteration
            try:
                return super().get(timeout=1)
            except Empty:
                pass

    def __next__(self):
        val = self.get_or_raise_on_stopped()
        if val is self.SENTINEL:
            self.stopped = True
            raise StopIteration
        return val

    def get(self, block=True, timeout=None):
        if self.stopped:
            raise ValueError("SentinelQueue stopped")
        return super().get(block, timeout)

    def put(self, item, block=True, timeout=None):
        if self.stopped:
            raise ValueError("SentinelQueue stopped")
        super().put(item, block, timeout)


class PipelineNode:
    name: str
    plugin: Optional[BasePlugin] = None
    position: Optional[tuple[float, float]] = None
    sort_column: int = 0
    sort_descending: bool = False
    notes: Optional[str] = None
    parent_nodes: set["PipelineNode"]
    child_nodes: set["PipelineNode"]
    config: Optional[list[tuple[str, str, str]]] = None
    result: Any = None
    is_dirty: bool = True

    output_queues: set[SentinelQueue]
    counter_in: int = 0
    counter_out: int = 0

    # XXX config is a cache for config loaded from the file
    # at config load time, if it is present it is loaded the
    # first time the plugin is prerun.

    def __init__(self, name, plugin=None, config=None, position=None, notes=None, sort_column=0, sort_descending=0):
        self.name = name
        self.plugin = plugin
        self.config = config or []
        self.position = position
        self.sort_column = sort_column
        self.sort_descending = sort_descending
        self.notes = notes
        self.parent_nodes = set()
        self.child_nodes = set()
        self.output_queues = set()

    def __hash__(self):
        return id(self)

    def is_ancestor_of(self, node):
        return (self in node.parent_nodes) or any((self.is_ancestor_of(n) for n in node.parent_nodes))

    def is_descendant_of(self, node):
        return (self in node.child_nodes) or any((self.is_descendant_of(n) for n in node.child_nodes))

    def add_output_queue(self):
        queue = SentinelQueue(maxsize=3)
        self.output_queues.add(queue)
        return queue

    def clear_output_queues(self):
        self.output_queues = set()

    def queue_output(self, result):
        for data in result:
            self.counter_out += 1
            # XXX can we do this out-of-order if any queues are full?
            for queue in self.output_queues:
                queue.put(data)

    def finish_output(self):
        for queue in self.output_queues:
            queue.finish()

    def run_multithread(self, queue: SentinelQueue, name: str, logger: Logger, row_limit: Optional[int] = None):
        assert isinstance(self.plugin, ProcessPlugin)
        for data_in in queue:
            self.counter_in += 1
            self.plugin.preprocess(data_in, name, logger)
            self.queue_output(self.plugin.process(data_in, name, logger))

    def run_subthread(self, queue: SentinelQueue, name: str, logger: Logger, row_limit: Optional[int] = None):
        assert isinstance(self.plugin, ProcessPlugin)

        for data_in in queue:
            self.counter_in += 1
            self.plugin.preprocess(data_in, name, logger)
            self.queue_output(self.plugin.process(data_in, name, logger))
        self.queue_output(self.plugin.finished(name, logger))

    def run_thread(self, logger: Logger, row_limit: Optional[int] = None):
        """For each PipelineNode, this is run in its own thread."""
        assert isinstance(self.plugin, (ProcessPlugin, FileInputPlugin))

        self.plugin.prepare([node.name for node in self.parent_nodes], row_limit)

        if len(self.parent_nodes) == 1:
            assert isinstance(self.plugin, ProcessPlugin)
            # there is only a single parent node, run several subthreads to
            # do the processing
            only_parent_node = list(self.parent_nodes)[0]
            only_parent_queue = only_parent_node.add_output_queue()
            subthreads = [
                Thread(target=self.run_multithread, args=(only_parent_queue, only_parent_node.name, logger, row_limit))
                for _ in range(0, 4)
            ]
            for subthread in subthreads:
                subthread.start()
            for subthread in subthreads:
                subthread.join()

            self.queue_output(self.plugin.finished(only_parent_node.name, logger))

        elif len(self.parent_nodes) > 1:
            assert isinstance(self.plugin, ProcessPlugin)
            # there are multiple parent nodes: spawn off a subthread to handle
            # each of them.
            subthreads = [
                Thread(
                    target=self.run_subthread,
                    args=(parent_node.add_output_queue(), parent_node.name, logger, row_limit),
                )
                for parent_node in self.parent_nodes
            ]
            for subthread in subthreads:
                subthread.start()
            for subthread in subthreads:
                subthread.join()

        self.queue_output(self.plugin.finalize(logger))
        self.finish_output()

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
        if not self.plugin:
            return
        self.load_config(logger)
        if self.is_dirty:
            assert isinstance(self.plugin, (ProcessPlugin, FileInputPlugin))
            self.result = []
            self.plugin.prepare([node.name for node in self.parent_nodes], row_limit)

            for parent_node in self.parent_nodes:
                assert isinstance(self.plugin, ProcessPlugin)
                parent_node.prerun(logger, row_limit)
                if parent_node.result:
                    for data_in in parent_node.result:
                        self.plugin.preprocess(data_in, parent_node.name, logger)
                    for data_in in parent_node.result:
                        self.result += list(self.plugin.process(data_in, parent_node.name, logger))
                self.result += list(self.plugin.finished(parent_node.name, logger))
            self.result += list(self.plugin.finalize(logger))
            self.is_dirty = False

    def mark_dirty(self):
        self.is_dirty = True
        for child_node in self.child_nodes:
            if not child_node.is_dirty:
                child_node.mark_dirty()

    def add_parent(self, parent):
        if (not self.plugin or self.plugin.num_inputs) and (not parent.plugin or parent.plugin.num_outputs):
            self.parent_nodes.add(parent)
            parent.child_nodes.add(self)
            self.mark_dirty()

    def del_parent(self, parent):
        self.parent_nodes.discard(parent)
        parent.child_nodes.discard(self)
        self.mark_dirty()

    def has_sibling(self):
        return any(len(pn.child_nodes) > 1 for pn in self.parent_nodes)

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
    def __init__(self):
        self.plugin_classes = get_plugin_classes()
        self.nodes = []

    def reset_node_name(self, node):
        node_names_seen = set(n.name for n in self.nodes if n != node)
        while node.name in node_names_seen:
            num = 1
            if match := re.match(r"(.*?)\s+(\d+)$", node.name):
                node.name = match.group(1)
                num = int(match.group(2))
            node.name += f" {num + 1}"

    def add_node(self, node):
        self.reset_node_name(node)
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

    def traverse_nodes_backwards(self):
        found_nodes = set(node for node in self.nodes if not node.child_nodes)
        yield from found_nodes

        while len(found_nodes) < len(self.nodes):
            for node in self.nodes:
                if node not in found_nodes and node.child_nodes.issubset(found_nodes):
                    yield node
                    found_nodes.add(node)

    def run(self, logger):
        threads_and_nodes = []
        for node in self.traverse_nodes_backwards():
            node.load_config(logger)
            threads_and_nodes.append((Thread(target=node.run_thread, args=(logger,)), node))

        for thread, _ in threads_and_nodes:
            thread.start()

        while True:
            print("------------------")
            for thread, node in threads_and_nodes[::-1]:
                if thread.is_alive():
                    print("%-40s %d %d" % (node.name, node.counter_in, node.counter_out))
            if not any(t.is_alive() for t, _ in threads_and_nodes):
                break
            time.sleep(10)

    def reset(self):
        for node in self.nodes:
            node.result = None
            node.is_dirty = True

    def reset_node_names(self):
        node_names_seen = set()
        for node in self.traverse_nodes():
            while node.name in node_names_seen:
                num = 0
                if match := re.match(r"(.*?)\s+(\d+)$", node.name):
                    node.name = match.group(1)
                    num = int(match.group(2))
                node.name += f" {num + 1}"
            node_names_seen.add(node.name)

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
