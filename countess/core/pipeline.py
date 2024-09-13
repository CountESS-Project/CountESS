import logging
import re
import time
from queue import Empty, Queue
from threading import Thread
from typing import Any, Iterable, Optional

from countess.core.plugins import BasePlugin, FileInputPlugin, ProcessPlugin, get_plugin_classes

PRERUN_ROW_LIMIT = 100000

logger = logging.getLogger(__name__)


class SentinelQueue(Queue):

    """This is an easy and a bit lazy way of making a queue iterable.
    The writer is expected to call `queue.finish()` when it is done and
    the reader can treat the queue like an iterable."""

    # catch attempts to 'put' more data onto the queue after it has finished.
    finished = False

    # Handle multiple threads reading from the
    # queue in parallel: once the sentinel has been received by any thread
    # all further attempts to read get StopIterations.
    stopped = False

    class SENTINEL:
        pass

    def finish(self):
        self.put(self.SENTINEL)
        self.finished = True

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

    def put(self, item, block=True, timeout=None):
        if self.finished:
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

    def __init__(
        self,
        name: str,
        plugin: Optional[BasePlugin] = None,
        position: Optional[tuple[float, float]] = None,
        notes: Optional[str] = None,
        sort_column: int = 0,
        sort_descending: bool = False,
    ):
        self.name = name
        self.plugin = plugin
        self.position = position or (0.5, 0.5)
        self.sort_column = sort_column
        self.sort_descending = sort_descending
        self.notes = notes
        self.parent_nodes = set()
        self.child_nodes = set()
        self.output_queues = set()
        self.config: list[tuple[str, str, str]] = []

    def set_config(self, key, value, base_dir):
        self.config.append((key, value, base_dir))

    def __hash__(self):
        return id(self)

    def is_ancestor_of(self, node: "PipelineNode") -> bool:
        return (self in node.parent_nodes) or any((self.is_ancestor_of(n) for n in node.parent_nodes))

    def is_descendant_of(self, node: "PipelineNode") -> bool:
        return (self in node.child_nodes) or any((self.is_descendant_of(n) for n in node.child_nodes))

    def add_output_queue(self) -> SentinelQueue:
        queue = SentinelQueue(maxsize=3)
        self.output_queues.add(queue)
        return queue

    def queue_output(self, result):
        for data in result:
            self.counter_out += 1
            # XXX can we do this out-of-order if any queues are full?
            for queue in self.output_queues:
                queue.put(data)
            logger.info("%s: %d/%d", self.name, self.counter_out, self.counter_in)

    def finish_output(self):
        for queue in self.output_queues:
            queue.finish()

    def run_multithread(self, queue: SentinelQueue, name: str, row_limit: Optional[int] = None):
        assert isinstance(self.plugin, ProcessPlugin)
        for data_in in queue:
            self.counter_in += 1
            self.plugin.preprocess(data_in, name)
            self.queue_output(self.plugin.process(data_in, name))

    def run_subthread(self, queue: SentinelQueue, name: str, row_limit: Optional[int] = None):
        assert isinstance(self.plugin, ProcessPlugin)

        for data_in in queue:
            self.counter_in += 1
            self.plugin.preprocess(data_in, name)
            self.queue_output(self.plugin.process(data_in, name))
        self.queue_output(self.plugin.finished(name))

    def run_thread(self, row_limit: Optional[int] = None):
        """For each PipelineNode, this is run in its own thread."""
        assert isinstance(self.plugin, (ProcessPlugin, FileInputPlugin))

        logger.info("%s: 0%%", self.name)

        self.plugin.prepare([node.name for node in self.parent_nodes], row_limit)

        if len(self.parent_nodes) == 1:
            assert isinstance(self.plugin, ProcessPlugin)
            # there is only a single parent node, run several subthreads to
            # do the processing
            only_parent_node = list(self.parent_nodes)[0]
            only_parent_queue = only_parent_node.add_output_queue()
            subthreads = [
                Thread(target=self.run_multithread, args=(only_parent_queue, only_parent_node.name, row_limit))
                for _ in range(0, 4)
            ]
            for subthread in subthreads:
                subthread.start()
            for subthread in subthreads:
                subthread.join()

            self.queue_output(self.plugin.finished(only_parent_node.name))

        elif len(self.parent_nodes) > 1:
            assert isinstance(self.plugin, ProcessPlugin)
            # there are multiple parent nodes: spawn off a subthread to handle
            # each of them.
            subthreads = [
                Thread(
                    target=self.run_subthread,
                    args=(parent_node.add_output_queue(), parent_node.name, row_limit),
                )
                for parent_node in self.parent_nodes
            ]
            for subthread in subthreads:
                subthread.start()
            for subthread in subthreads:
                subthread.join()

        self.queue_output(self.plugin.finalize())
        self.finish_output()

        logger.info("%s: 100%%", self.name)

    def load_config(self):
        assert isinstance(self.plugin, BasePlugin)
        if self.config:
            for key, val, base_dir in self.config:
                try:
                    self.plugin.set_parameter(key, val, base_dir)
                except KeyError:
                    logger.warning("Parameter %s=%s Not Found", key, val)
                except ValueError:
                    logger.warning("Parameter %s=%s Not Valid", key, val)
            self.config = None

    def prerun(self, row_limit=PRERUN_ROW_LIMIT):
        if not self.plugin:
            return
        self.load_config()
        if self.is_dirty:
            assert isinstance(self.plugin, (ProcessPlugin, FileInputPlugin))
            self.result = []
            self.plugin.prepare([node.name for node in self.parent_nodes], row_limit)

            for parent_node in self.parent_nodes:
                assert isinstance(self.plugin, ProcessPlugin)
                parent_node.prerun(row_limit)
                if parent_node.result:
                    for data_in in parent_node.result:
                        self.plugin.preprocess(data_in, parent_node.name)
                    for data_in in parent_node.result:
                        for data_out in self.plugin.process(data_in, parent_node.name):
                            self.result.append(data_out)
                            logger.info("%s: %s/0", self.name, len(self.result))
                for data_out in self.plugin.finished(parent_node.name):
                    self.result.append(data_out)
                    logger.info("%s: %s/0", self.name, len(self.result))
            for data_out in self.plugin.finalize():
                self.result.append(data_out)
                logger.info("%s: %s/0", self.name, len(self.result))
            self.is_dirty = False
            logger.info("%s: 100%%", self.name)

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

    def configure_plugin(self, key, value, base_dir="."):
        self.plugin.set_parameter(key, value, base_dir)
        self.mark_dirty()

    def detach(self):
        for parent_node in self.parent_nodes:
            parent_node.child_nodes.discard(self)
        for child_node in self.child_nodes:
            child_node.parent_nodes.discard(self)


class PipelineGraph:
    def __init__(self, nodes: Optional[list[PipelineNode]] = None):
        self.plugin_classes = get_plugin_classes()
        self.nodes = nodes or []

    def reset_node_name(self, node: PipelineNode):
        node_names_seen = set(n.name for n in self.nodes if n != node)
        while node.name in node_names_seen:
            num = 1
            if match := re.match(r"(.*?)\s+(\d+)$", node.name):
                node.name = match.group(1)
                num = int(match.group(2))
            node.name += f" {num + 1}"

    def add_node(self, node: PipelineNode):
        self.reset_node_name(node)
        self.nodes.append(node)

    def del_node(self, node: PipelineNode):
        node.detach()
        self.nodes.remove(node)

    def find_node(self, name: str) -> Optional[PipelineNode]:
        for node in self.nodes:
            if node.name == name:
                return node
        return None

    def traverse_nodes(self) -> Iterable[PipelineNode]:
        found_nodes = set(node for node in self.nodes if not node.parent_nodes)
        yield from found_nodes

        while len(found_nodes) < len(self.nodes):
            for node in self.nodes:
                if node not in found_nodes and node.parent_nodes.issubset(found_nodes):
                    yield node
                    found_nodes.add(node)

    def traverse_nodes_backwards(self) -> Iterable[PipelineNode]:
        found_nodes = set(node for node in self.nodes if not node.child_nodes)
        yield from found_nodes

        while len(found_nodes) < len(self.nodes):
            for node in self.nodes:
                if node not in found_nodes and node.child_nodes.issubset(found_nodes):
                    yield node
                    found_nodes.add(node)

    def run(self):
        threads_and_nodes = []
        logger.info("Starting")
        start_time = time.time()
        for node in self.traverse_nodes_backwards():
            node.load_config()
            threads_and_nodes.append((Thread(target=node.run_thread), node))

        for thread, _ in threads_and_nodes:
            thread.start()

        while any(t.is_alive() for t, _ in threads_and_nodes):
            logger.info("Elapsed time: %d", time.time() - start_time)
            time.sleep(10)

        logger.info("Finished, elapsed time: %d", time.time() - start_time)

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
