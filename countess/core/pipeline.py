import logging
import re
import secrets
import threading
import time
import traceback
from enum import Enum
from typing import Iterable, Optional

import duckdb

from countess.core.plugins import BasePlugin, DuckdbPlugin, get_plugin_classes
from countess.utils.duckdb import duckdb_source_to_view

logger = logging.getLogger(__name__)


class PipelineNodeStatus(Enum):
    DIRTY = 0
    WORKING = 1
    RESULT = 2
    ERROR = 3


class PipelineNode:
    name: str
    uuid: str
    plugin: Optional[BasePlugin] = None
    position: Optional[tuple[float, float]] = None
    sort_column: int = 0
    sort_descending: bool = False
    notes: Optional[str] = None
    parent_nodes: set["PipelineNode"]
    child_nodes: set["PipelineNode"]
    status: PipelineNodeStatus = PipelineNodeStatus.DIRTY

    # XXX config is a cache for config loaded from the file
    # at config load time, if it is present it is loaded the
    # first time the plugin is prerun.
    config: list[tuple[str, str, str]] = []

    def __init__(
        self,
        name: str,
        uuid: Optional[str] = None,
        plugin: Optional[BasePlugin] = None,
        position: Optional[tuple[float, float]] = None,
        notes: Optional[str] = None,
        sort_column: int = 0,
        sort_descending: bool = False,
    ):
        self.name = name
        self.uuid = uuid or secrets.token_hex(16)
        self.plugin = plugin
        self.position = position or (0.5, 0.5)
        self.sort_column = sort_column
        self.sort_descending = sort_descending
        self.notes = notes
        self.parent_nodes = set()
        self.child_nodes = set()
        self.config: list[tuple[str, str, str]] = []
        self.table_name: Optional[str] = None
        self.message: Optional[str] = None

    def set_config(self, key, value, base_dir):
        self.config.append((key, value, base_dir))

    def __hash__(self):
        return id(self)

    def is_ancestor_of(self, node: "PipelineNode") -> bool:
        return (self in node.parent_nodes) or any((self.is_ancestor_of(n) for n in node.parent_nodes))

    def is_descendant_of(self, node: "PipelineNode") -> bool:
        return (self in node.child_nodes) or any((self.is_descendant_of(n) for n in node.child_nodes))

    def load_config(self):
        assert isinstance(self.plugin, BasePlugin)
        if self.config:
            for key, val, base_dir in self.config:
                try:
                    self.plugin.set_parameter(key, val, base_dir)
                    logger.debug("Parameter %s=%s", key, val)
                except KeyError:
                    logger.warning("Parameter %s=%s Not Found", key, val)
                except ValueError:
                    logger.warning("Parameter %s=%s Not Valid", key, val)
            self.config = []

    def run(self, ddbc, row_limit: Optional[int] = None) -> None:
        logger.debug("PipelineNode.run %s", self.uuid)
        if not isinstance(self.plugin, DuckdbPlugin):
            return
        self.load_config()

        # set the node to WORKING now, so that any changes which happen
        # while we're recalculating will set it DIRTY again and thus
        # get detected on the next sweep
        self.status = PipelineNodeStatus.WORKING

        if self.table_name:
            ddbc.sql(f"DROP TABLE IF EXISTS {self.table_name}")

        try:
            sources = {pn.name: ddbc.table(pn.table_name) for pn in self.parent_nodes if pn.table_name}
            self.plugin.prepare_multi(ddbc, sources)
            result = self.plugin.execute_multi(ddbc, sources, row_limit)
            self.message = ""
            if result:
                logger.debug("PipelineNode.run got result %d rows", len(result))
                self.table_name = f"n_{self.uuid}"
                result.to_table(self.table_name)
                self.status = PipelineNodeStatus.RESULT
            else:
                logger.debug("PipelineNode.run got no result")
                self.message = ""
                self.status = PipelineNodeStatus.ERROR
        except duckdb.InterruptException:
            logger.debug("Query interrupted: retrying")
            self.table_name = None
            self.message = None
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("PipelineNode.run %s exception %s", self.uuid, repr(exc))
            self.message = "\n".join(traceback.format_exception(exc))
            self.status = PipelineNodeStatus.ERROR

    def mark_dirty(self):
        self.status = PipelineNodeStatus.DIRTY
        for child_node in self.child_nodes:
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
        self.mark_dirty()
        for parent_node in self.parent_nodes:
            parent_node.child_nodes.discard(self)
        for child_node in self.child_nodes:
            child_node.parent_nodes.discard(self)


class PipelineGraph:
    def __init__(self, nodes: Optional[list[PipelineNode]] = None, preview_row_limit: int = 1000000):
        self.plugin_classes = get_plugin_classes()
        self.nodes = nodes or []
        self.ddbc = duckdb.connect()
        self.ddbc.sql("SET python_enable_replacements = false")
        self.ddbc.create_function("warning", self.sql_warning)
        self.preview_row_limit = preview_row_limit

        self.thread: Optional[threading.Thread] = None
        self.thread_cursor: Optional[duckdb.DuckDBPyConnection] = None
        self.thread_stop: bool = False

    def reset(self):
        for node in self.nodes:
            node.status = PipelineNodeStatus.DIRTY
        self.start_thread()

    def __del__(self):
        self.stop_thread()

    def sql_warning(self, message: str, value: Optional[str]) -> Optional[str]:
        logger.warning(message)
        return value

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
        yield from sorted(found_nodes, key=lambda n: n.uuid)

        while len(found_nodes) < len(self.nodes):
            for node in sorted(self.nodes, key=lambda n: n.uuid):
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

    def run_node(self, node: PipelineNode):
        return node.run(self.ddbc)

    def stop_thread(self):
        logger.debug("PipelineGraph.stop_thread")
        self.thread_stop = True
        if self.thread and self.thread.is_alive():
            if self.thread_cursor:
                try:
                    self.thread_cursor.interrupt()
                except duckdb.ConnectionException:
                    pass
            self.thread.join()

    def start_thread(self):
        logger.debug("PipelineGraph.start_thread")
        self.thread_stop = False
        self.thread = threading.Thread(target=self._thread_target)
        self.thread.start()

    def update(self):
        logger.debug("PipelineGraph.update")
        self.stop_thread()
        self.start_thread()

    def progress(self) -> bool:
        assert self.thread_cursor
        still_working = False
        for node in self.traverse_nodes():
            if self.thread and self.thread.is_alive():
                if node.status == PipelineNodeStatus.DIRTY:
                    logger.info("%s: 0%%", node.name)
                    still_working = True
                elif node.status == PipelineNodeStatus.WORKING:
                    if isinstance(node.plugin, DuckdbPlugin):
                        qp = node.plugin.query_progress(self.thread_cursor)
                        if qp > 0:
                            logger.info("%s: %d%%", node.name, qp)
                        else:
                            logger.info("%s: 0/0", node.name)
                    else:
                        logger.info("%s: 0/0", node.name)
                    still_working = True
                else:
                    logger.info("%s: 100%%", node.name)
            else:
                logger.info("%s: 100%%", node.name)

        return still_working

    def _thread_target(self):
        """Check for dirty nodes and update their results."""
        logger.debug("Pipelinegraph._thread_target starting")

        # thread_cursor needs to be exposed to the main thread so that the main
        # thread can call self.thread_cursor.interrupt() in case of long running
        # slow queries.
        self.thread_cursor = self.ddbc.cursor()
        try:
            for node in self.traverse_nodes():
                if self.thread_stop:
                    logger.debug("Pipelinegraph._thread_target stopped")
                if node.status == PipelineNodeStatus.DIRTY:
                    logger.debug("Pipelinegraph._thread_target recalculating %s", node.uuid)
                    node.run(self.thread_cursor, self.preview_row_limit)
                else:
                    logger.debug("Pipelinegraph._thread_target %s is clean", node.uuid)
            logger.debug("Pipelinegraph._thread_target finished")
        except duckdb.InterruptException:
            logger.debug("Pipelinegraph._thread_target interrupted")
        finally:
            self.thread_cursor.close()

    def run(self):
        # Unlike 'start', we kick all the nodes off in one thread so that
        # their results can stay as views rather than tables.  There's a slight
        # penalty to this as multiple inputs can't run in parallel but it
        # should reduce memory usage since intermediate steps don't need to
        # be saved.  If DuckDB decides to start sharing views across cursors
        # we can improve on this a little.
        # see https://github.com/duckdb/duckdb/issues/1848

        start_time = time.time()
        logger.info("Starting: %s", start_time)
        for node in self.traverse_nodes():
            logger.info("... starting %s", node.name)
            node.load_config()
            sources = {pn.name: pn.result for pn in node.parent_nodes}
            node.plugin.prepare_multi(self.ddbc, sources)
            result = node.plugin.execute_multi(self.ddbc, sources)
            if result:
                node.result = duckdb_source_to_view(self.ddbc, result)
            else:
                node.result = None
            logger.info("... completed %s", node.name)

        finish_time = time.time()
        logger.info("Finished: %s, elapsed time: %d", finish_time, finish_time - start_time)

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

        # shuffle nodes back down to avoid really long connections.

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
