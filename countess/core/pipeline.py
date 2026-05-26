import logging
import re
import secrets
import threading
import time

from typing import Any, Iterable, Optional

import duckdb

from countess.core.plugins import BasePlugin, DuckdbPlugin, get_plugin_classes
from countess.utils.duckdb import duckdb_source_to_view

logger = logging.getLogger(__name__)


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
    is_dirty: bool = True
    result: Any = None

    # XXX config is a cache for config loaded from the file
    # at config load time, if it is present it is loaded the
    # first time the plugin is prerun.
    config: Optional[list[tuple[str, str, str]]] = None

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
            self.config = None

    def run(self, ddbc, row_limit: Optional[int] = None) -> None:
        logger.debug("PipelineNode.run %s %s", self.name, self.uuid)
        if not self.plugin:
            return
        self.load_config()

        # set the node to clean now, so that any changes which happen
        # while we're recalculating will set it dirty again and thus
        # get detected on the next sweep
        self.is_dirty = False

        assert isinstance(self.plugin, DuckdbPlugin)

        table_name = f"n_{self.uuid}"
        ddbc.sql(f"DROP TABLE IF EXISTS {table_name}")

        try:
            sources = {pn.name: pn.result for pn in self.parent_nodes}
            self.plugin.prepare_multi(ddbc, sources)
            result = self.plugin.execute_multi(ddbc, sources, row_limit)
            if result:
                result.to_table(table_name)
                self.result = ddbc.table(table_name)
            else:
                self.result = None
            logger.debug("PipelineNode.run %s %s saved %s", self.name, self.uuid, table_name)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self.result = str(exc)
            logger.warning("PipelineNode.run %s %s exception %s", self.name, self.uuid, exc)

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

        self.thread = threading.Thread(target=self._thread_target)
        self.thread.start()

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

    def _thread_target(self):
        """Runs during preview mode to monitor nodes and update their results."""
        cursor = self.ddbc.cursor()
        while True:
            for node in self.traverse_nodes():
                if node.is_dirty:
                    logger.debug("Recalculating %s %s Starting", node.name, node.uuid)
                    node.run(cursor, self.preview_row_limit)
                    logger.debug("Recalculating %s %s Done", node.name, node.uuid)
            time.sleep(0.1)

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
