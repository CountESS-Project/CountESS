from dataclasses import dataclass, field
from typing import Any, Optional

from countess.core.logger import Logger
from countess.core.plugins import BasePlugin, get_plugin_classes

PRERUN_ROW_LIMIT = 100000


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
        return (self in node.parent_nodes) or any(
            (self.is_ancestor_of(n) for n in node.parent_nodes)
        )

    def is_descendant_of(self, node):
        return (self in node.child_nodes) or any(
            (self.is_descendant_of(n) for n in node.child_nodes)
        )

    def get_input_data(self):
        if len(self.parent_nodes) == 0:
            return None
        elif len(self.parent_nodes) == 1:
            return list(self.parent_nodes)[0].result
        else:
            return dict((n.name, n.result) for n in self.parent_nodes if n.result is not None)

    def execute(self, logger: Logger, row_limit=None):
        assert row_limit is None or isinstance(row_limit, int)
        input_data = self.get_input_data()
        if self.plugin:
            try:
                self.result = self.plugin.run(input_data, logger, row_limit)
            except Exception as exc:  # pylint: disable=W0718
                logger.exception(exc)
        else:
            self.result = input_data

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

    def prepare(self, logger: Logger):
        assert isinstance(logger, Logger)

        input_data = self.get_input_data()
        if self.plugin:
            try:
                self.plugin.prepare(input_data, logger)
                self.load_config(logger)
            except Exception as exc:  # pylint: disable=W0718
                logger.exception(exc)

        else:
            self.result = input_data

    def prerun(self, logger: Logger, row_limit=PRERUN_ROW_LIMIT):
        assert isinstance(logger, Logger)

        if self.is_dirty and self.plugin:
            logger.info(f"Prerun {self.name} Start")
            for parent_node in self.parent_nodes:
                parent_node.prerun(logger, row_limit)
            self.prepare(logger)
            self.load_config(logger)

            self.execute(logger, row_limit)
            self.is_dirty = False
            logger.info(f"Prerun {self.name} Done")

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
            node.prepare(logger)
            node.execute(logger)

    def reset(self):
        for node in self.nodes:
            node.result = None
            node.is_dirty = True

    def tidy(self):
        """Tidies the graph (sets all the node positions)"""

        # XXX This is very arbitrary and not particularly efficient.
        # Some kind of FDP-like algorithm might be nice.

        nodes = list(self.traverse_nodes())

        # first calculate a stratum for each node.

        stratum = {}
        for node in nodes:
            if not node.parent_nodes:
                stratum[node] = 0
            else:
                stratum[node] = max(stratum[n] for n in node.parent_nodes) + 1
        for node in nodes[::-1]:
            if node.child_nodes:
                stratum[node] = min(stratum[n] for n in node.child_nodes) - 1
        max_stratum = max(stratum.values())

        position = {}
        for s in range(0, max_stratum + 1):
            y = (s + 0.5) / (max_stratum + 1)

            # now sort all the nodes by the average position of their parents,
            # to try and stop them forming a big tangle

            snodes = [
                (
                    sum(position[p] for p in node.parent_nodes) / len(node.parent_nodes)
                    if node.parent_nodes
                    else 0.5,
                    n,
                )
                for n, node in enumerate(nodes)
                if stratum[node] == s
            ]
            snodes.sort()

            # Assign node positions

            for p, (_, n) in enumerate(snodes):
                x = (p + 0.5) / len(snodes)
                nodes[n].position = (y, x)
                position[nodes[n]] = x
