import traceback
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Optional

from countess.core.plugins import BasePlugin, get_plugin_classes

PRERUN_ROW_LIMIT = 10000


@dataclass
class PipelineNode:
    name: str
    plugin: Optional[BasePlugin] = None
    position: Optional[tuple[float, float]] = None
    parent_nodes: set["PipelineNode"] = field(default_factory=set)
    child_nodes: set["PipelineNode"] = field(default_factory=set)
    result: Any = None
    output: Optional[str] = None
    is_dirty: bool = True

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

    def default_callback(self, a, b, s=""):
        print(f"{self.name:40s} {a:4d}/{b:4d} {s}")

    def get_input_data(self):
        if len(self.parent_nodes) == 0:
            return None
        elif len(self.parent_nodes) == 1:
            return list(self.parent_nodes)[0].result
        else:
            return dict(
                [(n.name, n.result) for n in self.parent_nodes if n.result is not None]
            )

    def execute(self, callback, row_limit=None):
        input_data = self.get_input_data()
        if self.plugin:
            try:
                self.result = self.plugin.run(input_data, callback, row_limit)
                self.output = None
            except Exception as exc:
                self.result = None
                self.output = traceback.format_exception(exc)
        else:
            self.result = input_data
            self.output = None

    def prepare(self):
        input_data = self.get_input_data()
        if self.plugin:
            try:
                self.plugin.prepare(input_data)
            except Exception as exc:
                self.result = None
                self.output = traceback.format_exception(exc)
        else:
            self.result = input_data
            self.output = None

    def prerun(self, callback=None, row_limit=PRERUN_ROW_LIMIT):
        if not callback:
            callback = self.default_callback
        if self.is_dirty and self.plugin:
            for parent_node in self.parent_nodes:
                parent_node.prerun(callback, row_limit)
            self.execute(callback, row_limit)
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

    def configure_plugin(self, key, value):
        self.plugin.set_parameter(key, value)
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

    def default_callback(self, n, a, b, s=""):
        print(f"{n:40s} {a:4d}/{b:4d} {s}")

    def traverse_nodes(self):
        found_nodes = set(node for node in self.nodes if not node.parent_nodes)
        yield from found_nodes

        while len(found_nodes) < len(self.nodes):
            for node in self.nodes:
                if node not in found_nodes and node.parent_nodes.issubset(found_nodes):
                    yield node
                    found_nodes.add(node)

    def run(self, progress_callback=None, output_callback=None):
        # XXX this is the last thing PipelineGraph actually does!
        # might be easier to just keep a set of nodes and sort through
        # them for output nodes, or something.
        if not progress_callback:
            progress_callback = self.default_callback
        if not output_callback:
            output_callback = print

        for node in self.traverse_nodes():
            # XXX TODO there's some opportunity for easy parallelization here,
            # by pushing each node into a pool as soon as its parents are
            # complete.
            node.execute(partial(progress_callback, node.name))
            if node.output and output_callback:
                output_callback(node.output)

    def reset(self):
        for node in self.nodes:
            node.result = None
            node.output = None
            node.is_dirty = True
