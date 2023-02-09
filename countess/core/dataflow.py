from dataclasses import dataclass
from typing import Any, Optional, Mapping

from countess.core.plugins import BasePlugin, get_plugin_classes, load_plugin

PRERUN_ROW_LIMIT = 1000

@dataclass
class PipelineGraphNode:
    name: str
    plugin: BasePlugin
    parent_nodes: set['PipelineGraphNode']
    child_nodes: set['PipelineGraphNode']
    result: Any = None
    output: Optional[str] = None
    is_dirty: bool = True

    def is_ancestor_of(self, node):
        return self in node.parent_nodes or [ self.is_ancestor_of(n) for n in node.parent_nodes ]

    def is_descendant_of(self, node):
        return self in node.child_nodes or [ self.is_descendant_of(n) for n in node.child_nodes ]

    def execute(self, callback, row_limit=None):
        input_data = dict([(n.name, n.result) for n in self.parent_nodes if n.result is not None])

        try:
            self.result = self.plugin.run(input_data, partial(callback, self.name), row_limit)
            self.output = None
        except Exception as exc:
            self.result = None
            self.output = traceback.format_exception(exc)

    def mark_dirty(self):
        self.is_dirty = True
        for child in child_nodes:
            if not child_node.is_dirty:
                child_node.mark_dirty()

    def final_descendants(self):
        if self.child_nodes: return set(n2 for n1 in self.child_nodes for n2 in n1.final_descendants())
        else: return set(self)


class PipelineGraph:

    # XXX probably excessive duplication here
    nodes: set[PipelineGraphNode]
    plugin_nodes: Mapping[BasePlugin, PipelineGraphNode]

    def __init__(self):

        self.plugin_classes = get_plugin_classes()
        self.nodes = set()
        self.plugin_nodes = dict()
 
    def default_callback(self, n, a, b, s=''):
        print(f"{n:40s} {a:4d}/{b:4d} {s}")

    def link_nodes(self, node1, node2):
        node1.child_nodes.add(node2)
        node2.parent_nodes.add(node1)

    def add_plugin(self, plugin, name=None):
        assert(plugin in self.plugin_classes)
        if name is None: name = plugin.name
        node = PipelineGraphNode(name, plugin, [])
        self.nodes.add(node)
        self.plugin_nodes[plugin] = node

    def add_plugin_link(self, plugin1, plugin2):
        self.link_nodes(self.plugin_nodes[plugin1], self.plugin_nodes[plugin2])

    def set_plugin_name(self, plugin, name):
        self.plugin_nodes[plugin].name = name

    def configure_plugin(self, plugin, key, value):
        plugin.set_parameter(key, value)
        self.plugin_nodes[plugin].mark_dirty()

    def del_node(self, node):
        for n in self.nodes:
            n.parent_nodes.discard(node)
        for n in self.nodes:
            n.child_nodes.discard(node)
        self.nodes.remove(node)

    def del_plugin(self, plugin):
        self.del_node(self.plugin_nodes.pop(plugin))

    def prerun_node(self, node, callback=None, row_limit=PRERUN_ROW_LIMIT):
        assert(node in self.nodes)
        if not callback: callback = self.default_callback

        for parent_node in node.parent_nodes:
            if parent_node.is_dirty:
                self.prerun_node(parent_node, row_limit=row_limit)

        node.execute(callback, row_limit)
        node.is_dirty = False

    def prerun_plugin(self, plugin, callback=None, row_limit=PRERUN_ROW_LIMIT):
        node = self.plugin_nodes[plugin]
        if node.is_dirty:
            self.prerun_node(node, callback, row_limit)

    def run(self, callback=None):
        if not callback: callback = self.default_callback

        ready_nodes = [ node for node in self.nodes if not node.parent_nodes ]
        finished_nodes = set()

        while ready_nodes:
            # XXX TODO there's some opportunity for easy parallelization here, by 
            # pushing each node into a pool as soon as its parents are complete.
            for node in ready_nodes:
                node.execute(callback)
                finished_nodes.add(node)
            ready_nodes = [
                node for node in self.nodes
                if node not in finished_nodes and node.parent_nodes.issubset(finished_nodes)
            ]

    def reset(self):
        for node in self.nodes:
            node.result = None
            node.output = None
            node.is_dirty = True
    
