from configparser import ConfigParser
from typing import Iterable
import ast
import re

from countess.core.plugins import load_plugin
from countess.core.dataflow import PipelineGraph, PipelineNode

def read_config(filenames: Iterable[str]) -> PipelineGraph:
    """Reads `filenames` and returns a PipelineGraph"""

    config_tree = {}
    cp = ConfigParser()
    cp.read(filenames)

    pipeline_graph = PipelineGraph()
    nodes_by_name = {}

    for section_name in cp.sections():
        config_dict = cp[section_name]

        module_name = config_dict['_module']
        class_name = config_dict['_class']
        version = config_dict.get('_version')
        hash_digest = config_dict.get('_hash')
        position_str = config_dict.get('_position')

        position = None
        if position_str:
            position_match = re.match(r'(\d+) (\d+)$', position_str)
            if position_match:
                position = int(position_match.group(1))/1000, int(position_match.group(2))/1000

        # XXX check version and hash_digest and emit warnings.

        plugin = load_plugin(module_name, class_name)
        node = PipelineNode(
            name = section_name,
            plugin = plugin,
            position = position,
        )
        pipeline_graph.nodes.append(node)

        for key, val in config_dict.items():
            if key.startswith('_parent'):
                node.add_parent(nodes_by_name[val])

        nodes_by_name[section_name] = node

        # XXX progress callback for preruns.
        node.prepare()

        for key, val in config_dict.items():
            if key.startswith('_'): continue
            node.configure_plugin(key, ast.literal_eval(val))

        node.prerun()

    return pipeline_graph


def write_config(pipeline_graph: PipelineGraph, filename: str):
    """Write `pipeline_graph`'s configuration out to `filename`"""

    cp = ConfigParser()

    for node in pipeline_graph.traverse_nodes():
        config_section = cp[node.name]
        config_section.update({
            '_module': node.plugin.__module__,
            '_class': node.plugin.__class__.__name__,
            '_version': node.plugin.version,
            '_hash': node.plugin.hash(),
            '_position': ' '.join([str(int(x*1000)) for x in node.position]),
        })
        config_section.update(dict([
            (f"_parent_{n}", parent.name)
            for n, parent in enumerate(node.parent_nodes)
        ]))
        config_section.update(plugin.get_parameters())

    with open(filename, "w") as fh:
        cp.write(fh)


def export_config_graphviz(pipeline_graph: PipelineGraph, filename: str):
    with open(filename, "w") as fh:
        fh.write("digraph {\n")
        for node in pipeline_graph.traverse_nodes():
            label = node.name.replace('"', r'\"')
            if node.child_nodes and not node.parent_nodes:
                fh.write(f'\t"{label}" [ shape="invhouse" ];\n')
            elif node.parent_nodes and not node.child_nodes:
                fh.write(f'\t"{label}" [ shape="house" ];\n')
            else:
                fh.write(f'\t"{label}" [ shape="box" ];\n')

            for child_node in node.child_nodes:
                label2 = child_node.name.replace('"', r'\"')
                fh.write(f'\t"{label}" -> "{label2}";\n')

        fh.write("}\n")

