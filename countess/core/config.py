import ast
import re
import sys
from configparser import ConfigParser
from functools import partial
from typing import Callable, Iterable

from countess.core.pipeline import PipelineGraph, PipelineNode
from countess.core.plugins import load_plugin


def default_progress_callback(n, a, b, s=""):
    print(f"{n:40s} {a:4d}/{b:4d} {s}")


def default_output_callback(output):
    sys.stderr.write(repr(output))


def read_config(
    filenames: Iterable[str],
    progress_callback: Callable = default_progress_callback,
    output_callback: Callable = default_output_callback,
) -> PipelineGraph:
    """Reads `filenames` and returns a PipelineGraph"""

    cp = ConfigParser()
    cp.read(filenames)

    pipeline_graph = PipelineGraph()
    nodes_by_name: dict[str, PipelineNode] = {}

    for section_name in cp.sections():
        config_dict = cp[section_name]

        if "_module" in config_dict:
            module_name = config_dict["_module"]
            class_name = config_dict["_class"]
            # XXX version = config_dict.get("_version")
            # XXX hash_digest = config_dict.get("_hash")
            plugin = load_plugin(module_name, class_name)
        else:
            plugin = None

        position_str = config_dict.get("_position")

        position = None
        if position_str:
            position_match = re.match(r"(\d+) (\d+)$", position_str)
            if position_match:
                position = (
                    int(position_match.group(1)) / 1000,
                    int(position_match.group(2)) / 1000,
                )

        # XXX check version and hash_digest and emit warnings.

        node = PipelineNode(
            name=section_name,
            plugin=plugin,
            position=position,
        )
        pipeline_graph.nodes.append(node)

        for key, val in config_dict.items():
            if key.startswith("_parent."):
                node.add_parent(nodes_by_name[val])

        nodes_by_name[section_name] = node

        if plugin:
            # XXX progress callback for preruns.
            node.prepare()

            for key, val in config_dict.items():
                if key.startswith("_"):
                    continue
                node.configure_plugin(key, ast.literal_eval(val))

            node.prerun(partial(progress_callback, node.name))
            if node.output and output_callback is not None:
                output_callback(node.output)

    return pipeline_graph


def write_config(pipeline_graph: PipelineGraph, filename: str):
    """Write `pipeline_graph`'s configuration out to `filename`"""

    cp = ConfigParser()

    for node in pipeline_graph.traverse_nodes():
        cp.add_section(node.name)
        if node.plugin:
            cp[node.name].update({
                "_module": node.plugin.__module__,
                "_class": node.plugin.__class__.__name__,
                "_version": node.plugin.version,
                "_hash": node.plugin.hash(),
            })
        if node.position:
            cp[node.name]['_position'] = " ".join(
                str(int(x * 1000)) for x in node.position
            )
        for n, parent in enumerate(node.parent_nodes):
            cp[node.name][f'_parent.{n}'] = parent.name
        if node.plugin:
            for k, v in node.plugin.get_parameters():
                cp[node.name][k] = repr(v)

    with open(filename, "w") as fh:
        cp.write(fh)


def export_config_graphviz(pipeline_graph: PipelineGraph, filename: str):
    with open(filename, "w") as fh:
        fh.write("digraph {\n")
        for node in pipeline_graph.traverse_nodes():
            label = node.name.replace('"', r"\"")
            if node.child_nodes and not node.parent_nodes:
                fh.write(f'\t"{label}" [ shape="invhouse" ];\n')
            elif node.parent_nodes and not node.child_nodes:
                fh.write(f'\t"{label}" [ shape="house" ];\n')
            else:
                fh.write(f'\t"{label}" [ shape="box" ];\n')

            for child_node in node.child_nodes:
                label2 = child_node.name.replace('"', r"\"")
                fh.write(f'\t"{label}" -> "{label2}";\n')

        fh.write("}\n")
