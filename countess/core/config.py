import ast
import os.path
import re
import sys
from configparser import ConfigParser

from countess.core.logger import ConsoleLogger, Logger
from countess.core.pipeline import PipelineGraph, PipelineNode
from countess.core.plugins import load_plugin


def default_progress_callback(n, a, b, s=""):
    print(f"{n:40s} {a:4d}/{b:4d} {s}")


def default_output_callback(output):
    sys.stderr.write(repr(output))


def read_config(
    filename: str,
    logger: Logger = ConsoleLogger(),
) -> PipelineGraph:
    """Reads `filenames` and returns a PipelineGraph"""

    cp = ConfigParser()
    cp.read(filename)

    base_dir = os.path.dirname(filename)

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
        notes = config_dict.get("_notes")

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
            notes=notes,
            is_dirty=True,
        )
        pipeline_graph.nodes.append(node)

        for key, val in config_dict.items():
            if key.startswith("_parent."):
                node.add_parent(nodes_by_name[val])

        node.config = [
            (key, ast.literal_eval(val), base_dir) for key, val in config_dict.items() if not key.startswith("_")
        ]

        nodes_by_name[section_name] = node

    return pipeline_graph


def write_config(pipeline_graph: PipelineGraph, filename: str):
    """Write `pipeline_graph`'s configuration out to `filename`"""

    cp = ConfigParser()
    base_dir = os.path.dirname(filename)

    node_names_seen = set()
    for node in pipeline_graph.traverse_nodes():
        while node.name in node_names_seen:
            num = 0
            if match := re.match(r"(.*?)\s+(\d+)$", node.name):
                node.name = match.group(1)
                num = int(match.group(2))
            node.name += f" {num + 1}"
        node_names_seen.add(node.name)

        cp.add_section(node.name)
        if node.plugin:
            cp[node.name].update(
                {
                    "_module": node.plugin.__module__,
                    "_class": node.plugin.__class__.__name__,
                    "_version": node.plugin.version,
                    "_hash": node.plugin.hash(),
                }
            )
        if node.position:
            xx, yy = node.position
            cp[node.name]["_position"] = "%d %d" % (xx * 1000, yy * 1000)
        if node.notes:
            cp[node.name]["_notes"] = node.notes
        for n, parent in enumerate(node.parent_nodes):
            cp[node.name][f"_parent.{n}"] = parent.name
        if node.config:
            for k, v, _ in node.config:
                cp[node.name][k] = repr(v)
        elif node.plugin:
            for k, v in node.plugin.get_parameters(base_dir):
                cp[node.name][k] = repr(v)

    with open(filename, "w", encoding="utf-8") as fh:
        cp.write(fh)


def export_config_graphviz(pipeline_graph: PipelineGraph, filename: str):
    with open(filename, "w", encoding="utf-8") as fh:
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
