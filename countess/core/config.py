import ast
import io
import logging
import os.path
import re
import sys
from configparser import ConfigParser

from countess.core.pipeline import PipelineGraph, PipelineNode
from countess.core.plugins import load_plugin

logger = logging.getLogger(__name__)


def read_config_dict(name: str, base_dir: str, config_dict: dict) -> PipelineNode:
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

    sort = config_dict.get("_sort", "0 0").split()

    # XXX check version and hash_digest and emit warnings.

    node = PipelineNode(
        name=name,
        uuid=config_dict.get("_uuid"),
        plugin=plugin,
        position=position,
        notes=notes,
        sort_column=int(sort[0]),
        sort_descending=bool(int(sort[1])),
    )
    for key, val in config_dict.items():
        if not key.startswith("_"):
            node.set_config(key, ast.literal_eval(val), base_dir)
    return node


def read_config(
    filenames: list[str],
) -> PipelineGraph:
    """Reads `filenames` and returns a PipelineGraph"""

    cp = ConfigParser()
    for filename in filenames:
        try:
            with open(filename, "r", encoding="utf-8") as fh:
                cp.read_file(fh)
        except OSError as exc:
            logger.error(str(exc))
            sys.exit(2)

    base_dir = os.path.dirname(filenames[0])

    pipeline_graph = PipelineGraph()
    nodes_by_name: dict[str, PipelineNode] = {}

    for section_name in cp.sections():
        config_dict = dict(cp[section_name])
        node = read_config_dict(section_name, base_dir, config_dict)

        for key, val in config_dict.items():
            if key.startswith("_parent."):
                node.add_parent(nodes_by_name[val])

        pipeline_graph.nodes.append(node)
        nodes_by_name[section_name] = node

    return pipeline_graph


def write_config(pipeline_graph: PipelineGraph, filename: str):
    """Write `pipeline_graph`'s configuration out to `filename`"""

    pipeline_graph.reset_node_names()

    cp = ConfigParser()
    base_dir = os.path.dirname(filename)

    for node in pipeline_graph.traverse_nodes():
        write_config_node(node, cp, base_dir)

    with open(filename, "w", encoding="utf-8") as fh:
        cp.write(fh)


def write_config_node_string(node: PipelineNode, base_dir: str = ""):
    cp = ConfigParser()
    write_config_node(node, cp, base_dir)
    buf = io.StringIO()
    cp.write(buf)
    return buf.getvalue()


def write_config_node(node: PipelineNode, cp: ConfigParser, base_dir: str):
    cp.add_section(node.name)
    if node.plugin:
        cp[node.name].update(
            {
                "_uuid": node.uuid,
                "_module": node.plugin.__module__,
                "_class": node.plugin.__class__.__name__,
                "_version": node.plugin.version,
                "_hash": node.plugin.hash(),
                "_sort": "%d %d" % (node.sort_column, 1 if node.sort_descending else 0),
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
        for k, v in node.plugin.get_parameters("", base_dir):
            cp[node.name][k] = repr(v)


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
