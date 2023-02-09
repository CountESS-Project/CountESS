from configparser import ConfigParser
from typing import Iterable

from countess.core.plugins import load_plugin
from countess.core.dataflow import PipelineGraph

def read_config(filenames: Iterable[str]) -> PipelineGraph:
    """Reads `filenames` and returns a PipelineGraph"""

    config_tree = {}
    cp = ConfigParser()
    cp.read(filenames)

    pipeline_graph = PipelineGraph()
    plugins_by_name = {}

    for section_name in config.sections():
        config_dict = config[section_name]

        module_name = config_dict['_module']
        class_name = config_dict['_class']
        version = config_dict['_version']

        plugin = load_plugin(module_name, class_name)
        pipeline_graph.add_plugin(plugin, section_name)

        for key, val in config_dict.items():
            if key.startswith('_parent'):
                pipeline_graph.add_plugin_link(
                    plugins_by_name[val],
                    plugin
                )

        # XXX progress callback
        pipeline_graph.prerun_plugin(plugin)

        for key, val in config[section_name]:
            if key.startswith('_'): continue
            plugin.set_parameter(key, ast.literal_eval(val))

        plugins_by_name[section_name] = plugin

    return pipeline_graph


def write_config(pipeline_graph: PipelineGraph, filename: str):
    """Write `pipeline_graph`'s configuration out to `filename`"""

    cp = ConfigParser()
    plugin_names = {}

    for plugin_name, plugin, parent_plugins in pipeline_graph.traverse_plugins():
        plugin_config = cp[plugin_name]
        plugin_config.update({
            '_module': plugin.__module__,
            '_class': plugin.__class__.__name__,
            '_version': plugin.version,
            '_hash': plugin.hash(),
        })
        cp[plugin_name].update(dict([
            (f"_parent_{n}", plugin_names[pp])
            for n, pp in enumerate(parent_plugins)
        ]))
        cp[plugin_name].update(plugin.get_parameters())
        
        plugin_names[plugin_name] = plugin

    with open(filename, "w") as fh:
        cp.write(fh)

