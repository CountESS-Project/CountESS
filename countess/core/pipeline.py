import logging
import importlib
from collections import defaultdict
from dataclasses import dataclass
import ast
import re

from importlib.metadata import entry_points
from typing import Type, Mapping, Iterable, Tuple, Optional, Any, Callable
from functools import partial
from countess.core.plugins import BasePlugin
import traceback

PRERUN_ROW_LIMIT=1000

def debug_progress_callback(name):
    return lambda a, b, s='': print(f"{n}: {a}/{b} {s}")
    
@dataclass
class PipelineItem:
    plugin: BasePlugin
    result: Any = None
    output: Optional[str] = None
    
class Pipeline:
    """Represents a series of plugins linked up to each other.  Plugins can be added
    and removed from the pipeline if they are able to deal with each other's input"""

    items: list[PipelineItem]

    def __init__(self):
       
        self.plugin_classes: list[Type[BasePlugin]] = []
        for ep in entry_points(group="countess_plugins"):
            plugin_class = ep.load()
            if issubclass(plugin_class, BasePlugin):
                self.plugin_classes.append(plugin_class)
            else:
                logging.warning(f"{plugin_class} is not a valid CountESS plugin")

        self.items = []

    def set_plugin_config(self, position: Optional[int], config: Mapping[str,bool|int|float|str]):
        plugin = self.plugins[-1] if position is None else self.plugins[position]

        for k, v in config.items():
            if k in plugin.parameters:
                plugin.parameters[k].value = v
                plugin.update()

    def load_plugin_config(self, plugin_name: str, config: Mapping[str,bool|int|float|str]) -> BasePlugin:

        """Loads plugin config from a `plugin_name` and a `config` dictionary"""

        module_name = config.pop("_module")
        class_name = config.pop("_class")
        version = config.pop("_version")

        module = importlib.import_module(module_name)
        plugin_class = getattr(module, class_name)
        assert issubclass(plugin_class, BasePlugin)

        # XXX compare version with module.VERSION

        plugin = plugin_class(plugin_name)
        plugin.prepare(self.items[-1].result if self.items else None)

        for key, value in config.items():
            if key.startswith('_'): continue
            # XXX should this be optional? Postel's Law vs. Least Surprise.
            value = ast.literal_eval(value)
            plugin.set_parameter(key, value)
            plugin.update()

        self.add_plugin(plugin)
        self.prerun(len(self.items)-1)
        return plugin

    def get_plugin_configs(self) -> Iterable[Tuple[str, Mapping[str,bool|int|float|str]]]:
        """Writes plugin configs as a series of names and dictionaries"""
        for number, item in enumerate(self.items):
            config_list = [
                ('_module', item.plugin.__module__),
                ('_class', item.plugin.__class__.__name__),
                ('_version', item.plugin.version),
                ('_hash', item.plugin.hash()),
            ] + [(k, repr(v)) for k, v in item.plugin.get_parameters()]

            yield item.plugin.name, config_list

    def get_new_plugin_name(self, plugin):
        names = [ ii.plugin.name for ii in self.items if ii.plugin.name.startswith(plugin.name) ]
        numbers = [int(re.match(r'.*?(\s\d+)?$', nn).group(1) or 1) for nn in names]
        try:
            return f"{plugin.name} {max(numbers)+1}"
        except ValueError:
            return plugin.name

    def add_plugin(self, plugin: BasePlugin, position: int = None):
        """Adds a plugin at `position`, if that's possible.
        It might not be possible if the plugin chain would not be compatible,
        in which case we throw an assertion error"""
        # XXX should check for compatibility of plugins before & after
        # XXX would it be easier to pass an "after: Plugin" instead of position?

        if position is None:
            position = len(self.items)
        assert 0 <= position <= len(self.items)

        plugin.name = self.get_new_plugin_name(plugin)
        self.items.insert(position, PipelineItem(plugin))
        self.prepare(position)

    def del_plugin(self, position: int):
        """Deletes the plugin at `position`"""
        # XXX should check for compatibility of plugins before & after
        # XXX would it be easier to pass "plugin: Plugin" instead of position?

        self.items.pop(position)

    def choose_plugin_classes(self, position: Optional[int]=None):
        if position is None:
            position = len(self.items)
        previous_result = self.items[position - 1].result if position > 0 else None

        # XXX doesn't check subsequent plugins will be happy with our output
        for plugin_class in self.plugin_classes:
            if plugin_class.accepts(previous_result):
                yield plugin_class

    def prepare(self, position: int):
        prev_result = self.items[position-1].result if position > 0 else None
        item = self.items[position]
        try:
            item.plugin.prepare(prev_result)
        except Exception as exc:
            item.result = None
            item.output = traceback.format_exception(exc)

    def run(self, position: int, callback: Callable[[int, int, Optional[str]], None], row_limit: Optional[int]=None):

        # XXX this should actually run the plugins in a different process so 
        # they can be terminated, etc.

        item = self.items[position]
        prev_result = self.items[position-1].result if position > 0 else None

        try:
            item.result = item.plugin.run(prev_result, callback, row_limit)
            item.output = None
        except Exception as exc:
            item.result = None
            item.output = traceback.format_exception(exc)

    def prerun_callback(self, n: int, a: int, b: int, s: Optional[str]=None):
        # print(f"PRERUN {n} {a}/{b} {s}")
        pass

    def prerun(self, position: int):
        self.run(position, partial(self.prerun_callback, position), PRERUN_ROW_LIMIT)
        return self.items[position].result, self.items[position].output
