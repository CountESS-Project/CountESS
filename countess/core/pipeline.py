import logging
import importlib
from collections import defaultdict

from importlib.metadata import entry_points
from typing import Type, Mapping, Iterable, Tuple, Optional, Any
from functools import partial
from countess.core.plugins import BasePlugin


# XXX This isn't very nice and is making me rethink the whole
# nested config strategy.

def flatten_config(cfg: dict|list, path: str=""):
    if type(cfg) is dict:
        cfg_pairs = sorted(cfg.items())
    elif type(cfg) is list:
        cfg_pairs = enumerate(cfg)
    for k, v in cfg_pairs:
        if type(v) in (dict, list):
            yield from flatten_config(v, f"{path}.{k}" if path else k)
        else:
            yield f"{path}.{k}" if path else k, str(v)

def unflatten_config(cfg_list):
    cfg = {}
    kkl = defaultdict(list)
    for k, v in cfg_list:
        k0, *kk = k.split(".", 1)
        if kk:
            kkl[k0].append((kk[0], v))
        else:
            cfg[k0] = v
    for k0, cfg_list in kkl.items():
        cfg[k0] = unflatten_config(cfg_list)
    return cfg


class Pipeline:
    """Represents a series of plugins linked up to each other.  Plugins can be added
    and removed from the pipeline if they are able to deal with each other's input"""

    def __init__(self):
        self.plugins: list[BasePlugin] = []
        self.plugin_classes: list[Type[BasePlugin]] = []

        for ep in entry_points(group="countess_plugins"):
            plugin_class = ep.load()
            if issubclass(plugin_class, BasePlugin):
                self.plugin_classes.append(plugin_class)
            else:
                logging.warning(f"{plugin_class} is not a valid CountESS plugin")

    def set_plugin_config(self, position: Optional[int], config: Mapping[str,bool|int|float|str]):
        plugin = self.plugins[-1] if position is None else self.plugins[position]

        for k, v in config.items():
            if k in plugin.parameters:
                plugin.parameters[k].value = v

    def load_plugin_config(self, plugin_name: str, config: Mapping[str,bool|int|float|str]) -> BasePlugin:

        """Loads plugin config from a `plugin_name` and a `config` dictionary"""
        module_name, class_name = plugin_name.split(":")
        plugin_class = getattr(importlib.import_module(module_name), class_name)
        assert issubclass(plugin_class, BasePlugin)

        plugin = plugin_class()
        previous_plugin = self.plugins[-1] if self.plugins else None
        previous_prerun = self.plugins[-1].prerun_cache if self.plugins else None

        self.add_plugin(plugin)
        self.set_plugin_config(None, config)

        plugin.prerun(previous_prerun)
        plugin.update()

        cfg_tree = unflatten_config(config.items())
        self.set_plugin_config(None, cfg_tree)

        prerun_value = plugin.prerun(previous_prerun)
        plugin.update()

        return plugin

    def get_plugin_configs(self) -> Iterable[Tuple[str, Mapping[str,bool|int|float|str]]]:
        """Writes plugin configs as a series of names and dictionaries"""
        for plugin in self.plugins:
            plugin_name = plugin.__module__ + ":" + plugin.__class__.__name__
            config = dict(((k, p.value) for k, p in plugin.parameters.items()))
            yield plugin_name, flatten_config(config)

    def add_plugin(self, plugin: BasePlugin, position: int = None):
        """Adds a plugin at `position`, if that's possible.
        It might not be possible if the plugin chain would not be compatible,
        in which case we throw an assertion error"""
        # XXX would it be easier to pass an "after: Plugin" instead of position?

        if position is None:
            position = len(self.plugins)
        assert 0 <= position <= len(self.plugins)
        if position > 0:
            previous_plugin = self.plugins[position - 1]
            assert plugin.can_follow(previous_plugin)
        else:
            previous_plugin = None

        if position < len(self.plugins):
            next_plugin = self.plugins[position]
            assert next_plugin.can_follow(plugin)
        else:
            next_plugin = None

        self.plugins.insert(position, plugin)

        plugin.update()

    def del_plugin(self, position: int):
        """Deletes the plugin at `position` if that's possible.
        It might not be possible if the plugins before and after the deletion aren't compatible,
        in which case we throw an assertion error"""
        # XXX would it be easier to pass "plugin: Plugin" instead of position?

        assert 0 <= position < len(self.plugins)

        previous_plugin = self.plugins[position - 1] if position > 0 else None

        if position < len(self.plugins) - 1:
            next_plugin = self.plugins[position + 1]
            assert next_plugin.can_follow(previous_plugin)
        else:
            next_plugin = None

        self.plugins.pop(position)

        if next_plugin:
            next_plugin.update()

    def move_plugin(self, position: int, new_position: int):
        assert 0 <= position < len(self.plugins)
        assert 0 <= new_position < len(self.plugins)

        # XXX TODO
        raise NotImplementedError("surprisingly involved")

    #def update_plugin(self, position: int):
    #    """Updates the plugin at `position` and then all the subsequent plugins,
    #    to allow changes to carry through the pipeline"""
    #    assert 0 <= position < len(self.plugins)
    #    
    #    for plugin in self.plugins[position:]:
    #        plugin.update()

    def choose_plugin_classes(self, position: Optional[int]=None):
        if position is None:
            position = len(self.plugins)

        previous_plugin_class = (
            self.plugins[position - 1].__class__ if position > 0 else None
        )
        next_plugin_class = (
            self.plugins[position].__class__ if position < len(self.plugins) else None
        )

        for plugin_class in self.plugin_classes:
            if plugin_class.can_follow(previous_plugin_class):
                if next_plugin_class is None or next_plugin_class.can_follow(
                    plugin_class
                ):
                    yield plugin_class

    def prerun(self, position: int=0):
        assert 0 <= position < len(self.plugins)
        obj = self.plugins[position-1].prerun_cache if position > 0 else None
        for plugin in self.plugins[position:]:
            obj = plugin.prerun(obj)
        
    def run(self, progress_callback):
        obj = None
        for num, plugin in enumerate(self.plugins):
            cb = partial(progress_callback, num)
            obj = plugin.run(obj, cb)
        return obj
