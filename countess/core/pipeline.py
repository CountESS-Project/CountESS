import logging
import importlib

from importlib.metadata import entry_points
from typing import Type, Mapping, Iterable, Tuple, Optional
from functools import partial
from countess.core.plugins import BasePlugin


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

    def load_plugin_config(self, plugin_name: str, config: Mapping[str,bool|int|float|str]) -> BasePlugin:
        """Loads plugin config from a `plugin_name` and a `config` dictionary"""
        module_name, class_name = plugin_name.split(":")
        plugin_class = getattr(importlib.import_module(module_name), class_name)
        assert issubclass(plugin_class, BasePlugin)

        plugin = plugin_class()
        self.add_plugin(plugin)

        for k, v in config.items():
            # XXX cheesy but this way of handling files will be replaced soon
            if k.startswith("file.") and k.endswith(".filename"):
                plugin.add_file(v)
            else:
                plugin.parameters[k].set_value(v)

        return plugin

    def get_plugin_configs(self) -> Iterable[Tuple[str, Mapping[str,bool|int|float|str]]]:
        """Writes plugin configs as a series of names and dictionaries"""
        for plugin in self.plugins:
            config = dict(((k, p.value) for k, p in plugin.parameters.items()))
            yield f"{plugin.__class__.__module__}:{plugin.__class__.__name__}", config

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

        if previous_plugin:
            plugin.set_previous_plugin(previous_plugin)
            plugin.update()
        if next_plugin:
            next_plugin.set_previous_plugin(plugin)

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
            next_plugin.set_previous_plugin(previous_plugin)
            next_plugin.update()

    def move_plugin(self, position: int, new_position: int):
        assert 0 <= position < len(self.plugins)
        assert 0 <= new_position < len(self.plugins)

        # XXX TODO
        raise NotImplementedError("surprisingly involved")

    def update_plugin(self, position: int):
        """Updates the plugin at `position` and then all the subsequent plugins,
        to allow changes to carry through the pipeline"""
        assert 0 <= position < len(self.plugins)
        
        for plugin in self.plugins[position:]:
            plugin.update()

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
        print(f"PIPELINE {self} PRERUN {position}")
        obj = self.plugins[position-1].prerun_cache if position > 0 else None
        for plugin in self.plugins[position:]:
            obj = plugin.prerun(obj)
        
    def run(self, progress_callback):
        obj = None
        for num, plugin in enumerate(self.plugins):
            cb = partial(progress_callback, num)
            obj = plugin.run(obj, cb)
        return obj
