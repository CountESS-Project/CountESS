from importlib.metadata import entry_points
import logging
import dask.dataframe as dd
from dask.callbacks import Callback

from typing import Optional, Any, Type
from collections.abc import Iterable, Callable, Mapping

class _BasePlugin:

    name: str = ''
    title: str = ''
    description: str = ''
    params: Mapping[Any, Any] = {}

    input_classes:Iterable[Type[Any]] = [ type(None) ]
    output_class: Type[Any] = type(None)

class BasePlugin(_BasePlugin):
    """Base class for all plugins.  Plugins exist as entrypoints, but also 
    PluginManager check that they subclass this class before accepting them."""


    @classmethod
    def accepts_none(cls):
        """Can this plugin be run as an input plugin, taking its input
        from a file or whatever rather than from the pipeline?"""
        return type(None) in cls.input_classes

    @classmethod
    def accepts_class(cls, output_class: Any):
        """Can this plugin accept input from any of the `output_classes`?"""
        return any(issubclass(output_class,ic) for ic in cls.input_classes)

    @classmethod
    def accepts_plugin(cls, previous_plugin: _BasePlugin):
        """Can this plugin accept input from this previous plugin?"""
        return cls.accepts_class(previous_plugin.output_class)

    @classmethod
    def returns_none(cls):
        return cls.output_class == type(None)

    def run_with_progress_callback(self, obj: Any, callback: Callable[[int, int, Optional[str]], None]):
        """Not every plugin is going to support progress callbacks, plugins
        which do should override this method to call `callback` sporadically
        with two numbers estimating a fraction of the work completed."""
        return self.run(obj)

    def run(self, obj: Any):
        """Override this method"""
        raise NotImplementedError(f"{self.__class__}.run()")


class DaskBasePlugin(BasePlugin):
    """Base class for plugins which accept and return dask DataFrames"""

    def run_with_progress_callback(self, ddf, callback: Callable[[int, int, Optional[str]], None]):

        # XXX there's a slight disconnect here: is this plugin indicating that the
        # input and output format are dask dataframes or that the computing done by
        # this plugin is in Dask?  I mean, if one then probably the other, but it's
        # possible we'll want to develop a plugin which takes some arbitrary file,
        # does computation in Dask and then returns a pandas dataframe, at which
        # point do we implement DaskInputPluginWhichReturnsPandas(DaskBasePlugin)?

        class DaskProgressCallback(Callback):

            def __init__(self, progress_callback: Callable[[int, int, Optional[str]], None]):
                self.progress_callback = progress_callback

            def _start_state(self, dsk, state):
                self.total_tasks = len(state["ready"]) + len(state["waiting"])

            def _posttask(self, key, result, dsk, state, worker_id):
                self.progress_callback(len(state["finished"]), self.total_tasks)

            def _finish(self, dsk, state, failed):
                # XXX do something with "failed"
                self.progress_callback(self.total_tasks, self.total_tasks)

        with DaskProgressCallback(callback):
            return self.run(ddf)

# XXX Potentially there's a PandasBasePlugin which can use a technique much like
# tqdm does in tqdm/std.py to monkeypatch pandas.apply and friends and provide
# progress feedback.

class DaskTransformPlugin(DaskBasePlugin):

    input_classes = [ dd.DataFrame ]
    output_class = dd.DataFrame


class DaskInputPlugin(DaskBasePlugin):

    input_classes = [ type(None) ]
    output_class = dd.DataFrame


class DaskOutputPlugin(DaskBasePlugin):

    input_classes = [ dd.DataFrame ]
    output_class = type(None)



class PluginManager:
    
    def __init__(self):

        self.plugins = []
        for ep in entry_points(group='countess_plugins'):
            plugin = ep.load()
            if issubclass(plugin, BasePlugin):
                self.plugins.append(plugin)
            else:
                logging.warning(f"{plugin} is not a valid CountESS plugin")


    def get_input_plugins(self) -> Iterable[BasePlugin]:
        return [ pp for pp in self.plugins if pp.accepts_none() ]

    def get_transform_plugins(self) -> Iterable[BasePlugin]:
        return [ pp for pp in self.plugins if not pp.accepts_none() and not pp.returns_none() ]

    def get_output_plugins(self) -> Iterable[BasePlugin]:
        return [ pp for pp in self.plugins if pp.returns_none() ]


    def get_plugins_between(self, prev_plugin: Optional[BasePlugin]=None, next_plugin: Optional[BasePlugin]=None):
        return [
            pp for pp in self.plugins
            if (
                pp.accepts_plugin(prev_plugin) and
                (next_plugin is None or next_plugin.accepts_plugin(pp))
            )
        ]
