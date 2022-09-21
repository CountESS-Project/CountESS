from typing import Optional, Any, Type, NamedTuple, TYPE_CHECKING
from collections import namedtuple, defaultdict
from collections.abc import Iterable, Callable, Mapping, MutableMapping

from countess.core.parameters import (
    BaseParam,
    BooleanParam,
    IntegerParam,
    FloatParam,
    StringParam,
    FileParam,
)

from importlib.metadata import entry_points
import logging
import dask.dataframe as dd
from dask.callbacks import Callback
import pandas as pd
import numpy as np

import re

"""
Plugin lifecycle:
* Selector of plugins calls cls.follows_plugin(previous_plugin_instance)
  which returns True if a plugin of this class can follow that one
  (or None if this is the first plugin)
* When plugin is selected, it gets __init__ed.
* plugin.parameters gets read to get the name of configuration fields.
* plugin.configure(name, value) gets called, potentially a lot of times.
* plugin.parameters gets checked again whenever the gui displays a configuration
  screen, in case options affect each other.
* For a Dask plugin, plugin.get_columns() may get called


"""


class BasePlugin:
    """Base class for all plugins.  Plugins exist as entrypoints, but also
    PluginManager checks that plugins subclass this class before accepting them
    as plugins."""

    name: str = ""
    title: str = ""
    description: str = ""
    parameters: MutableMapping[str, BaseParam] = {}

    @classmethod
    def can_follow(cls, plugin: Optional[Type[BasePlugin]]|Optional[BasePlugin]):
        """returns True if this plugin/plugin class can follow the plugin/plugin class
        `plugin` ... the class hierarchy enforces what methods must be present, eg:
        `DaskBasePlugin` has `output_columns` which identifies what will be passed."""
        return plugin is not None and (
                isinstance(plugin, BasePlugin) or
                (type(plugin) is type and issubclass(plugin, BasePlugin))
        )

    def __init__(self):
        # Parameters store the actual values they are set to, so we copy them so that
        # if the same plugin is used twice in a pipeline it will have its own parameters.
        self.parameters = dict( ( (k, v.copy() for k, v in self.parameters.items() ) )

    def set_previous_plugin(self, previous_plugin: Optional["BasePlugin"]):
        self.previous_plugin = previous_plugin

    def update(self):
        pass

    def run_with_progress_callback(
        self, obj: Any, callback: Callable[[int, int, Optional[str]], None]
    ):
        """Not every plugin is going to support progress callbacks, plugins
        which do should override this method to call `callback` sporadically
        with two numbers estimating a fraction of the work completed, and an
        optional string describing what they're up to, eg:
            callback(42, 107, 'Thinking hard about stuff')
        the user interface code will display this in some way or another."""
        return self.run(obj)

    def run(self, obj: Any):
        """Override this method"""
        raise NotImplementedError(f"{self.__class__}.run()")

    def add_parameter(self, name: str, param: BaseParam):
        self.parameters[name] = param.copy()
        return self.parameters[name]


class FileInputMixin:
    """Mixin class to indicate that this plugin can read files from local storage."""

    # XXX clumsy ... maybe store file params separately instead of encoding them into
    # self.parameters which isn't working nicely

    file_number = 0

    # used by the GUI file dialog
    file_types = [("Any", "*")]

    file_params: MutableMapping[str, BaseParam] = {}

    @classmethod
    def can_follow(cls, plugin: Optional[Type[BasePlugin]]|Optional[BasePlugin]):
        # the `not TYPE_CHECKING` clause is a workaround for mypy not really understanding
        # mixin classes.
        return plugin is None or (not TYPE_CHECKING and super().can_follow(plugin))

    def add_file(self, filename):
        self.file_number += 1
        self.add_parameter(
            f"file.{self.file_number}.filename", FileParam("Filename", filename)
        )
        self.add_file_params(filename, self.file_number)

    def add_file_params(self, filename, file_number):
        return dict(
            [
                (key, self.add_parameter(f"file.{file_number}.{key}", param))
                for key, param in self.file_params.items()
            ]
        )

    def remove_file(self, filenumber):
        for k in list(self.parameters.keys()):
            if k.startswith(f"file.{filenumber}."):
                del self.parameters[k]

    def remove_file_by_parameter(self, parameter):
        for k, v in list(self.parameters.items()):
            if v == parameter:
                if m := re.match(r"file\.(\d+)\.", k):
                    self.remove_file(m.group(1))

    def get_file_params(self):
        for n in range(1, self.file_number + 1):
            if f"file.{n}.filename" in self.parameters:
                yield dict(
                    [("filename", self.parameters[f"file.{n}.filename"])]
                    + [
                        (key, self.parameters[f"file.{n}.{key}"])
                        for key, param in self.file_params.items()
                    ]
                )


class DaskBasePlugin(BasePlugin):
    """Base class for plugins which accept and return dask DataFrames"""

    # XXX there's a slight disconnect here: is this plugin class indicating that the
    # input and output format are dask dataframes or that the computing done by
    # this plugin is in Dask?  I mean, if one then probably the other, but it's
    # possible we'll want to develop a plugin which takes some arbitrary file,
    # does computation in Dask and then returns a pandas dataframe, at which
    # point do we implement DaskInputPluginWhichReturnsPandas(DaskBasePlugin)?

    @classmethod
    def can_follow(cls, plugin: Optional[Type[BasePlugin]]|Optional[BasePlugin]):
        return plugin is not None and (
                isinstance(plugin, DaskBasePlugin) or
                (type(plugin) is type and issubclass(plugin, DaskBasePlugin))
        )

    def output_columns(self) -> Iterable[str]:
        return []

    def run_with_progress_callback(
        self, ddf, callback: Callable[[int, int, Optional[str]], None]
    ):
        class DaskProgressCallback(Callback):
            """Matches Dask's idea of a progress callback to ours."""

            # XXX there's probably neater ways, like just passing some lambdas to Callback

            def __init__(
                self, progress_callback: Callable[[int, int, Optional[str]], None]
            ):
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


class DaskInputPlugin(FileInputMixin, DaskBasePlugin):
    """A specialization of the DaskBasePlugin to allow it to follow nothing, eg: come first.
    DaskInputPlugins don't care about existing columns because they're adding their own records anyway."""

    file_params: MutableMapping[str, BaseParam]

    def run_with_progress_callback(
        self, ddf: Optional[dd.DataFrame], callback
    ) -> dd.DataFrame:
        """Input plugins are likely I/O bound so instead of using the Dask progress callback
        mechanism this uses a simple count of files read."""

        file_params = list(self.get_file_params())

        dfs = [] if ddf is None else [ddf]

        n_files = len(file_params)
        for num, fp in enumerate(file_params):
            callback(num, n_files, "Loading")
            df = self.read_file_to_dataframe(**fp)
            if isinstance(df, pd.DataFrame):
                df = dd.from_pandas(df, chunksize=100_000_000)
            dfs.append(df)

        # XXX what actually is the logical operation here a) between files in one load
        # and b) between existing dataframe and the new ones.

        if len(dfs) > 0:
            return dd.concat(dfs)
        else:
            # completely empty dataframe!
            empty_ddf = dd.from_pandas(pd.DataFrame.from_records([]), npartitions=1)
            assert isinstance(empty_ddf, dd.DataFrame)
            return empty_ddf

    def read_file_to_dataframe(self, **kwargs) -> dd.DataFrame | pd.DataFrame:
        raise NotImplementedError(
            f"Implement {self.__class__.__name__}.read_file_to_dataframe"
        )


class DaskTransformPlugin(DaskBasePlugin):
    """a Transform plugin takes columns from the input data frame."""

    _output_columns: Iterable[str] = []

    def output_columns(self):
        return self._output_columns


class DaskScoringPlugin(DaskTransformPlugin):
    """Specific kind of tranform which turns counts into scores"""

    def update(self):
        input_columns = sorted(
            (c for c in self.previous_plugin.output_columns() if c.startswith("count"))
        )

        # XXX Allow to set names of score columns?  The whole [5:] thing is clumsy
        for col in input_columns:
            scol = "score" + col[5:]
            if scol not in self.parameters:
                self.parameters[scol] = StringParam(
                    f"{scol} compares {col} and ...", default="NONE"
                )
            self.parameters[scol].choices = ["NONE"] + [
                x for x in input_columns if x != col
            ]

        scols = [k for k in self.parameters.keys() if k.startswith("score")]
        for scol in scols:
            col = "count" + scol[5:]
            if col not in input_columns:
                del self.parameters[scol]

    def output_columns(self):
        return list(self._output_columns) + [
            k for k, v in self.parameters.items() if v.value != "NONE"
        ]

    def run(self, ddf_in: dd.DataFrame):
        # XXX count_0 is not universally applicable
        ddf = ddf_in.copy()

        for scol, param in self.parameters.items():
            if param.value != "NONE":
                ccol = "count_" + scol[6:]
                ddf[scol] = self.score(ddf[ccol], ddf[param.value])

        score_cols = [c for c in ddf.columns if c.startswith("score_")]
        return ddf.replace([np.inf, -np.inf], np.nan).dropna(
            how="all", subset=score_cols
        )

    def score(self, col1, col0):
        return NotImplementedError(
            "Subclass DaskScoringPlugin and provide a score() method"
        )


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

    def choose_plugin_classes(self, position: int):
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
