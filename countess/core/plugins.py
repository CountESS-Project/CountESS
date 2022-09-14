from typing import Optional, Any, Type, NamedTuple
from collections import namedtuple, defaultdict
from collections.abc import Iterable, Callable, Mapping

from countess.core.parameters import BaseParam, BooleanParam, IntegerParam, FloatParam, StringParam, FileParam

from importlib.metadata import entry_points
import logging
import dask.dataframe as dd
from dask.callbacks import Callback
import pandas as pd
import numpy as np

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
    PluginManager check that plugins subclass this class before accepting them
    as plugins."""

    name: str = ''
    description: str = ''
    parameters: Mapping[str,BaseParam] = None

    @classmethod
    def can_follow_plugin(cls, previous_plugin: 'BasePlugin'):
        """Can this plugin class be used to follow 'previous_plugin'? """
        return isinstance(previous_plugin, BasePlugin)

    def __init__(self, previous_plugin: 'BasePlugin'):
        self.previous_plugin = previous_plugin

    @property
    def parameters(self):
        self._parameters = {}
        if self.__class__.parameters:
            self._parameters = dict([ (k, v.copy()) for k, v in self.parameters.items() ])
        return self._parameters

    def run_with_progress_callback(self, obj: Any, callback: Callable[[int, int, Optional[str]], None]):
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
        if self.parameters is None: self.parameters = {}
        self.parameters[name] = param.copy()
        return self.parameters[name]

class FileInputMixin:
    """Mixin class to indicate that this plugin can read files from local storage"""
    
    # XXX clumsy ... maybe store file params separately instead of encoding them into
    # self.parameters which isn't working nicely
    
    file_number = 0
   
    # used by the GUI file dialog
    file_types = [ ('Any', '*') ]

    file_params: Iterable[BaseParam] = []

    def add_file(self, filename):
        self.file_number += 1
        self.add_parameter(f'file.{self.file_number}.filename', FileParam("Filename", filename))
        self.add_file_params(filename, self.file_number)
    
    def add_file_params(self, filename, file_number):
        return dict([
            (key, self.add_parameter(f'file.{file_number}.{key}', param))
            for key, param in self.file_params.items()
        ])
    
    def remove_file(self, filenumber):
        for k in self.parameters.keys():
            if k.startswith(f'file.{filenumber}.'):
                del self.parameters[k]

    def get_file_params(self):
        for n in range(1, self.file_number+1):
            if f'file.{n}.filename' in self.parameters:
                yield dict(
                    [ ('filename', self.parameters[f'file.{n}.filename']) ] + 
                    [
                        ( key, self.parameters[f'file.{n}.{key}'])
                        for key, param in self.file_params.items()
                    ]
                )

    def run_with_progress_callback(self, ddf: Optional[dd.DataFrame], callback) -> dd.DataFrame:
        file_params = list(self.get_file_params())

        dfs = [] if ddf is None else [ddf]

        n_files = len(file_params)
        for num, fp in enumerate(file_params):
            callback(num, n_files, "Loading")
            df = self.read_file_to_dataframe(**fp)
            if isinstance(df, pd.DataFrame):
                df = dd.from_pandas(df, chunksize=100_000_000)
            dfs.append(df)

        if len(dfs) > 0:
            return dd.concat(dfs)
        else:
            # completely empty dataframe!
            return dd.from_pandas(pd.DataFrame.from_records([]), npartitions=1)

    def read_file_to_dataframe(self, **kwargs) -> dd.DataFrame|pd.DataFrame:
        raise NotImplementedError(f"Implement {self.__class__.__name__}.read_file_to_dataframe")

        
class DaskBasePlugin(BasePlugin):
    """Base class for plugins which accept and return dask DataFrames"""

    # XXX there's a slight disconnect here: is this plugin indicating that the
    # input and output format are dask dataframes or that the computing done by
    # this plugin is in Dask?  I mean, if one then probably the other, but it's
    # possible we'll want to develop a plugin which takes some arbitrary file,
    # does computation in Dask and then returns a pandas dataframe, at which
    # point do we implement DaskInputPluginWhichReturnsPandas(DaskBasePlugin)?

    _output_columns = set()
        
    @classmethod
    def can_follow_plugin(cls, previous_plugin: BasePlugin):
        return isinstance(previous_plugin, DaskBasePlugin)
    
    #def output_columns(self) -> Iterable[str]:
    #    return self._output_columns
    
    def run_with_progress_callback(self, ddf, callback: Callable[[int, int, Optional[str]], None]):
        
        class DaskProgressCallback(Callback):
            """Matches Dask's idea of a progress callback to ours."""
            # XXX there's probably neater ways, like just passing some lambdas to Callback

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

class DaskInputPlugin(FileInputMixin, DaskBasePlugin):
    """A specialization of the DaskBasePlugin to allow it to follow nothing, eg: come first.
    DaskInputPlugins don't care about existing columns because they're adding their own records anyway."""
    
    @classmethod
    def can_follow_plugin(cls, previous_plugin: Optional[BasePlugin]):
        """An input plugin can follow None (to allow it to be the first plugin"""
        return previous_plugin is None or super().can_follow_plugin(previous_plugin)
    

class DaskTransformPlugin(DaskBasePlugin):
    """a Transform plugin takes columns from the input data frame."""
    
    @classmethod
    def can_follow_plugin(cls, previous_plugin: BasePlugin):
        return isinstance(previous_plugin, DaskBasePlugin)
        
    #def output_columns(self) -> Iterable[str]:
    #    return []
    

class DaskScoringPlugin(DaskTransformPlugin):
    """Specific kind of tranform which turns counts into scores"""
 
    _parameters = None

    @property
    def parameters(self):
        if self._parameters is None: self._parameters = {}

        columns = self.previous_plugin.get_columns()

        r = {}
        for col in columns:
            if col.startswith('count_'):
                scol = "score_" + col[6:]
                if scol not in self._parameters:
                    self._parameters[scol] = StringParam(f"{scol} compares {col} and ...")
                self._parameters[scol].choices = ["NONE"] + list(columns)
                r[scol] = self._parameters[scol]

        return r

    def run(self, ddf_in: dd.DataFrame):
        # XXX count_0 is not universally applicable
        ddf = ddf_in.copy()

        for scol, param in self.parameters.items():
            if param.value != 'NONE':
                ccol = "count_" + scol[6:]
                ddf[scol] = self.score(ddf[ccol], ddf[param.value])
   
        score_cols = [ c for c in ddf.columns if c.startswith("score_") ]
        return ddf.replace([np.inf, -np.inf], np.nan).dropna(how="all", subset=score_cols)
            
    def score(self, col1, col0):
        return NotImplementedError("Subclass DaskScoringPlugin and provide a score() method")

    def output_columns(self) -> Iterable[str]:
        return self.parameters.keys()


class PluginManager:
    
    def __init__(self):

        self.plugins = []
        for ep in entry_points(group='countess_plugins'):
            plugin = ep.load()
            if issubclass(plugin, BasePlugin):
                self.plugins.append(plugin)
            else:
                logging.warning(f"{plugin} is not a valid CountESS plugin")

    def plugin_classes_following(self, previous_plugin):
        for plugin in self.plugins:
            if plugin.can_follow_plugin(previous_plugin):
                yield plugin
