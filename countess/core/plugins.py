import logging
from collections.abc import Callable, Iterable, Mapping, MutableMapping
from importlib.metadata import entry_points
from itertools import islice
from typing import TYPE_CHECKING, Any, NamedTuple, Optional, Type

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore
from dask.callbacks import Callback

from countess.core.parameters import (
    BaseParam,
    BooleanParam,
    ChoiceParam,
    FileParam,
    FloatParam,
    IntegerParam,
    StringParam,
)

"""
Plugin lifecycle:
* Selector of plugins calls cls.can_follow(previous_class_or_instance)
  which returns True if a plugin of this class can follow that one
  (or None if this is the first plugin).  This lets the interface present
  a list of plausible plugins you might want to use in the pipeline.
* When plugin is selected, it gets __init__ed.
* plugin.parameters gets read to get the name of configuration fields.
* For a Dask plugin, plugin.get_columns() may get called


"""

PRERUN_ROW_LIMIT = 100

# XXX sadly this doesn't work
# ProgressCallbackType = NewType('ProgressCallbackType', Callable[[int, int, Optional[str]], None])


class DaskProgressCallback(Callback):
    """Matches Dask's idea of a progress callback to ours."""

    def __init__(self, progress_callback: Callable[[int, int, Optional[str]], None]):
        self.progress_callback = progress_callback

    def _start_state(self, dsk, state):
        self.total_tasks = len(state["ready"]) + len(state["waiting"])

    def _posttask(self, key, result, dsk, state, worker_id):
        self.progress_callback(len(state["finished"]), self.total_tasks)

    def _finish(self, dsk, state, failed):
        # XXX do something with "failed"
        self.progress_callback(self.total_tasks, self.total_tasks)


class BasePlugin:
    """Base class for all plugins.  Plugins exist as entrypoints, but also
    PluginManager checks that plugins subclass this class before accepting them
    as plugins."""

    name: str = ""
    title: str = ""
    description: str = ""
    parameters: MutableMapping[str, BaseParam] = {}

    @classmethod
    def can_follow(cls, plugin: Optional[Type["BasePlugin"]] | Optional["BasePlugin"]):
        """returns True if this plugin/plugin class can follow the plugin/plugin class
        `plugin` ... the class hierarchy enforces what methods must be present, eg:
        `DaskBasePlugin` has `output_columns` which identifies what will be passed."""
        return plugin is not None and (
            isinstance(plugin, BasePlugin)
            or (type(plugin) is type and issubclass(plugin, BasePlugin))
        )

    def __init__(self):
        # Parameters store the actual values they are set to, so we copy them so that
        # if the same plugin is used twice in a pipeline it will have its own parameters.
        self.parameters = dict(((k, v.copy()) for k, v in self.parameters.items()))

    def set_previous_plugin(self, previous_plugin: Optional["BasePlugin"]):
        self.previous_plugin = previous_plugin

    def update(self):
        pass

    def run(
        self,
        obj: Any,
        callback: Optional[Callable[[int, int, Optional[str]], None]] = None,
    ):
        """Not every plugin is going to support progress callbacks, plugins
        which do should override this method to call `callback` sporadically
        with two numbers estimating a fraction of the work completed, and an
        optional string describing what they're up to, eg:
            callback(42, 107, 'Thinking hard about stuff')
        the user interface code will display this in some way or another."""
        raise NotImplementedError(f"{self.__class__}.run()")

    def prerun(self, obj: Any) -> Any:
        return self.run(obj)

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
    def can_follow(cls, plugin: Optional[Type[BasePlugin]] | Optional[BasePlugin]):
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


def crop_dataframe(
    ddf: dd.DataFrame, row_limit: int = PRERUN_ROW_LIMIT
) -> dd.DataFrame:
    """Takes a dask dataframe `ddf` and returns a frame with at most `limit` rows"""
    if len(ddf) > row_limit:
        x, y = islice(ddf.index, 0, row_limit, row_limit - 1)
        ddf = ddf[x:y]
    return ddf


class DaskBasePlugin(BasePlugin):
    """Base class for plugins which accept and return dask DataFrames"""

    # XXX there's a slight disconnect here: is this plugin class indicating that the
    # input and output format are dask dataframes or that the computing done by
    # this plugin is in Dask?  I mean, if one then probably the other, but it's
    # possible we'll want to develop a plugin which takes some arbitrary file,
    # does computation in Dask and then returns a pandas dataframe, at which
    # point do we implement DaskInputPluginWhichReturnsPandas(DaskBasePlugin)?

    @classmethod
    def can_follow(cls, plugin: Optional[Type[BasePlugin]] | Optional[BasePlugin]):
        return plugin is not None and (
            isinstance(plugin, DaskBasePlugin)
            or (type(plugin) is type and issubclass(plugin, DaskBasePlugin))
        )

    def output_columns(self) -> Iterable[str]:
        return []

    def run(
        self,
        ddf: dd.DataFrame,
        callback: Optional[Callable[[int, int, Optional[str]], None]] = None,
    ):
        if not callback:
            return self.run_dask(ddf)

        with DaskProgressCallback(callback):
            return self.run_dask(ddf)

    def prerun(self, ddf: dd.DataFrame) -> dd.DataFrame:
        return crop_dataframe(self.run_dask(crop_dataframe(ddf)))

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        raise NotImplementedError(f"Implement {self.__class__.__name__}.run_dask()")


# XXX Potentially there's a PandasBasePlugin which can use a technique much like
# tqdm does in tqdm/std.py to monkeypatch pandas.apply and friends and provide
# progress feedback.


class DaskInputPlugin(FileInputMixin, DaskBasePlugin):
    """A specialization of the DaskBasePlugin to allow it to follow nothing, eg: come first.
    DaskInputPlugins don't care about existing columns because they're adding their own records anyway."""

    def load_files(self, row_limit: Optional[int] = None) -> Iterable[dd.DataFrame]:
        for fp in self.get_file_params():
            df = self.read_file_to_dataframe(fp, row_limit)
            if isinstance(df, pd.DataFrame):
                df = dd.from_pandas(df, chunksize=100_000_000)
            yield df

    def combine_dfs(self, dfs: list[dd.DataFrame]) -> dd.DataFrame:
        if len(dfs) > 0:
            # XXX what actually is the logical operation here a) between files in one load
            # and b) between existing dataframe and the new ones.
            return dd.concat(dfs)
        else:
            # completely empty dataframe!
            empty_ddf = dd.from_pandas(pd.DataFrame.from_records([]), npartitions=1)
            assert isinstance(empty_ddf, dd.DataFrame)
            return empty_ddf

    def run(
        self,
        ddf: Optional[dd.DataFrame],
        callback: Optional[Callable[..., None]] = None,
    ) -> dd.DataFrame:
        """Input plugins are likely I/O bound so instead of using the Dask progress callback
        mechanism this uses a simple count of files read."""

        dfs = [] if ddf is None else [ddf]
        num_files = len(list(self.get_file_params()))
        if callback:
            callback(0, num_files + 1, "Loading")
        for num, df in enumerate(self.load_files()):
            if callback:
                callback(num, num_files, "Loading")
            dfs.append(df)
        if callback:
            callback(num_files, num_files)

        return self.combine_dfs(dfs)

    def prerun(self, ddf: Optional[dd.DataFrame]) -> dd.DataFrame:

        dfs: list[dd.DataFrame] = [] if ddf is None else [ddf]
        for df in self.load_files(row_limit=PRERUN_ROW_LIMIT):
            dfs.append(df)
        return crop_dataframe(self.combine_dfs(dfs))

    def read_file_to_dataframe(
        self, file_params: Mapping[str, BaseParam], row_limit: Optional[int] = None
    ) -> dd.DataFrame | pd.DataFrame:
        raise NotImplementedError(
            f"Implement {self.__class__.__name__}.read_file_to_dataframe"
        )


class DaskTransformPlugin(DaskBasePlugin):
    """a Transform plugin takes columns from the input data frame."""

    _output_columns: Iterable[str] = []

    def output_columns(self):
        return self._output_columns


class DaskScoringPlugin(DaskTransformPlugin):
    """Specific kind of transform which turns counts into scores"""

    def update(self):
        input_columns = sorted(
            (c for c in self.previous_plugin.output_columns() if c.startswith("count"))
        )

        # XXX Allow to set names of score columns?  The whole [5:] thing is clumsy
        # and horrible but doing this properly maybe needs ArrayParams

        for col in input_columns:
            scol = "score" + col[5:]
            if scol not in self.parameters:
                self.parameters[scol] = ChoiceParam(
                    f"{scol} compares {col} and ...", "NONE", ["NONE"]
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

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:

        for scol, param in self.parameters.items():
            if isinstance(param, ChoiceParam) and param.value != "NONE":
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

