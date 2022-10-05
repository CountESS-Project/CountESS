import logging
from collections.abc import Callable, Iterable, Mapping, MutableMapping
from importlib.metadata import entry_points
from itertools import islice
from typing import TYPE_CHECKING, Any, NamedTuple, Optional, Type
import re

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore
from dask.callbacks import Callback

from countess.core.parameters import (
    ArrayParam,
    BaseParam,
    BooleanParam,
    ChoiceParam,
    FileParam,
    FloatParam,
    IntegerParam,
    MultiParam,
    StringParam,
)
from countess.utils.dask import empty_dask_dataframe, crop_dask_dataframe, concat_dask_dataframes

"""
Plugin lifecycle:
* Selector of plugins calls cls.can_follow(previous_class_or_instance_or_none)
  with either a preceding plugin class or instance (or None if this is the first plugin)
  which returns True if a plugin of this class can follow that one (or come first)
  This lets the interface present a list of plausible plugins you might want to use in the pipeline.
* When plugin is selected, it gets __init__ed.
* The plugin then gets .prerun() with a cut-down input.
  * .prerun() gets run again any time a preceding plugin changes
  * .prerun() can alter .parameters or throw exceptions, etc.
* plugin.parameters gets read to get the name of configuration fields.


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
    version: str = "0.0.0"

    parameters: MutableMapping[str, BaseParam] = {}

    prerun_cache: Any=None

    @classmethod
    def can_follow(cls, plugin: Optional[Type["BasePlugin"]] | Optional["BasePlugin"]):
        """returns True if this plugin/plugin class can follow the plugin/plugin class
        `plugin` ... eg: can the .run() method accept the right kind of object."""
        return plugin is not None and (
            isinstance(plugin, BasePlugin)
            or (type(plugin) is type and issubclass(plugin, BasePlugin))
        )

    def __init__(self):
        # Parameters store the actual values they are set to, so we copy them so that
        # if the same plugin is used twice in a pipeline it will have its own parameters.
        self.parameters = dict(((k, v.copy()) for k, v in self.parameters.items()))

    def update(self):
        pass

    def run(
        self,
        obj: Any,
        callback: Optional[Callable[[int, int, Optional[str]], None]] = None,
    ):
        """Plugins which support progress monitoring should override this method
        to call `callback` sporadically with two numbers estimating a fraction of
        the work completed, and an optional string describing what they're doing:
            callback(42, 107, 'Thinking hard about stuff')
        The user interface code will then display this to the user while the 
        pipeline is running."""
        raise NotImplementedError(f"{self.__class__}.run()")

    def prerun(self, obj: Any) -> Any:
        """Plugins can detect their input using "prerun", which will be called with
        a small number of rows while the plugins are being configured.  This lets each
        plugin detect its input and offer specific configuration and/or throw exceptions
        which will be displayed to the user"""
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

    def run(
        self,
        ddf: dd.DataFrame,
        callback: Optional[Callable[[int, int, Optional[str]], None]] = None,
    ):
        if not callback:
            return self.run_dask(ddf.copy())

        with DaskProgressCallback(callback):
            return self.run_dask(ddf.copy())

    def prerun(self, ddf: dd.DataFrame) -> dd.DataFrame:
        self.prerun_cache = crop_dask_dataframe(
                self.run_dask(ddf.copy()),
                PRERUN_ROW_LIMIT
        )
        return self.prerun_cache

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        raise NotImplementedError(f"Implement {self.__class__.__name__}.run_dask()")


# XXX Potentially there's a PandasBasePlugin which can use a technique much like
# tqdm does in tqdm/std.py to monkeypatch pandas.apply and friends and provide
# progress feedback.


class DaskInputPlugin(FileInputMixin, DaskBasePlugin):
    """A specialization of the DaskBasePlugin to allow it to follow nothing, eg: come first."""

    def load_files(self, row_limit: Optional[int] = None) -> Iterable[dd.DataFrame]:
        fps = list(self.get_file_params())
        if not fps: return

        per_file_row_limit = int(row_limit / len(fps) + 1) if row_limit else None
        for fp in fps:
            print(f"{self.__class__.__name__} load_files {fp['filename'].value}")
            df = self.read_file_to_dataframe(fp, per_file_row_limit)
            if isinstance(df, pd.DataFrame):
                df = dd.from_pandas(df, chunksize=100_000_000)
            yield df

    def combine_dfs(self, dfs: list[dd.DataFrame]) -> dd.DataFrame:
        """Consistently handles cases for zero and one input dataframe"""
        # XXX what actually is the logical operation here a) between files in one load
        # and b) between existing dataframe and the new ones.
        # Merge or concat?

        return concat_dask_dataframes(dfs)

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
            callback(0, num_files, "Loading")
        for num, df in enumerate(self.load_files()):
            dfs.append(df)
            if callback:
                callback(num+1, num_files, "Loading")
        if callback:
            callback(num_files, num_files)

        return self.combine_dfs(dfs)

    def prerun(self, ddf: Optional[dd.DataFrame]) -> dd.DataFrame:

        dfs: list[dd.DataFrame] = [] if ddf is None else [ddf]
        for df in self.load_files(row_limit=PRERUN_ROW_LIMIT):
            dfs.append(df)
        self.prerun_cache = crop_dask_dataframe(self.combine_dfs(dfs), PRERUN_ROW_LIMIT)
        return self.prerun_cache

    def read_file_to_dataframe(
        self, file_params: Mapping[str, BaseParam], row_limit: Optional[int] = None
    ) -> dd.DataFrame | pd.DataFrame:
        raise NotImplementedError(
            f"Implement {self.__class__.__name__}.read_file_to_dataframe"
        )


class DaskTransformPlugin(DaskBasePlugin):
    """a Transform plugin takes columns from the input data frame."""

    input_columns: list[str] = []

    def prerun(self, ddf: dd.DataFrame) -> dd.DataFrame:
        self.input_columns = sorted(ddf.columns)
        print(f"{self.__class__.__name__} prerun {self.input_columns}")
        return super().prerun(ddf)


class DaskScoringPlugin(DaskTransformPlugin):
    """Specific kind of transform which turns counts into scores"""

    parameters = {
        'score': ArrayParam('Scores', MultiParam('Score', {
            'score': StringParam("Score Column"),
            'after': ChoiceParam("After Column"),
            'before': ChoiceParam("Before Column"),
        }))
    }

    def update(self):

        for p in self.parameters['score'].params:
            for pp in p.params.values():
                pp.choices = self.input_columns

        # XXX Allow to set names of score columns?  The whole [5:] thing is clumsy
        # and horrible but doing this properly maybe needs ArrayParams

        #for col in self.input_columns:
        #    scol = "score" + col[5:]
        #    if scol not in self.parameters:
        #        self.parameters[scol] = ChoiceParam(
        #            f"{scol} compares {col} and ...", "NONE", ["NONE"]
        #        )
        #    self.parameters[scol].choices = ["NONE"] + [
        #        x for x in self.input_columns if x != col
        #    ]
#
#        scols = [k for k in self.parameters.keys() if k.startswith("score")]
#        for scol in scols:
#            col = "count" + scol[5:]
#            if col not in self.input_columns:
#                del self.parameters[scol]

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        print(f"{self} run_dask {ddf.columns} {len(ddf)}")
        for scol, param in self.parameters.items():
            if isinstance(param, ChoiceParam) and param.value != "NONE":
                ccol = "count" + scol[5:]
                ddf[scol] = self.score(ddf[ccol], ddf[param.value])

        score_cols = [c for c in ddf.columns if c.startswith("score")]
        print(f"{score_cols} {ddf.columns} {len(ddf)}")
        return ddf.replace([np.inf, -np.inf], np.nan).dropna(
            how="all", subset=score_cols
        )

    def score(self, col1, col0):
        return NotImplementedError(
            "Subclass DaskScoringPlugin and provide a score() method"
        )

class DaskTranslationPlugin(DaskTransformPlugin):

    translate_type = str

    parameters = {
        "input": ChoiceParam("Input Column", "", choices=[""]),
        "output": StringParam("Output Column", ""),
    }

    def update(self):
        self.parameters["input"].choices = [""] + self.input_columns

    def translate(self, value):
        raise NotImplementedError(f"Implement {self.__class__.__name__}.translate")

    def translate_row(self, row, input_column):
        return self.translate(row[input_column] if input_column else row.name)

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        input_column = self.parameters["input"].value
        output_column = self.parameters["output"].value or '__translate'

        ddf[output_column] = ddf.apply(self.translate_row, axis=1, args=(input_column,), meta=pd.Series(self.translate_type()))

        if output_column == '__translate':
            ddf = ddf.groupby('__translate').sum()

        return ddf


