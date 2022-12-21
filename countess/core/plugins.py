import logging
from collections.abc import Callable, Iterable, Mapping, MutableMapping
from importlib.metadata import entry_points
from itertools import islice
from typing import TYPE_CHECKING, Any, NamedTuple, Optional, Type, List
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
    FileArrayParam,
    FileParam,
    FloatParam,
    IntegerParam,
    MultiParam,
    StringParam,
    LEVELS,
)
from countess.utils.dask import empty_dask_dataframe, crop_dask_dataframe, concat_dask_dataframes, merge_dask_dataframes

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

        # XXX should we allow django-esque declarations like this?  Namespace gets 
        # cluttered, though.

        for key in dir(self):
            if isinstance(getattr(self, key), BaseParam):
                self.parameters[key] = getattr(self, key).copy()
                setattr(self, key, self.parameters[key])

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

    def set_parameter(self, key: str, value: bool|int|float|str):
        param = self.parameters
        for k in key.split("."):
            param = param[k]
        param.value = value

    def get_config(self):
        for k, p in self.parameters.items():
            v = p.value

            yield from p.get_config(k)


class FileInputMixin:
    """Mixin class to indicate that this plugin can read files from local storage."""

    file_number = 0

    # used by the GUI file dialog
    file_types = [("Any", "*")]
    file_params = {}

    parameters: MutableMapping[str, BaseParam] = {
        'files': FileArrayParam('Files', FileParam('File'))
    }

    @classmethod
    def can_follow(cls, plugin: Optional[Type[BasePlugin]] | Optional[BasePlugin]):
        # the `not TYPE_CHECKING` clause is a workaround for mypy not really understanding
        # mixin classes.
        return plugin is None or (not TYPE_CHECKING and super().can_follow(plugin))


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

    def __init__(self):
        # Add in filenames and levels 
        super().__init__()
        file_params = { "filename": FileParam("Filename", file_types=self.file_types) }
        for name, label in LEVELS:
            file_params[name] = StringParam(label, "")
        file_params.update(self.file_params)

        self.parameters['files'] = FileArrayParam('Files', 
            MultiParam('File', file_params)
        )

    def load_files(self, row_limit: Optional[int] = None) -> Iterable[dd.DataFrame]:
        fps = self.parameters['files'].params
        if not fps: return

        per_file_row_limit = int(row_limit / len(fps) + 1) if row_limit else None
        for file_param in fps:
            levels = [file_param[level_name].value for level_name, _ in LEVELS]
            if any(levels):
                column_suffix = "_".join(levels)
            else:
                column_suffix = None
            df = self.read_file_to_dataframe(file_param, column_suffix, per_file_row_limit)
            if isinstance(df, pd.DataFrame):
                df = dd.from_pandas(df, chunksize=100_000_000)
            yield df

    def combine_dfs(self, dfs: list[dd.DataFrame]) -> dd.DataFrame:
        """First stage: collect all the files together in whatever
        way is appropriate.  Override this to do it differently
        or do more work on the dataframes (eg: counting, renaming, etc)"""
        return merge_dask_dataframes(dfs, how='outer')

    def merge_dfs(self, prev_ddf: dd.DataFrame, this_ddf: dd.DataFrame) -> dd.DataFrame:
        """Merge the new data into the old data.  Only called
        if there is a previous plugin to merge data from."""
        join_how = self.parameters['join_how'].value
        if join_how == 'concat':
            return concat_dask_dataframes([prev_ddf, this_ddf])
        else:
            return merge_dask_dataframes([prev_ddf, this_ddf], how=join_how)

    def run(
        self,
        prev_ddf: Optional[dd.DataFrame],
        callback: Optional[Callable[..., None]] = None,
        row_limit: Optional[int] = None,
    ) -> dd.DataFrame:
        """Input plugins are likely I/O bound so instead of using the Dask progress callback
        mechanism this uses a simple count of files read."""

        num_files = len(self.parameters['files'].params)
        if callback:
            callback(0, num_files, "Loading")

        dfs = []
        for num, df in enumerate(self.load_files(row_limit)):
            dfs.append(df)
            if callback:
                callback(num+1, num_files, "Loading")

        if callback:
            callback(num_files, num_files)

        if prev_ddf is not None:
            return self.merge_dfs(prev_ddf, self.combine_dfs(dfs))
        else:
            return self.combine_dfs(dfs)

    def prerun(self, ddf: Optional[dd.DataFrame]) -> dd.DataFrame:
        if ddf is not None and 'join_how' not in self.parameters:
            self.parameters['join_how'] = ChoiceParam("Join Direction", "outer", ["outer", "inner", "left", "right", "concat"])
        if ddf is None and 'join_how' in self.parameters:
            del self.parameters['join_how']

        self.prerun_cache = crop_dask_dataframe(
            self.run(ddf, None, PRERUN_ROW_LIMIT),
            PRERUN_ROW_LIMIT
        )
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

    def update(self):
        pass

    def prerun(self, ddf: dd.DataFrame) -> dd.DataFrame:
        self.input_columns = sorted(ddf.columns)
        self.update()
        return super().prerun(ddf)


class DaskScoringPlugin(DaskTransformPlugin):
    """Specific kind of transform which turns counts into scores"""

    max_counts = 5

    parameters = { 'scores': ArrayParam('Scores', MultiParam('Score', {
        'score': StringParam("Score Column"),
        'counts': ArrayParam('Counts', ChoiceParam('Column'), min_size=2, max_size=max_counts),
    }), min_size=1)}

    def update(self):
        print(f"0 {self}.update")
        count_columns = [c for c in self.input_columns if c.startswith("count")]

        for pp in self.parameters['scores']:
            print(f"1 {pp}")
            for ppp in pp.counts:
                print(f"2 {pp} {ppp}")
                #if ppp.value and ppp.value not in count_columns: pp.counts.del_subparam(ppp)
                ppp.choices = self.input_columns

        #if len(count_columns) > 1:
        #    after_columns = set(p.after.value for p in self.parameters['scores'] if p.after.value)
        #    for cc in count_columns[1:]:
        #        if cc not in after_columns:
        #            pp = self.parameters['scores'].add_row()
        #            pp.before.value = count_columns[0]
        #            pp.after.value = cc
        #            pp.score.value = "score" + cc[5:]

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:

        score_cols = []
        for pp in self.parameters['scores']:
            scol = pp.score.value
            ccols = [ ppp.value for ppp in pp.counts ]

            if scol and all(ccols):
                ddf[scol] = self.score([ddf[col] for col in ccols])
                score_cols.append(scol)

        return ddf.replace([np.inf, -np.inf], np.nan).dropna(
            how="all", subset=score_cols
        )

    def score(self, columns: List[dd.Series]) -> dd.Series:
        raise NotImplementedError(
            "Subclass DaskScoringPlugin and provide a score() method"
        )

class DaskReindexPlugin(DaskTransformPlugin):

    translate_type = str

    def translate(self, value):
        raise NotImplementedError(f"Implement {self.__class__.__name__}.translate")

    def translate_row(self, row):
        return self.translate(row.name)

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        ddf['__reindex'] = ddf.apply(self.translate_row, axis=1, meta=pd.Series(self.translate_type()))
        return ddf.groupby('__reindex').sum()


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


