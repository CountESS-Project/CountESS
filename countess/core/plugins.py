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
    ColumnChoiceParam,
    ColumnOrNoneChoiceParam,
    FileArrayParam,
    FileParam,
    FloatParam,
    IntegerParam,
    MultiParam,
    StringParam,
)
from countess.utils.dask import empty_dask_dataframe, crop_dask_dataframe, concat_dask_dataframes, merge_dask_dataframes

"""
Plugin lifecycle:
* Selector of plugins calls cls.accepts(previous_data) with the input data which will
  be provided to this plugin (or None if this is the first plugin)
  which returns True if a plugin of this class can accept that data.
  This lets the interface present a list of plausible plugins you might want to use
  in the pipeline.
* When plugin is selected, it gets __init__ed.
* The plugin then gets .prepare()d with a cut-down input.
  * .prepare() gets run again any time a preceding plugin changes
  * .prepare() can alter .parameters or throw exceptions, etc.
* plugin.parameters gets read to get the name of configuration fields.
"""

PRERUN_ROW_LIMIT = 100


class BasePlugin:
    """Base class for all plugins.  Plugins exist as entrypoints, but also
    PluginManager checks that plugins subclass this class before accepting them
    as plugins."""

    name: str = ""
    title: str = ""
    description: str = ""
    version: str = "0.0.0"

    parameters: MutableMapping[str, BaseParam] = {}

    @classmethod
    def accepts(cls, data) -> bool:
        """Work out if this plugin class can accept `data` as an input by
        trying it and finding out.  Subclasses should override this if they 
        have an easier way."""
        
        try:
            cls().prepare(data)
            return True
        except (ImportError, TypeError, ValueError, AssertionError):
            return False

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

    def prepare(self, data) -> bool:
        """The plugin gets a preview version of its input data so it can 
        check types, column names, etc.  Should throw an exception if this isn't 
        a suitable data input."""
        pass

    def update(self):
        pass

    def run(
        self,
        obj: Any,
        callback: Callable[[int, int, Optional[str]], None],
        row_limit: Optional[int] = None,
    ):
        """Plugins which support progress monitoring should override this method
        to call `callback` sporadically with two numbers estimating a fraction of
        the work completed, and an optional string describing what they're doing:
            callback(42, 107, 'Thinking hard about stuff')
        The user interface code will then display this to the user while the 
        pipeline is running."""
        raise NotImplementedError(f"{self.__class__}.run()")

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
    def accepts(self, data) -> bool:
        """Input Plugins can accept anything as their input, since they're getting
        their data from a file anyway."""
        return True

    def run(
        self,
        previous: Any,
        callback: Callable[[int, int, Optional[str]], None],
        row_limit: Optional[int] = None,
    ):

        df = self.load_files(callback, row_limit)

        if type(previous) is list:
            return [ df ] + previous
        elif previous is not None:
            return [ df, previous ]
        else:
            return df


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


class DaskBasePlugin(BasePlugin):
    """Base class for plugins which accept and return dask DataFrames"""

    # XXX there's a slight disconnect here: is this plugin class indicating that the
    # input and output format are dask dataframes or that the computing done by
    # this plugin is in Dask?  I mean, if one then probably the other, but it's
    # possible we'll want to develop a plugin which takes some arbitrary file,
    # does computation in Dask and then returns a pandas dataframe, at which
    # point do we implement DaskInputPluginWhichReturnsPandas(DaskBasePlugin)?

    @classmethod
    def accepts(self, data) -> bool:
        return isinstance(data, (dd.DataFrame, pd.DataFrame)) or \
                type(data) is list and isinstance(data[0], (dd.DataFrame, pd.DataFrame))

    def run(
        self,
        data,
        callback: Callable[[int, int, Optional[str]], None],
        row_limit: Optional[int],
    ):
        with DaskProgressCallback(callback):
            if type(data) is list:
                return [ self.run_dask(data[0].copy()) ] + data[1:]
            else:
                return self.run_dask(data.copy())

    def run_dask(self, ddf: dd.DataFrame) -> dd.DataFrame:
        raise NotImplementedError(f"Implement {self.__class__.__name__}.run_dask()")


# XXX Potentially there's a PandasBasePlugin which can use a technique much like
# tqdm does in tqdm/std.py to monkeypatch pandas.apply and friends and provide
# progress feedback.

class DaskInputPlugin(FileInputMixin, DaskBasePlugin):
    """A specialization of the DaskBasePlugin to allow it to follow nothing, eg: come first."""

    def __init__(self):
        # Add in filenames
        super().__init__()
        file_params = { "filename": FileParam("Filename", file_types=self.file_types) }
        file_params.update(self.file_params)

        self.parameters['files'] = FileArrayParam('Files', 
            MultiParam('File', file_params)
        )

    def combine_dfs(self, dfs: list[dd.DataFrame]) -> dd.DataFrame:
        """First stage: collect all the files together in whatever
        way is appropriate.  Override this to do it differently
        or do more work on the dataframes (eg: counting, renaming, etc)"""
        return concat_dask_dataframes(dfs)

    def load_files(self, callback: Callable[[int, int, Optional[str]], None], row_limit: Optional[int] = None) -> Iterable[dd.DataFrame]:
        fps = self.parameters['files'].params
        if not fps: return

        if len(fps) == 1:
            with DaskProgressCallback(callback):
                file_param = self.parameters['files'].params[0]
                ddf = self.read_file_to_dataframe(file_param, None, row_limit)
        else:
            # Input plugins are likely I/O bound so if there's more than one
            # file, instead of using the Dask progress callback mechanism
            # this uses a simple count of files read."""
            per_file_row_limit = int(row_limit / len(fps) + 1) if row_limit else None
            callback(0, num_files, "Loading")
            dfs = []
            for num, fp in enumerate(fps):
                df = self.read_file_to_dataframe(fp, None, per_file_row_limit)
                dfs.append(df)
                callback(num+1, num_files, "Loading")
            callback(num_files, num_files)
            ddf = self.combine_dfs(dfs)

        return ddf

    def read_file_to_dataframe(
        self, file_params: Mapping[str, BaseParam], row_limit: Optional[int] = None
    ) -> dd.DataFrame | pd.DataFrame:
        raise NotImplementedError(
            f"Implement {self.__class__.__name__}.read_file_to_dataframe"
        )

def _set_column_choice_params(parameter, column_names):
    if isinstance(parameter, ArrayParam):
        _set_column_choice_params(parameter.param, column_names)
        for p in parameter.params:
            _set_column_choice_params(p, column_names)
    elif isinstance(parameter, MultiParam):
        for p in parameter.params.values():
            _set_column_choice_params(p, column_names)
    elif isinstance(parameter, ColumnOrNoneChoiceParam):
        parameter.set_choices(['--'] + column_names) 
    elif isinstance(parameter, ColumnChoiceParam):
        parameter.set_choices(column_names)


class DaskTransformPlugin(DaskBasePlugin):
    """a Transform plugin takes columns from the input data frame."""

    input_columns: list[str] = []

    def prepare(self, data):
        if data is None:
            self.input_columns = []
        else:
            self.input_columns = sorted(data.columns)

        for p in self.parameters.values():
            _set_column_choice_params(p, self.input_columns)


class DaskScoringPlugin(DaskTransformPlugin):
    """Specific kind of transform which turns counts into scores"""

    max_counts = 5

    parameters = { 'scores': ArrayParam('Scores', MultiParam('Score', {
        'score': StringParam("Score Column"),
        'counts': ArrayParam('Counts', ChoiceParam('Column'), min_size=2, max_size=max_counts),
    }), min_size=1)}

    def prepare(self, data):
        super().prepare(data)
        count_columns = [c for c in self.input_columns if c.startswith("count")]

        for pp in self.parameters['scores']:
            for ppp in pp.counts:
                ppp.choices = self.input_columns

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

    def prepare(self, ddf):
        super().prepare(ddf)
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


