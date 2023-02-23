import hashlib
import importlib
import importlib.metadata
import logging
from collections.abc import Callable, Iterable, Mapping, MutableMapping
from typing import Any, List, Optional

import dask.dataframe as dd
import numpy as np
import pandas as pd  # type: ignore
from dask.callbacks import Callback

from countess.core.parameters import (
    ArrayParam,
    BaseParam,
    ChoiceParam,
    ColumnChoiceParam,
    ColumnOrNoneChoiceParam,
    FileArrayParam,
    FileParam,
    MultiParam,
    StringParam,
)
from countess.utils.dask import concat_dataframes, crop_dataframe
from countess.core.logger import Logger

"""
Plugin lifecycle:
  To create a new plugin:
    * Plugin is __init__()ed
    * Plugin.prepare(data) gets called with the connected inputs, to let
      the plugin know what its inputs are.  This method should not do anything
      CPU intensive, it just checks the type of inputs are suitable.
    * Plugin.set_parameter(key, value) gets called, potentially many times.
    * Plugin.prerun() gets called and this should return sample output.
  When inputs have changed:
    * Call prepare() again.
  To change configuration
    * Call Plugin.set_parameter(), potentially several times.
    * Call Plugin.prerun() to generate new output
"""

PRERUN_ROW_LIMIT = 100


def get_plugin_classes():
    plugin_classes = []
    for ep in importlib.metadata.entry_points(group="countess_plugins"):
        plugin_class = ep.load()
        if issubclass(plugin_class, BasePlugin):
            plugin_classes.append(plugin_class)
        else:
            logging.warning(f"{plugin_class} is not a valid CountESS plugin")
    return plugin_classes


def load_plugin(module_name, class_name):
    module = importlib.import_module(module_name)
    plugin_class = getattr(module, class_name)
    assert issubclass(plugin_class, BasePlugin)
    return plugin_class()


class BasePlugin:
    """Base class for all plugins.  Plugins exist as entrypoints, but also
    PluginManager checks that plugins subclass this class before accepting them
    as plugins."""

    name: str = ""
    title: str = ""
    description: str = ""
    version: str = "0.0.0"

    parameters: MutableMapping[str, BaseParam] = {}

    def __init__(self, plugin_name=None):
        # Parameters store the actual values they are set to, so we copy them
        # so that if the same plugin is used twice in a pipeline it will have
        # its own parameters.

        if plugin_name is not None:
            self.name = plugin_name

        self.parameters = dict((k, v.copy()) for k, v in self.parameters.items())

        # XXX should we allow django-esque declarations like this?
        # Code gets cleaner, Namespace gets cluttered, though.

        for key in dir(self):
            if isinstance(getattr(self, key), BaseParam):
                self.parameters[key] = getattr(self, key).copy()
                setattr(self, key, self.parameters[key])

    def prepare(self, data: Any, logger: Logger):
        """The plugin gets a preview version of its input data so it can
        check types, column names, etc.  Should throw an exception if this
        isn't a suitable data input."""
        return True

    def run(
        self,
        obj: Any,
        logger: Logger,
        row_limit: Optional[int] = None,
    ):
        """Plugins which support progress monitoring should override this
        method to call `callback` sporadically with two numbers estimating a
        fraction of the work completed, and an optional string describing
        what they're doing:
            callback(42, 107, 'Thinking hard about stuff')
        The user interface code will then display this to the user while the
        pipeline is running."""
        raise NotImplementedError(f"{self.__class__}.run()")

    def add_parameter(self, name: str, param: BaseParam):
        self.parameters[name] = param.copy()
        return self.parameters[name]

    def set_parameter(self, key: str, value: bool | int | float | str):
        param = self.parameters
        for k in key.split("."):
            # XXX types are a mess here
            param = param[k]  # type: ignore
        param.value = value  # type: ignore

    def get_parameters(self):
        for key, parameter in self.parameters.items():
            yield from parameter.get_parameters(key)

    def get_parameter_hash(self):
        """Build a hash of all configuration parameters"""
        h = hashlib.sha256()
        for k, v in self.parameters.items():
            h.update((k + "\0" + v.get_hash_value()).encode("utf-8"))
        return h

    def hash(self):
        """Returns a hex digest of the hash of all configuration parameters"""
        return self.get_parameter_hash().hexdigest()


class FileInputMixin:
    """Mixin class to indicate that this plugin can read files from local
    storage."""

    file_number = 0

    # used by the GUI file dialog
    file_types = [("Any", "*")]
    file_params: MutableMapping[str, BaseParam] = {}

    parameters: MutableMapping[str, BaseParam] = {
        "files": FileArrayParam("Files", FileParam("File"))
    }

    def prepare(self, obj: Any, logger: Logger) -> bool:
        if obj is not None:
            logger.error("FileInputMixin doesn't take inputs")
            return False
        return True

    def load_files(
        self,
        logger: Logger,
        row_limit: Optional[int] = None,
    ):
        raise NotImplementedError("FileInputMixin.load_files")

    def run(
        self,
        previous: Any,
        logger: Logger,
        row_limit: Optional[int] = None,
    ):

        df = self.load_files(logger, row_limit)

        return df
        if type(previous) is list:
            return [df] + previous
        elif previous is not None:
            return [df, previous]
        else:
            return df


class DaskProgressCallback(Callback):
    """Matches Dask's idea of a progress callback to our logger class."""

    def __init__(self, logger: Logger):
        self.logger = logger

    def _start_state(self, dsk, state):
        self.total_tasks = len(state["ready"]) + len(state["waiting"])
        pass

    def _posttask(self, key, result, dsk, state, worker_id):
        self.logger.progress("Running", 100 * len(state["finished"]) // self.total_tasks)

    def _finish(self, dsk, state, failed):
        # XXX do something with "failed"
        self.logger.progress("Done", 100)


class DaskBasePlugin(BasePlugin):
    """Base class for plugins which accept and return dask/pandas DataFrames"""

    def prepare_dask(self, df: pd.DataFrame|dd.DataFrame, logger: Logger) -> bool:
        assert isinstance(df, (pd.DataFrame, dd.DataFrame))
        return True

    def prepare(self, data: pd.DataFrame|dd.DataFrame|Mapping[str,pd.DataFrame|dd.DataFrame], logger: Logger) -> bool:
        if isinstance(data, Mapping):
            data = concat_dataframes(data.values())

        return self.prepare_dask(data, logger)

    def run_dask(self, df: pd.DataFrame|dd.DataFrame, logger: Logger) -> pd.DataFrame|dd.DataFrame:
        raise NotImplementedError(f"Implement {self.__class__.__name__}.run_dask()")

    def run(
        self,
        data: pd.DataFrame|dd.DataFrame|Mapping[str,pd.DataFrame|dd.DataFrame],
        logger: Logger,
        row_limit: Optional[int] = None,
    ) -> pd.DataFrame|dd.DataFrame:
        assert isinstance(logger, Logger)
        assert row_limit is None or type(row_limit) is int

        with DaskProgressCallback(logger):
            logger.progress("Starting", 0)
            if isinstance(data, Mapping): 
                dfs = []
                for key, obj in data.items():
                    assert isinstance(obj, (pd.DataFrame, dd.DataFrame))
                    df = self.run_dask(obj, logger)
                    assert isinstance(df, (pd.DataFrame, dd.DataFrame))
                    dfs.append(crop_dataframe(df, row_limit))
                logger.progress("Finished", 100)
                return concat_dataframes(dfs)
            else:
                assert isinstance(data, (pd.DataFrame, dd.DataFrame))
                df = self.run_dask(data, logger)
                assert isinstance(df, (pd.DataFrame, dd.DataFrame))
                logger.progress("Finished", 100)
                return crop_dataframe(df, row_limit)


# XXX Potentially there's a PandasBasePlugin which can use a technique much
# like tqdm does in tqdm/std.py to monkeypatch pandas.apply and friends and
# provide progress feedback.


class DaskInputPlugin(FileInputMixin, DaskBasePlugin):
    """A specialization of the DaskBasePlugin to allow it to follow nothing,
    eg: come first."""

    def __init__(self, *a, **k):
        # Add in filenames
        super().__init__(*a, **k)
        file_params = {"filename": FileParam("Filename", file_types=self.file_types)}
        file_params.update(self.file_params)

        self.parameters = dict(
            [("files", FileArrayParam("Files", MultiParam("File", file_params)))]
            + list(self.parameters.items())
        )

    def combine_dfs(self, dfs: list[dd.DataFrame]) -> dd.DataFrame:
        """First stage: collect all the files together in whatever
        way is appropriate.  Override this to do it differently
        or do more work on the dataframes (eg: counting, renaming, etc)"""
        return concat_dataframes(dfs)

    def load_files(
        self,
        logger: Logger,
        row_limit: Optional[int] = None,
    ) -> Iterable[dd.DataFrame]:
        assert isinstance(self.parameters["files"], ArrayParam)
        fps = self.parameters["files"].params
        if not fps:
            return []

        if len(fps) == 1:
            with DaskProgressCallback(logger):
                file_param = self.parameters["files"].params[0]
                assert isinstance(file_param, MultiParam)
                ddf = self.read_file_to_dataframe(file_param, row_limit)
                ddf = self.combine_dfs([ddf])
        else:
            num_files = len(fps)
            # Input plugins are likely I/O bound so if there's more than one
            # file, instead of using the Dask progress callback mechanism
            # this uses a simple count of files read."""
            per_file_row_limit = int(row_limit / len(fps) + 1) if row_limit else None
            logger.progress("Loading", 0)
            dfs = []
            for num, fp in enumerate(fps):
                assert isinstance(fp, MultiParam)
                df = self.read_file_to_dataframe(fp, logger, per_file_row_limit)
                dfs.append(df)
                logger.progress("Loading", 100 * (num+1) // (num_files + 1))
            callback(num_files, num_files + 1, "Combining")
            ddf = self.combine_dfs(dfs)
            logger.progress("Done", 100)

        return ddf

    def read_file_to_dataframe(
            self, file_params: MultiParam, logger: Logger, row_limit: Optional[int] = None
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
    elif isinstance(parameter, ColumnChoiceParam):
        parameter.set_choices(column_names)


class DaskTransformPlugin(DaskBasePlugin):
    """a Transform plugin takes columns from the input data frame."""

    input_columns: list[str] = []

    def prepare_dask(self, data: dd.DataFrame|pd.DataFrame, logger):
        self.input_columns = sorted(data.columns)

        for p in self.parameters.values():
            _set_column_choice_params(p, self.input_columns)


class DaskScoringPlugin(DaskTransformPlugin):
    """Specific kind of transform which turns counts into scores"""
    # XXX not really useful?

    max_counts = 5

    parameters = {
        "scores": ArrayParam(
            "Scores",
            MultiParam(
                "Score",
                {
                    "score": StringParam("Score Column"),
                    "counts": ArrayParam(
                        "Counts", ChoiceParam("Column"), min_size=2, max_size=max_counts
                    ),
                },
            ),
            min_size=1,
        )
    }

    def prepare_dask(self, data, logger):
        super().prepare(data, logger)
        for pp in self.parameters["scores"]:
            for ppp in pp.counts:
                ppp.choices = self.input_columns

    def run_dask(self, ddf: dd.DataFrame, logger: Logger) -> dd.DataFrame:
        assert isinstance(self.parameters["scores"], ArrayParam)
        score_cols = []
        for pp in self.parameters["scores"]:
            scol = pp.score.value
            ccols = [ppp.value for ppp in pp.counts]

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
    # XXX not really useful?

    translate_type = str

    def translate(self, value):
        raise NotImplementedError(f"Implement {self.__class__.__name__}.translate")

    def translate_row(self, row):
        return self.translate(row.name)

    def run_dask(self, ddf: dd.DataFrame, logger: Logger) -> dd.DataFrame:
        ddf["__reindex"] = ddf.apply(
            self.translate_row, axis=1, meta=pd.Series(self.translate_type())
        )
        return ddf.groupby("__reindex").sum()
