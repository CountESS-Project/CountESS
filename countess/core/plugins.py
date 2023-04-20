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

import hashlib
import importlib
import importlib.metadata
import logging
import os.path
import sys
from collections.abc import Mapping, MutableMapping
from typing import Any, List, Optional

import numpy as np
import pandas as pd

from countess.core.logger import Logger
from countess.core.parameters import (
    ArrayParam,
    BaseParam,
    ChoiceParam,
    FileArrayParam,
    FileParam,
    FileSaveParam,
    MultiParam,
    StringParam,
)

PRERUN_ROW_LIMIT = 100


def get_plugin_classes():
    plugin_classes = set()
    for ep in importlib.metadata.entry_points(group="countess_plugins"):
        try:
            plugin_class = ep.load()
        except (ModuleNotFoundError, ImportError, NotImplementedError) as exc:
            logging.warning("%s could not be loaded: %s", ep, exc)
            continue

        if issubclass(plugin_class, BasePlugin):
            plugin_classes.add(plugin_class)
        else:
            # XXX how to warn about this?
            logging.warning("%s is not a valid CountESS plugin", plugin_class)
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
    description: str = ""
    link: Optional[str] = None

    parameters: MutableMapping[str, BaseParam] = {}

    @property
    def version(self) -> str:
        return sys.modules[self.__module__].VERSION

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

    def update(self):
        """Notify the plugin that one or more of its parameters have been changed.
        If this change affects other parameters, it can return True to indicate that
        all parameters should be re-displayed"""
        return False

    def run(
        self,
        data: Any,
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

    def set_parameter(self, key: str, value: bool | int | float | str, base_dir: str = "."):
        param = self.parameters
        for k in key.split("."):
            # XXX types are a mess here
            param = param[k]  # type: ignore
        if isinstance(param, (FileParam, FileSaveParam)):
            assert isinstance(value, str)
            param.value = os.path.join(base_dir, value)
        else:
            param.value = value  # type: ignore

    def get_parameters(self, base_dir="."):
        for key, parameter in self.parameters.items():
            yield from parameter.get_parameters(key, base_dir)

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


def crop_dataframe(df: pd.DataFrame, row_limit: Optional[int]):
    if row_limit is None:
        return df
    return df[0:row_limit]


class PandasBasePlugin(BasePlugin):
    """Base class for plugins which accept and return pandas DataFrames"""

    def run(
        self,
        data: Any,
        logger: Logger,
        row_limit: Optional[int] = None,
    ):
        raise NotImplementedError(f"{self.__class__}.run()")


class PandasTransformPlugin(PandasBasePlugin):
    input_columns: list[str] = []

    def prepare_df(self, df: pd.DataFrame, logger: Logger):
        assert isinstance(df, pd.DataFrame)
        self.input_columns = list(df.columns)

        for p in self.parameters.values():
            p.set_column_choices(self.input_columns)

    # XXX prepare and run handle multiple inputs differntly?

    def prepare(
        self,
        data: pd.DataFrame | Mapping[str, pd.DataFrame],
        logger: Logger,
    ) -> bool:
        if isinstance(data, Mapping):
            data = pd.concat(data.values())

        return self.prepare_df(data, logger)

    def run_df(self, df: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        raise NotImplementedError(f"Implement {self.__class__.__name__}.run_df()")

    def run(
        self,
        data: pd.DataFrame | Mapping[str, pd.DataFrame],
        logger: Logger,
        row_limit: Optional[int] = None,
    ) -> pd.DataFrame:
        assert isinstance(logger, Logger)
        assert row_limit is None or isinstance(row_limit, int)

        logger.progress("Starting", 0)
        if isinstance(data, Mapping):
            dfs = []
            for obj in data.values():
                assert isinstance(obj, pd.DataFrame)
                df = self.run_df(obj, logger)
                assert isinstance(df, pd.DataFrame)
                dfs.append(crop_dataframe(df, row_limit))
            logger.progress("Finished", 100)
            return pd.concat(dfs)
        else:
            assert isinstance(data, pd.DataFrame)
            df = self.run_df(data, logger)
            assert isinstance(df, pd.DataFrame)
            logger.progress("Finished", 100)
            return crop_dataframe(df, row_limit)


# XXX Potentially there's a PandasBasePlugin which can use a technique much
# like tqdm does in tqdm/std.py to monkeypatch pandas.apply and friends and
# provide progress feedback.


class PandasInputPlugin(FileInputMixin, PandasBasePlugin):
    """A specialization of the PandasBasePlugin to allow it to follow nothing,
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

    def combine_dfs(self, dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """First stage: collect all the files together in whatever
        way is appropriate.  Override this to do it differently
        or do more work on the dataframes (eg: counting, renaming, etc)"""
        return pd.concat(dfs)

    def load_files(
        self,
        logger: Logger,
        row_limit: Optional[int] = None,
    ) -> pd.DataFrame:
        assert isinstance(self.parameters["files"], ArrayParam)
        fps = self.parameters["files"].params
        if not fps:
            return pd.DataFrame()

        if len(fps) == 1:
            file_param = self.parameters["files"].params[0]
            assert isinstance(file_param, MultiParam)
            ddf = self.read_file_to_dataframe(file_param, logger, row_limit)
            ddf = self.combine_dfs([ddf])
        else:
            num_files = len(fps)
            # Input plugins are likely I/O bound so if there's more than one
            # file, instead of using the Pandas progress callback mechanism
            # this uses a simple count of files read."""
            per_file_row_limit = int(row_limit / len(fps) + 1) if row_limit else None
            logger.progress("Loading", 0)
            dfs = []
            for num, fp in enumerate(fps):
                assert isinstance(fp, MultiParam)
                df = self.read_file_to_dataframe(fp, logger, per_file_row_limit)
                dfs.append(df)
                logger.progress("Loading", 100 * (num + 1) // (num_files + 1))
            logger.progress("Combining", 100 * num_files // num_files + 1)
            ddf = self.combine_dfs(dfs)
            logger.progress("Done", 100)

        return ddf

    def read_file_to_dataframe(
        self, file_params: MultiParam, logger: Logger, row_limit: Optional[int] = None
    ) -> pd.DataFrame:
        raise NotImplementedError(f"Implement {self.__class__.__name__}.read_file_to_dataframe")


class PandasScoringPlugin(PandasTransformPlugin):
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

    def prepare_df(self, df, logger):
        super().prepare(df, logger)
        for pp in self.parameters["scores"]:
            for ppp in pp.counts:
                ppp.choices = self.input_columns

    def run_df(self, df: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        assert isinstance(self.parameters["scores"], ArrayParam)
        score_cols = []
        for pp in self.parameters["scores"]:
            scol = pp.score.value
            ccols = [ppp.value for ppp in pp.counts]

            if scol and all(ccols):
                df[scol] = self.score([df[col] for col in ccols])
                score_cols.append(scol)

        df = df.replace([np.inf, -np.inf], np.nan)
        assert isinstance(df, pd.DataFrame)
        df.dropna(how="all", subset=score_cols, inplace=True)
        return df

    def score(self, columns: List[pd.Series]) -> pd.Series:
        raise NotImplementedError("Subclass PandasScoringPlugin and provide a score() method")


class PandasReindexPlugin(PandasTransformPlugin):
    # XXX not really useful?

    translate_type = str

    def translate(self, value):
        raise NotImplementedError(f"Implement {self.__class__.__name__}.translate")

    def translate_row(self, row):
        return self.translate(row.name)

    def run_df(self, df: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        df["__reindex"] = df.apply(
            self.translate_row, axis=1, meta=pd.Series(self.translate_type())
        )
        return df.groupby("__reindex").sum()
