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
from typing import Any, Dict, Iterable, Optional, Union

import numpy as np
import pandas as pd

from countess.core.logger import Logger
from countess.core.parameters import (
    ArrayParam,
    BaseParam,
    ColumnChoiceParam,
    FileArrayParam,
    FileParam,
    FileSaveParam,
    MultiParam,
    StringParam,
)
from countess.utils.pandas import get_all_columns

PRERUN_ROW_LIMIT = 100000


def get_plugin_classes():
    plugin_classes = set()
    try:
        # Python >= 3.10
        entry_points = importlib.metadata.entry_points().select(group="countess_plugins")
    except AttributeError:
        # Python < 3.10
        entry_points = importlib.metadata.entry_points()["countess_plugins"]

    for ep in entry_points:
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
    additional: str = ""
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

    def add_parameter(self, name: str, param: BaseParam):
        self.parameters[name] = param.copy()
        return self.parameters[name]

    def set_parameter(self, key: str, value: Union[bool, int, float, str], base_dir: str = "."):
        param = self.parameters
        for k in key.split("."):
            # XXX types are a mess here
            param = param[k]  # type: ignore
        if isinstance(param, (FileParam, FileSaveParam)) and value is not None:
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

    def prepare(self):
        pass

    def process_inputs(self, inputs: Mapping[str, Iterable[Any]], logger: Logger, row_limit: Optional[int]) -> Iterable[Any]:
        raise NotImplementedError(f"{self.__class__}.process_inputs()")


class FileInputMixin:
    """Mixin class to indicate that this plugin can read files from local
    storage."""

    file_number = 0
    name = ""

    # used by the GUI file dialog
    file_types = [("Any", "*")]
    file_params: MutableMapping[str, BaseParam] = {}

    def load_files(self, logger: Logger, row_limit: Optional[int] = None) -> Iterable[Any]:
        raise NotImplementedError("FileInputMixin.load_files")

    def process_inputs(self, inputs: Mapping[str, Iterable[Any]], logger: Logger, row_limit: Optional[int]) -> Iterable[Any]:
        if len(inputs) > 0:
            logger.warning(f"{self.name} doesn't take inputs")
            raise ValueError(f"{self.name} doesn't take inputs")

        return self.load_files(logger, row_limit)


class PandasBasePlugin(BasePlugin):
    def process_inputs(
        self, inputs: Mapping[str, Iterable[pd.DataFrame]], logger: Logger, row_limit: Optional[int]
    ) -> Iterable[pd.DataFrame]:
        raise NotImplementedError(f"{self.__class__}.process_inputs()")


class PandasSimplePlugin(PandasBasePlugin):
    """Base class for plugins which accept and return pandas DataFrames"""

    input_columns: Dict[str, np.dtype] = {}

    def process_inputs(
        self, inputs: Mapping[str, Iterable[pd.DataFrame]], logger: Logger, row_limit: Optional[int]
    ) -> Iterable[pd.DataFrame]:
        self.input_columns = {}
        iterators = set(iter(input) for input in inputs.values())
        while iterators:
            for it in list(iterators):
                try:
                    df_in = next(it)
                    assert isinstance(df_in, pd.DataFrame)
                    self.input_columns.update(get_all_columns(df_in))

                    df_out = self.process_dataframe(df_in, logger)
                    assert isinstance(df_out, pd.DataFrame)
                    yield df_out
                except StopIteration:
                    iterators.remove(it)

        for p in self.parameters.values():
            p.set_column_choices(self.input_columns.keys())

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        raise NotImplementedError(f"{self.__class__}.process_dataframe()")


# XXX this might be excessively DRY but we'll see.


class PandasTransformBasePlugin(PandasSimplePlugin):
    """Base classes for the six (!) PandasTransformXToXPlugin superclasses."""

    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        return NotImplementedError(f"{self.__class__}.series_to_dataframe()")

    def dataframe_to_series(self, dataframe: pd.DataFrame, logger: Logger) -> pd.Series:
        return NotImplementedError(f"{self.__class__}.dataframe_to_series()")

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        series = self.dataframe_to_series(dataframe, logger)
        df2 = self.series_to_dataframe(series)
        df3 = dataframe.merge(df2, left_index=True, right_index=True)
        return df3

    def process_value(self, value, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_value()")


# XXX instead of just asserting the existence of the parameters should we
# actually create them in these mixins?


class PandasTransformSingleToXMixin:
    """Transformer which takes a single column, the name of which is specified
    in a ColumnChoiceParam called "column" """

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        assert isinstance(self.parameters["column"], ColumnChoiceParam)

    def dataframe_to_series(self, dataframe: pd.DataFrame, logger: Logger) -> pd.Series:
        column_name = self.parameters["column"].value
        if column_name in dataframe:
            return dataframe[column_name].apply(self.process_value, logger=logger)
        else:
            null_values = [self.process_value(None, logger)] * len(dataframe)
            s = pd.Series(null_values, index=dataframe.index)
            return s


class PandasTransformRowToXMixin:
    """Transformer which takes an entire row. Less efficient than processing
    just a single value, but a bit more flexible."""

    def dataframe_to_series(self, dataframe: pd.DataFrame, logger: Logger) -> pd.Series:
        return dataframe.apply(self.process_row, axis=1, logger=logger)


class PandasTransformXToSingleMixin:
    """Transformer which returns a single value, putting into the column specified
    by the StringParam called "output" """

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        assert isinstance(self.parameters["output"], StringParam)

    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        return series.to_frame(name=self.parameters["output"].value)


class PandasTransformXToTupleMixin:
    """Transformer which returns a tuple of values, putting them into columns
    specifed by the StringParams "name" in the ArrayParam "output" """

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        assert isinstance(self.parameters["output"], ArrayParam)
        assert all(isinstance(pp["name"], StringParam) for pp in self.parameters["output"])

    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        column_names = [pp.name.value for pp in self.parameters["output"]]
        df = pd.DataFrame(series.tolist(), columns=column_names, index=series.index)
        return df


class PandasTransformXToDictMixin:
    """Transformer which returns a dictionary of values, putting them into
    columns named after the dictionary keys."""


    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        return pd.DataFrame(series.tolist(), index=series.index)


# Six combinations of the five mixins!


class PandasTransformSingleToSinglePlugin(PandasTransformXToSingleMixin, PandasTransformSingleToXMixin, PandasTransformBasePlugin):
    """Transformer which takes a single column and returns a single value"""

    def process_value(self, value, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_value()")


class PandasTransformSingleToTuplePlugin(PandasTransformXToTupleMixin, PandasTransformSingleToXMixin, PandasTransformBasePlugin):
    """Transformer which takes a single column and returns a tuple of values"""

    def process_value(self, value, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_value()")


class PandasTransformSingleToDictPlugin(PandasTransformXToDictMixin, PandasTransformSingleToXMixin, PandasTransformBasePlugin):
    """Transformer which takes a single column and returns a dictionary of values"""

    def process_value(self, value, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_value()")


class PandasTransformRowToSinglePlugin(PandasTransformXToSingleMixin, PandasTransformRowToXMixin, PandasTransformBasePlugin):
    """Transformer which takes a whole row and returns a single value"""

    def process_row(self, row: pd.Series, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_row()")


class PandasTransformRowToTuplePlugin(PandasTransformXToTupleMixin, PandasTransformRowToXMixin, PandasTransformBasePlugin):
    """Transformer which takes a whole row and returns a tuple of values"""

    def process_row(self, row: pd.Series, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_row()")


class PandasTransformRowToDictPlugin(PandasTransformXToDictMixin, PandasTransformRowToXMixin, PandasTransformBasePlugin):
    """Transformer which takes a whole row and returns a dictionary of values"""

    def process_row(self, row: pd.Series, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_row()")


class PandasInputPlugin(FileInputMixin, PandasBasePlugin):
    """A specialization of the PandasBasePlugin to allow it to follow nothing,
    eg: come first."""

    def __init__(self, *a, **k):
        # Add in filenames
        super().__init__(*a, **k)
        file_params = {"filename": FileParam("Filename", file_types=self.file_types)}
        file_params.update(self.file_params)

        self.parameters = dict(
            [("files", FileArrayParam("Files", MultiParam("File", file_params)))] + list(self.parameters.items())
        )

    def load_files(self, logger: Logger, row_limit: Optional[int] = None) -> Iterable[pd.DataFrame]:
        assert isinstance(self.parameters["files"], ArrayParam)
        fps = self.parameters["files"].params

        num_files = len(fps)
        per_file_row_limit = int(row_limit / len(fps) + 1) if row_limit else None
        logger.progress("Loading", 0)
        for num, fp in enumerate(fps):
            assert isinstance(fp, MultiParam)
            yield self.read_file_to_dataframe(fp, logger, per_file_row_limit)
            logger.progress("Loading", 100 * (num + 1) // (num_files + 1))
        logger.progress("Done", 100)

    def read_file_to_dataframe(self, file_params: MultiParam, logger: Logger, row_limit: Optional[int] = None) -> pd.DataFrame:
        raise NotImplementedError(f"Implement {self.__class__.__name__}.read_file_to_dataframe")
