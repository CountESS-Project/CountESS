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
from typing import Dict, Iterable, List, Optional, Union

import numpy as np
import pandas as pd

from countess.core.logger import Logger
from countess.core.parameters import (
    ArrayParam,
    BaseParam,
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
    show_preview: bool = True

    @property
    def version(self) -> str:
        raise NotImplementedError(f"{self.__class__}.version")

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
            if k == "_label" and hasattr(param, "label"):
                param.label = value
                return

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


class ProcessPlugin(BasePlugin):
    """A plugin which accepts data from one or more sources"""

    def prepare(self, sources: List[str], row_limit: Optional[int] = None):
        pass

    def process(self, data, source: str, logger: Logger) -> Iterable[pd.DataFrame]:
        """Called with each `data` input from `source`, calls
        `callback` to send messages to the next plugin"""
        raise NotImplementedError(f"{self.__class__}.process")

    def finished(self, source: str, logger: Logger) -> Iterable[pd.DataFrame]:
        """Called when a `source` is finished and not able to
        send any more messages.  Can be ignored by most things."""
        # override this if you need to do anything
        return []

    def finalize(self, logger: Logger) -> Iterable[pd.DataFrame]:
        """Called when all sources are finished.  Can be
        ignored by most things.  This should reset the
        plugin to be ready for another use."""
        # override this if you need to do anything
        return []


class SimplePlugin(ProcessPlugin):
    def process(self, data, source: str, logger: Logger) -> Iterable[pd.DataFrame]:
        """Called with each `data` input from `source`, calls
        `callback` to send messages to the next plugin"""
        raise NotImplementedError(f"{self.__class__}.process")


class FileInputPlugin(BasePlugin):
    """Mixin class to indicate that this plugin can read files from local
    storage."""

    file_number = 0
    name = ""

    # used by the GUI file dialog
    file_types: List[tuple[str, Union[str, list[str]]]] = [("Any", "*")]
    file_params: MutableMapping[str, BaseParam] = {}

    def num_files(self) -> int:
        """return the number of 'files' which are to be loaded.  The pipeline
        will call code equivalent to
        `[ p.load_file(n, logger, row_limit) for n in range(0, p.num_files() ]`
        although potentially using threads, multiprocessing, etc."""
        raise NotImplementedError("FileInputMixin.num_files")

    def load_file(self, file_number: int, logger: Logger, row_limit: Optional[int] = None) -> Iterable:
        """Called potentially from multiple processes, see FileInputMixin.num_files()"""
        raise NotImplementedError("FileInputMixin.load_file")


class PandasProcessPlugin(ProcessPlugin):
    DATAFRAME_BUFFER_SIZE = 100000

    def process(self, data: pd.DataFrame, source: str, logger: Logger) -> Iterable[pd.DataFrame]:
        raise NotImplementedError(f"{self.__class__}.process")


class PandasSimplePlugin(SimplePlugin):
    """Base class for plugins which accept and return pandas DataFrames.
    Subclassing this hides all the distracting aspects of the pipeline
    from the plugin implementor, who only needs to override process_dataframe"""

    input_columns: Dict[str, np.dtype] = {}

    def prepare(self, sources: list[str], row_limit: Optional[int] = None):
        self.input_columns = {}

    def process(self, data: pd.DataFrame, source: str, logger: Logger) -> Iterable[pd.DataFrame]:
        """Just deal with each dataframe as it comes.  PandasSimplePlugins don't care about `source`."""
        assert isinstance(data, pd.DataFrame)

        self.input_columns.update(get_all_columns(data))

        try:
            result = self.process_dataframe(data, logger)
            if result is not None:
                assert isinstance(result, pd.DataFrame)
                if len(result) > 0:
                    yield result

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.exception(exc)

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> Optional[pd.DataFrame]:
        """Override this to process a single dataframe"""
        raise NotImplementedError(f"{self.__class__}.process_dataframe()")

    def finalize(self, logger: Logger) -> Iterable[pd.DataFrame]:
        yield from super().finalize(logger)
        for p in self.parameters.values():
            p.set_column_choices(self.input_columns.keys())


# class MapReduceFinalizePlugin(BasePlugin):
#    def map(self, data, logger: Logger) -> Iterable:
#        return []
#
#    def reduce(self, data: Iterable):
#        pass
#
#    def finalize(self, data: Iterable):
#        pass
#
#
# class PandasMapReduceFinalizePlugin(MapReduceFinalizePlugin):
#    def map(self, data, logger: Logger) -> Iterable[pd.DataFrame]:
#        return []
#
#    def reduce(self, data: Iterable[pd.DataFrame]) -> pd.DataFrame:
#        return pd.DataFrame()
#
#    def finalize(self, data: pd.DataFrame):
#        pass


class PandasProductPlugin(PandasProcessPlugin):
    """Some plugins need to have all the data from two sources presented to them,
    which is tricky in a pipelined environment.  This superclass handles the two
    input sources and calls .process_dataframes with pairs of dataframes.
    It is currently only used by JoinPlugin"""

    source1 = None
    source2 = None
    mem1: Optional[List] = None
    mem2: Optional[List] = None

    def prepare(self, sources: list[str], row_limit: Optional[int]=None):
        if len(sources) != 2:
            raise ValueError(f"{self.__class__} required exactly two inputs")
        self.source1, self.source2 = sources

        super().prepare(sources, row_limit)

        self.mem1 = []
        self.mem2 = []

    def process(self, data: pd.DataFrame, source: str, logger: Logger) -> Iterable[pd.DataFrame]:
        if source == self.source1:
            if self.mem1 is not None:
                self.mem1.append(data)
            assert self.mem2 is not None
            for val2 in self.mem2:
                df = self.process_dataframes(data, val2, logger)
                if len(df):
                    yield df

        elif source == self.source2:
            if self.mem2 is not None:
                self.mem2.append(data)
            assert self.mem1 is not None
            for val1 in self.mem1:
                df = self.process_dataframes(val1, data, logger)
                if len(df):
                    yield df

        else:
            raise ValueError(f"Unknown source {source}")

    def finished(self, source: str, logger: Logger) -> Iterable:
        if source == self.source1:
            # source1 is finished, mem2 is no longer needed
            self.source1 = None
            self.mem2 = None
        elif source == self.source2:
            # source2 is finished, mem1 is no longer needed
            self.source2 = None
            self.mem1 = None
        else:
            raise ValueError(f"Unknown source {source}")
        return []

    def finalize(self, logger: Logger) -> Iterable:
        # free up any memory taken up by memoization
        self.mem1 = None
        self.mem2 = None
        return []

    def process_dataframes(self, dataframe1: pd.DataFrame, dataframe2: pd.DataFrame, logger: Logger) -> pd.DataFrame:
        raise NotImplementedError(f"{self.__class__}.process_dataframes")


# XXX this might be excessively DRY but we'll see.
#
#                              --> PandasTransformAToXMixin --
#                             /       dataframe_to_series     \
# PandasTransformBasePlugin --                                 --> PandasTransformAToBMixin
#                             \       series_to_dataframe     /
#                              --> PandasTransformXToBMixin --
#
# A is one of Single (takes a value), Row (takes a pd.Series) or Dict (takes a dict)
# B is one of Single (returns a value), Tuple (returns a tuple) or Dict (returns a dict)
#
# Looked at from the point of view of the data it looks like:
#
#     P_T_BasePlugin       P_T_AToXMixin            P_T_AToBPlugin     P_T_XToBPlugin        PTBP
#
#                                               /-> process_row ---\
#     process_dataframe -> dataframe_to_series ---> process_value ---> series_to_dataframe -> merge
#                                               \-> process_dict --/
#
# Which probably seems overcomplicated but it also seems to work.
# Most plugins just need to pick a PandasTransform class and run with it.  A lot of the time it's
# going to be PandasTransformSingleToSinglePlugin.


class PandasTransformBasePlugin(PandasSimplePlugin):
    """Base classes for the nine (!!) PandasTransformXToXPlugin superclasses."""

    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        raise NotImplementedError(f"{self.__class__}.series_to_dataframe()")

    def dataframe_to_series(self, dataframe: pd.DataFrame, logger: Logger) -> pd.Series:
        raise NotImplementedError(f"{self.__class__}.dataframe_to_series()")

    def process_dataframe(self, dataframe: pd.DataFrame, logger: Logger) -> Optional[pd.DataFrame]:
        try:
            series = self.dataframe_to_series(dataframe, logger)
            df2 = self.series_to_dataframe(series)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.exception(exc)
            return None
        df3 = dataframe.merge(df2, left_index=True, right_index=True)
        return df3


# XXX instead of just asserting the existence of the parameters should we
# actually create them in these mixins?


class PandasTransformSingleToXMixin:  # type: ignore [attr-defined]
    """Transformer which takes a single column, the name of which is specified
    in a ColumnChoiceParam called "column" """

    def process_value(self, value, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_value()")

    def dataframe_to_series(self, dataframe: pd.DataFrame, logger: Logger) -> pd.Series:
        column_name = self.parameters["column"].value  # type: ignore [attr-defined]
        if column_name in dataframe:
            return dataframe[column_name].apply(self.process_value, logger=logger)
        elif column_name == dataframe.index.name:
            return dataframe.index.to_series().apply(self.process_value, logger=logger)
        elif column_name in dataframe.index.names:
            return dataframe.index.to_frame()[column_name].apply(self.process_value, logger=logger)
        else:
            null_values = [self.process_value(None, logger)] * len(dataframe)
            s = pd.Series(null_values, index=dataframe.index)
            return s


class PandasTransformRowToXMixin:
    """Transformer which takes an entire row. Less efficient than processing
    just a single value, but a bit more flexible."""

    def process_row(self, row: pd.Series, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_row()")

    def dataframe_to_series(self, dataframe: pd.DataFrame, logger: Logger) -> pd.Series:
        return dataframe.apply(self.process_row, axis=1, args=(logger,))


class PandasTransformDictToXMixin:
    """Transformer which takes a row as a dictionary"""

    def dataframe_to_series(self, dataframe: pd.DataFrame, logger: Logger) -> pd.Series:
        # XXX there is a bug in Pandas 2.1.x which prevents
        # args and kwargs getting passed through when raw=True
        # this seems to be fixed in Pandas 2.2.0.dev so
        # hopefully this lambda can be removed some day.
        # https://github.com/pandas-dev/pandas/issues/55009
        return dataframe.apply(
            lambda x: self.process_raw(x, list(dataframe.columns), logger),
            axis=1,
            raw=True,
        )

    def process_dict(self, data, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_dict()")

    def process_raw(self, data, columns, logger: Logger) -> pd.Series:
        return self.process_dict(dict(zip(columns, data)), logger=logger)


class PandasTransformXToSingleMixin:
    """Transformer which returns a single value, putting into the column specified
    by the StringParam called "output" """

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        assert isinstance(self.parameters["output"], StringParam)

    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        return series.to_frame(name=self.parameters["output"].value)  # type: ignore [attr-defined]


class PandasTransformXToTupleMixin:
    """Transformer which returns a tuple of values, putting them into columns
    specifed by the StringParams "name" in the ArrayParam "output" """

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        assert hasattr(self, "parameters")
        assert isinstance(self.parameters["output"], ArrayParam)
        assert all(isinstance(pp["name"], StringParam) for pp in self.parameters["output"])

    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        assert hasattr(self, "parameters")
        assert isinstance(self.parameters["output"], ArrayParam)
        assert all(isinstance(pp["name"], StringParam) for pp in self.parameters["output"])
        column_names = [
            pp.name.value or "Column %d" % n for n, pp in enumerate(self.parameters["output"], 1)
        ]  # type: ignore [attr-defined]

        series.dropna(inplace=True)
        data = series.tolist()
        if len(data):
            max_cols = max(len(d) for d in data)
            column_names = column_names[:max_cols]
            df = pd.DataFrame(data, columns=column_names, index=series.index)
            return df
        else:
            return pd.DataFrame()


class PandasTransformXToDictMixin:
    """Transformer which returns a dictionary of values, putting them into
    columns named after the dictionary keys."""

    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        series.dropna(inplace=True)
        return pd.DataFrame(series.tolist(), index=series.index)


# Nine combinations of the six mixins!


class PandasTransformSingleToSinglePlugin(
    PandasTransformXToSingleMixin, PandasTransformSingleToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a single column and returns a single value"""

    def process_value(self, value, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_value()")


class PandasTransformSingleToTuplePlugin(
    PandasTransformXToTupleMixin, PandasTransformSingleToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a single column and returns a tuple of values"""

    def process_value(self, value, logger: Logger) -> Optional[Iterable]:
        raise NotImplementedError(f"{self.__class__}.process_value()")


class PandasTransformSingleToDictPlugin(
    PandasTransformXToDictMixin, PandasTransformSingleToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a single column and returns a dictionary of values"""

    def process_value(self, value, logger: Logger) -> Optional[Dict]:
        raise NotImplementedError(f"{self.__class__}.process_value()")


class PandasTransformRowToSinglePlugin(
    PandasTransformXToSingleMixin, PandasTransformRowToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a whole row and returns a single value"""

    def process_row(self, row: pd.Series, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_row()")


class PandasTransformRowToTuplePlugin(
    PandasTransformXToTupleMixin, PandasTransformRowToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a whole row and returns a tuple of values"""

    def process_row(self, row: pd.Series, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_row()")


class PandasTransformRowToDictPlugin(
    PandasTransformXToDictMixin, PandasTransformRowToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a whole row and returns a dictionary of values"""

    def process_row(self, row: pd.Series, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_row()")


class PandasTransformDictToSinglePlugin(
    PandasTransformXToSingleMixin, PandasTransformDictToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a whole row and returns a single value"""

    def process_dict(self, data, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_dict()")


class PandasTransformDictToTuplePlugin(
    PandasTransformXToTupleMixin, PandasTransformDictToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a whole row and returns a tuple of values"""

    def process_dict(self, data, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_dict()")


class PandasTransformDictToDictPlugin(
    PandasTransformXToDictMixin, PandasTransformDictToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a whole row and returns a dictionary of values"""

    def process_dict(self, data, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.process_dict()")


class PandasInputPlugin(FileInputPlugin):
    """A specialization of the PandasBasePlugin to allow it to follow nothing,
    eg: come first."""

    def num_files(self):
        raise NotImplementedError(f"{self.__class__}.num_files()")

    def load_file(self, file_number: int, logger: Logger, row_limit: Optional[int] = None) -> Iterable[pd.DataFrame]:
        raise NotImplementedError(f"{self.__class__}.load_file()")


class PandasInputFilesPlugin(PandasInputPlugin):
    def __init__(self, *a, **k):
        # Add in filenames
        super().__init__(*a, **k)
        file_params = {"filename": FileParam("Filename", file_types=self.file_types)}
        file_params.update(self.file_params)

        self.parameters = dict(
            [("files", FileArrayParam("Files", MultiParam("File", file_params)))] + list(self.parameters.items())
        )

    def num_files(self):
        return len(self.parameters["files"].params)

    def load_file(self, file_number: int, logger: Logger, row_limit: Optional[int] = None) -> Iterable[pd.DataFrame]:
        assert isinstance(self.parameters["files"], ArrayParam)
        file_params = self.parameters["files"][file_number]
        yield self.read_file_to_dataframe(file_params, logger, row_limit)

    def read_file_to_dataframe(self, file_params, logger, row_limit=None) -> pd.DataFrame:
        raise NotImplementedError(f"{self.__class__}.read_file_to_dataframe")


class PandasOutputPlugin(PandasProcessPlugin):
    def process_inputs(self, inputs: Mapping[str, Iterable[pd.DataFrame]], logger: Logger, row_limit: Optional[int]):
        iterators = set(iter(input) for input in inputs.values())

        while iterators:
            for it in list(iterators):
                try:
                    df_in = next(it)
                    assert isinstance(df_in, pd.DataFrame)
                    self.output_dataframe(df_in, logger)
                except StopIteration:
                    iterators.remove(it)

    def output_dataframe(self, dataframe: pd.DataFrame, logger: Logger):
        raise NotImplementedError(f"{self.__class__}.output_dataframe")
