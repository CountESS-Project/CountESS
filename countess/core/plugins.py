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

import glob
import hashlib
import importlib
import importlib.metadata
import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Type, Union

import numpy as np
import pandas as pd

from countess.core.parameters import (
    ArrayParam,
    BaseParam,
    FileArrayParam,
    FileParam,
    HasSubParametersMixin,
    MultiParam,
    StringParam,
)
from countess.utils.pandas import get_all_columns
from countess.utils.parallel import multiprocess_map

PRERUN_ROW_LIMIT: int = 100000

logger = logging.getLogger(__name__)


def get_plugin_classes() -> Iterable[Type["BasePlugin"]]:
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


def load_plugin(module_name: str, class_name: str, plugin_name: Optional[str] = None) -> "BasePlugin":
    module = importlib.import_module(module_name)
    plugin_class = getattr(module, class_name)
    assert issubclass(plugin_class, BasePlugin)
    return plugin_class(plugin_name)


class BasePlugin(HasSubParametersMixin):
    """Base class for all plugins.  Plugins exist as entrypoints, but also
    PluginManager checks that plugins subclass this class before accepting them
    as plugins."""

    name: str = ""
    description: str = ""
    additional: str = ""
    link: Optional[str] = None
    num_inputs: int = 1
    num_outputs: int = 1

    show_preview: bool = True

    @property
    def version(self) -> str:
        raise NotImplementedError(f"{self.__class__}.version")

    def __init__(self, plugin_name: Optional[str] = None):
        super().__init__()

        if plugin_name is not None:
            self.name = plugin_name

    def get_parameter_hash(self):
        """Build a hash of all configuration parameters"""
        h = hashlib.sha256()
        for k, v in self.params.items():
            h.update((k + "\0" + v.get_hash_value()).encode("utf-8"))
        return h

    def hash(self):
        """Returns a hex digest of the hash of all configuration parameters"""
        return self.get_parameter_hash().hexdigest()


class ProcessPlugin(BasePlugin):
    """A plugin which accepts data from one or more sources.  Each source is
    executed in its own thread and the results are collated through a thread-safe
    queue."""

    def prepare(self, sources: List[str], row_limit: Optional[int] = None):
        pass

    def preprocess(self, data, source: str) -> None:
        """Called with each `data` input from `source` before `process` is called
        for that data, to set up config etc.  Can't return anything."""

    def process(self, data, source: str) -> Iterable[pd.DataFrame]:
        """Called with each `data` input from `source`, yields results"""
        raise NotImplementedError(f"{self.__class__}.process")

    def finished(self, source: str) -> Iterable[pd.DataFrame]:
        """Called when a `source` is finished and not able to
        send any more messages.  Can be ignored by most things."""
        # override this if you need to do anything
        return []

    def finalize(self) -> Iterable[pd.DataFrame]:
        """Called when all sources are finished.  Can be
        ignored by most things.  This should reset the
        plugin to be ready for another use."""
        # override this if you need to do anything
        return []


class SimplePlugin(ProcessPlugin):
    def process(self, data, source: str) -> Iterable[pd.DataFrame]:
        """Called with each `data` input from `source`, calls
        `callback` to send messages to the next plugin"""
        raise NotImplementedError(f"{self.__class__}.process")


class FileInputPluginFilesMultiParam(MultiParam):
    """MultiParam which only asks for one thing, a filename.  Using a MultiParam wrapper
    because that makes it easier to extend the FileInputPlugin."""

    filename = FileParam("Filename")


class FileInputPlugin(BasePlugin):
    files = FileArrayParam("Files", FileInputPluginFilesMultiParam("File"))
    file_types: Sequence[tuple[str, Union[str, list[str]]]] = [("Any", "*")]
    row_limit: Optional[int] = None
    num_inputs = 0

    def __init__(self, *a, **k):
        # Add in filenames
        super().__init__(*a, **k)
        self.files.file_types = self.file_types

    def filenames_and_params(self):
        for file_param in self.files:
            for filename in glob.iglob(file_param.filename.value):
                yield filename, file_param

    def read_file_to_dataframe(self, filename: str, file_param: BaseParam, row_limit=None) -> Any:
        """May be called from multiple processes at once.  Note that the 'filename' parameter
        overrides the 'file_param.filename.value' as the latter may be a glob."""
        raise NotImplementedError("{self.class}.read_file_to_dataframe")

    def load_file(self, filename_and_param: Tuple[str, BaseParam], row_limit: Optional[int] = None) -> Iterable:
        filename, file_param = filename_and_param
        logger.debug("FileInputPlugin.load_file load file %s", filename)
        df = self.read_file_to_dataframe(filename, file_param, row_limit)
        logger.debug("FileInputPlugin.load_file read %d rows", len(df))
        yield df

    def prepare(self, sources: List[str], row_limit: Optional[int] = None):
        assert len(sources) == 0
        self.row_limit = row_limit

    def finalize(self) -> Iterable:
        logger.debug("FileInputPlugin.finalize starting %s", self.name)
        filenames_and_params = list(self.filenames_and_params())
        num_files = len(filenames_and_params)
        logger.info("%s: 0%%", self.name)
        if num_files > 1:
            row_limit_per_file = self.row_limit // num_files if self.row_limit else None
            for n, r in enumerate(multiprocess_map(self.load_file, filenames_and_params, row_limit_per_file)):
                if n < num_files:
                    logger.info("%s: %d%%", self.name, int(100 * n / num_files))
                yield r
        elif num_files == 1:
            yield from self.load_file(filenames_and_params[0], self.row_limit)
        logger.info("%s: 100%%", self.name)
        logger.debug("FileInputPlugin.finalize finished %s", self.name)


class PandasProcessPlugin(ProcessPlugin):
    DATAFRAME_BUFFER_SIZE = 100000

    input_columns: Dict[str, np.dtype] = {}

    def prepare(self, sources: list[str], row_limit: Optional[int] = None):
        self.input_columns = {}

    def preprocess(self, data: pd.DataFrame, source: str) -> None:
        self.input_columns.update(get_all_columns(data))

    def process(self, data: pd.DataFrame, source: str) -> Iterable[pd.DataFrame]:
        raise NotImplementedError(f"{self.__class__}.process")

    def finalize(self) -> Iterable[pd.DataFrame]:
        yield from super().finalize()
        self.set_column_choices(self.input_columns.keys())


class PandasConcatProcessPlugin(PandasProcessPlugin):
    # Like PandsaProcessPlugin but collect all the inputs together before trying to do anything
    # with them.

    def __init__(self, *a, **k) -> None:
        super().__init__(*a, **k)
        self.dataframes: list[pd.DataFrame] = []

    def prepare(self, sources: list[str], row_limit: Optional[int] = None):
        super().prepare(sources, row_limit)
        self.dataframes = []

    def process(self, data: pd.DataFrame, source: str) -> Iterable:
        self.dataframes.append(data)
        return []

    def finalize(self) -> Iterable[pd.DataFrame]:
        data_in = pd.concat(self.dataframes)
        data_out = self.process_dataframe(data_in)
        if data_out is not None:
            yield data_out
        yield from super().finalize()

    def process_dataframe(self, dataframe: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Override this to process a single dataframe"""
        raise NotImplementedError(f"{self.__class__}.process_dataframe()")


class PandasSimplePlugin(PandasProcessPlugin):
    """Base class for plugins which accept and return pandas DataFrames.
    Subclassing this hides all the distracting aspects of the pipeline
    from the plugin implementor, who only needs to override process_dataframe"""

    def process(self, data: pd.DataFrame, source: str) -> Iterable[pd.DataFrame]:
        """Just deal with each dataframe as it comes.  PandasSimplePlugins don't care about `source`."""
        assert isinstance(data, pd.DataFrame)

        try:
            result = self.process_dataframe(data)
            if result is not None:
                assert isinstance(result, pd.DataFrame)
                if len(result) > 0:
                    yield result

        except Exception as exc:  # pylint: disable=broad-exception-caught
            logger.warning("Exception", exc_info=exc)  # pragma: no cover

    def process_dataframe(self, dataframe: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Override this to process a single dataframe"""
        raise NotImplementedError(f"{self.__class__}.process_dataframe()")


# class MapReduceFinalizePlugin(BasePlugin):
#    def map(self, data) -> Iterable:
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
#    def map(self, data) -> Iterable[pd.DataFrame]:
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

    # XXX with process and finished being called from multiple threads now,
    # there need to be some locks put around things

    source1 = None
    source2 = None
    mem1: Optional[List] = None
    mem2: Optional[List] = None
    num_inputs = 2

    def prepare(self, sources: list[str], row_limit: Optional[int] = None):
        if len(sources) != 2:
            raise ValueError(f"{self.__class__} required exactly two inputs")
        self.source1, self.source2 = sources

        super().prepare(sources, row_limit)

        self.mem1 = []
        self.mem2 = []

    def process(self, data: pd.DataFrame, source: str) -> Iterable[pd.DataFrame]:
        if source == self.source1:
            if self.mem1 is not None:
                self.mem1.append(data)
            assert self.mem2 is not None
            for val2 in self.mem2:
                df = self.process_dataframes(data, val2)
                if len(df):
                    yield df

        elif source == self.source2:
            if self.mem2 is not None:
                self.mem2.append(data)
            assert self.mem1 is not None
            for val1 in self.mem1:
                df = self.process_dataframes(val1, data)
                if len(df):
                    yield df

        else:
            raise ValueError(f"Unknown source {source}")

    def finished(self, source: str) -> Iterable:
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

    def finalize(self) -> Iterable:
        # free up any memory taken up by memoization
        self.mem1 = None
        self.mem2 = None
        return []

    def process_dataframes(self, dataframe1: pd.DataFrame, dataframe2: pd.DataFrame) -> pd.DataFrame:
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

    def dataframe_to_series(self, dataframe: pd.DataFrame) -> pd.Series:
        raise NotImplementedError(f"{self.__class__}.dataframe_to_series()")

    def process_dataframe(self, dataframe: pd.DataFrame) -> Optional[pd.DataFrame]:
        dataframe_merged = None
        try:
            # 1. A dataframe with duplicates in its index can't be merged back correctly
            # in Step 4, so we add in an extra RangeIndex to guarantee uniqueness,
            # and remove it again afterwards in Step 5.
            if dataframe.index.has_duplicates:
                dataframe.set_index(pd.RangeIndex(0, len(dataframe), name="__tmpidx"), append=True, inplace=True)

            # 2. the dataframe is transformed into
            # a series of results by PandasTransform...ToXMixin.dataframe_to_series,
            # which is expected to take each row of the dataframe, do something with
            # it and return a series of objects (values, tuples or dicts).
            series = self.dataframe_to_series(dataframe)

            # 3. the series is expanded back out into rows by
            # PandasTransformXTo...Mixin.series_to_dataframe()
            dataframe_out = self.series_to_dataframe(series)

            # 4. The expanded result is merged back into the original dataframe
            dataframe_merged = dataframe.merge(dataframe_out, left_index=True, right_index=True)

            # 5. Remove extra RangeIndex if we added it in step 1.
            if "__tmpidx" in dataframe_merged.index.names:
                dataframe_merged.reset_index("__tmpidx", drop=True, inplace=True)

        except Exception as exc:  # pylint: disable=broad-exception-caught  # pragma: no cover
            logger.warning("Exception", exc_info=exc)  # pragma: no cover

        return dataframe_merged


# XXX instead of just asserting the existence of the parameters should we
# actually create them in these mixins?


class PandasTransformSingleToXMixin:  # type: ignore [attr-defined]
    """Transformer which takes a single column, the name of which is specified
    in a ColumnChoiceParam called "column" """

    def process_value(self, value):
        raise NotImplementedError(f"{self.__class__}.process_value()")

    def dataframe_to_series(self, dataframe: pd.DataFrame) -> pd.Series:
        column_name = self.params["column"].value  # type: ignore [attr-defined]
        if column_name in dataframe:
            return dataframe[column_name].apply(self.process_value)
        elif column_name == dataframe.index.name:
            return dataframe.index.to_series().apply(self.process_value)
        elif column_name in dataframe.index.names:
            return dataframe.index.to_frame()[column_name].apply(self.process_value)
        else:
            null_values = [self.process_value(None)] * len(dataframe)
            s = pd.Series(null_values, index=dataframe.index)
            return s


class PandasTransformRowToXMixin:
    """Transformer which takes an entire row. Less efficient than processing
    just a single value, but a bit more flexible."""

    def process_row(self, row: pd.Series):
        raise NotImplementedError(f"{self.__class__}.process_row()")

    def dataframe_to_series(self, dataframe: pd.DataFrame) -> pd.Series:
        return dataframe.apply(self.process_row, axis=1)


class PandasTransformDictToXMixin:
    """Transformer which takes a row as a dictionary"""

    def dataframe_to_series(self, dataframe: pd.DataFrame) -> pd.Series:
        if dataframe.index.names == [None]:
            # unnamed index,
            # XXX there is a bug in Pandas 2.1.x which prevents
            # args and kwargs getting passed through when raw=True
            # this seems to be fixed in Pandas 2.2.0.dev so
            # hopefully this lambda can be removed some day.
            # https://github.com/pandas-dev/pandas/issues/55009
            return dataframe.apply(
                lambda x: self.process_raw(x, list(dataframe.columns)),
                axis=1,
                raw=True,
            )

        columns = list(dataframe.index.names) + list(dataframe.columns)

        if len(dataframe.index.names) == 1:
            # single index
            values = (
                self.process_raw(list(index_and_data_tuple), columns)
                for index_and_data_tuple in dataframe.itertuples(name=None)
            )
        else:
            # multiindex
            values = (
                self.process_raw(list(index_tuple) + list(data_tuple), columns)
                for index_tuple, *data_tuple in dataframe.itertuples(name=None)
            )

        return pd.Series(values, index=dataframe.index)

    def process_dict(self, data):
        raise NotImplementedError(f"{self.__class__}.process_dict()")

    def process_raw(self, data, columns) -> pd.Series:
        return self.process_dict(dict(zip(columns, data)))


class PandasTransformXToSingleMixin:
    """Transformer which returns a single value, putting into the column specified
    by the StringParam called "output" """

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        assert isinstance(self.params["output"], StringParam)

    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        return series.to_frame(name=self.params["output"].value)  # type: ignore [attr-defined]


class PandasTransformXToTupleMixin:
    """Transformer which returns a tuple of values, putting them into columns
    specifed by the StringParams "name" in the ArrayParam "output" """

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        assert hasattr(self, "params")
        assert isinstance(self.params["output"], ArrayParam)
        assert all(isinstance(pp["name"], StringParam) for pp in self.params["output"])

    def series_to_dataframe(self, series: pd.Series) -> pd.DataFrame:
        assert hasattr(self, "params")
        assert isinstance(self.params["output"], ArrayParam)
        assert all(isinstance(pp["name"], StringParam) for pp in self.params["output"])
        column_names = [
            pp.name.value or "Column %d" % n for n, pp in enumerate(self.params["output"], 1)
        ]  # type: ignore [attr-defined]

        series.dropna(inplace=True)
        data = series.tolist()
        max_cols = max(len(d) for d in data) if len(data) else 0
        column_names = column_names[:max_cols]
        return pd.DataFrame(data, columns=column_names, index=series.index)


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

    def process_value(self, value):
        raise NotImplementedError(f"{self.__class__}.process_value()")


class PandasTransformSingleToTuplePlugin(
    PandasTransformXToTupleMixin, PandasTransformSingleToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a single column and returns a tuple of values"""

    def process_value(self, value) -> Optional[Iterable]:
        raise NotImplementedError(f"{self.__class__}.process_value()")


class PandasTransformSingleToDictPlugin(
    PandasTransformXToDictMixin, PandasTransformSingleToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a single column and returns a dictionary of values"""

    def process_value(self, value) -> Optional[Dict]:
        raise NotImplementedError(f"{self.__class__}.process_value()")


class PandasTransformRowToSinglePlugin(
    PandasTransformXToSingleMixin, PandasTransformRowToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a whole row and returns a single value"""

    def process_row(self, row: pd.Series):
        raise NotImplementedError(f"{self.__class__}.process_row()")


class PandasTransformRowToTuplePlugin(
    PandasTransformXToTupleMixin, PandasTransformRowToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a whole row and returns a tuple of values"""

    def process_row(self, row: pd.Series):
        raise NotImplementedError(f"{self.__class__}.process_row()")


class PandasTransformRowToDictPlugin(
    PandasTransformXToDictMixin, PandasTransformRowToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a whole row and returns a dictionary of values"""

    def process_row(self, row: pd.Series):
        raise NotImplementedError(f"{self.__class__}.process_row()")


class PandasTransformDictToSinglePlugin(
    PandasTransformXToSingleMixin, PandasTransformDictToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a whole row and returns a single value"""

    def process_dict(self, data):
        raise NotImplementedError(f"{self.__class__}.process_dict()")


class PandasTransformDictToTuplePlugin(
    PandasTransformXToTupleMixin, PandasTransformDictToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a whole row and returns a tuple of values"""

    def process_dict(self, data):
        raise NotImplementedError(f"{self.__class__}.process_dict()")


class PandasTransformDictToDictPlugin(
    PandasTransformXToDictMixin, PandasTransformDictToXMixin, PandasTransformBasePlugin
):
    """Transformer which takes a whole row and returns a dictionary of values"""

    def process_dict(self, data):
        raise NotImplementedError(f"{self.__class__}.process_dict()")


class PandasInputFilesPlugin(FileInputPlugin):
    def read_file_to_dataframe(self, filename: str, file_param: BaseParam, row_limit=None) -> pd.DataFrame:
        """May be called from multiple processes at once.  Note that the 'filename' parameter
        overrides the 'file_param.filename.value' as the latter may be a glob."""
        raise NotImplementedError("{self.class}.read_file_to_dataframe")


class PandasInputPlugin(PandasProcessPlugin):
    num_inputs = 0

    def process(self, data: pd.DataFrame, source: str) -> Iterable[pd.DataFrame]:
        return []

    def finalize(self) -> Iterable[pd.DataFrame]:
        raise NotImplementedError("{self.class}.finalize")


class PandasOutputPlugin(PandasProcessPlugin):
    num_outputs = 0

    def process(self, data: pd.DataFrame, source: str) -> Iterable[pd.DataFrame]:
        raise NotImplementedError(f"{self.__class__}.process")
