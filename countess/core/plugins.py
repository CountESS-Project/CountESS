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
import multiprocessing
from typing import Any, Iterable, Mapping, Optional, Sequence, Tuple, Type, Union

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation
import pyarrow

from countess.core.parameters import BaseParam, FileArrayParam, FileParam, HasSubParametersMixin, MultiParam
from countess.utils.duckdb import duckdb_concatenate, duckdb_escape_identifier, duckdb_source_to_view

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

    def preconfigure(self) -> None:
        """Called after everything else, to set up any configuration
        which may have changed"""
        return None


class DuckdbPlugin(BasePlugin):
    """Base class for all DuckDB-based plugins"""

    # XXX expand this, or find in library somewhere
    ALLOWED_TYPES = {"INTEGER", "VARCHAR", "FLOAT", "DOUBLE", "DECIMAL"}

    def execute_multi(
        self, ddbc: DuckDBPyConnection, sources: Mapping[str, DuckDBPyRelation]
    ) -> Optional[DuckDBPyRelation]:
        raise NotImplementedError(f"{self.__class__}.execute_multi")


class DuckdbSimplePlugin(DuckdbPlugin):
    def execute_multi(
        self, ddbc: DuckDBPyConnection, sources: Mapping[str, DuckDBPyRelation]
    ) -> Optional[DuckDBPyRelation]:
        tables = list(sources.values())
        if len(sources) > 1:
            source = duckdb_source_to_view(ddbc, duckdb_concatenate(tables))
        elif len(sources) == 1:
            source = tables[0]
        else:
            source = None

        logger.debug("DuckdbSimplePlugin execute_multi %s", source.alias)

        self.set_column_choices([] if source is None else source.columns)
        
        return self.execute(ddbc, source)

    def execute(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> Optional[DuckDBPyRelation]:
        raise NotImplementedError(f"{self.__class__}.execute")


class DuckdbInputPlugin(DuckdbPlugin):
    num_inputs = 0

    def execute_multi(
        self, ddbc: DuckDBPyConnection, sources: Mapping
    ) -> Optional[DuckDBPyRelation]:
        assert len(sources) == 0
        return self.execute(ddbc, None)

    def execute(self, ddbc: DuckDBPyConnection, source: None) -> Optional[DuckDBPyRelation]:
        raise NotImplementedError(f"{self.__class__}.execute")


class DuckdbStatementPlugin(DuckdbSimplePlugin):
    def statement(self, ddbc: DuckDBPyConnection, source_table_name: str) -> str:
        raise NotImplementedError(f"{self.__class__}.statement")

    def execute(self, ddbc, source):
        source_table_name = f"r_{id(self)}"
        try:
            ddbc.register(source_table_name, source)
            return ddbc.sql(self.statement(ddbc, source_table_name))
        finally:
            ddbc.unregister(source_table_name)


class LoadFileMultiParam(MultiParam):
    """MultiParam which only asks for one thing, a filename.  Using a MultiParam wrapper
    because that makes it easier to extend the FileInputPlugin."""

    filename = FileParam("Filename")


class DuckdbLoadFilePlugin(DuckdbInputPlugin):
    files = FileArrayParam("Files", LoadFileMultiParam("File"))
    file_types: Sequence[tuple[str, Union[str, list[str]]]] = [("Any", "*")]

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.files.file_types = self.file_types

    def filenames_and_params(self):
        for file_param in self.files:
            for filename in glob.iglob(file_param.filename.value):
                yield filename, file_param

    def execute(self, ddbc: DuckDBPyConnection, source: None) -> Optional[DuckDBPyRelation]:
        filenames_and_params = list(self.filenames_and_params())

        cursor = ddbc
        return duckdb_source_to_view(ddbc, duckdb_concatenate(
            [
                self.load_file(cursor, filename, file_param, num)
                for num, (filename, file_param) in enumerate(filenames_and_params)
            ]
        ))

    def load_file(
        self, cursor: DuckDBPyConnection, filename: str, file_param: BaseParam, file_number: int
    ) -> DuckDBPyRelation:
        raise NotImplementedError(f"{self.__class__}.load_file")


class DuckdbSaveFilePlugin(DuckdbSimplePlugin):
    num_outputs = 0

    def execute(self, ddbc, source):
        raise NotImplementedError(f"{self.__class__}.execute")


class DuckdbFilterPlugin(DuckdbSimplePlugin):
    def input_columns(self) -> dict[str, str]:
        raise NotImplementedError(f"{self.__class__}.input_columns")

    def execute(self, ddbc, source):
        """Perform a query which calls `self.transform` for every row."""

        escaped_input_columns = {duckdb_escape_identifier(k): str(v).upper() for k, v in self.input_columns()}
        assert all(v in self.ALLOWED_TYPES for v in escaped_input_columns.values())

        # Make up an arbitrary unique name for our temporary function
        function_name = f"f_{id(self)}"
        function_call = function_name + "(" + ",".join(escaped_input_columns.keys()) + ")"
        input_types = escaped_input_columns().values()

        logger.debug("DuckDbFilterPlugin.query function_name %s", function_name)
        logger.debug("DuckDbFilterPlugin.query input_types %s", input_types)

        try:
            ddbc.create_function(
                name=function_name,
                function=self.filter,
                parameters=input_types,
                return_type="boolean",
                null_handling="special",
                side_effects=False,
            )
            return source.filter(function_call)
        finally:
            ddbc.remove_function(function_name)

    def filter(self, *_) -> bool:
        """This will be called for each row, with the columns nominated in
        `self.input_columns` as parameters.  Returns a boolean."""
        raise NotImplementedError(f"{self.__class__}.transform")


class DuckdbTransformPlugin(DuckdbSimplePlugin):
    def dropped_columns(self) -> set[str]:
        return set()

    def output_columns(self) -> dict[str, str]:
        """Return a dictionary of `column name` -> `dbtype` 
        which will be used to construct the user-defined 
        function.  The columns returned by transform() must
        match the columns declared here."""

        raise NotImplementedError(f"{self.__class__}.output_columns")

    def execute(self, ddbc, source):
        """Perform a query which calls `self.transform` for every row."""

        # if you happen to have an output column with the same name as an
        # input column this drops it, as well as any columns being explicitly
        # dropped.
        drop_columns_set = set(list(self.output_columns().keys()) + list(self.dropped_columns()))

        # Make up an arbitrary unique name for our temporary function
        function_name = f"f_{id(self)}"

        # Output type has to be completely defined, with types and all
        output_type = "STRUCT(" + ",".join(
            f"{duckdb_escape_identifier(k)} {str(v).upper()}"
            for k, v in self.output_columns().items()
            if k is not None and v is not None
        ) + ")"

        # source columns which aren't being dropped get copied into the projection
        # in their original order, followed by the generated output columns.
        keep_columns = " ".join(f"{duckdb_escape_identifier(k)}," for k in source.columns if k not in drop_columns_set)

        logger.debug("DuckDbTransformPlugin.query function_name %s", function_name)
        logger.debug("DuckDbTransformPlugin.query output_type %s", output_type)
        logger.debug("DuckDbTransformPlugin.query keep_columns %s", keep_columns)

        # if the function already exists, remove it
        try:
            ddbc.remove_function(function_name)
            logger.debug("DuckDbTransformPlugin.query removed function %s", function_name)
        except duckdb.InvalidInputException as exc:
            if not str(exc).startswith("Invalid Input Error: No function by the name of '"):
                # some other error
                logger.debug("DuckDbTransformPlugin.query can't remove function %s: %s", function_name, exc)

        # XXX it'd be nice to have an an arrow version of this
        # to allow easy parallelization, but see:
        # https://github.com/duckdb/duckdb/issues/15626
        # Appears to be fixed in 1.1.4.dev4815

        ddbc.create_function(
            name=function_name,
            function=self.transform_arrow,
            type='arrow',
            return_type=output_type,
            null_handling="special",
            side_effects=False,
        )

        # the "SELECT func(_row) FROM {table} _row" bit passes
        # a whole row to the function, sadly there's no way
        # to express this in a `.project()`.

        sql_command = f"SELECT {keep_columns} UNNEST({function_name}(_row)) FROM {source.alias} _row"
        logger.debug("DuckDbTransformPlugin.query sql_command %s", sql_command)

        self.prepare(source)

        return duckdb_source_to_view(ddbc, ddbc.sql(sql_command))

    def prepare(self, source: DuckDBPyRelation):
        """Called before the transform functions are run, to prepare anything
        which needs preparation ..."""
        pass

    def transform_arrow(self, data: pyarrow.array) -> pyarrow.array:
        logger.debug("DuckDbTransformPlugin.transform_arrow %d", len(data))
        pool = multiprocessing.Pool(processes=4)
        return pyarrow.array(
            pool.imap_unordered(
                self.transform,
                data.to_pylist()
            )
        )

    def transform(self, data: dict[str, Any]) -> Union[dict[str, Any], Tuple[Any], None]:
        """This will be called for each row. Return a tuple with the same
        value types as (or a dictionary with the same keys and value types as)
        those nominated by `self.output_columns`, or None to return all NULLs."""
        raise NotImplementedError(f"{self.__class__}.transform")
