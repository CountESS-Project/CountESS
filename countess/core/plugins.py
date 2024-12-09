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
from typing import Any, Iterable, List, Optional, Sequence, Type, Union

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess.core.parameters import (
    BaseParam,
    FileArrayParam,
    FileParam,
    HasSubParametersMixin,
    MultiParam,
)
from countess.utils.duckdb import duckdb_concatenate, duckdb_escape_identifier
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
    ALLOWED_TYPES = {'INTEGER', 'VARCHAR', 'FLOAT'}

    def execute_multi(self, ddbc: DuckDBPyConnection, sources: List[DuckDBPyRelation]) -> Optional[DuckDBPyRelation]:
        raise NotImplementedError(f"{self.__class__}.execute_multi")


class DuckdbSimplePlugin(DuckdbPlugin):

    def execute_multi(self, ddbc: DuckDBPyConnection, sources: List[DuckDBPyRelation]) -> Optional[DuckDBPyRelation]:
        if len(sources) > 1:
            return self.execute(ddbc, duckdb_concatenate(sources))
        elif len(sources) == 1:
            return self.execute(ddbc, sources[0])
        else:
            return self.execute(ddbc, None)

    def execute(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> Optional[DuckDBPyRelation]:
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


class DuckdbLoadFilePlugin(DuckdbSimplePlugin):

    files = FileArrayParam("Files", LoadFileMultiParam("File"))
    file_types: Sequence[tuple[str, Union[str, list[str]]]] = [("Any", "*")]
    num_inputs = 0

    def filenames_and_params(self):
        for file_param in self.files:
            for filename in glob.iglob(file_param.filename.value):
                yield filename, file_param

    def execute(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> Optional[DuckDBPyRelation]:
        assert source is None

        filenames_and_params = list(self.filenames_and_params())

        cursor = ddbc
        return duckdb_concatenate([
            self.load_file(cursor, filename, file_param, num)
            for num, (filename, file_param) in enumerate(filenames_and_params)
        ])

    def load_file(self, cursor: DuckDBPyConnection, filename: str,
                  file_param: BaseParam, file_number: int) -> DuckDBPyRelation:
        raise NotImplementedError(f"{self.__class__}.load_file")


class DuckdbFilterPlugin(DuckdbSimplePlugin):

    def input_columns(self) -> dict[str,str]:
        raise NotImplementedError(f"{self.__class__}.input_columns")

    def execute(self, ddbc, source):
        """Perform a query which calls `self.transform` for every row."""

        escaped_input_columns = {
            duckdb_escape_identifier(k): str(v).upper()
            for k, v in self.input_columns()
        }
        assert all(v in self.ALLOWED_TYPES for v in escaped_input_columns.values())

        # Make up an arbitrary unique name for our temporary function
        function_name = f"f_{id(self)}"
        function_call = function_name + "(" + ",".join(escaped_input_columns.keys()) + ")"
        input_types = escaped_input_columns().values()

        logger.debug("DuckDbFilterPlugin.query function_name %s", function_name)
        logger.debug("DuckDbFilterPlugin.query input_types %s", input_types)

        try:
            ddbc.create_function(
                name = function_name,
                function = self.filter,
                parameters = input_types,
                return_type = 'boolean',
                null_handling='special',
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

    def input_columns(self) -> dict[str,str]:
        raise NotImplementedError(f"{self.__class__}.input_columns")

    def output_columns(self) -> dict[str,str]:
        raise NotImplementedError(f"{self.__class__}.output_columns")

    def execute(self, ddbc, source):
        """Perform a query which calls `self.transform` for every row."""

        escaped_input_columns = {
            duckdb_escape_identifier(k): str(v).upper()
            for k, v in self.input_columns().items()
            if k is not None and v is not None
        }

        escaped_output_columns = {
            duckdb_escape_identifier(k): str(v).upper()
            for k, v in self.output_columns().items()
            if k is not None and v is not None
        }

        # if you happen to have an output column with the same name as an
        # input column this drops it, as well as any columns being explicitly
        # dropped.
        drop_columns_set = set(list(self.output_columns().keys()) + list(self.dropped_columns()))

        # source columns which aren't being dropped get copied into the projection
        # in their original order, followed by the generated output columns.
        escaped_keep_columns = [
            duckdb_escape_identifier(k)
            for k in source.columns
            if k not in drop_columns_set
        ]

        assert all(v in self.ALLOWED_TYPES for v in escaped_input_columns.values())
        assert all(v in self.ALLOWED_TYPES for v in escaped_output_columns.values())

        # Make up an arbitrary unique name for our temporary function
        function_name = f"f_{id(self)}"

        # this generates a clause like `f(x,y).a as a, f(x,y).b as b, f(x,y).c as c`
        # which looks inefficient but duckdb only calls `f(x,y)` once per row
        # so long as the function is created with `side_effects=False` ...
        function_call = function_name + "(" + ",".join( k or 'NULL' for k in escaped_input_columns.keys()) + ")"
        function_calls = [
            f"{function_call}.{oc} AS {oc}"
            for oc in escaped_output_columns.keys()
            if oc is not None
        ]
        project_fields = ", ".join(escaped_keep_columns + function_calls)

        input_types = list(escaped_input_columns.values())
        output_type = "STRUCT(" + ",".join(
            f"{k} {v}" for k, v in escaped_output_columns.items()
            if k is not None
        ) + ")"

        logger.debug("DuckDbTransformPlugin.query function_name %s", function_name)
        logger.debug("DuckDbTransformPlugin.query input_types %s", input_types)
        logger.debug("DuckDbTransformPlugin.query output_type %s", output_type)
        logger.debug("DuckDbTransformPlugin.query project_fields %s", project_fields)

        try:
            ddbc.create_function(
                name = function_name,
                function = self.transform_tuple,
                parameters = input_types,
                return_type = output_type,
                null_handling='special',
                side_effects=False,
            )
            return source.project(project_fields)
        finally:
            pass
            #ddbc.remove_function(function_name)

    def transform_tuple(self, *data):
        logger.debug("DuckDbTransformPlugin.transform_tuple %s", data)
        r =  self.transform(dict(zip(
            [k for k in self.input_columns().keys() if k is not None],
            data
        )))
        logger.debug("DuckDbTransformPlugin.transform_tuple %s", r)
        return r

    def transform(self, data: dict[str,Any]):
        """This will be called for each row, with the columns nominated in 
        `self.input_columns` as parameters.  Return a tuple with the same
        value types as (or a dictionary with the same keys and value types as)
        those nominated by `self.output_columns`, or None to return all NULLs."""
        raise NotImplementedError(f"{self.__class__}.transform")
