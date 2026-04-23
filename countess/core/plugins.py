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
import multiprocessing.pool
import os.path
import secrets
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Type, Union

import duckdb
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess.core.parameters import (
    BaseParam,
    BooleanParam,
    FileArrayParam,
    FileParam,
    HasSubParametersMixin,
    MultiParam,
)
from countess.utils.duckdb import duckdb_combine, duckdb_dtype_is_numeric, duckdb_escape_literal, duckdb_source_to_view

PRERUN_ROW_LIMIT: int = 100000

logger = logging.getLogger(__name__)


def get_plugin_classes() -> Iterable[Type["BasePlugin"]]:
    plugin_classes = set()
    entry_points = importlib.metadata.entry_points().select(group="countess_plugins")

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


def load_plugin(module_name: str, class_name: str, plugin_name: Optional[str] = None) -> Optional["BasePlugin"]:
    try:
        module = importlib.import_module(module_name)
        plugin_class = getattr(module, class_name)
        assert issubclass(plugin_class, BasePlugin)
        return plugin_class(plugin_name)
    except ModuleNotFoundError:
        logger.error("Module Not Found: %s", module_name)
        return None


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

    def prepare_multi(self, ddbc: DuckDBPyConnection, sources: Mapping[str, DuckDBPyRelation]) -> None:
        pass

    def set_column_choices_from_duckdb(self, param: BasePlugin, source: DuckDBPyRelation):
        if source is None:
            param.set_column_choices({})
        else:
            param.set_column_choices(
                {name: duckdb_dtype_is_numeric(dt) for name, dt in zip(source.columns, source.dtypes)}
            )

    def execute_multi(
        self, ddbc: DuckDBPyConnection, sources: Mapping[str, DuckDBPyRelation], row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        raise NotImplementedError(f"{self.__class__}.execute_multi")

    def query_progress(self, ddbc: DuckDBPyConnection):
        try:
            # this is still a PR, if it doesn't exist then guess 50%.
            return ddbc.query_progress()
        except AttributeError:
            return 50


class DuckdbSimplePlugin(DuckdbPlugin):
    def prepare_multi(self, ddbc: DuckDBPyConnection, sources: Mapping[str, DuckDBPyRelation]) -> None:
        self.prepare(ddbc, duckdb_combine(ddbc, list(sources.values())))

    def prepare(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> None:
        if source:
            self.set_column_choices_from_duckdb(self, source)

    def execute_multi(
        self, ddbc: DuckDBPyConnection, sources: Mapping[str, DuckDBPyRelation], row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        combined_source = duckdb_combine(ddbc, sources.values())
        if combined_source is None:
            return None
        return self.execute(ddbc, combined_source, row_limit)

    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        raise NotImplementedError(f"{self.__class__}.execute")


class DuckdbSqlPlugin(DuckdbSimplePlugin):
    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        sql = self.sql(source.alias, source.columns)
        logger.debug(f"{self.__class__}.execute sql %s", sql)
        if sql:
            try:
                return ddbc.sql(sql)
            except duckdb.ProgrammingError as exc:
                logger.warning(exc)
        return None

    def sql(self, table_name: str, columns: Iterable[str]) -> Optional[str]:
        raise NotImplementedError(f"{self.__class__}.sql")


class DuckdbInputPlugin(DuckdbPlugin):
    num_inputs = 0

    def execute_multi(
        self, ddbc: DuckDBPyConnection, sources: Mapping, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        assert len(sources) == 0
        return self.execute(ddbc, None, row_limit)

    def execute(
        self, ddbc: DuckDBPyConnection, source: None, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        raise NotImplementedError(f"{self.__class__}.execute")


class DuckdbStatementPlugin(DuckdbSimplePlugin):
    def statement(self, ddbc: DuckDBPyConnection, source_table_name: str, row_limit: Optional[int] = None) -> str:
        raise NotImplementedError(f"{self.__class__}.statement")

    def execute(self, ddbc, source, row_limit: Optional[int] = None):
        source_table_name = f"r_{id(self)}"
        try:
            ddbc.register(source_table_name, source)
            return ddbc.sql(self.statement(ddbc, source_table_name, row_limit))
        finally:
            ddbc.unregister(source_table_name)


class LoadFileMultiParam(MultiParam):
    """MultiParam which only asks for one thing, a filename.  Using a MultiParam wrapper
    because that makes it easier to extend the FileInputPlugin."""

    filename = FileParam("Filename")


class DuckdbLoadFilePlugin(DuckdbInputPlugin):
    files = FileArrayParam("Files", LoadFileMultiParam("File"))
    file_types: Sequence[tuple[str, Union[str, list[str]]]] = [("Any", "*")]

    progress: float = 0

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.files.file_types = self.file_types

    def filenames_and_params(self) -> Iterable[tuple[str, BaseParam]]:
        for file_param in self.files:
            if file_param.filename.value:
                yield file_param.filename.get_file_path(), file_param

    def execute(
        self, ddbc: DuckDBPyConnection, source: None, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        filenames_and_params = list(self.filenames_and_params())
        if len(filenames_and_params) == 0:
            return None

        row_limit_per_file = (row_limit // len(filenames_and_params)) if row_limit else None
        progress_per_file = 100 / len(filenames_and_params)
        self.progress = 0

        def _generator():
            for filename, file_param in filenames_and_params:
                yield self.load_file_wrapper(ddbc, filename, file_param, row_limit_per_file)
                self.progress += progress_per_file

        return self.combine(ddbc, _generator())

    def load_file_wrapper(
        self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam, row_limit: Optional[int] = None
    ) -> duckdb.DuckDBPyRelation:
        return self.load_file(cursor, filename, file_param, row_limit)

    def load_file(
        self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam, row_limit: Optional[int] = None
    ) -> duckdb.DuckDBPyRelation:
        raise NotImplementedError(f"{self.__class__}.load_file")

    def combine(
        self, ddbc: duckdb.DuckDBPyConnection, tables: Iterable[duckdb.DuckDBPyRelation]
    ) -> Optional[duckdb.DuckDBPyRelation]:
        return duckdb_combine(ddbc, tables)

    def query_progress(self, ddbc: duckdb.DuckDBPyConnection):
        return self.progress


class LoadFileDeGlobMixin:
    """A mixin for LoadFilePlugin subclasses which don't want to deal with
    globs (wildcards) in filenames ... this does it for you."""

    def filenames_and_params(self):
        for file_param in self.files:
            for filename in glob.iglob(file_param.filename.get_file_path()):
                logger.debug(
                    "LoadFileDeGlobMixin filenames_and_params %s %s", repr(file_param.filename.value), repr(filename)
                )
                yield filename, file_param


class LoadFileWithFilenameMixin:
    """A mixin for LoadFilePlugin subclasses which want to be able to offer
    a filename column option but for which there isn't an easier way to support
    this."""

    filename_column = BooleanParam("Filename Column?", False)

    def load_file_wrapper(
        self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam, row_limit: Optional[int] = None
    ) -> duckdb.DuckDBPyRelation:
        rel = self.load_file(cursor, filename, file_param, row_limit)
        if self.filename_column.value:
            assert isinstance(file_param, LoadFileMultiParam)
            try:
                filename_value = os.path.relpath(filename, file_param["filename"].base_dir)
            except ValueError:
                # relpath can fail on windows
                filename_value = filename
            proj = f"*, {duckdb_escape_literal(filename_value)} as filename"
            logger.debug("LoadFileWithFilenameMixin load_file_wrapper proj %s", proj)
            return duckdb_source_to_view(cursor, rel.project(proj))
        else:
            return rel

    def load_file(
        self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam, row_limit: Optional[int] = None
    ) -> duckdb.DuckDBPyRelation:
        raise NotImplementedError(f"{self.__class__}.load_file")


class DuckdbParallelLoadFilePlugin(DuckdbLoadFilePlugin):
    def execute(
        self, ddbc: DuckDBPyConnection, source: None, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        tablename_base = f"t_{id(self)}_"
        self.progress = 0

        # First, destroy any temporary tables which might have been used in the previous run.
        for (tablename,) in ddbc.sql(
            f"SELECT table_name from information_schema.tables where table_name like '{tablename_base}%'"
        ).fetchall():
            logger.debug("DuckdbParallelLoadFilePlugin.execute drop table %s", tablename)
            ddbc.sql(f"DROP TABLE IF EXISTS {tablename}")

        filenames_and_params = list(self.filenames_and_params())

        if len(filenames_and_params) == 0:
            return None
        elif len(filenames_and_params) == 1:
            filename, file_param = filenames_and_params[0]
            tablename = tablename_base + "X"
            self.load_file_wrapper(ddbc, filename, file_param, row_limit).create(tablename)
            self.progress = 100
            return self.combine(ddbc, [ddbc.table(tablename)])
        else:
            row_limit_per_file = int(row_limit // len(filenames_and_params)) if row_limit else None
            progress_per_file = 100 / len(filenames_and_params)

            def _load(x):
                # This is run in multiple threads, threads need their own cursors, views aren't shared across
                # cursors (or at least not reliably?) so we save each loaded file as a table and return the
                # table name so the main thread can join them.
                num, (filename, file_param) = x
                self.progress += progress_per_file / 2
                cursor = ddbc.cursor()
                tablename = f"{tablename_base}{num}"
                logger.debug("DuckdbParallelLoadFilePlugin.execute _load table %s %s", tablename, repr(filename))
                self.load_file_wrapper(cursor, filename, file_param, row_limit_per_file).create(tablename)
                self.progress += progress_per_file / 2
                logger.debug("DuckdbParallelLoadFilePlugin.execute _load table %s done", tablename)
                return tablename

            # run a bunch of _loads in parallel threads, collecting them in whatever order they return.
            # if there's more files than threads the ThreadPool takes care of queueing them up.
            # we'll then join them back up back here in the main thread.
            with multiprocessing.pool.ThreadPool() as pool:
                tablenames = sorted(pool.imap_unordered(_load, enumerate(filenames_and_params)))
            return self.combine(ddbc, [ddbc.table(tn) for tn in tablenames])

    def load_file(
        self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam, row_limit: Optional[int] = None
    ) -> duckdb.DuckDBPyRelation:
        raise NotImplementedError(f"{self.__class__}.load_file")


class DuckdbLoadFileWithTheLotPlugin(LoadFileDeGlobMixin, LoadFileWithFilenameMixin, DuckdbParallelLoadFilePlugin):
    """This recombines the various parts which got broken out into mixins back into
    the original one-with-the-lot load file plugin base"""

    def load_file(
        self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam, row_limit: Optional[int] = None
    ) -> duckdb.DuckDBPyRelation:
        raise NotImplementedError(f"{self.__class__}.load_file")


class DuckdbSaveFilePlugin(DuckdbSimplePlugin):
    num_outputs = 0

    def execute(self, ddbc, source, row_limit: Optional[int] = None):
        raise NotImplementedError(f"{self.__class__}.execute")


class DuckdbTransformPlugin(DuckdbSimplePlugin):
    def remove_fields(self, field_names: list[str]) -> list[Optional[str]]:
        return []

    def add_fields(self) -> Mapping[Optional[str], Optional[type]]:
        return {}

    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> DuckDBPyRelation:
        """Perform a query which calls `self.transform` for every row."""

        schema = dict(zip(source.columns, source.dtypes))
        for field in self.remove_fields(source.columns):
            if field in schema:
                del schema[field]
        for field, ttype in self.add_fields().items():
            if field:
                schema[field] = duckdb.sqltypes.DuckDBPyType(ttype)
        return_type = duckdb.struct_type(schema)

        def wrapper(data: dict[str, Any]) -> Optional[Dict[str, Any]]:
            try:
                return self.transform(data)
            except Exception as exc:
                logger.warning(str(exc))

        function_name = "f_" + secrets.token_hex(16)
        ddbc.create_function(
            function_name,
            wrapper,
            return_type=return_type,
            null_handling="special",  # type: ignore[arg-type]
        )

        view = duckdb_source_to_view(ddbc, source)

        return ddbc.sql(f"SELECT UNNEST({function_name}(_ROW)) FROM (SELECT * FROM {view.alias}) AS _ROW")

    def transform(self, data: dict[str, Any]) -> Optional[Dict[str, Any]]:
        """This will be called for each row. Return a tuple with the same
        value types as (or a dictionary with the same keys and value types as)
        those nominated by `self.output_columns`, or None to return all NULLs."""
        raise NotImplementedError(f"{self.__class__}.transform")


class DuckdbThreadedTransformPlugin(DuckdbTransformPlugin):
    """DuckDB should be handling the parallelization now?"""

    def transform(self, data: dict[str, Any]) -> Optional[Dict[str, Any]]:
        raise NotImplementedError(f"{self.__class__}.transform")


class DuckdbParallelTransformPlugin(DuckdbTransformPlugin):
    """DuckDB should be handling the parallelization now?"""

    def transform(self, data: dict[str, Any]) -> Optional[Dict[str, Any]]:
        raise NotImplementedError(f"{self.__class__}.transform")
