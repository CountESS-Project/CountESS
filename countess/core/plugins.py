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
from typing import Any, Dict, Iterable, Mapping, Optional, Sequence, Type, Union

import duckdb
import psutil
import pyarrow  # type: ignore
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess.core.parameters import (
    BaseParam,
    BooleanParam,
    FileArrayParam,
    FileParam,
    HasSubParametersMixin,
    MultiParam,
)
from countess.utils.duckdb import duckdb_combine, duckdb_dtype_is_numeric, duckdb_escape_literal
from countess.utils.files import clean_filename
from countess.utils.pyarrow import python_type_to_arrow_dtype

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

    def prepare_multi(self, ddbc: DuckDBPyConnection, sources: Mapping[str, DuckDBPyRelation]) -> None:
        pass

    def execute_multi(
        self, ddbc: DuckDBPyConnection, sources: Mapping[str, DuckDBPyRelation], row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        raise NotImplementedError(f"{self.__class__}.execute_multi")


class DuckdbSimplePlugin(DuckdbPlugin):
    def prepare_multi(self, ddbc: DuckDBPyConnection, sources: Mapping[str, DuckDBPyRelation]) -> None:
        self.prepare(ddbc, duckdb_combine(ddbc, list(sources.values())))

    def prepare(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> None:
        if source is None:
            self.set_column_choices({})
        else:
            self.set_column_choices({c: duckdb_dtype_is_numeric(d) for c, d in zip(source.columns, source.dtypes)})

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
            except duckdb.duckdb.DatabaseError as exc:
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
    filename_column = BooleanParam("Filename Column?", False)
    file_types: Sequence[tuple[str, Union[str, list[str]]]] = [("Any", "*")]

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.files.file_types = self.file_types

    def filenames_and_params(self):
        for file_param in self.files:
            for filename in glob.iglob(file_param.filename.value):
                yield filename, file_param

    def execute(
        self, ddbc: DuckDBPyConnection, source: None, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        tablenames_filenames_and_params = [
            (f"t_{id(self)}_{num}", filename, file_param)
            for num, (filename, file_param) in enumerate(self.filenames_and_params())
        ]
        if len(tablenames_filenames_and_params) == 0:
            return None
        row_limit_per_file = (row_limit // len(tablenames_filenames_and_params)) if row_limit else None
        logger.debug("row_limit_per_file %s", row_limit_per_file)

        def _load(tn_fn_fp):
            # This may be run in a different thread, so it needs to create a cursor
            # to be thread-safe, and it uses a table rather than
            # a view to store the data once it has been filtered.

            tablename, filename, file_param = tn_fn_fp
            cursor = ddbc.cursor()
            rel = self.load_file(cursor, filename, file_param, row_limit_per_file)
            if self.filename_column:
                filename_literal = duckdb_escape_literal(clean_filename(filename))
                rel = rel.project(f"*, {filename_literal} as filename")
            if row_limit_per_file is not None:
                rel = rel.limit(row_limit_per_file)
            cursor.sql(f"DROP TABLE IF EXISTS {tablename}")
            logger.debug("DuckdbLoadFilePlugin.execute._load sql %s", rel.sql_query())
            rel.create(tablename)
            return tablename

        if len(tablenames_filenames_and_params) > 1:
            with multiprocessing.pool.ThreadPool() as pool:
                tablenames = list(pool.imap_unordered(_load, tablenames_filenames_and_params))
            return self.combine(ddbc, [ddbc.table(tn) for tn in tablenames])
        else:
            tablename = _load(tablenames_filenames_and_params[0])
            return self.combine(ddbc, [ddbc.table(tablename)])

    def load_file(
        self, cursor: duckdb.DuckDBPyConnection, filename: str, file_param: BaseParam, row_limit: Optional[int] = None
    ) -> duckdb.DuckDBPyRelation:
        raise NotImplementedError(f"{self.__class__}.load_file")

    def combine(
        self, ddbc: duckdb.DuckDBPyConnection, tables: Iterable[duckdb.DuckDBPyRelation]
    ) -> Optional[duckdb.DuckDBPyRelation]:
        return duckdb_combine(ddbc, tables)


class DuckdbSaveFilePlugin(DuckdbSimplePlugin):
    num_outputs = 0

    def execute(self, ddbc, source, row_limit: Optional[int] = None):
        raise NotImplementedError(f"{self.__class__}.execute")


class DuckdbTransformPlugin(DuckdbSimplePlugin):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.view_name = f"v_{id(self)}"

    def get_reader(self, source):
        return source.to_arrow_table().to_reader(max_chunksize=2048)

    def remove_fields(self, field_names: list[str]) -> list[Optional[str]]:
        return []

    def add_fields(self) -> Mapping[Optional[str], Optional[type]]:
        return {}

    def fix_schema(self, schema: pyarrow.Schema) -> pyarrow.Schema:
        for field_name in self.remove_fields(schema.names):
            if field_name in schema.names:
                schema = schema.remove(schema.get_field_index(field_name))
        for field_name, ttype in self.add_fields().items():
            if field_name and ttype is not None:
                schema = schema.append(pyarrow.field(field_name, python_type_to_arrow_dtype(ttype)))
        return schema

    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> DuckDBPyRelation:
        """Perform a query which calls `self.transform` for every row."""

        reader = self.get_reader(source)
        ddbc.register(self.view_name, pyarrow.Table.from_batches(self.transform_batch(batch) for batch in reader))
        return ddbc.view(self.view_name)

    def transform_batch(self, batch: pyarrow.RecordBatch) -> pyarrow.RecordBatch:
        schema = self.fix_schema(batch.schema)
        return pyarrow.RecordBatch.from_pylist(
            [t for t in (self.transform(row) for row in batch.to_pylist()) if t is not None], schema=schema
        )

    def transform(self, data: dict[str, Any]) -> Optional[Dict[str, Any]]:
        """This will be called for each row. Return a tuple with the same
        value types as (or a dictionary with the same keys and value types as)
        those nominated by `self.output_columns`, or None to return all NULLs."""
        raise NotImplementedError(f"{self.__class__}.transform")


class DuckdbThreadedTransformPlugin(DuckdbTransformPlugin):
    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> DuckDBPyRelation:
        with multiprocessing.pool.ThreadPool(processes=psutil.cpu_count()) as pool:
            reader = self.get_reader(source)
            ddbc.register(self.view_name, pyarrow.Table.from_batches(pool.imap_unordered(self.transform_batch, reader)))
        return ddbc.view(self.view_name)

    def transform(self, data: dict[str, Any]) -> Optional[Dict[str, Any]]:
        raise NotImplementedError(f"{self.__class__}.transform")


class DuckdbParallelTransformPlugin(DuckdbTransformPlugin):
    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> DuckDBPyRelation:
        with multiprocessing.Pool(processes=psutil.cpu_count()) as pool:
            reader = self.get_reader(source)
            ddbc.register(self.view_name, pyarrow.Table.from_batches(pool.imap_unordered(self.transform_batch, reader)))
        return ddbc.view(self.view_name)

    def transform(self, data: dict[str, Any]) -> Optional[Dict[str, Any]]:
        raise NotImplementedError(f"{self.__class__}.transform")
