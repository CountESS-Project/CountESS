import decimal
import logging
import secrets
from typing import Iterable, List, Optional, Union

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from duckdb.typing import DuckDBPyType

logger = logging.getLogger(__name__)


# XXX This stuff would probably be better as a DuckdbWrapper class


def duckdb_dtype_is_integer(dtype: DuckDBPyType) -> bool:
    return dtype.id in (
        "bigint",
        "hugeint",
        "integer",
        "smallint",
        "tinyint",
        "ubigint",
        "uhugeint",
        "uinteger",
        "usmallint",
        "utinyint",
    )


def duckdb_dtype_is_numeric(dtype: DuckDBPyType) -> bool:
    return dtype.id in ("float", "decimal", "double") or duckdb_dtype_is_integer(dtype)


def duckdb_escape_identifier(identifier: str) -> str:
    if identifier is None:
        return None
    return '"' + identifier.replace('"', '""') + '"'


def duckdb_escape_literal(literal: Union[str, int, float, list, None]) -> str:
    # XXX this shouldn't be a thing *BUT* not all functions seem to accept parameter
    # bindings, or at least it isn't clearly documented as such.
    if literal is None:
        return "NULL"
    elif type(literal) is str:
        return "'" + literal.replace("'", "''") + "'"
    elif type(literal) in (int, float, decimal.Decimal):
        return str(literal)
    elif type(literal) is bool:
        return 'TRUE' if literal else 'FALSE'
    elif type(literal) is list:
        return "[" + ",".join(duckdb_escape_literal(x) for x in literal) + "]"
    else:
        raise TypeError("can't escape literal of type %s" % type(literal))


def duckdb_add_null_columns(table: DuckDBPyRelation, columns: List[str]) -> DuckDBPyRelation:
    return table.project(
        ", ".join(("NULL AS " if c not in table.columns else "") + duckdb_escape_identifier(c) for c in columns)
    )


def duckdb_concatenate(tables: List[DuckDBPyRelation]) -> DuckDBPyRelation:
    # can't have an no-columns table, therefore can't concatenate 0 tables.
    assert len(tables)

    # uses a dict not a set to preserve column order
    all_columns = list({c: 1 for t in tables for c in t.columns}.keys())
    logger.debug("duckdb_concatenate all_columns %s", all_columns)

    result = duckdb_add_null_columns(tables[0], all_columns)
    for table in tables[1:]:
        result = result.union(duckdb_add_null_columns(table, all_columns))

    return result


def duckdb_source_to_view(cursor: DuckDBPyConnection, source: DuckDBPyRelation):
    view_name = "v_" + secrets.token_hex(16)
    logger.debug("creating view %s for %s", view_name, source.alias)
    source.to_view(view_name)
    return cursor.view(view_name)


def duckdb_source_to_table(cursor: DuckDBPyConnection, source: DuckDBPyRelation):
    table_name = "t_" + secrets.token_hex(16)
    logger.debug("creating table %s for %s", table_name, source.alias)
    source.to_table(table_name)
    return cursor.table(table_name)


def duckdb_choose_special(strings: Iterable[str]) -> str:
    """Pick a special string for use in identifiers etc, which is
    a unicode character which is not special for regexes (because of the
    COLUMN function) and is not in any of the `strings` and isn't some
    weird combining character which will make the logs unreadable."""
    chars = set(c for s in strings for c in s)
    for c in "~!@#%&_=:;,/":
        if c not in chars:
            return c
    for n in range(0xA1, 0x2FF):
        if chr(n) not in chars:
            return chr(n)
    raise NotImplementedError("couldn't choose separator")
