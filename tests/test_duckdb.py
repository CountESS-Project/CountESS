from decimal import Decimal

import pytest

from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal

literal_tests = [
    (None, "NULL"),
    ("123456", "'123456'"),
    ("hello'world", "'hello''world'"),
    ('hello"world', "'hello\"world'"),
    (123456, "123456"),
    (123.456, "123.456::DOUBLE"),
    (Decimal("123.456"), "123.456::DECIMAL(6,3)"),
    (Decimal("1230000"), "1230000::DECIMAL(7,0)"),
    (Decimal("0.00000000123"), "1.23E-9::DECIMAL(11,11)"),
    (Decimal("123E6"), "1.23E+8::DECIMAL(9,0)"),
    (Decimal("123E-6"), "0.000123::DECIMAL(6,6)"),
    (True, "TRUE"),
    (False, "FALSE"),
    ([1, "foo", True], "[1,'foo',TRUE]"),
]


@pytest.mark.parametrize("lit,sql", literal_tests)
def test_expressions(lit, sql):
    assert duckdb_escape_literal(lit) == sql


identifier_tests = [
    ("hello", '"hello"'),
    ("hel'lo", '"hel\'lo"'),
    ('hel"lo', '"hel""lo"'),
]


@pytest.mark.parametrize("ident,sql", identifier_tests)
def test_identifiers(ident, sql):
    assert duckdb_escape_identifier(ident) == sql
