import duckdb
import pytest

from countess.utils.expression_to_sql import Block

expr_tests = [
    ("a == b", '"a"=="b"'),
    ("a != b", '"a"!="b"'),
    ("4 * 3 + 2 * 6", "4*3+2*6"),
    ("4 * (3 + 2) * 6", "4*(3+2)*6"),
    ("log(0.7)", "LN(0.7::DECIMAL(1,1))"),
    ("1 if foo else 3", 'CASE WHEN "foo" THEN 1 ELSE 3 END'),
    ("foo == None", '"foo" IS NULL'),
    ("foo != None", '"foo" IS NOT NULL'),
    ("atan2(1,2)", "ATAN2(1,2)"),
    ("sum(foo,bar,107)", 'LIST_SUM(["foo","bar",107])'),
    ("foo > pi()", '"foo">PI()'),
    ("`hello world` == `false`", '"hello world"=="false"'),
    (r'"hello\"world"', "'hello\"world'"),
    (r"'hello\'world'", r"'hello''world'"),
    (r"`hello world`", r'"hello world"'),
    (r"concat('foo', bar, 'baz')", "CONCAT('foo',\"bar\",'baz')"),
    (r"pi()", "PI()"),
]


@pytest.mark.parametrize("expr,sql", expr_tests)
def test_expressions(expr, sql):
    assert Block.from_string(expr)[0].sql() == sql


bad_expr_tests = [
    "pi(7)",
    "exp()",
    "atan2(1.234)",
    "concat()",
    "contains('foo')",
]


@pytest.mark.parametrize("expr", bad_expr_tests)
def test_bad_expressions(expr):
    with pytest.raises(ValueError):
        Block.from_string(expr)[0].sql()


assign_tests = [
    ("a = 1", '1 AS "a"'),
    ("a = b = c = 2", '2 AS "a",2 AS "b",2 AS "c"'),
    ("d = None", 'NULL AS "d"'),
]


@pytest.mark.parametrize("expr,sql", assign_tests)
def test_assignments(expr, sql):
    assert Block.from_string(expr).sql_selects()[0] == sql


filter_tests = [
    ("a < b", '"a"<"b"'),
    ("b <= c", '"b"<="c"'),
    ("a < b <= c", '"a"<"b" AND "b"<="c"'),
    ("__filter = a < b", '"a"<"b"'),
]


@pytest.mark.parametrize("expr,sql", filter_tests)
def test_filters(expr, sql):
    assert Block.from_string(expr).sql_wheres()[0] == sql


def test_project_and_filter():
    rel = duckdb.sql("select * from (values (1,2), (3,4), (5,6)) t(a,b)")
    blk = Block.from_string("c = a+b\nb<5\nd = a + c")
    out = blk.project_and_filter(rel)

    assert out.columns == ["a", "b", "c", "d"]
    assert out.fetchall() == [(1, 2, 3, 4), (3, 4, 7, 10)]
