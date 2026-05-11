import duckdb
import pytest

from countess.utils.expression_to_sql import Block

tests = [
    ("a == b", '"a"=="b"'),
    ("a != b", '"a"!="b"'),
    ("4 * 3 + 2 * 6", "4*3+2*6"),
    ("4 * (3 + 2) * 6", "4*(3+2)*6"),
    ("log(0.7)", "LOG((0.7::DECIMAL))"),
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
]


@pytest.mark.parametrize("expr,sql", tests)
def test_expressions(expr, sql):
    assert Block.from_string(expr)[0].sql() == sql


def test_project_and_filter():
    rel = duckdb.sql("select * from (values (1,2), (3,4), (5,6)) t(a,b)")
    blk = Block.from_string("c = a+b\nb<5\nd = a + c")
    out = blk.project_and_filter(rel)

    assert out.columns == ["a", "b", "c", "d"]
    assert out.fetchall() == [(1, 2, 3, 4), (3, 4, 7, 10)]
