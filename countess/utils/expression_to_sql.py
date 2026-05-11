import ast
import logging
import re
from typing import Any, Optional

import pypeg2  # type: ignore[import-untyped]

from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal

logger = logging.getLogger(__name__)

FUNC_OPS = {
    "ABS",
    "LEN",
    "SIN",
    "ASIN",
    "COS",
    "ACOS",
    "TAN",
    "ATAN",
    "ATAN2",
    "SQRT",
    "LOG",
    "LOG2",
    "LOG10",
    "POW",
    "EXP",
    "CONCAT",
    "FLOOR",
    "CEIL",
    "CONTAINS",
    "STARTS_WITH",
    "ENDS_WITH",
    "LOWER",
    "UPPER",
    "REGEXP_MATCHES",
    "REVERSE",
    "TRANSLATE",
    "TRIM",
    "SIGN",
    "PI",
}

LIST_OPS = {
    "SUM",
    "PRODUCT",
    "AVG",
    "MEDIAN",
    "VAR_POP",
    "STD_POP",
    "VAR_SAMP",
    "STD_SAMP",
    "MAX",
    "MIN",
}


class SqlTemplatingSymbol(pypeg2.Symbol):
    def sql(self):
        return str(self.name)


class BooleanLiteral(SqlTemplatingSymbol):
    regex = re.compile(r"True|False")

    def sql(self):
        return f"('{self.name[0]}'::BOOLEAN)"


class NullLiteral(SqlTemplatingSymbol):
    regex = re.compile(r"None|NULL")

    def sql(self):
        return "NULL"


class IntegerLiteral(SqlTemplatingSymbol):
    regex = re.compile(r"[0-9]+")


class DecimalLiteral(SqlTemplatingSymbol):
    regex = re.compile(r"[0-9]+\.[0-9]+")

    def sql(self):
        return "(%s::DECIMAL)" % self.name


class SingleQuotedStringLiteral(SqlTemplatingSymbol):
    regex = re.compile(r"'[^'\\]*(?:\\.[^'\\]*)*'")

    def sql(self):
        return duckdb_escape_literal(ast.literal_eval(self.name))


class DoubleQuotedStringLiteral(SqlTemplatingSymbol):
    regex = re.compile(r'"[^"\\]*(?:\\.[^"\\]*)*"')

    def sql(self):
        return duckdb_escape_literal(ast.literal_eval(self.name))


class Label(SqlTemplatingSymbol):
    regex = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")

    def sql(self):
        return duckdb_escape_identifier(self.name)


class BacktickQuotedLabel(SqlTemplatingSymbol):
    regex = re.compile(r"`[^`\n]*`")

    def sql(self):
        return duckdb_escape_identifier(self.name[1:-1])


class SqlTemplatingList(pypeg2.List):
    before = ""
    between = ""
    after = ""

    def sql(self):
        return self.before + self.between.join(s.sql() for s in self) + self.after


class ParenExpr(SqlTemplatingList):
    grammar: Optional[Any] = None  # filled in later
    before = "("
    after = ")"


class FunctionCall(pypeg2.Concat):
    grammar: Optional[Any] = None  # filled in later

    def sql(self):
        func_name = str(self[0].name).upper()
        func_params = ",".join(s.sql() for s in self[1:])

        if func_name in LIST_OPS:
            return f"LIST_{func_name}([{func_params}])"
        else:
            return f"{func_name}({func_params})"


class Value(pypeg2.Concat):
    grammar = [
        FunctionCall,
        BooleanLiteral,
        NullLiteral,
        Label,
        BacktickQuotedLabel,
        DecimalLiteral,
        IntegerLiteral,
        SingleQuotedStringLiteral,
        DoubleQuotedStringLiteral,
        ParenExpr,
    ]

    def sql(self):
        return self[0].sql()


class UnaOp(SqlTemplatingSymbol):
    regex = re.compile(r"[+-]")


class UnaExpr(SqlTemplatingList):
    grammar = pypeg2.optional(UnaOp), Value


class PowOp(SqlTemplatingSymbol):
    regex = re.compile(r"\*\*")


class PowExpr(SqlTemplatingList):
    grammar = UnaExpr, pypeg2.maybe_some((PowOp, UnaExpr))


class MulOp(SqlTemplatingSymbol):
    regex = re.compile(r"[*/]")


class MulExpr(SqlTemplatingList):
    grammar = PowExpr, pypeg2.maybe_some((MulOp, PowExpr))


class AddOp(SqlTemplatingSymbol):
    regex = re.compile(r"[+-]")


class AddExpr(SqlTemplatingList):
    grammar = MulExpr, pypeg2.maybe_some((AddOp, MulExpr))


class CompOp(SqlTemplatingSymbol):
    regex = re.compile(r"<=?|>=?|[!=]=")


class CompExpr(pypeg2.List):
    grammar = AddExpr, pypeg2.maybe_some((CompOp, [NullLiteral, AddExpr]))

    def sql(self):
        def _subcomp(left, comp, right):
            comp_op = comp.sql()
            if isinstance(right, NullLiteral):
                if comp_op == "==":
                    return left.sql() + " IS NULL"
                if comp_op == "!=":
                    return left.sql() + " IS NOT NULL"
            return left.sql() + comp_op + right.sql()

        if len(self) >= 3:
            return " AND ".join(_subcomp(self[n], self[n + 1], self[n + 2]) for n in range(0, len(self) - 1, 2))
        else:
            return self[0].sql()


class NotOp(SqlTemplatingSymbol):
    regex = re.compile(r"not")


class NotExpr(SqlTemplatingList):
    grammar = pypeg2.maybe_some(NotOp), CompExpr


class AndExpr(SqlTemplatingList):
    grammar = NotExpr, pypeg2.maybe_some("and", NotExpr)
    between = " AND "


class OrExpr(SqlTemplatingList):
    grammar = AndExpr, pypeg2.maybe_some("or", AndExpr)
    between = " OR "


class TernExpr(pypeg2.List):
    grammar = OrExpr, pypeg2.optional("if", OrExpr, "else", OrExpr)

    def sql(self):
        try:
            return f"CASE WHEN {self[1].sql()} THEN {self[0].sql()} ELSE {self[2].sql()} END"
        except IndexError:
            return self[0].sql()


# fill in grammars for ParenExpr and FunctionCall
ParenExpr.grammar = "(", TernExpr, ")"
FunctionCall.grammar = Label, "(", pypeg2.optional(OrExpr, pypeg2.maybe_some(",", OrExpr)), ")"


class Assignment(pypeg2.Concat):
    grammar = pypeg2.some(Label, "="), [NullLiteral, TernExpr]

    def names(self):
        return [s.name for s in self[0:-1]]

    def sql(self):
        return ",".join(self[-1].sql() + " AS " + s.sql() for s in self[0:-1])


class Filter(pypeg2.Concat):
    # for backwards compatibility
    grammar = "__filter", "=", OrExpr

    def sql(self):
        return self[0].sql()


class Block(pypeg2.Concat):
    grammar = pypeg2.some([Filter, Assignment, TernExpr])

    @classmethod
    def from_string(cls, s):
        return pypeg2.parse(s, cls)

    def sql_selects(self):
        return [s.sql() for s in self if isinstance(s, Assignment)]

    def sql_wheres(self):
        return [s.sql() for s in self if not isinstance(s, Assignment)]

    def sql(self, source):
        return f"""
            SELECT {', '.join(self.sql_selects())}
            FROM {source}
            WHERE {' AND '.join(self.sql_wheres())}
        """

    def project_and_filter(self, source):
        for s in self:
            if isinstance(s, Assignment):
                projection = [c for c in source.columns if c not in s.names()]
                if not isinstance(s[-1], NullLiteral):
                    projection.append(s.sql())
                logger.debug("PROJECTION: %s", projection)
                source = source.project(",".join(projection))
            else:
                ffilter = s.sql()
                logger.debug("FILTER: %s", ffilter)
                source = source.filter(ffilter)
        return source
