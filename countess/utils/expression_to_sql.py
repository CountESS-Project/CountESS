import re

import pypeg2

class SqlTemplatingSymbol(pypeg2.Symbol):
    def sql(self):
        return str(self.name)

class IntegerLiteral(SqlTemplatingSymbol):
    regex = re.compile(r'[0-9]+')

class DecimalLiteral(SqlTemplatingSymbol):
    regex = re.compile(r'[0-9]+\.[0-9]+')

    def sql(self):
        return str(self.name) + "::DECIMAL"

class SingleQuotedStringLiteral(SqlTemplatingSymbol):
    regex = re.compile(r"'(?:\\.|[^'\n])*'")

class DoubleQuotedStringLiteral(SqlTemplatingSymbol):
    regex = re.compile(r'"(?:\\.|[^"\n])*"')

class Label(SqlTemplatingSymbol):
    regex = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")

class SqlTemplatingList(pypeg2.List):
    before = ''
    between = ''
    after = ''

    def sql(self):
        return self.before + self.between.join(s.sql() for s in self) + self.after

class ParenExpr(SqlTemplatingList):
    grammar = None  # filled in later
    before = "("
    after = ")"

class FunctionName(SqlTemplatingSymbol):
    regex = re.compile(r'[a-z]+')

    def __init__(self, name, namespace=None):
        print(f"FUNCTION NAME CHECK: {name}")
        if name not in ('hello'):
            raise ValueError("unknown function")
        super().__init__(name, namespace)

class FunctionCall(pypeg2.Concat):
    grammar = None  # filled in later

    def sql(self):
        return f"{self[0].sql()}({self[1].sql()})"

class Value(pypeg2.Concat):
    grammar = [FunctionCall, Label, DecimalLiteral, IntegerLiteral, SingleQuotedStringLiteral, DoubleQuotedStringLiteral, ParenExpr]

    def sql(self):
        return self[0].sql()

FunctionCall.grammar = FunctionName, "(", Value, ")"

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

class NotOp(SqlTemplatingSymbol):
    regex = re.compile(r"not")

class NotExpr(SqlTemplatingList):
    grammar = pypeg2.maybe_some(NotOp), AddExpr

class AndExpr(SqlTemplatingList):
    grammar = NotExpr, pypeg2.maybe_some("and", NotExpr)
    between = ' AND '

class OrExpr(SqlTemplatingList):
    grammar = AndExpr, pypeg2.maybe_some("or", AndExpr)
    between = ' OR '

ParenExpr.grammar = "(", OrExpr, ")"

class Assignment(pypeg2.Concat):
    grammar = Label, "=", OrExpr

    def sql(self):
        return self[1].sql() + " AS " + self[0].sql()

class Filter(pypeg2.Concat):
    # for backwards compatibility
    grammar = "__filter", "=", OrExpr

    def sql(self):
        return self[0].sql()

class Block(pypeg2.Concat):
    grammar = pypeg2.some([Filter, Assignment, OrExpr])

    def sql(self):
        selects = []
        wheres = []
        for s in self:
            if isinstance(s, Assignment):
                selects.append(s.sql())
            else:
                wheres.append(s.sql())

        sql = f"SELECT {', '.join(selects)}"
        if wheres:
            sql += "WHERE { ' AND '.join(wheres) }"
        return sql


block = r"""
hello = "world"
foo = 107+42/3.001
baz = hello("quuux")
ford = 10**2
sdfklsdf = 10**-2
"""

pp = pypeg2.parse(block, Block)

print(pp)
print(pp.sql())

