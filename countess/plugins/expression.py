import ast
import logging
import re
from typing import Dict, Optional

import pypeg2
from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import TextParam
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal

logger = logging.getLogger(__name__)

FUNCOPS = {
    "abs",
    "len",
    "sin",
    "cos",
    "tan",
    "sqrt",
    "log",
    "log2",
    "log10",
    "pow",
    "exp",
    "concat",
    "least",
    "greatest",
    "floor",
    "ceil",
    "contains",
    "starts_with",
    "ends_with",
    "lower",
    "upper",
    "regexp_matches",
    "reverse",
    "translate",
    "trim",
}
CASTOPS = {
    "int": "integer",
    "float": "float",
    "str": "string",
    "bool": "boolean",
}
LISTOPS = {
    "sum": "list_sum",
    "product": "list_product",
    "avg": "list_avg",
    "median": "list_median",
    "var": "list_var_pop",
    "std": "list_stddev_pop",
    "var_samp": "list_var_samp",
    "std_samp": "list_stddev_samp",
}


class ExpressionPlugin(DuckdbSimplePlugin):
    name = "Expression"
    description = "Apply simple expressions to each row"
    additional = """
        Expressions are applied to each row.  Syntax is python-like, but to concatenate strings,
        use 'concat' function.  For selection use python-like "a if b else c".  To remove a column,
        set it to constant None.   Set a variable '__filter' to False to remove rows.

        Available operators: + - * ** / // and or not
        
        Available functions:
    """ + " ".join(
        sorted(list(FUNCOPS) + list(LISTOPS.keys()) + list(CASTOPS.keys()))
    )

    version = VERSION

    code = TextParam("Expressions")
    projection: Optional[Dict[str, str]] = None

    def prepare(self, *a) -> None:
        super().prepare(*a)
        self.projection = {}
        try:
            ast_root = ast.parse(self.code.value or "")
        except SyntaxError as exc:
            logger.debug("Syntax Error %s", exc)
            return

        for ast_node in ast_root.body:
            try:
                if type(ast_node) is ast.Assign:
                    expr = _transmogrify(ast_node.value)
                    for ast_target in ast_node.targets:
                        if type(ast_target) is ast.Name:
                            self.projection[ast_target.id] = expr
                elif type(ast_node) is ast.Expr:
                    tgt = re.sub(r"_+$", "", re.sub(r"\W+", "_", ast.unparse(ast_node)))
                    expr = _transmogrify(ast_node.value)
                    self.projection[tgt] = _transmogrify(ast_node.value)
            except (NotImplementedError, KeyError) as exc:
                logger.debug("Bad AST Node: %s %s", ast_node, exc)

    def execute(
        self, ddbc: DuckDBPyConnection, source: DuckDBPyRelation, row_limit: Optional[int] = None
    ) -> Optional[DuckDBPyRelation]:
        if not self.projection:
            return source

        old_columns = [duckdb_escape_identifier(c) for c in source.columns if c not in self.projection]
        new_columns = [
            v + " AS " + duckdb_escape_identifier(k)
            for k, v in self.projection.items()
            if v != "NULL" and k != "__filter"
        ]
        projection = ", ".join(old_columns + new_columns)

        logger.debug("ExpressionPlugin.execute projection %s", projection)
        if "__filter" in self.projection:
            filter_ = self.projection["__filter"]
            logger.debug("ExpressionPlugin.execute filter %s", filter_)
            return source.project(projection).filter(filter_)
        else:
            return source.project(projection)



### PyPEG2 parser & expression generator follows




class SqlTemplatingSymbol(pypeg2.Symbol):
    def sql(self):
        return str(self.name)

class IntegerLiteral(SqlTemplatingSymbol):
    regex = re.compile(r'[0-9]+')

class DecimalLiteral(SqlTemplatingSymbol):
    regex = re.compile(r'[0-9]+\.[0-9]+')

    def sql(self):
        return "(%s::DECIMAL)" % self.name

class SingleQuotedStringLiteral(SqlTemplatingSymbol):
    regex = re.compile(r"'(?:\\.|[^'\n])*'")

    def sql(self):
        return duckdb_escape_literal(self.name[1:-1])

class DoubleQuotedStringLiteral(SqlTemplatingSymbol):
    regex = re.compile(r'"(?:\\.|[^"\n])*"')

    def sql(self):
        return duckdb_escape_literal(self.name[1:-1])

class Label(SqlTemplatingSymbol):
    regex = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")

    def sql(self):
        return duckdb_escape_identifier(self.name)

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

class FunctionCall(pypeg2.Concat):
    grammar = None  # filled in later

    def sql(self):
        function_name, *function_params = self
        if function_name in FUNCOPS:
            return function_name + "(" + ','.join(fp.sql() for fp in function_params) + ")"
        if function_name in LISTOPS:
            return LISTOPS[function_name] + "(" + ','.join(fp.sql() for fp in function_params) + ")"
        if function_name in CASTOPS:
            return f"TRY_CAST({fp[0].sql()} AS {CASTOPS[function_name]})"

        raise ValueError("Unknown function %s" % function_name)

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

class Statement(pypeg2.Concat):
    grammar = pypeg2.optional(Label, "="), OrExpr

    def sql_clause(self):
        return self[-1].sql()

    def sql_target(self):
        return self[0].sql() if len(self) > 1 else None


def parse_block(block: str):
    for line in block.split('\n'):
        if not line:
            continue
        pp = pypeg2.parse(line, Statement)
        print (pp.sql_clause(), pp.sql_target())

