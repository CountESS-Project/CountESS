import logging
from typing import Optional
import ast
import re

from duckdb import DuckDBPyConnection, DuckDBPyRelation
from countess import VERSION
from countess.core.parameters import BooleanParam, PerColumnArrayParam, TextParam
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal

logger = logging.getLogger(__name__)

UNOPS = { ast.UAdd: '+', ast.USub: '-' }
BINOPS = { ast.Add: '+', ast.Mult: '*', ast.Div: '/', ast.Sub: '-', ast.FloorDiv: '//', ast.Mod: '%', ast.Pow: '**' }
FUNCOPS = {'abs', 'len', 'sin', 'cos', 'tan', 'sqrt', 'log', 'log2', 'log10', 'pow', 'exp' }
COMPOP = { ast.Eq: '=', ast.NotEq: '!=', ast.Lt: '<', ast.LtE: '<=', ast.Gt: '>', ast.GtE: '>=' }

def _transmogrify(ast_node):
    """Transform an AST node back into a string which can be parsed by DuckDB's expression
    parser.  This is a pretty small subset of all the things you might write but on the
    other hand it saved actually writing a parser."""
    # XXX might have to write a parser anyway since the AST parser handles decimal
    # literals badly.  Worry about that later.
    if type(ast_node) is ast.Name:
        return duckdb_escape_identifier(ast_node.id)
    elif type(ast_node) is ast.Constant:
        return duckdb_escape_literal(ast_node.value)
    elif type(ast_node) is ast.UnaryOp and type(ast_node.op) in UNOPS:
        return "(" + UNOPS[type(ast_node.op)] + _transmogrify(ast_node.operand) + ")"
    elif type(ast_node) is ast.BinOp and type(ast_node.op) in BINOPS:
        return "(" + _transmogrify(ast_node.left) +  BINOPS[type(ast_node.op)] + _transmogrify(ast_node.right) + ")"
    elif type(ast_node) is ast.Compare and all(type(op) in COMPOP for op in ast_node.ops):
        args = [ _transmogrify(x) for x in [ ast_node.left ] + ast_node.comparators ]
        return "(" + (" AND ".join(
            args[num] + COMPOP[type(op)] + args[num+1]
            for num, op in enumerate(ast_node.ops)
        )) + ")"
    elif type(ast_node) is ast.Call and ast_node.func.id in FUNCOPS:
        args = [ _transmogrify(x) for x in ast_node.args ]
        return ast_node.func.id + "(" + (",".join(args)) + ")"
    else:
        raise NotImplementedError(f"Unknown Node {ast_node}")


class ExpressionPlugin(DuckdbSimplePlugin):
    name = "Expression"
    description = "Apply simple expressions to each row"
    version = VERSION

    code = TextParam("Expressions")
    drop = PerColumnArrayParam("Drop Columns", BooleanParam("Drop"))
    projection = None

    def prepare(self, *a) -> None:
        super().prepare(*a)
        self.projection = []
        try:
            ast_root = ast.parse(self.code.value or "")
        except SyntaxError as exc:
            logger.debug("Syntax Error %s", exc)

        for ast_node in ast_root.body:
            try:
                if type(ast_node) is ast.Assign:
                    for ast_target in ast_node.targets:
                        if type(ast_target) is ast.Name:
                            expr = _transmogrify(ast_node.value)
                            tgt = duckdb_escape_identifier(ast_target.id)
                            self.projection.append(f"{expr} AS {tgt}")
                elif type(ast_node) is ast.Expr:
                    tgt = duckdb_escape_identifier(re.sub(r'_+$', '', re.sub(r'\W+', '_', ast.unparse(ast_node))))
                    expr = _transmogrify(ast_node.value)
                    self.projection.append(f"{expr} AS {tgt}")

            except (NotImplementedError, KeyError) as exc:
                logger.debug("Bad AST Node: %s %s", ast_node, exc)

    def execute(self, ddbc: DuckDBPyConnection, source: Optional[DuckDBPyRelation]) -> Optional  [DuckDBPyRelation]:

        column_params = dict(self.drop.get_column_params())
        if any(v.value for k, v in column_params.items()):
            projection = [
                duckdb_escape_identifier(column)
                for column in source.columns
                if not column_params[column].value
            ]
        else:
            projection = [ '*' ]

        sql = ', '.join(projection + self.projection)
        print(">>> " + sql)
        logger.debug("ExpressionPlugin.execute %s", sql)
        return source.project(sql)
