import ast
import logging
import re
from typing import Dict, Optional

from duckdb import DuckDBPyConnection, DuckDBPyRelation

from countess import VERSION
from countess.core.parameters import TextParam
from countess.core.plugins import DuckdbSimplePlugin
from countess.utils.duckdb import duckdb_escape_identifier, duckdb_escape_literal

logger = logging.getLogger(__name__)

UNOPS = {ast.UAdd: "+", ast.USub: "-", ast.Not: "not "}
BINOPS = {
    ast.Add: "+",
    ast.Mult: "*",
    ast.Div: "/",
    ast.Sub: "-",
    ast.FloorDiv: "//",
    ast.Mod: "%",
    ast.Pow: "**",
}
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
BOOLOPS = {
    ast.And: "AND",
    ast.Or: "OR",
}

COMPOPS = {ast.Eq: "=", ast.NotEq: "!=", ast.Lt: "<", ast.LtE: "<=", ast.Gt: ">", ast.GtE: ">="}


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
        binop = BINOPS[type(ast_node.op)]
        left = _transmogrify(ast_node.left)
        right = _transmogrify(ast_node.right)
        return f"({left} {binop} {right})"
    elif type(ast_node) is ast.BoolOp and type(ast_node.op) in BOOLOPS:
        boolop = BOOLOPS[type(ast_node.op)]
        return "(" + (f" {boolop} ".join(_transmogrify(v) for v in ast_node.values)) + ")"
    elif type(ast_node) is ast.Subscript:
        value = _transmogrify(ast_node.value)
        if type(ast_node.slice) is ast.Slice:
            lower = _transmogrify(ast_node.slice.lower)
            upper = _transmogrify(ast_node.slice.upper)
            return f"({value}[{lower}:{upper}])"
        else:
            index = _transmogrify(ast_node.slice)
            return f"({value}[{index}])"
    elif type(ast_node) is ast.Compare and all(type(op) in COMPOPS for op in ast_node.ops):
        args = [_transmogrify(x) for x in [ast_node.left] + ast_node.comparators]
        comps = [args[num] + COMPOPS[type(op)] + args[num + 1] for num, op in enumerate(ast_node.ops)]
        return "(" + (" AND ".join(comps)) + ")"
    elif type(ast_node) is ast.IfExp:
        expr1 = _transmogrify(ast_node.test)
        expr2 = _transmogrify(ast_node.body)
        expr3 = _transmogrify(ast_node.orelse)
        return f"CASE WHEN {expr1} THEN {expr2} ELSE {expr3} END"
    elif type(ast_node) is ast.Call and type(ast_node.func) is ast.Name:
        args = ",".join(_transmogrify(x) for x in ast_node.args)
        if ast_node.func.id in FUNCOPS:
            return f"{ast_node.func.id}({args})"
        elif ast_node.func.id in LISTOPS:
            func = LISTOPS[ast_node.func.id]
            return f"{func}([{args}])"
        elif ast_node.func.id in CASTOPS:
            type_ = CASTOPS[ast_node.func.id]
            return f"TRY_CAST({args} as {type_})"
        else:
            raise NotImplementedError(f"Unknown Function {ast_node.func.id}")

    else:
        raise NotImplementedError(f"Unknown Node {ast_node}")


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
