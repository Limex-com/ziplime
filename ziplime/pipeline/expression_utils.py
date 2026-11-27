"""Expression operator tables and tuple helpers."""

from itertools import chain
from ziplime.pipeline.terms.term import Term


ops_to_methods = {
    "+": "__add__",
    "-": "__sub__",
    "*": "__mul__",
    "/": "__div__",
    "%": "__mod__",
    "**": "__pow__",
    "&": "__and__",
    "|": "__or__",
    "^": "__xor__",
    "<": "__lt__",
    "<=": "__le__",
    "==": "__eq__",
    "!=": "__ne__",
    ">=": "__ge__",
    ">": "__gt__",
}


methods_to_ops = {v: k for k, v in ops_to_methods.items()}


ops_to_commuted_methods = {
    "+": "__radd__",
    "-": "__rsub__",
    "*": "__rmul__",
    "/": "__rdiv__",
    "%": "__rmod__",
    "**": "__rpow__",
    "&": "__rand__",
    "|": "__ror__",
    "^": "__rxor__",
    "<": "__gt__",
    "<=": "__ge__",
    "==": "__eq__",
    "!=": "__ne__",
    ">=": "__le__",
    ">": "__lt__",
}


unary_ops_to_methods = {
    "-": "__neg__",
    "~": "__invert__",
}


UNARY_OPS = {"-"}


MATH_BINOPS = {"+", "-", "*", "/", "**", "%"}


FILTER_BINOPS = {"&", "|"}  # NumExpr doesn't support xor.


COMPARISONS = {"<", "<=", "!=", ">=", ">", "=="}


NUMEXPR_MATH_FUNCS = {
    "sin",
    "cos",
    "tan",
    "arcsin",
    "arccos",
    "arctan",
    "sinh",
    "cosh",
    "tanh",
    "arcsinh",
    "arccosh",
    "arctanh",
    "log",
    "log10",
    "log1p",
    "exp",
    "expm1",
    "sqrt",
    "abs",
}


def _ensure_element(tup: tuple[Term, ...], elem: Term):
    """
    Create a tuple containing all elements of tup, plus elem.

    Returns the new tuple and the index of elem in the new tuple.
    """
    try:
        return tup, tup.index(elem)
    except ValueError:
        return tuple(chain(tup, (elem,))), len(tup)


def method_name_for_op(op: str, commute: bool = False):
    """
    Get the name of the Python magic method corresponding to `op`.

    Parameters
    ----------
    op : str {'+','-','*', '/','**','&','|','^','<','<=','==','!=','>=','>'}
        The requested operation.
    commute : bool
        Whether to return the name of an equivalent method after flipping args.

    Returns
    -------
    method_name : str
        The name of the Python magic method corresponding to `op`.
        If `commute` is True, returns the name of a method equivalent to `op`
        with inputs flipped.

    Examples
    --------
    >>> method_name_for_op('+')
    '__add__'
    >>> method_name_for_op('+', commute=True)
    '__radd__'
    >>> method_name_for_op('>')
    '__gt__'
    >>> method_name_for_op('>', commute=True)
    '__lt__'
    """
    if commute:
        return ops_to_commuted_methods[op]
    return ops_to_methods[op]


def unary_op_name(op: str):
    return unary_ops_to_methods[op]


def is_comparison(op: str):
    return op in COMPARISONS
