"""Support for convert."""

import numpy as np


NUMPY_ELEMENTWISE = {
    "abs": "abs", "absolute": "abs", "sign": "sign", "floor": "floor", "ceil": "ceil",
    "round": "round", "around": "round", "sqrt": "sqrt", "square": "square", "exp": "exp",
    "expm1": "expm1", "log": "log", "log1p": "log1p", "log2": "log2", "log10": "log10",
    "sin": "sin", "cos": "cos", "tan": "tan", "tanh": "tanh", "arctan": "arctan",
    "power": "power", "maximum": "maximum", "minimum": "minimum", "fmax": "maximum",
    "fmin": "minimum", "clip": "clip", "where": "where", "isnan": "isnan", "isinf": "isinf",
    "isfinite": "isfinite", "nan_to_num": "nan_to_num", "logical_and": "logical_and",
    "logical_or": "logical_or", "logical_not": "logical_not",
}


NUMPY_REDUCTIONS = {"mean", "sum", "min", "max", "amin", "amax", "median", "prod", "std", "var",
                    "nanmean", "nansum", "nanmin", "nanmax"}


NUMPY_CONSTANTS = {"nan", "inf", "pi", "e", "NaN", "Inf", "newaxis", "float64", "float32",
                   "int64", "int32", "bool_"}


NUMPY_SUPPORTED = set(NUMPY_ELEMENTWISE) | NUMPY_REDUCTIONS | {"trunc"} | NUMPY_CONSTANTS


_RUNTIME_INF = [np.inf]


M, DEAD, SKIP, RT, RESULT = "_gpu_m", "_gpu_dead", "_gpu_skip", "_gpu_rt", "_gpu_result"


HOOKS = {"initialize", "compute_signals", "handle_data_gpu", "analyze"}


MUTATING = {"append", "extend", "update", "pop", "insert", "add", "clear", "remove", "setdefault",
            "popitem", "discard", "sort", "reverse"}
