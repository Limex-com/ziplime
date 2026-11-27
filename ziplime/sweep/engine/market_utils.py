"""Market utils for engine."""

import numpy as np


def _subset(arguments: dict, rows: np.ndarray) -> dict:
    """The run's arguments for some of its combinations only."""
    picked = dict(arguments)
    picked["configs"] = [arguments["configs"][k] for k in rows]
    picked["states"] = [arguments["states"][k] for k in rows]
    index = arguments["signal_index"]
    if isinstance(index, dict):
        picked["signal_index"] = {name: values[rows] for name, values in index.items()}
    elif index is not None:
        picked["signal_index"] = np.asarray(index)[rows]
    else:  # one signal set per combination, in order
        picked["signals"] = {name: stack[rows] for name, stack in arguments["signals"].items()}
    picked["schedule"] = [flags[:, rows] if np.ndim(flags) == 2 else flags
                          for flags in arguments["schedule"]]
    cancel = arguments["cancel_at_session_end"]
    if isinstance(cancel, np.ndarray):
        picked["cancel_at_session_end"] = cancel[rows]
    return picked
