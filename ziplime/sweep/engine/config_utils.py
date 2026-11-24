"""Config utils for engine."""

import numpy as np
from ziplime.sweep.backends import Backend
from ziplime.sweep.engine.config import _Config


def _stack_states(states: list, backend: Backend):
    """What initialize put on the context, per combination: shared values as they are, numbers that
    differ as (combinations × 1) arrays."""
    import copy

    if not states:
        return [], {}
    from ziplime.sweep.lazy import Objects, is_object_state

    names = list(states[0])
    result = {}
    for name in names:
        values = [state.get(name) for state in states]
        if any(is_object_state(value) for value in values):
            # A fitted model and the like: one per distinct run of initialize, chosen by position
            distinct = list({id(value): value for value in values}.values())
            if len(distinct) == 1:
                result[name] = Objects(distinct)
            else:
                position = {id(value): k for k, value in enumerate(distinct)}
                result[name] = Objects(distinct, backend.array(
                    np.array([position[id(value)] for value in values], dtype=float)[:, None]))
            continue
        try:
            same = all(value == values[0] for value in values)
        except Exception:
            same = all(value is values[0] for value in values)
        if same:
            result[name] = copy.deepcopy(values[0])
        else:
            result[name] = _Config.stack(values, backend)
    return names, result
