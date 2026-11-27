"""Scheduling utils for engine."""

import os
from ziplime.sweep.backends import Backend
from ziplime.sweep.engine.support import BARS_PER_CALL
from ziplime.sweep.engine.support import BYTES_PER_ELEMENT_BAR
from ziplime.sweep.engine.support import EVALUATE_EVERY
from ziplime.sweep.engine.support import QUEUE_BUDGET


def _bars_per_call() -> int:
    return max(1, int(os.environ.get("ZIPLIME_SWEEP_BARS_PER_CALL", BARS_PER_CALL)))


def _evaluate_every(backend: Backend, elements: int) -> int:
    """How many bars MLX queues before evaluating. On Apple silicon the graph's intermediates go to
    unified memory and 32 bars is cheap; on CUDA they fill the card's own memory, so the queue is
    sized to a budget."""
    override = os.environ.get("ZIPLIME_SWEEP_EVALUATE_EVERY")
    if override:
        return max(1, int(override))
    if backend.metal or not backend.lazy:
        return EVALUATE_EVERY
    return int(min(EVALUATE_EVERY, max(1, QUEUE_BUDGET // (BYTES_PER_ELEMENT_BAR * max(1, elements)))))
