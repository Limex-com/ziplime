"""Serialized adjustments remain usable through their original public module."""

import pickle

import numpy as np

from ziplime.lib.adjustment import Float64Multiply


def test_legacy_adjustment_pickle_restores_and_applies_its_original_bounds() -> None:
    payload = b"cziplime.lib.adjustment\nFloat64Multiply\n(I0\nI1\nI0\nI0\nF2.0\ntR."
    adjustment = pickle.loads(payload)
    values = np.array([[1.0, 10.0], [2.0, 20.0], [3.0, 30.0]])

    assert type(adjustment) is Float64Multiply
    adjustment.mutate(values)

    np.testing.assert_array_equal(values, [[2.0, 10.0], [4.0, 20.0], [3.0, 30.0]])
