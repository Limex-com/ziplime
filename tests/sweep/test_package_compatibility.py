"""Recorded sweep values remain usable after reorganizing their modules."""

import pickle
from pathlib import Path
from unittest.mock import Mock

from ziplime.sweep import lazy
from ziplime.sweep.market import HistoryRows


def test_legacy_parameter_pickle_is_consumed_by_replay() -> None:
    parameter = pickle.loads(b"cziplime.sweep.lazy\nParam\n(I5\ntR.")

    assert lazy.replay(parameter, Mock(spec=HistoryRows), 0, 0, (17,)) == 17


def test_strategy_diagnostics_skip_the_reorganized_lazy_implementation() -> None:
    location = lazy.where_in_strategy()

    assert Path(__file__).name in location
