import datetime
import json

import pytest

from ziplime.finance.domain.cash_event import CashEvent
from ziplime.finance.domain.cash_event_kind import CashEventKind


@pytest.mark.parametrize("kind", list(CashEventKind))
@pytest.mark.parametrize("serialized", [False, True])
def test_cash_event_normalizes_kinds_and_preserves_serialized_values(kind, serialized):
    event = CashEvent(
        id="posting",
        dt=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
        key=("MOEX", "account", "EUR"),
        amount=100,
        kind=kind.value if serialized else kind,
    )
    assert event.kind is kind
    assert CashEventKind(json.loads(json.dumps({"kind": event.kind}))["kind"]) is kind


@pytest.mark.parametrize("kind", ["trade", "withdrawal", "", None])
def test_cash_event_rejects_unsupported_kinds_at_construction(kind):
    with pytest.raises(ValueError):
        CashEvent(
            id="posting",
            dt=datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc),
            key=("MOEX", "account", "EUR"),
            amount=100,
            kind=kind,
        )
