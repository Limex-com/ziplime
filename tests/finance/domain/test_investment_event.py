import json

import pytest

from tests.finance.investment_fixtures import KEY, START
from ziplime.finance.domain.investment_event import InvestmentEvent
from ziplime.finance.domain.investment_event_kind import InvestmentEventKind


@pytest.mark.parametrize("kind", list(InvestmentEventKind))
@pytest.mark.parametrize("serialized", [False, True])
def test_investment_event_normalizes_kinds_and_preserves_serialized_values(kind, serialized):
    event = InvestmentEvent(
        id="posting",
        dt=START,
        kind=kind.value if serialized else kind,
        key=KEY,
        currency="EUR",
    )
    assert event.kind is kind
    assert InvestmentEventKind(json.loads(json.dumps({"kind": event.kind}))["kind"]) is kind


@pytest.mark.parametrize("kind", ["opening", "dividend", "", None])
def test_investment_event_rejects_unsupported_kinds_at_construction(kind):
    with pytest.raises(ValueError):
        InvestmentEvent(id="posting", dt=START, kind=kind, key=KEY, currency="EUR")
