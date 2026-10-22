from collections import deque
from math import copysign, isclose

from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.finance.domain.investment_event import InvestmentEvent, InvestmentKey
from ziplime.finance.domain.investment_lot import InvestmentLot


class InvestmentProjection:
    """Mutable FIFO replay in one currency, including late fill-linked fees.

    Parameters
    ----------
    currency : str
        Reporting ISO code shared by costs, PNL, income, and fees.
    conversion : CurrencyConversionService
        Historical as-of FX service used at each event's posting time.

    Attributes
    ----------
    currency : str
        Projection currency.
    conversion : CurrencyConversionService
        Shared event-time FX service.
    lots : dict[InvestmentKey, deque[InvestmentLot]]
        Remaining signed FIFO lots by ``(exchange, account, listing)``.
        Empty queues retain closed-position accounting identities.
    realized : dict[InvestmentKey, float]
        Lifetime realized PNL including closing/allocated acquisition fees.
    income : dict[InvestmentKey, float]
        Lifetime paid income, net of liabilities.
    fees : dict[InvestmentKey, float]
        Lifetime fee charges, already included in lot costs or realized PNL.
    _fills : dict[tuple[InvestmentKey, str], tuple[float, float]]
        Original absolute fill and opening-leg quantities, before split adjustment,
        used to divide reversal fees between closing and opening units.
    _opened : dict[tuple[InvestmentKey, str], float]
        Opening-leg quantities adjusted for subsequent splits, used to allocate
        late acquisition fees between still-held units and already-realized PNL.
    """

    def __init__(self, currency: str, conversion: CurrencyConversionService):
        """Create an empty currency-specific projection and fee-allocation state."""
        self.currency = currency
        self.conversion = conversion
        self.lots: dict[InvestmentKey, deque[InvestmentLot]] = {}
        self.realized: dict[InvestmentKey, float] = {}
        self.income: dict[InvestmentKey, float] = {}
        self.fees: dict[InvestmentKey, float] = {}
        self._fills: dict[tuple[InvestmentKey, str], tuple[float, float]] = {}
        self._opened: dict[tuple[InvestmentKey, str], float] = {}

    def apply(self, event: InvestmentEvent) -> None:
        """Apply one previously validated event in chronological posting order.

        Parameters
        ----------
        event : InvestmentEvent
            Native posting validated/deduplicated by ``InvestmentBook``. Fee
            events must follow their referenced fill in the same scoped key.

        Notes
        -----
        Mutates FIFO lots, realized PNL, income, and fee totals. Shorts and
        reversals use signed cost; split quantities preserve historical money.
        Late fees are apportioned between already-closed units and remaining
        opening units. Applying the same event twice is not idempotent here.

        Raises
        ------
        ValueError
            If a fee lacks its fill ID or a required event-time FX quote is unavailable.
        KeyError
            If a fee's referenced fill was not replayed first.
        """
        key = event.key
        lots = self.lots.setdefault(key, deque())
        if event.kind == "split":
            for lot in lots:
                lot.quantity /= event.amount
            for identity in self._opened:
                if identity[0] == key:
                    self._opened[identity] /= event.amount
            return
        rate = self.conversion.rate(base=event.currency, quote=self.currency, dt=event.dt)
        if event.kind == "income":
            self.income[key] = self.income.get(key, 0.0) + event.amount * rate
            return
        if event.kind == "fee":
            if event.transaction_id is None:
                raise ValueError("Investment fees require a transaction_id.")
            identity = (key, event.transaction_id)
            original, opening = self._fills[identity]
            fee = event.amount * rate
            self.fees[key] = self.fees.get(key, 0.0) + fee
            opening_fee = fee * opening / original
            remaining = [lot for lot in lots if lot.transaction_id == event.transaction_id]
            remaining_quantity = sum(abs(lot.quantity) for lot in remaining)
            opened_quantity = self._opened[identity]
            retained_fee = opening_fee * remaining_quantity / opened_quantity if opened_quantity else 0.0
            if remaining_quantity:
                for lot in remaining:
                    lot.cost += retained_fee * abs(lot.quantity) / remaining_quantity
            self.realized[key] = self.realized.get(key, 0.0) - (fee - retained_fee)
            return
        quantity = event.quantity
        unit_value = event.unit_value * rate
        while lots and quantity * lots[0].quantity < 0:
            lot = lots[0]
            matched = min(abs(quantity), abs(lot.quantity))
            direction = copysign(1.0, lot.quantity)
            matched_cost = lot.cost * matched / abs(lot.quantity)
            pnl = direction * matched * unit_value - matched_cost
            self.realized[key] = self.realized.get(key, 0.0) + pnl
            lot.cost -= matched_cost
            lot.quantity -= direction * matched
            quantity += direction * matched
            if isclose(a=lot.quantity, b=0.0, abs_tol=1e-12):
                lots.popleft()
        identity = (key, event.id)
        self._fills[identity] = (abs(event.quantity), abs(quantity))
        self._opened[identity] = abs(quantity)
        if not isclose(a=quantity, b=0.0, abs_tol=1e-12):
            lots.append(InvestmentLot(transaction_id=event.id, quantity=quantity, cost=quantity * unit_value))
