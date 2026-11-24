"""Universe and time normalization for trading algorithms."""

import functools
import datetime
from typing import Callable
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.domain.continuous_future import ContinuousFuture


def _named_universe(
    universe: (
        dict[str, ExchangeAsset | ContinuousFuture]
        | list[ExchangeAsset | ContinuousFuture]
        | tuple[ExchangeAsset | ContinuousFuture, ...]
        | set[ExchangeAsset | ContinuousFuture]
        | frozenset[ExchangeAsset | ContinuousFuture]
    ),
) -> dict:
    """Accept either shape a strategy already writes its universe in.

    A dict names the panel's columns explicitly. A plain list -- which is what every strategy in
    `examples/huggingface` already builds -- is named by ticker, so vectorising one of those is a
    `compute_signals` function and nothing else.
    """
    if isinstance(universe, dict):
        return universe
    if not isinstance(universe, (list, tuple, set, frozenset)):
        raise TypeError(
            f"context.universe has to be a list of listings or a dict of name -> listing, got "
            f"{type(universe).__name__}.")

    named = {}
    for listing in universe:
        symbol = getattr(listing, "symbol", None)
        if symbol is None:
            raise TypeError(
                f"context.universe holds a {type(listing).__name__}, which has no symbol to name "
                f"a column by. Pass listings, or a dict of name -> listing.")
        if symbol in named:
            # Two venues, one ticker. Naming both "T" would silently drop one of them from the
            # panel, so the strategy has to say which name means which listing.
            raise ValueError(
                f"Two listings in context.universe are both called {symbol!r} "
                f"({named[symbol].mic} and {listing.mic}). Give them names: "
                f"context.universe = {{'{symbol}.{named[symbol].mic}': ..., "
                f"'{symbol}.{listing.mic}': ...}}.")
        named[symbol] = listing
    return named


def _as_datetime(value: datetime.date | None, tz: datetime.tzinfo, default: datetime.date) -> datetime.datetime:
    """A source's start bound as an aware datetime, however it was stored."""
    if isinstance(value, datetime.datetime):
        return value if value.tzinfo else value.replace(tzinfo=tz)
    if isinstance(value, datetime.date):
        return datetime.datetime.combine(value, datetime.time.min, tzinfo=tz)
    return datetime.datetime.combine(default, datetime.time.min, tzinfo=tz)


def _resolves_continuous_future(method: Callable):
    """Let an order method take a continuous future: it is placed in the contract held right now.

    The specifier itself is a data request, not a listing, so the order goes to the contract the
    chain's roll rule names for the current bar, and that contract is remembered as the one the
    position lives in -- :meth:`TradingAlgorithm._roll_continuous_futures` moves it when the
    front changes. ``None`` (no order) when the chain has no live contract.
    """
    @functools.wraps(method)
    async def wrapper(self, *args, **kwargs):
        if args and isinstance(args[0], ContinuousFuture):
            contract = await self._contract_for(args[0])
            if contract is None:
                return None
            args = (contract, *args[1:])
        elif isinstance(kwargs.get("asset"), ContinuousFuture):
            contract = await self._contract_for(kwargs["asset"])
            if contract is None:
                return None
            kwargs["asset"] = contract
        return await method(self, *args, **kwargs)
    return wrapper
