"""The same strategy written the ordinary way, for comparison.

Identical decisions, identical orders -- and three indicators rebuilt from a fresh trailing window
on every session, for numbers a single pass already knows. This file exists to be measured
against `v01_vectorised.py`, and to prove the vectorised one is the same strategy.
"""
import datetime as dt

from ziplime.finance.execution import MarketOrder

FAST, SLOW, VOL_WINDOW, MOMENTUM = 20, 60, 20, 63
MAX_EXPOSURE = 0.60
VOL_CAP = 0.45
WINDOW = max(SLOW, MOMENTUM) + 1


async def initialize(context):
    context.universe = {
        "SPY": await context.symbol("SPY", mic="ARCX"),
        "QQQ": await context.symbol("QQQ", mic="XNMS"),
    }


async def _closes(context, data, listing):
    """The same window the signal panel's row sits at the end of: history, then today."""
    past = await data.history(assets=[listing], bar_count=WINDOW - 1, fields=["close"],
                              frequency=dt.timedelta(days=1))
    today = await data.current(assets=[listing], fields=["close"])
    return [*past["close"].to_list(), *today["close"].to_list()]


async def handle_data(context, data):
    windows = {name: await _closes(context, data, listing)
               for name, listing in context.universe.items()}
    if any(len(closes) < WINDOW for closes in windows.values()):
        return

    momentum = {name: closes[-1] / closes[-1 - MOMENTUM] - 1.0 for name, closes in windows.items()}
    ranked = sorted(momentum, key=lambda n: momentum[n], reverse=True)

    book = context.portfolio.portfolio_value
    for name, listing in context.universe.items():
        closes = windows[name]
        fast = sum(closes[-FAST:]) / FAST
        slow = sum(closes[-SLOW:]) / SLOW
        rets = [closes[i] / closes[i - 1] - 1.0 for i in range(len(closes) - VOL_WINDOW, len(closes))]
        mean = sum(rets) / len(rets)
        variance = sum((r - mean) ** 2 for r in rets) / (len(rets) - 1)
        volatility = (variance ** 0.5) * (252 ** 0.5)

        trending = fast > slow
        calm = volatility < VOL_CAP
        leader = ranked[0] == name
        held = await context.portfolio.get_asset_positions_amount(listing)

        if trending and calm and leader and held == 0:
            price = closes[-1]
            room = MAX_EXPOSURE * book - abs(context.portfolio.positions_exposure)
            amount = int(room // price)
            if amount > 0:
                await context.order(asset=listing, amount=amount, style=MarketOrder())
        elif held > 0 and not (trending and calm):
            await context.order(asset=listing, amount=-held, style=MarketOrder())
