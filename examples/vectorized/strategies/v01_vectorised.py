"""Momentum with a volatility brake: the arithmetic vectorised, the decisions left in the loop.

The split is the whole point, and it falls where you would expect once you look for it.

Vectorised, because each is a trailing window over prices and nothing else:
  * the fast and slow averages,
  * realised volatility,
  * the cross-sectional momentum rank that decides which name is the better buy today.

Left in the bar loop, because none of them can be known in advance at any price:
  * position size, which comes from portfolio value after every fill before it,
  * the exposure cap, which depends on what is already held,
  * the volatility brake, which compares today's volatility against the position it would open.

Its twin `v01_bar_by_bar.py` computes the same three signals from `data.history` on every bar. The
two place identical orders -- the runner checks it -- and one of them is four times faster.
"""
from ziplime.finance.execution import MarketOrder

FAST, SLOW, VOL_WINDOW, MOMENTUM = 20, 60, 20, 63
#: Sessions of history before the run's first bar -- enough for the *longest* window, not just the
#: slow average. Declare it short and the indicator is still NaN when the run starts, which a
#: comparison quietly turns into False rather than an error.
WARMUP = max(SLOW, MOMENTUM)

#: Stop buying above this share of the book, and never open into volatility above the cap.
MAX_EXPOSURE = 0.60
VOL_CAP = 0.45


async def initialize(context):
    context.universe = {
        "SPY": await context.symbol("SPY", mic="ARCX"),
        "QQQ": await context.symbol("QQQ", mic="XNMS"),
    }


def compute_signals(context, prices):
    """One pass over the whole history. Every line is a trailing window or element-wise.

    That is not a stylistic preference -- it is the condition under which this produces the same
    numbers as computing bar by bar, and `verify_causality` refuses the run if it is broken.
    """
    close = prices.close
    returns = close.pct_change()
    return {
        "fast": close.rolling(FAST).mean(),
        "slow": close.rolling(SLOW).mean(),
        "volatility": returns.rolling(VOL_WINDOW).std() * (252 ** 0.5),
        # Which name has run further over the last quarter. Ranked across the row, so it stays
        # causal: each bar's rank uses only that bar's values.
        "momentum": close.pct_change(MOMENTUM).rank(axis=1, ascending=False),
    }


async def handle_data(context, data):
    signals = context.signals
    if not all(signals.is_ready(name) for name in ("fast", "slow", "volatility", "momentum")):
        return

    book = context.portfolio.portfolio_value
    for name, listing in context.universe.items():
        trending = float(signals["fast"][name]) > float(signals["slow"][name])
        calm = float(signals["volatility"][name]) < VOL_CAP
        leader = float(signals["momentum"][name]) == 1.0
        held = await context.portfolio.get_asset_positions_amount(listing)

        if trending and calm and leader and held == 0:
            quote = await data.current(assets=[listing], fields=["close"])
            price = float(quote["close"][0])
            room = MAX_EXPOSURE * book - abs(context.portfolio.positions_exposure)
            amount = int(room // price)
            if amount > 0:
                await context.order(asset=listing, amount=amount, style=MarketOrder())
        elif held > 0 and not (trending and calm):
            await context.order(asset=listing, amount=-held, style=MarketOrder())
