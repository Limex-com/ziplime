"""Mount a dataset explicitly, pinned to a commit, and act on insider cluster buying.

The other example names its dataset inline and takes the defaults. This one mounts it in
``initialize`` instead, which is what to do when the defaults are not enough:

    context.insiders = await context.huggingface_dataset(
        "ZipLime/insider-trading", config="features", revision="ba0785ef...")

**Pinning matters more than it looks.** These datasets are append-only and grow -- an
insider-trading corpus gains rows every day the SEC accepts a Form 4. Left unpinned, the same
backtest run a month apart reads different data and produces a different number, with nothing in
the output to say why. The commit is recorded here so this example keeps returning what its
README says it returns.

The signal is the dataset's own ``is_cluster_buy``: at least three distinct insiders making
open-market purchases in a trailing 14-day window, judged by knowledge time. Insiders sell for
many reasons and buy for approximately one, and several of them buying at once is the version of
that with the least noise in it. The component counts ship alongside so a different rule can be
used; this example takes the published one as-is.

Not a recommendation: five large caps over three years is a demonstration of the mechanism, not
evidence about the signal.
"""
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

#: All five example names list on Nasdaq Global Select.
EXCHANGE = "XNGS"

STRATEGY_INFO = {
    "window": "insider",
    "description": "Buy on insider cluster buying, from a dataset pinned to a commit",
}

DATASET = "ZipLime/insider-trading"
#: Pinned deliberately -- see the module docstring.
REVISION = "ba0785efcede0b3a13af48dc658a1d39bc87ad1e"

LOOKBACK = 30
REBALANCE_EVERY_DAYS = 14


async def initialize(context: TradingAlgorithm):
    # The MIC is named because META is listed on two venues, and resolving by ticker alone would
    # have to pick one -- silently, and not necessarily the one the price bundle holds.
    context.universe = [await context.symbol(ticker, mic=EXCHANGE)
                        for ticker in ("MSFT", "NVDA", "AAPL", "AMZN", "META")]
    # Only the two columns the rule needs: the mount keeps what it is asked for and drops the
    # other 35, which matters on a config of four million rows.
    context.insiders = await context.huggingface_dataset(
        DATASET, config="features", revision=REVISION,
        fields=["is_cluster_buy", "n_unique_buyers_30d", "buy_notional_usd_30d"])
    context.last_rebalance = None
    context.reported = False


async def handle_data(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()
    if context.last_rebalance and (today - context.last_rebalance).days < REBALANCE_EVERY_DAYS:
        return
    context.last_rebalance = today

    window = await data.history(assets=context.universe, bar_count=LOOKBACK,
                                fields=["is_cluster_buy", "n_unique_buyers_30d"],
                                data_source=context.insiders)
    if window.is_empty():
        return

    # This window opens in January 2012 and META did not list until that May, so the universe is
    # narrowed to what is actually tradable today rather than assumed to be constant: a dataset
    # can carry rows for a name well before -- or long after -- it is possible to trade it.
    #
    # Two checks, because they catch different things. `can_trade` answers from the listing and
    # the calendar; it does not read prices, and a listing's recorded start date is only as good
    # as the vendor that supplied it. A quote is the ground truth, so the price is checked too.
    # Read `can_trade` positionally: it returns a pandas Series indexed by the assets themselves,
    # and indexing that by an asset object is not reliably scalar.
    tradable = data.can_trade(assets=context.universe)
    listed = [asset for asset, ok in zip(context.universe, tradable.to_numpy()) if ok]
    if not listed:
        return
    quotes = await data.current(assets=listed, fields=["price"])
    priced = {sid for sid, price in zip(quotes["sid"].to_list(), quotes["price"].to_list())
              if price and price > 0}
    live = [asset for asset in listed if asset.sid in priced]
    if not live:
        return

    clustered = {row["sid"] for row in window.iter_rows(named=True) if row["is_cluster_buy"]}
    clustered &= {asset.sid for asset in live}
    if not context.reported and clustered:
        context.reported = True
        held = [a.symbol for a in live if a.sid in clustered]
        print(f"{today} cluster buying in {', '.join(held)} "
              f"(dataset {context.insiders.revision.describe()})")

    weight = 1.0 / len(clustered) if clustered else 0.0
    for asset in live:
        await context.order_target_percent(
            asset=asset, target=weight if asset.sid in clustered else 0.0, style=MarketOrder())
