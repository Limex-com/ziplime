"""Read congressional disclosures straight from the Hub, with no ingest step.

One line mounts the dataset:

    df = await data.history(assets=..., bar_count=...,
                            data_source="hf://ZipLime/congress-trading/features")

The first call resolves the dataset to a commit, downloads the Parquet parts the simulation window
can reach, maps its tickers onto the asset database and keeps the result in memory. Every later
bar is served from there, so the cost is paid once.

**What is being read is a disclosure, not a trade.** A member of Congress reports a transaction up
to 45 days after making it, and this dataset is indexed on the day the report became public --
which is the only day a strategy could have acted on it. The lag is in the data:
``median_disclosure_lag_days`` on these rows runs from 0 to several hundred, the long tail being
annual filings that surface the following year.

The rule below is deliberately plain -- hold the names Congress has been net buying, weighted
equally -- because the example is about the plumbing, not the signal. The result is not a
recommendation, and a five-name universe over three years is far too small to conclude anything.
"""
from ziplime.api import date_rules
from ziplime.domain.bar_data import BarData
from ziplime.finance.execution import MarketOrder
from ziplime.trading.trading_algorithm import TradingAlgorithm

#: All five example names list on Nasdaq Global Select.
EXCHANGE = "XNGS"

STRATEGY_INFO = {
    "description": "Hold the names Congress has been net buying, read live from the Hub",
}

#: The dataset address. Mounted on first read; nothing is downloaded before that.
CONGRESS = "hf://ZipLime/congress-trading/features"

#: Trailing disclosure days to weigh. The dataset publishes a row only on days a name was
#: disclosed, so 40 rows is a much longer stretch of calendar than 40 sessions.
LOOKBACK = 40


async def initialize(context: TradingAlgorithm):
    # The MIC is named because META is listed on two venues, and resolving by ticker alone would
    # have to pick one -- silently, and not necessarily the one the price bundle holds.
    context.universe = [await context.symbol(ticker, mic=EXCHANGE)
                        for ticker in ("MSFT", "NVDA", "AAPL", "AMZN", "META")]
    context.reported = False
    context.schedule_function(rebalance, date_rules.month_start())


async def rebalance(context: TradingAlgorithm, data: BarData):
    today = context.simulation_dt.date()

    flow = await data.history(assets=context.universe, bar_count=LOOKBACK,
                              fields=["net_notional_usd", "n_disclosures"],
                              data_source=CONGRESS)
    if flow.is_empty():
        return

    # Net disclosed dollars per name over the window. Keyed on sid, because data.history takes a
    # set of assets and its rows come back in no particular order.
    net = {row["sid"]: row["net_notional_usd"]
           for row in flow.group_by("sid").sum().iter_rows(named=True)}
    bought = [asset for asset in context.universe if net.get(asset.sid, 0.0) > 0]

    if not context.reported:
        context.reported = True
        print(f"{today} first read of {CONGRESS}")
        for asset in context.universe:
            print(f"    {asset.symbol:6s} net disclosed over {LOOKBACK} disclosure days: "
                  f"{net.get(asset.sid, 0.0):>14,.0f} USD")

    weight = 1.0 / len(bought) if bought else 0.0
    for asset in context.universe:
        await context.order_target_percent(
            asset=asset, target=weight if asset in bought else 0.0, style=MarketOrder())
