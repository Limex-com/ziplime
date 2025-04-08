import datetime

from ziplime.finance.execution import ExecutionStyle, LimitOrder, MarketOrder

from ziplime.algorithm import TradingAlgorithm
from ziplime.domain.bar_data import BarData


async def initialize(context: TradingAlgorithm):
    context.assets = [await context.symbol('AAPL')]
    context.assett = await context.symbol("AMZN")  # Apple and Amazon
    context.counter = 0


async def handle_data(context: TradingAlgorithm, data: BarData):
    context.counter += 1
    print(f"Handle data for: {context.datetime}")

    for asset in context.assets:
        if context.counter > 1:
            # long_mavg_df = data.history(
            #     assets=[asset, context.assett],
            #     fields=['close'],
            #     bar_count=10,
            #     frequency=datetime.timedelta(minutes=3)
            # )
            # asset_series = long_mavg_df.filter(long_mavg_df["sid"] == asset.sid)["close"]
            # print(asset_series.head(1))
            # submitted_order = await context.order(asset=asset, amount=10, style=LimitOrder(limit_price=1))
            submitted_order = await context.order(asset=asset, amount=1, style=MarketOrder())

            if submitted_order:
                print(f"Submitted order: {submitted_order}")
            else:
                print("Order not submitted.")
            print(context.portfolio)
            # print(asset_series)
