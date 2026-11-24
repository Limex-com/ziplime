"""Roots for yahoo_futures."""

from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.data.data_sources.yahoo.yahoo_futures.support import GRAIN
from ziplime.data.data_sources.yahoo.yahoo_futures.support import MONTHLY
from ziplime.data.data_sources.yahoo.yahoo_futures.support import QUARTERLY
from ziplime.data.data_sources.yahoo.yahoo_futures.yahoo_futures_root import YahooFuturesRoot


YAHOO_FUTURES_ROOTS: dict[str, YahooFuturesRoot] = {
    "ES": YahooFuturesRoot(
        root_symbol="ES", description="E-mini S&P 500", mic="XCME", exchange_suffix=".CME",
        multiplier=50.0, tick_size=0.25, contract_months=QUARTERLY,
        settlement_type=SettlementType.CASH),
    "NQ": YahooFuturesRoot(
        root_symbol="NQ", description="E-mini Nasdaq 100", mic="XCME", exchange_suffix=".CME",
        multiplier=20.0, tick_size=0.25, contract_months=QUARTERLY,
        settlement_type=SettlementType.CASH),
    "CL": YahooFuturesRoot(
        root_symbol="CL", description="WTI crude oil", mic="XNYM", exchange_suffix=".NYM",
        multiplier=1_000.0, tick_size=0.01, contract_months=MONTHLY),
    "NG": YahooFuturesRoot(
        root_symbol="NG", description="Henry Hub natural gas", mic="XNYM", exchange_suffix=".NYM",
        multiplier=10_000.0, tick_size=0.001, contract_months=MONTHLY),
    "GC": YahooFuturesRoot(
        root_symbol="GC", description="Gold", mic="XCEC", exchange_suffix=".CMX",
        multiplier=100.0, tick_size=0.10, contract_months=MONTHLY,
        reliable_continuous_volume=False),
    "SI": YahooFuturesRoot(
        root_symbol="SI", description="Silver", mic="XCEC", exchange_suffix=".CMX",
        multiplier=5_000.0, tick_size=0.005, contract_months=MONTHLY,
        reliable_continuous_volume=False),
    "ZC": YahooFuturesRoot(
        # Corn is quoted in cents per bushel on a 5 000 bushel contract, so one price point is
        # 5000/100 = 50 dollars. Reading the contract size as the multiplier overstates it 100x.
        root_symbol="ZC", description="Corn", mic="XCBT", exchange_suffix=".CBT",
        multiplier=50.0, tick_size=0.25, contract_months=GRAIN),
    "6E": YahooFuturesRoot(
        root_symbol="6E", description="Euro FX", mic="XCME", exchange_suffix=".CME",
        multiplier=125_000.0, tick_size=0.00005, contract_months=QUARTERLY),
}


ROOTS_BY_CONTINUOUS_TICKER = {r.continuous_ticker: r for r in YAHOO_FUTURES_ROOTS.values()}
