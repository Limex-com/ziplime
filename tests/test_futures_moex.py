"""Tests for the MOEX FORTS futures machinery: ticker conventions, chains and rolls.

The Finam connector is optional -- an international build may ship without it -- so everything that
depends on it is skipped rather than failing when the package is absent.
"""
import datetime
import importlib.util
import unittest

import polars as pl

from ziplime.assets.domain.continuous_future import ContinuousFuture
from ziplime.assets.domain.ordered_contracts import OrderedContracts, delivery_predicate
from ziplime.assets.domain.roll_finder import CalendarRollFinder, VolumeRollFinder
from ziplime.assets.entities.commodity import Commodity
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.futures_contract import FuturesContract
HAS_FINAM = importlib.util.find_spec("ziplime.data.data_sources.finam") is not None
if HAS_FINAM:
    from ziplime.data.data_sources.finam.finam_client import parse_decimal, to_time_frame
    from ziplime.data.data_sources.finam.moex_futures import (
        MOEX_FUTURES_ROOTS, ExpiryRule, format_ticker, generate_contract_tickers, last_trading_day,
        parse_contract_name, parse_ticker, third_thursday,
    )

RTSX = ExchangeInfo(mic="RTSX", name="MOEX derivatives", canonical_name="MOEX derivatives",
                    country_code="RU")
RUB = Currency(id=1, isin=None, asset_name="RUB", start_date=datetime.date(1900, 1, 1),
               end_date=datetime.date(2099, 1, 1), first_traded=datetime.date(1900, 1, 1),
               auto_close_date=datetime.date(2099, 1, 1))
SI_ROOT = Commodity(id=2, isin=None, asset_name="Si@RTSX", start_date=datetime.date(1900, 1, 1),
                    end_date=datetime.date(2099, 1, 1), first_traded=datetime.date(1900, 1, 1),
                    auto_close_date=datetime.date(2099, 1, 1))


def make_contract(sid: int, ticker: str, start: datetime.date,
                  expiry: datetime.date) -> ExchangeAsset:
    contract = FuturesContract(
        id=sid + 1000, isin=None, asset_name=ticker, start_date=start, end_date=expiry,
        first_traded=start, auto_close_date=expiry, root_exchange_asset=None,
        root_asset=SI_ROOT, root_symbol="Si", notice_date=expiry, expiration_date=expiry,
        multiplier=1.0, tick_size=1.0,
    )
    return ExchangeAsset(sid=sid, symbol=ticker, start_date=start, end_date=expiry,
                         first_traded=start, auto_close_date=expiry, external_id="",
                         exchange=RTSX, asset=contract, quote=RUB)


#: Three consecutive quarterly Si contracts, listed well before they expire.
CHAIN = [
    make_contract(1, "SiH5", datetime.date(2023, 3, 16), datetime.date(2025, 3, 20)),
    make_contract(2, "SiM5", datetime.date(2023, 6, 21), datetime.date(2025, 6, 19)),
    make_contract(3, "SiU5", datetime.date(2023, 9, 21), datetime.date(2025, 9, 18)),
]


class StubAssetService:
    """Minimal stand-in exposing only what a roll finder asks for."""

    def __init__(self, contracts):
        self._contracts = contracts

    async def get_ordered_contracts(self, root_symbol: str, mic: str | None = None):
        return OrderedContracts(root_symbol=root_symbol, contracts=self._contracts)


class StubBundle:
    """Bundle stub exposing the volume frame a VolumeRollFinder reads."""

    class _Calendar:
        tz = "Europe/Moscow"

    trading_calendar = _Calendar()

    def __init__(self, frame: pl.DataFrame):
        self._frame = frame

    def get_dataframe(self):
        return self._frame


@unittest.skipUnless(HAS_FINAM, "Finam connector not installed")
class MoexTickerConventionTests(unittest.TestCase):
    def test_parse_and_format_round_trip(self):
        self.assertEqual(parse_ticker("SiZ6", reference_year=2026), ("Si", 12, 2026))
        self.assertEqual(parse_ticker("SiH7", reference_year=2026), ("Si", 3, 2027))
        self.assertEqual(format_ticker("Si", 12, 2026), "SiZ6")

    def test_single_year_digit_resolves_against_a_reference_year(self):
        # The same ticker means a different decade depending on when it is read.
        self.assertEqual(parse_ticker("SiZ0", reference_year=2021)[2], 2020)
        self.assertEqual(parse_ticker("SiZ0", reference_year=2029)[2], 2030)

    def test_perpetual_tickers_carry_no_delivery_month(self):
        self.assertEqual(parse_ticker("USDRUBF"), ("USDRUBF", None, None))
        self.assertEqual(format_ticker("IMOEXF", 12, 2026), "IMOEXF")

    def test_rejects_non_futures_tickers(self):
        with self.assertRaises(ValueError):
            parse_ticker("SBER")

    def test_third_thursday_matches_observed_last_trading_days(self):
        # Verified against the last bar Finam returns for these expired contracts.
        self.assertEqual(third_thursday(2025, 12), datetime.date(2025, 12, 18))
        self.assertEqual(third_thursday(2024, 12), datetime.date(2024, 12, 19))
        self.assertEqual(third_thursday(2020, 12), datetime.date(2020, 12, 17))
        self.assertEqual(last_trading_day("Si", 12, 2025), datetime.date(2025, 12, 18))

    def test_commodity_roots_defer_their_expiry_to_the_api(self):
        # Verified against GetAsset: BR-12.26 expires 2026-12-01 and NG-12.26 on 2026-12-29,
        # neither of which any local rule reproduces, so both are marked FROM_API.
        for root_symbol in ("BR", "NG"):
            self.assertIs(MOEX_FUTURES_ROOTS[root_symbol].expiry_rule, ExpiryRule.FROM_API)

    def test_contract_name_resolves_the_decade_the_ticker_cannot(self):
        # SiZ9 is both the 2019 and the 2029 contract; only the name says which.
        self.assertEqual(parse_contract_name("Si-12.19"), (12, 2019))
        self.assertEqual(parse_contract_name("Si-12.29"), (12, 2029))
        # Older listings append the ticker in parentheses.
        self.assertEqual(parse_contract_name("Si-11.12(SiX2)"), (11, 2012))
        self.assertIsNone(parse_contract_name(""))
        self.assertIsNone(parse_contract_name("IMOEXF"))
        self.assertIsNone(parse_contract_name("Si-13.26"))

    def test_multipliers_are_the_value_of_a_price_point_not_the_contract_size(self):
        # Derived from GetAssetParams margin / risk rate divided by price, and re-checked at
        # ingestion time. Si's contract_size is 1000 ($1000) but one price point is one rouble.
        expected = {"Si": 1.0, "Eu": 1.0, "CR": 1000.0, "MX": 1.0, "MM": 10.0,
                    "SR": 1.0, "GZ": 1.0, "LK": 1.0, "RN": 1.0, "VB": 1.0, "GL": 1.0,
                    "USDRUBF": 1000.0, "CNYRUBF": 1000.0, "EURRUBF": 1000.0,
                    "IMOEXF": 10.0, "SBERF": 100.0, "GAZPF": 100.0, "GLDRUBF": 1.0}
        for root_symbol, multiplier in expected.items():
            self.assertEqual(MOEX_FUTURES_ROOTS[root_symbol].multiplier, multiplier, root_symbol)

    def test_usd_quoted_roots_are_flagged_fx_dependent(self):
        # MOEX settles their variation margin in roubles at a rate that moves daily.
        for root_symbol in ("RI", "BR", "NG", "GD", "SV"):
            root = MOEX_FUTURES_ROOTS[root_symbol]
            self.assertEqual(root.quote_currency, "USD", root_symbol)
            self.assertTrue(root.fx_dependent, root_symbol)

    def test_generated_chain_covers_the_requested_window(self):
        tickers = generate_contract_tickers("Si", datetime.date(2025, 1, 1),
                                            datetime.date(2025, 12, 31))
        self.assertIn("SiH5", tickers)
        self.assertIn("SiZ5", tickers)
        self.assertNotIn("SiZ3", tickers)


@unittest.skipUnless(HAS_FINAM, "Finam connector not installed")
class FinamClientHelperTests(unittest.TestCase):
    def test_decimal_unwrapping_handles_scientific_notation(self):
        self.assertEqual(parse_decimal({"value": "83020.0"}), 83020.0)
        self.assertEqual(parse_decimal({"value": "2.5e8"}), 2.5e8)
        self.assertIsNone(parse_decimal(None))
        self.assertIsNone(parse_decimal({"value": ""}))

    def test_frequency_maps_to_finam_timeframe(self):
        self.assertEqual(to_time_frame(datetime.timedelta(days=1)), "TIME_FRAME_D")
        self.assertEqual(to_time_frame(datetime.timedelta(minutes=1)), "TIME_FRAME_M1")
        with self.assertRaises(ValueError):
            to_time_frame(datetime.timedelta(seconds=3))


class OrderedContractsTests(unittest.TestCase):
    def test_chain_is_ordered_by_expiration(self):
        ordered = OrderedContracts("Si", list(reversed(CHAIN)))
        self.assertEqual([c.symbol for c in ordered.contracts], ["SiH5", "SiM5", "SiU5"])

    def test_contract_before_auto_close(self):
        ordered = OrderedContracts("Si", CHAIN)
        self.assertEqual(
            ordered.contract_before_auto_close(datetime.date(2025, 1, 10)).symbol, "SiH5")
        self.assertEqual(
            ordered.contract_before_auto_close(datetime.date(2025, 4, 1)).symbol, "SiM5")

    def test_contract_at_offset_walks_the_chain(self):
        ordered = OrderedContracts("Si", CHAIN)
        self.assertEqual(
            ordered.contract_at_offset(1, 1, start_cap=datetime.date(2025, 1, 10)).symbol, "SiM5")
        self.assertIsNone(ordered.contract_at_offset(3, 1, start_cap=datetime.date(2025, 1, 10)))

    def test_active_chain_excludes_contracts_not_yet_listed(self):
        ordered = OrderedContracts("Si", CHAIN)
        active = ordered.active_chain(1, datetime.date(2023, 7, 1))
        self.assertEqual([c.symbol for c in active], ["SiH5", "SiM5"])

    def test_delivery_predicate_reads_forts_and_cme_tickers(self):
        quarterly = {"H", "M", "U", "Z"}
        self.assertTrue(delivery_predicate(quarterly, CHAIN[0]))          # SiH5
        self.assertFalse(delivery_predicate({"F"}, CHAIN[0]))


class RollFinderTests(unittest.IsolatedAsyncioTestCase):
    async def test_calendar_roll_moves_ahead_of_auto_close(self):
        finder = CalendarRollFinder(asset_service=StubAssetService(CHAIN), roll_offset_days=3)
        # SiH5 expires 2025-03-20, so three days earlier the chain has already moved on.
        self.assertEqual(
            (await finder.get_contract_center("Si", datetime.date(2025, 3, 16))).symbol, "SiH5")
        self.assertEqual(
            (await finder.get_contract_center("Si", datetime.date(2025, 3, 18))).symbol, "SiM5")

    async def test_volume_roll_follows_liquidity_one_session_late(self):
        # Liquidity moves during 2025-03-11, so the roll can only act on 2025-03-12: a session's
        # volume is not known while it is still trading.
        rows = []
        for day, front_volume, back_volume in (
            (datetime.date(2025, 3, 10), 1_000.0, 100.0),
            (datetime.date(2025, 3, 11), 400.0, 900.0),   # liquidity has moved
            (datetime.date(2025, 3, 12), 300.0, 950.0),
        ):
            stamp = datetime.datetime.combine(day, datetime.time.min,
                                              tzinfo=datetime.timezone.utc)
            rows.append({"sid": 1, "date": stamp, "volume": front_volume})
            rows.append({"sid": 2, "date": stamp, "volume": back_volume})
        bundle = StubBundle(pl.DataFrame(rows).with_columns(
            pl.col("date").dt.convert_time_zone("Europe/Moscow")))

        finder = VolumeRollFinder(asset_service=StubAssetService(CHAIN), data_source=bundle,
                                  grace_period_days=0)
        self.assertEqual(
            (await finder.get_contract_center("Si", datetime.date(2025, 3, 11))).symbol, "SiH5",
            "the crossover session itself must not be used to decide")
        self.assertEqual(
            (await finder.get_contract_center("Si", datetime.date(2025, 3, 12))).symbol, "SiM5")

    async def test_rolls_are_returned_oldest_first_with_handover_dates(self):
        finder = CalendarRollFinder(asset_service=StubAssetService(CHAIN), roll_offset_days=0)
        rolls = await finder.get_rolls("Si", datetime.date(2025, 3, 1), datetime.date(2025, 7, 1))
        self.assertEqual([(c.symbol, r) for c, r in rolls],
                         [("SiH5", datetime.date(2025, 3, 20)),
                          ("SiM5", datetime.date(2025, 6, 19)),
                          ("SiU5", None)])


class ContinuousFutureTests(unittest.TestCase):
    def test_is_alive_for_session(self):
        cf = ContinuousFuture(sid=1, root_symbol="Si", offset=0, roll_style="volume",
                              start_date=datetime.date(2024, 1, 1),
                              end_date=datetime.date(2026, 1, 1),
                              exchange_info=RTSX, adjustment="mul")
        self.assertTrue(cf.is_alive_for_session(datetime.date(2025, 1, 1)))
        self.assertFalse(cf.is_alive_for_session(datetime.date(2027, 1, 1)))


if __name__ == "__main__":
    unittest.main()


class FuturesVariationMarginTests(unittest.IsolatedAsyncioTestCase):
    """Futures settle daily in cash and carry no position value."""

    def _ledger(self):
        import pandas as pd
        from ziplime.finance.domain.ledger import Ledger
        ledger = Ledger(
            trading_sessions=pd.DatetimeIndex([datetime.date(2025, 1, 2),
                                               datetime.date(2025, 1, 3)]),
            data_frequency=datetime.timedelta(days=1),
        )
        ledger._portfolio.cash = 1_000_000.0
        ledger._portfolio.portfolio_value = 1_000_000.0
        return ledger

    def _open_long(self, ledger, contract, price):
        """Open one long contract. The transaction itself creates the position."""
        from ziplime.finance.domain.transaction import Transaction
        ledger.process_transaction(Transaction(
            id="open", amount=1, dt=datetime.datetime(2025, 1, 2, tzinfo=datetime.timezone.utc),
            price=price, exchange_name="RTSX", trading_account_id="account-1", asset=contract))
        ledger.position_tracker.update_position(
            asset=contract, exchange_name="RTSX", trading_account_id="account-1",
            last_sale_price=price)

    async def test_a_losing_day_reduces_cash(self):
        # Regression: only positive payouts used to be applied, while the position's mark was
        # advanced regardless, so a long futures position could never lose money.
        ledger = self._ledger()
        contract = CHAIN[0]
        self._open_long(ledger, contract, price=100_000.0)
        cash_after_open = ledger.portfolio.cash

        ledger.position_tracker.update_position(
            asset=contract, exchange_name="RTSX", trading_account_id="account-1",
            last_sale_price=99_000.0)
        ledger._dirty_portfolio = True
        await ledger.update_portfolio()

        self.assertAlmostEqual(ledger.portfolio.cash, cash_after_open - 1_000.0, places=6)

    async def test_a_winning_day_increases_cash(self):
        ledger = self._ledger()
        contract = CHAIN[0]
        self._open_long(ledger, contract, price=100_000.0)
        cash_after_open = ledger.portfolio.cash

        ledger.position_tracker.update_position(
            asset=contract, exchange_name="RTSX", trading_account_id="account-1",
            last_sale_price=101_500.0)
        ledger._dirty_portfolio = True
        await ledger.update_portfolio()

        self.assertAlmostEqual(ledger.portfolio.cash, cash_after_open + 1_500.0, places=6)

    async def test_opening_a_future_does_not_pay_out_the_notional(self):
        # An equity purchase costs price * amount in cash; a futures position does not.
        ledger = self._ledger()
        self._open_long(ledger, CHAIN[0], price=100_000.0)
        self.assertEqual(ledger.portfolio.cash, 1_000_000.0)

    async def test_payout_uses_the_contract_multiplier(self):
        ledger = self._ledger()
        contract = make_contract(9, "SiZ5", datetime.date(2024, 1, 3),
                                 datetime.date(2025, 12, 18))
        contract = ExchangeAsset(
            **{**contract.__dict__,
               "asset": FuturesContract(**{**contract.asset.__dict__, "multiplier": 10.0})})
        self._open_long(ledger, contract, price=100.0)
        cash_after_open = ledger.portfolio.cash

        ledger.position_tracker.update_position(
            asset=contract, exchange_name="RTSX", trading_account_id="account-1",
            last_sale_price=105.0)
        ledger._dirty_portfolio = True
        await ledger.update_portfolio()

        # 5 points * 10 per point * 1 contract
        self.assertAlmostEqual(ledger.portfolio.cash, cash_after_open + 50.0, places=6)


@unittest.skipUnless(HAS_FINAM, "Finam connector not installed")
class AllAssetsPaginationTests(unittest.IsolatedAsyncioTestCase):
    """AllAssets returns 3000 rows per page; without following next_cursor it looks truncated."""

    def _client(self, pages):
        from ziplime.data.data_sources.finam.finam_client import FinamClient
        client = FinamClient(secret="test")
        calls = []

        async def fake_request(path, params=None):
            calls.append(dict(params or {}))
            return pages[len(calls) - 1]

        client._request = fake_request
        return client, calls

    async def test_follows_the_cursor_to_the_end(self):
        pages = [
            {"assets": [{"ticker": "A"}], "next_cursor": "100"},
            {"assets": [{"ticker": "B"}], "next_cursor": "200"},
            {"assets": [{"ticker": "C"}], "next_cursor": "0"},
        ]
        client, calls = self._client(pages)
        assets = await client.all_assets()
        self.assertEqual([a["ticker"] for a in assets], ["A", "B", "C"])
        self.assertEqual([c["cursor"] for c in calls], [0, 100, 200])

    async def test_stops_on_a_repeated_cursor(self):
        # A server that keeps handing back the same page must not loop forever.
        pages = [{"assets": [{"ticker": "A"}], "next_cursor": "7"},
                 {"assets": [{"ticker": "B"}], "next_cursor": "7"}]
        client, _ = self._client(pages)
        assets = await client.all_assets()
        self.assertEqual([a["ticker"] for a in assets], ["A", "B"])


@unittest.skipUnless(HAS_FINAM, "Finam connector not installed")
class ContractLifetimeWindowTests(unittest.TestCase):
    """Bars are only requested over a contract's own listing window."""

    def _source(self):
        from ziplime.data.data_sources.finam.finam_client import FinamClient
        from ziplime.data.data_sources.finam.finam_data_source import FinamDataSource
        return FinamDataSource.for_assets(client=FinamClient(secret="test"), assets=[CHAIN[0]])

    def test_request_is_clipped_to_the_listing(self):
        source = self._source()
        tz = datetime.timezone.utc
        start, end = source._window_for(
            "SiH5", datetime.datetime(2020, 1, 1, tzinfo=tz), datetime.datetime(2026, 1, 1, tzinfo=tz))
        self.assertEqual(start.date(), datetime.date(2023, 3, 16))   # listed
        self.assertEqual(end.date(), datetime.date(2025, 3, 21))     # expiry + 1 day

    def test_unknown_symbols_keep_the_requested_window(self):
        source = self._source()
        tz = datetime.timezone.utc
        requested = (datetime.datetime(2020, 1, 1, tzinfo=tz), datetime.datetime(2026, 1, 1, tzinfo=tz))
        self.assertEqual(source._window_for("UNKNOWN", *requested), requested)
