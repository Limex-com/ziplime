import datetime
import multiprocessing
import string
import time
from typing import Self

import structlog

import polars as pl
import yfinance as yf
from yfinance.exceptions import YFException

from ziplime.assets.domain.assets_import import AssetsImport
from ziplime.assets.entities.commodity import Commodity
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.entities.futures_root import FuturesRoot
from ziplime.data.data_sources.asset_data_source import AssetDataSource
from ziplime.data.data_sources.yahoo.yahoo_finance_constants import YAHOO_EXCHANGE_MAP
from ziplime.data.data_sources.yahoo.yahoo_futures import (
    YAHOO_FUTURES_ROOTS, YahooFuturesRoot, candidate_tickers, fallback_expiry, parse_expiry,
    parse_contract_ticker,
)


class YahooFinanceAssetDataSource(AssetDataSource):
    def __init__(self, maximum_threads: int | None = None):
        super().__init__()
        self._logger = structlog.get_logger(__name__)
        if maximum_threads is not None:
            self._maximum_threads = min(multiprocessing.cpu_count() * 2, maximum_threads)
        else:
            self._maximum_threads = multiprocessing.cpu_count() * 2

    async def get_assets(self, exchanges: list[ExchangeInfo], **kwargs) -> AssetsImport:
        exchanges_by_code = {exchange.mic: exchange for exchange in exchanges}
        lookup_letters = list(string.ascii_lowercase)
        result_df = None
        for letter in lookup_letters:
            try:
                res = yf.Lookup(letter.upper()).get_stock(count=1000)
            except YFException as e:
                time.sleep(20)
                res = yf.Lookup(letter.upper()).get_stock(count=1000)

            yahoo_df = pl.from_pandas(res, include_index=True).select("symbol", "exchange").with_columns(
                pl.col("exchange").replace({k: v["mic"] for k, v in YAHOO_EXCHANGE_MAP.items()}).alias("mic")
            )
            if result_df is None:
                result_df = yahoo_df
            else:
                result_df.extend(yahoo_df)
            self._logger.info(f"Fetched {len(result_df)} from YahooFinance with lookup for {letter.upper()}")
            print(f"Fetched {len(result_df)} from YahooFinance with lookup for {letter.upper()}")
            time.sleep(1)
        assets_df = result_df.unique()

        asset_start_date = datetime.datetime(year=1900, month=1, day=1, tzinfo=datetime.timezone.utc)
        asset_end_date = datetime.datetime(year=2099, month=1, day=1, tzinfo=datetime.timezone.utc)

        assets_df = assets_df.with_columns(pl.lit('USD').alias('currency'))

        equities = [
            Equity(
                asset_name=asset["symbol"],
                id=None,
                start_date=asset_start_date,
                end_date=asset_end_date,
                auto_close_date=asset_end_date,
                first_traded=asset_start_date,
                isin=""
            ) for asset in assets_df.iter_rows(named=True)
        ]

        currencies = [Currency(
            asset_name=currency,
            id=None,
            start_date=asset_start_date,
            end_date=asset_end_date,
            auto_close_date=asset_end_date,
            first_traded=asset_start_date,
            isin=None
        ) for currency in assets_df["currency"].unique()]

        currency_by_name = {
            c.asset_name: c for c in currencies
        }

        exchange_assets = [
            ExchangeAsset(
                sid=None,
                symbol=asset_df["symbol"],
                exchange=exchanges_by_code.get(asset_df["mic"], ExchangeInfo(mic=asset_df["mic"], name=asset_df["mic"],
                                                                             canonical_name=asset_df["mic"],
                                                                             country_code="US")),
                start_date=asset_start_date,
                end_date=asset_end_date,
                auto_close_date=asset_end_date,
                first_traded=asset_start_date,
                asset=asset,
                quote=currency_by_name[asset_df["currency"]],
                external_id=""
            )
            for asset, asset_df in zip(equities, assets_df.iter_rows(named=True))
        ]

        return AssetsImport(
            currencies=currencies, equities=equities, exchange_assets=exchange_assets
        )

    async def get_futures(self, root_symbols: list[str] | None = None,
                          months_ahead: int = 24,
                          probe_start: datetime.date | None = None,
                          **kwargs) -> AssetsImport:
        """Discover the live contract chain of each root and build its listings.

        Yahoo has no endpoint that lists a chain, so the contracts are generated from each root's
        listing cycle and probed: the ones that answer with prices are live, and the ones that do
        not have either expired -- Yahoo removes them -- or were never listed. That is the real
        chain, discovered rather than assumed.

        Contract specifications come from
        :data:`~ziplime.data.data_sources.yahoo.yahoo_futures.YAHOO_FUTURES_ROOTS`, because Yahoo
        publishes no multiplier at all. A root not in that table is skipped rather than given a
        multiplier of 1.0.

        Args:
            root_symbols: Roots to build. Defaults to every root in the specification table.
            months_ahead: How far forward to look for contracts.
            probe_start: Earliest delivery month to consider. Defaults to the current month, since
                anything earlier has expired and been removed.

        Returns:
            An :class:`AssetsImport` carrying the chains, their contracts and the listings.
        """
        wanted = [r for r in (root_symbols or YAHOO_FUTURES_ROOTS) if r in YAHOO_FUTURES_ROOTS]
        skipped = set(root_symbols or []) - set(wanted)
        if skipped:
            self._logger.warning(
                "Skipping futures roots with no contract specification: Yahoo publishes no "
                "multiplier, and assuming one would misstate every position.",
                roots=sorted(skipped))
        if not wanted:
            return AssetsImport(exchange_assets=[])

        probe_start = probe_start or datetime.date.today().replace(day=1)
        far_past = datetime.date(1900, 1, 1)
        far_future = datetime.date(2099, 1, 1)
        currencies = {
            code: Currency(id=None, isin=None, asset_name=code, start_date=far_past,
                           end_date=far_future, first_traded=far_past, auto_close_date=far_future)
            for code in sorted({YAHOO_FUTURES_ROOTS[r].quote_currency for r in wanted})
        }

        exchanges: dict[str, ExchangeInfo] = {}
        commodities: dict[str, Commodity] = {}
        roots: list[FuturesRoot] = []
        contracts: list[FuturesContract] = []
        listings: list[ExchangeAsset] = []

        for root_symbol in wanted:
            spec = YAHOO_FUTURES_ROOTS[root_symbol]
            live = self._live_contracts(spec, probe_start=probe_start, months_ahead=months_ahead)
            if not live:
                self._logger.warning("No live contracts found for a futures root",
                                     root=root_symbol)
                continue

            exchange = exchanges.setdefault(spec.mic, ExchangeInfo(
                mic=spec.mic, name=spec.mic, canonical_name=spec.mic, country_code="US"))
            # The chain needs an underlying and these roots have no tradeable one in this
            # database, so a commodity row stands in -- the shape the schema requires.
            underlying = commodities.setdefault(root_symbol, Commodity(
                id=None, isin=None, asset_name=f"{root_symbol}@{spec.mic}", start_date=far_past,
                end_date=far_future, first_traded=far_past, auto_close_date=far_future))

            roots.append(FuturesRoot(
                root_symbol=spec.root_symbol, description=spec.description, exchange=exchange,
                root_asset=underlying, multiplier=spec.multiplier, tick_size=spec.tick_size,
                quote_currency=spec.quote_currency, settlement_type=spec.settlement_type,
                margin_currency=spec.quote_currency))

            for ticker, expiry, first_bar in live:
                # A deliverable contract has to be out of the book before the delivery window, so
                # its auto close sits a few sessions before expiry, while it is still tradable.
                if spec.settlement_type.is_deliverable:
                    notice = expiry - datetime.timedelta(days=5)
                    auto_close, end_date = notice, expiry
                else:
                    notice = expiry
                    auto_close, end_date = expiry + datetime.timedelta(days=1), expiry

                contract = FuturesContract(
                    id=None, isin=None, asset_name=ticker, start_date=first_bar,
                    end_date=end_date, first_traded=first_bar, auto_close_date=auto_close,
                    root_exchange_asset=None, root_asset=underlying,
                    root_symbol=spec.root_symbol, notice_date=notice, expiration_date=expiry,
                    multiplier=spec.multiplier, tick_size=spec.tick_size,
                    settlement_type=spec.settlement_type,
                    margin_currency=spec.quote_currency)
                contracts.append(contract)
                listings.append(ExchangeAsset(
                    sid=None, symbol=ticker, start_date=first_bar, end_date=end_date,
                    first_traded=first_bar, auto_close_date=auto_close, external_id=ticker,
                    exchange=exchange, asset=contract,
                    quote=currencies[spec.quote_currency]))

        self._logger.info("Resolved Yahoo futures chains", roots=len(roots),
                          contracts=len(contracts))
        return AssetsImport(
            exchange_assets=listings, currencies=list(currencies.values()),
            commodities=list(commodities.values()), futures_roots=roots, futures=contracts)

    def _live_contracts(self, spec: YahooFuturesRoot, probe_start: datetime.date,
                        months_ahead: int) -> list[tuple[str, datetime.date, datetime.date]]:
        """Probe a root's candidate tickers and return the ones that answer with prices.

        Returns ``(ticker, expiry, first bar date)`` per live contract, ordered by expiry. The
        first bar date is read from the data rather than inferred from the expiry: a contract lists
        years before it settles, on no schedule this code could reproduce, and a listing whose
        ``start_date`` disagrees with its own bars is treated as not yet trading on the sessions in
        between.
        """
        candidates = candidate_tickers(spec, start=probe_start, months_ahead=months_ahead)
        if not candidates:
            return []
        # One batched request for the full history rather than one per candidate: it establishes
        # which contracts are live *and* when each one started trading, and a root can have two
        # dozen candidates.
        try:
            frame = yf.download(candidates, period="max", progress=False, auto_adjust=True,
                                multi_level_index=True, group_by="Ticker",
                                threads=self._maximum_threads)
        except Exception as error:
            self._logger.warning("Could not probe a futures chain", root=spec.root_symbol,
                                 error=str(error))
            return []
        if frame is None or len(frame) == 0:
            return []

        present = set(frame.columns.get_level_values(0))
        live = []
        for ticker in candidates:
            if ticker not in present:
                continue
            closes = frame[ticker]["Close"].dropna()
            if closes.empty:
                continue
            expiry = self._contract_expiry(spec, ticker)
            if expiry is None:
                continue
            live.append((ticker, expiry, closes.index[0].date()))
        live.sort(key=lambda item: item[1])
        return live

    def _contract_expiry(self, spec: YahooFuturesRoot, ticker: str) -> datetime.date | None:
        """The expiry Yahoo reports for a contract, falling back to the conventional one."""
        try:
            expiry = parse_expiry(yf.Ticker(ticker).info or {})
        except Exception as error:
            expiry = None
            self._logger.debug("No metadata for a futures contract", ticker=ticker,
                               error=str(error))
        if expiry is not None:
            return expiry

        parsed = parse_contract_ticker(ticker)
        if parsed is None:
            return None
        expiry = fallback_expiry(spec, parsed.month_code, parsed.year)
        self._logger.warning(
            "Yahoo did not report an expiry; using the conventional one, which may be off by a "
            "few days", ticker=ticker, assumed_expiry=str(expiry))
        return expiry

    async def get_futures_exchanges(self, root_symbols: list[str] | None = None
                                    ) -> list[ExchangeInfo]:
        """The exchanges the requested futures roots list on."""
        wanted = [r for r in (root_symbols or YAHOO_FUTURES_ROOTS) if r in YAHOO_FUTURES_ROOTS]
        mics = sorted({YAHOO_FUTURES_ROOTS[r].mic for r in wanted})
        return [ExchangeInfo(mic=mic, name=mic, canonical_name=mic, country_code="US")
                for mic in mics]

    async def get_constituents(self, index: str) -> pl.DataFrame:
        assets = self._limex_client.constituents(index)
        return assets

    async def get_exchanges(self, **kwargs) -> list[ExchangeInfo]:
        exchanges = [
            ExchangeInfo(mic=exchange["mic"], name=exchange["name"], canonical_name=exchange["name"],
                         country_code=exchange["country_code"])
            for exchange in YAHOO_EXCHANGE_MAP.values()
        ]
        exchanges.append(
            ExchangeInfo(mic="", name="Unknown", canonical_name="Unknown",
                         country_code="US"
                         )
        )
        return exchanges

    @classmethod
    def from_env(cls) -> Self:
        return cls()
