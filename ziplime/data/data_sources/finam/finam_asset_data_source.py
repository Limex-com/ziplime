"""Reference-data source that builds MOEX assets from the Finam Trade API.

Covers two instrument families:

* **FORTS futures** -- :meth:`FinamAssetDataSource.get_assets`, enumerated from ``AllAssets`` and
  specified from the local root table plus ``GetAsset``.
* **Bonds** -- :meth:`FinamAssetDataSource.get_bonds`, enumerated from the same ``AllAssets`` and
  specified from each issue's realised calendar (``GetPastBondsEvents``); see
  :mod:`ziplime.data.data_sources.finam.finam_bonds` for how the terms are read off it.
"""
import asyncio
import datetime
from typing import Any, Self

import polars as pl
import structlog

from ziplime.assets.domain.assets_import import AssetsImport
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.bond_event import BondEvent
from ziplime.assets.entities.commodity import Commodity
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.exchange_info import ExchangeInfo
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.entities.futures_root import FuturesRoot
from ziplime.data.data_sources.asset_data_source import AssetDataSource
from ziplime.data.data_sources.finam.finam_client import (
    FinamClient, parse_date, parse_decimal,
)
from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.data.data_sources.finam.finam_bonds import (
    MISX_MIC, build_bond, is_bond_listing,
)
from ziplime.data.data_sources.finam.cme_futures import (
    CME_FUTURES_ROOTS, DELIVERY_NOTICE_SESSIONS, cme_fallback_expiry, parse_cme_contract_name,
    parse_cme_ticker,
)
from ziplime.data.data_sources.finam.moex_futures import (
    MOEX_FUTURES_ROOTS,
    RTSX_MIC,
    ExpiryRule,
    MoexFuturesRoot,
    last_trading_day,
    parse_contract_name,
    parse_ticker,
    root_symbol_of,
)

#: Every root this connector knows, across venues. Keyed by ziplime root symbol; each entry carries
#: its own MIC, so one ingest can span MOEX and the CME contracts Finam mirrors. Non-MOEX roots are
#: namespaced ``CODE.MIC`` because a venue's own code is not unique -- ``NG`` is natural gas on both.
DEFAULT_FUTURES_ROOTS: dict[str, MoexFuturesRoot] = {**MOEX_FUTURES_ROOTS, **CME_FUTURES_ROOTS}

#: Country per exchange, for the ExchangeInfo rows written to the asset database.
MIC_COUNTRY = {"RTSX": "RU", "MISX": "RU", "XNYM": "US", "XCEC": "US", "XCME": "US", "XCBT": "US"}

#: Currency MOEX quotes and settles rouble bonds in.
DEFAULT_BOND_CURRENCY = "RUB"

#: FORTS lists a contract roughly two years before it expires, so a contract that expired this
#: long before the window may still have traded inside it.
LISTING_LEAD = datetime.timedelta(days=760)

#: A **cash-settled** contract is liquidated the session after it stops trading. Setting auto_close
#: to the expiration date itself would have the engine close the position at the start of the last
#: trading day, before the strategy could trade or roll on it.
AUTO_CLOSE_LAG = datetime.timedelta(days=1)


def subtract_business_days(day: datetime.date, count: int) -> datetime.date:
    """Step back ``count`` weekdays from ``day``.

    An approximation of the exchange calendar -- it does not know holidays -- used only to place
    the notice date of a deliverable contract a few sessions before it stops trading.
    """
    result = day
    while count > 0:
        result -= datetime.timedelta(days=1)
        if result.weekday() < 5:
            count -= 1
    return result

#: A multiplier is accepted if the exchange's margin implies a notional within this factor of
#: ``price * multiplier``. Margin carries a broker buffer, so the tolerance is wide -- it is meant
#: to catch a wrong power of ten, not to measure anything.
MULTIPLIER_TOLERANCE = 2.0


class FinamAssetDataSource(AssetDataSource):
    """Builds :class:`AssetsImport` batches of MOEX FORTS futures.

    Contracts are enumerated from ``AllAssets``, which pages through every instrument Finam knows
    including archived ones, so expired contracts -- most of what a backtest needs -- come with
    their real identity rather than a guessed ticker. Expiration dates come from ``GetAsset``,
    which answers for archived contracts too.

    Price multipliers come from
    :data:`~ziplime.data.data_sources.finam.moex_futures.MOEX_FUTURES_ROOTS`, because the API's
    ``contract_size`` describes the underlying rather than the value of a price point. When the
    exchange publishes margin for a contract, the multiplier is checked against it and a mismatch
    is logged.

    Args:
        client: Configured :class:`FinamClient`.
        root_overrides: Per-root specification overrides, merged over ``MOEX_FUTURES_ROOTS``.
        verify_multipliers: Cross-check multipliers against exchange margin during ingestion.
    """

    def __init__(self, client: FinamClient,
                 root_overrides: dict[str, MoexFuturesRoot] | None = None,
                 verify_multipliers: bool = True,
                 logger=None):
        super().__init__()
        self._client = client
        self._roots = {**DEFAULT_FUTURES_ROOTS, **(root_overrides or {})}
        self._verify_multipliers = verify_multipliers
        #: One margin-currency lookup per root; the endpoint refuses archived contracts anyway.
        self._margin_currency_cache: dict[str, str] = {}
        self._logger = logger or structlog.get_logger(__name__)

    @classmethod
    def from_env(cls, **kwargs) -> Self:
        return cls(client=FinamClient.from_env(), **kwargs)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def get_exchanges(self, root_symbols: list[str] | None = None,
                            **kwargs) -> list[ExchangeInfo]:
        """Return an :class:`ExchangeInfo` per venue the configured roots trade on."""
        exchanges = await self._client.exchanges()
        by_mic = {exchange["mic"]: exchange for exchange in exchanges}
        mics = sorted({self._roots[r].mic for r in (root_symbols or self._roots)
                       if r in self._roots})
        result = []
        for mic in mics or [RTSX_MIC]:
            raw = by_mic.get(mic, {"mic": mic, "name": mic})
            result.append(ExchangeInfo(mic=mic, name=raw["name"], canonical_name=raw["name"],
                                       country_code=MIC_COUNTRY.get(mic, "US")))
        return result

    async def get_assets(self, exchanges: list[ExchangeInfo],
                         root_symbols: list[str] | None = None,
                         start_date: datetime.date | None = None,
                         end_date: datetime.date | None = None,
                         **kwargs) -> AssetsImport:
        """Build every FORTS contract of the requested roots that traded in the date range.

        Args:
            exchanges: Exchanges to attach listings to; matched per root by MIC.
            root_symbols: Roots to ingest. Defaults to every root in the specification table.
            start_date: Earliest date of interest. Contracts that expired before it are skipped.
            end_date: Latest date of interest. Contracts listed after it are skipped.
        """
        # One exchange per venue: a root's listings must carry its own MIC, not the first one.
        exchanges_by_mic = {e.mic: e for e in exchanges}
        today = datetime.date.today()
        end_date = end_date or today
        start_date = start_date or (today - datetime.timedelta(days=730))
        root_symbols = [r for r in (root_symbols or self._roots) if r in self._roots]
        for mic in {self._roots[r].mic for r in root_symbols}:
            if mic not in exchanges_by_mic:
                exchanges_by_mic.update(
                    {e.mic: e for e in await self.get_exchanges(root_symbols=root_symbols)})
                break

        far_past = datetime.date(1900, 1, 1)
        far_future = datetime.date(2099, 1, 1)

        currencies = {
            name: Currency(id=None, isin=None, asset_name=name, start_date=far_past,
                           end_date=far_future, first_traded=far_past, auto_close_date=far_future)
            for name in sorted({self._roots[r].quote_currency for r in root_symbols})
        }

        # FORTS roots have no tradeable underlying of their own in this database, and
        # futures_contracts.root_asset_id is NOT NULL, so each chain gets a commodity standing in.
        commodities = {
            root_symbol: Commodity(id=None, isin=None,
                                   asset_name=f"{root_symbol}@{self._roots[root_symbol].mic}",
                                   start_date=far_past, end_date=far_future,
                                   first_traded=far_past, auto_close_date=far_future)
            for root_symbol in root_symbols
        }

        futures_roots = [
            FuturesRoot(
                root_symbol=root_symbol,
                description=self._roots[root_symbol].description,
                exchange=exchanges_by_mic[self._roots[root_symbol].mic],
                root_asset=commodities[root_symbol],
                multiplier=self._roots[root_symbol].multiplier,
                tick_size=self._roots[root_symbol].tick_size,
                quote_currency=self._roots[root_symbol].quote_currency,
                settlement_type=self._roots[root_symbol].settlement_type,
                margin_currency=self._roots[root_symbol].margin_currency,
            )
            for root_symbol in root_symbols
        ]

        listings = await self._discover_contracts(root_symbols=root_symbols,
                                                  start_date=start_date, end_date=end_date)
        self._logger.info("Discovered FORTS contracts", contracts=len(listings),
                          roots=len(futures_roots))

        if self._verify_multipliers:
            # Margin is published only for listed contracts, and it describes the root rather than
            # the individual contract, so one check per root is both sufficient and much cheaper.
            await asyncio.gather(*(
                self._verify_multiplier(root=self._roots[root_symbol], listings=listings)
                for root_symbol in root_symbols
            ))

        contracts = await asyncio.gather(*(
            self._build_contract(listing=listing, root=self._roots[root_symbol],
                                 root_asset=commodities[root_symbol])
            for root_symbol, listing in listings
        ))

        futures: list[FuturesContract] = []
        exchange_assets: list[ExchangeAsset] = []
        for (root_symbol, listing), contract in zip(listings, contracts):
            if contract is None:
                continue
            futures.append(contract)
            exchange_assets.append(
                ExchangeAsset(
                    sid=None,
                    symbol=listing["ticker"],
                    start_date=contract.start_date,
                    end_date=contract.end_date,
                    first_traded=contract.first_traded,
                    auto_close_date=contract.auto_close_date,
                    external_id=str(listing.get("id", "")),
                    exchange=exchanges_by_mic[self._roots[root_symbol].mic],
                    asset=contract,
                    quote=currencies[self._roots[root_symbol].quote_currency],
                )
            )

        self._logger.info("Resolved FORTS contracts", contracts=len(futures))
        return AssetsImport(
            exchange_assets=exchange_assets,
            currencies=list(currencies.values()),
            commodities=list(commodities.values()),
            futures_roots=futures_roots,
            futures=futures,
        )

    async def get_bonds(self, exchanges: list[ExchangeInfo] | None = None,
                        tickers: list[str] | None = None,
                        isins: list[str] | None = None,
                        mic: str = MISX_MIC,
                        start_date: datetime.date | None = None,
                        end_date: datetime.date | None = None,
                        include_matured: bool = False,
                        limit: int | None = None,
                        quote_currency: str | None = None,
                        **kwargs) -> AssetsImport:
        """Build bonds and their schedules from ``AllAssets`` plus each issue's calendar.

        Every bond costs one ``GetPastBondsEvents`` call (several, for a long-lived issue with
        quarterly coupons), and MOEX lists tens of thousands of them, so a selection is required in
        practice: name the ``tickers`` or ``isins`` you want, or cap the sweep with ``limit``.

        A **redeemed issue cannot be ingested**: both calendar endpoints answer with nothing for an
        archived listing, so there is no schedule, no nominal and no maturity to be had. Every
        archived bond sampled behaved this way, which is why ``include_matured`` defaults to False
        -- asking for them costs two requests each and returns nothing. A live bond's *history* is
        fully available, so backtests work; a universe of already-redeemed bonds is not obtainable
        from this API.

        Args:
            exchanges: Exchanges to attach listings to. Fetched if not supplied.
            tickers: Restrict to these tickers. Case-insensitive.
            isins: Restrict to these ISINs.
            mic: Exchange to ingest from; MOEX main market by default.
            start_date: Skip issues that matured before this date -- they cannot appear in a
                backtest that starts on it.
            end_date: Skip issues first traded after this date.
            include_matured: Also try archived listings. They have no calendar, so this normally
                only wastes requests; it exists so the behaviour can be re-checked.
            limit: Stop after this many bonds. Without ``tickers``/``isins`` this is the only
                thing standing between an ingest and every listed issue on MOEX.
            quote_currency: Force a currency instead of reading it from each bond's payments.

        Returns:
            An :class:`AssetsImport` carrying the bonds, their events and their listings.
        """
        exchanges_by_mic = {e.mic: e for e in (exchanges or [])}
        if mic not in exchanges_by_mic:
            exchanges_by_mic.update({e.mic: e for e in await self.get_bond_exchanges(mic=mic)})

        listings = await self._discover_bonds(
            mic=mic, tickers=tickers, isins=isins, include_matured=include_matured, limit=limit)
        self._logger.info("Discovered bond listings", listings=len(listings), mic=mic)

        far_past = datetime.date(1900, 1, 1)
        far_future = datetime.date(2099, 1, 1)
        currencies: dict[str, Currency] = {}

        def currency_for(code: str) -> Currency:
            # One Currency entity per code actually seen: MOEX lists rouble and dollar issues side
            # by side, and a listing has to be quoted in its own.
            if code not in currencies:
                currencies[code] = Currency(
                    id=None, isin=None, asset_name=code, start_date=far_past,
                    end_date=far_future, first_traded=far_past, auto_close_date=far_future)
            return currencies[code]

        built = await asyncio.gather(*(
            self._build_bond(listing=listing, mic=mic, quote_currency=quote_currency)
            for listing in listings
        ))

        bonds: list[Bond] = []
        bond_events: list[BondEvent] = []
        exchange_assets: list[ExchangeAsset] = []
        skipped = 0
        for listing, result in zip(listings, built):
            if result is None:
                skipped += 1
                continue
            bond, events = result
            if start_date is not None and bond.maturity_date < start_date:
                continue
            if end_date is not None and bond.start_date > end_date:
                continue
            bonds.append(bond)
            bond_events.extend(events)
            exchange_assets.append(ExchangeAsset(
                sid=None,
                symbol=listing["ticker"],
                start_date=bond.start_date,
                end_date=bond.end_date,
                first_traded=bond.first_traded,
                auto_close_date=bond.auto_close_date,
                external_id=str(listing.get("id", "")),
                exchange=exchanges_by_mic[mic],
                asset=bond,
                quote=currency_for(bond.quote_currency),
            ))

        # Said out loud rather than left to be inferred from a short result: an archived listing
        # has no calendar, so a sweep that includes them drops most of what it asked for.
        self._logger.info("Resolved bonds", bonds=len(bonds), events=len(bond_events),
                          skipped_without_calendar=skipped)
        return AssetsImport(
            exchange_assets=exchange_assets,
            currencies=list(currencies.values()),
            bonds=bonds,
            bond_events=bond_events,
        )

    async def get_bond_exchanges(self, mic: str = MISX_MIC) -> list[ExchangeInfo]:
        """Return the :class:`ExchangeInfo` of the venue bonds are ingested from."""
        by_mic = {exchange["mic"]: exchange for exchange in await self._client.exchanges()}
        raw = by_mic.get(mic, {"mic": mic, "name": mic})
        return [ExchangeInfo(mic=mic, name=raw["name"], canonical_name=raw["name"],
                             country_code=MIC_COUNTRY.get(mic, "RU"))]

    async def _discover_bonds(self, mic: str, tickers: list[str] | None,
                              isins: list[str] | None, include_matured: bool,
                              limit: int | None) -> list[dict[str, Any]]:
        """Select the bond rows of interest out of ``AllAssets``."""
        wanted_tickers = {t.upper() for t in tickers} if tickers else None
        wanted_isins = {i.upper() for i in isins} if isins else None

        found = []
        for listing in await self._client.all_assets():
            if not is_bond_listing(listing) or listing.get("mic") != mic:
                continue
            if not include_matured and listing.get("is_archived"):
                continue
            ticker = (listing.get("ticker") or "").upper()
            isin = (listing.get("isin") or "").upper()
            if wanted_tickers is not None and ticker not in wanted_tickers:
                continue
            if wanted_isins is not None and isin not in wanted_isins:
                continue
            found.append(listing)

        found.sort(key=lambda listing: listing.get("ticker") or "")
        if not found:
            self._logger.warning("No bond listings matched", mic=mic, tickers=tickers,
                                 isins=isins, include_matured=include_matured)
        if limit is not None and len(found) > limit:
            self._logger.warning("Capping the bond sweep", requested=limit, available=len(found))
            found = found[:limit]
        return found

    async def _build_bond(self, listing: dict[str, Any], mic: str,
                          quote_currency: str | None) -> tuple[Bond, list[BondEvent]] | None:
        """Fetch one bond's whole calendar and specification, and assemble the entities.

        Both halves of the calendar are needed: ``past`` stops at today and ``future`` starts
        there, and the redemption -- which is what dates the bond's maturity -- only appears in the
        forward half.
        """
        symbol = f"{listing['ticker']}@{mic}"
        try:
            events = await self._client.bond_events(symbol=symbol)
        except Exception as error:
            self._logger.warning("Could not read a bond calendar", symbol=symbol, error=str(error))
            return None
        details = None
        try:
            details = await self._client.get_asset(symbol)
        except Exception as error:
            # Specifications are a bonus; the calendar alone is enough to derive the terms.
            self._logger.debug("Bond specification unavailable", symbol=symbol, error=str(error))
        return build_bond(listing=listing, events=events, details=details,
                          quote_currency=quote_currency, logger=self._logger)

    def _root_symbol_of(self, ticker: str, mic: str) -> str | None:
        """Return the root a listing belongs to, using that venue's ticker convention."""
        if mic == RTSX_MIC:
            return root_symbol_of(ticker)
        parsed = parse_cme_ticker(ticker, mic=mic)
        return parsed[0] if parsed else None

    def _month_year_of(self, root: MoexFuturesRoot, ticker: str, name: str
                       ) -> tuple[int, int] | None:
        """Read the delivery month and year, preferring the name over the ticker."""
        if root.mic == RTSX_MIC:
            return parse_contract_name(name)
        return parse_cme_contract_name(name) or (
            (lambda p: (p[1], p[2]) if p else None)(parse_cme_ticker(ticker, mic=root.mic)))

    def _fallback_expiry(self, root: MoexFuturesRoot, month: int, year: int) -> datetime.date:
        """Expiry to use when the API supplies none."""
        if root.mic == RTSX_MIC:
            return last_trading_day(root.root_symbol, month, year)
        return cme_fallback_expiry(month, year)

    async def _discover_contracts(self, root_symbols: list[str],
                                  start_date: datetime.date,
                                  end_date: datetime.date) -> list[tuple[str, dict[str, Any]]]:
        """Return ``(root_symbol, listing)`` for every contract of interest, oldest first.

        Contracts are matched by ticker root and dated from their name, which carries a two-digit
        year -- the ticker's single year digit cannot tell ``SiZ9`` of 2019 from ``SiZ9`` of 2029.
        """
        wanted = set(root_symbols)
        assets = await self._client.all_assets()

        found: list[tuple[str, dict[str, Any], datetime.date | None]] = []
        undated = 0
        wanted_mics = {self._roots[r].mic for r in wanted}
        for asset in assets:
            mic = asset.get("mic")
            if asset.get("type") != "FUTURES" or mic not in wanted_mics:
                continue
            ticker = asset.get("ticker") or ""
            root_symbol = self._root_symbol_of(ticker, mic)
            if root_symbol not in wanted or self._roots[root_symbol].mic != mic:
                continue

            root = self._roots[root_symbol]
            if root.expiry_rule is ExpiryRule.PERPETUAL:
                found.append((root_symbol, asset, None))
                continue

            month_year = self._month_year_of(root, ticker, asset.get("name") or "")
            if month_year is None:
                # The oldest listings (2009-2010) carry no name; the ticker alone cannot place them
                # in a decade, so they are skipped rather than guessed at.
                undated += 1
                continue
            month, year = month_year
            expiry = self._fallback_expiry(root, month, year)
            # Keep anything that could have traded inside the window.
            if expiry < start_date or expiry - LISTING_LEAD > end_date:
                continue
            found.append((root_symbol, asset, expiry))

        if undated:
            self._logger.warning("Skipped undated FORTS listings", count=undated)
        found.sort(key=lambda item: (item[2] or datetime.date(2099, 1, 1), item[1]["ticker"]))
        return [(root_symbol, asset) for root_symbol, asset, _ in found]

    async def _build_contract(self, listing: dict[str, Any], root: MoexFuturesRoot,
                              root_asset: Commodity) -> FuturesContract | None:
        """Assemble a :class:`FuturesContract` from a listing plus its API details."""
        ticker = listing["ticker"]
        name = listing.get("name") or ticker
        multiplier, tick_size = root.multiplier, root.tick_size
        expiration: datetime.date | None = None

        details = await self._client.get_asset(f"{ticker}@{root.mic}")
        if details and details.get("ticker"):
            name = details.get("name") or name
            expiration = parse_date(details.get("expiration_date"))
            # Archived contracts come back with min_step=0; only trust a positive step.
            min_step = details.get("min_step")
            decimals = details.get("decimals")
            if min_step and float(min_step) > 0 and decimals is not None:
                tick_size = float(min_step) / (10 ** int(decimals))

        if expiration is None:
            if root.expiry_rule is ExpiryRule.PERPETUAL:
                expiration = datetime.date(2099, 1, 1)
            else:
                month_year = self._month_year_of(root, ticker, name)
                if not month_year or month_year[0] is None:
                    self._logger.warning("No expiration for contract, skipping", ticker=ticker)
                    return None
                month, year = month_year
                expiration = self._fallback_expiry(root, month, year)
                if root.expiry_rule is ExpiryRule.FROM_API:
                    self._logger.warning(
                        "Falling back to the quarterly expiry rule for a root whose expiry the "
                        "API normally supplies; the date may be wrong",
                        ticker=ticker, expiration=expiration)

        # FORTS lists a contract about two years ahead; the exact first traded day is settled by
        # the market-data ingest, which only writes sessions that actually have bars.
        first_traded = (expiration - LISTING_LEAD
                        if root.expiry_rule is not ExpiryRule.PERPETUAL
                        else datetime.date(1900, 1, 1))

        if root.is_deliverable:
            # A deliverable contract has to be out of the book before the delivery window opens,
            # so the notice date sits a few sessions ahead of the last trading day and auto close
            # lands on it -- while the contract is still tradable, at a real price.
            notice_date = subtract_business_days(expiration, DELIVERY_NOTICE_SESSIONS)
            auto_close_date = notice_date
            end_date = expiration
        else:
            notice_date = expiration
            auto_close_date = expiration + AUTO_CLOSE_LAG
            end_date = expiration

        margin_currency = await self._margin_currency(ticker=ticker, root=root)

        return FuturesContract(
            id=None,
            isin=listing.get("isin") or None,
            asset_name=name,
            start_date=first_traded,
            # end_date is the last tradable session; auto_close is when an open position is
            # liquidated -- after it for a cash-settled contract, before it for a deliverable one.
            end_date=end_date,
            first_traded=first_traded,
            auto_close_date=auto_close_date,
            root_exchange_asset=None,
            root_asset=root_asset,
            root_symbol=root.root_symbol,
            notice_date=notice_date,
            expiration_date=expiration,
            multiplier=multiplier,
            tick_size=tick_size,
            settlement_type=root.settlement_type,
            margin_currency=margin_currency,
        )

    async def _margin_currency(self, ticker: str, root: MoexFuturesRoot) -> str:
        """Return the currency the exchange collects margin in.

        Taken from ``GetAssetParams`` when it answers -- ``long_initial_margin.currency_code`` is
        authoritative and shows, for instance, that MOEX collects roubles for a contract it quotes
        in dollars. Archived contracts are refused by that endpoint, so the root's configured
        currency is the fallback.
        """
        if root.root_symbol in self._margin_currency_cache:
            return self._margin_currency_cache[root.root_symbol]
        currency = root.margin_currency
        try:
            params = await self._client.get_asset_params(f"{ticker}@{root.mic}")
            reported = ((params or {}).get("long_initial_margin") or {}).get("currency_code")
            if reported:
                if reported != root.margin_currency:
                    self._logger.warning(
                        "Exchange margin currency differs from the configured one",
                        root=root.root_symbol, configured=root.margin_currency, reported=reported)
                currency = reported
        except Exception as error:
            self._logger.debug("Margin currency lookup skipped", ticker=ticker, error=str(error))
        self._margin_currency_cache[root.root_symbol] = currency
        return currency

    async def _verify_multiplier(self, root: MoexFuturesRoot,
                                 listings: list[tuple[str, dict[str, Any]]]) -> None:
        """Warn when the exchange's margin disagrees with a root's configured multiplier.

        ``long_initial_margin / long_risk_rate`` is the notional the exchange is collateralising.
        Dividing it by the traded price gives the value of one price point, which should match the
        multiplier. Margin is refused for archived securities, so the check runs against a contract
        that is still listed; USD-quoted contracts are margined in roubles, so their implied value
        carries an FX factor and is skipped.
        """
        if root.fx_dependent:
            return
        ticker = next((listing["ticker"] for root_symbol, listing in listings
                       if root_symbol == root.root_symbol and not listing.get("is_archived")), None)
        if ticker is None:
            return
        multiplier = root.multiplier
        try:
            params = await self._client.get_asset_params(f"{ticker}@{root.mic}")
            if not params:
                return
            margin = params.get("long_initial_margin") or {}
            notional = float(margin.get("units", 0)) + float(margin.get("nanos", 0)) / 1e9
            risk_rate = float((params.get("long_risk_rate") or {}).get("value") or 0)
            if not notional or not risk_rate:
                return
            bars = await self._client.bars(
                f"{ticker}@{root.mic}", "TIME_FRAME_D",
                datetime.datetime.now(tz=datetime.timezone.utc) - datetime.timedelta(days=10),
                datetime.datetime.now(tz=datetime.timezone.utc),
            )
            if not bars:
                return
            price = parse_decimal(bars[-1]["close"])
            if not price:
                return
            implied = (notional / (risk_rate / 100.0)) / price
            if not (1 / MULTIPLIER_TOLERANCE) <= implied / multiplier <= MULTIPLIER_TOLERANCE:
                self._logger.warning(
                    "Configured multiplier disagrees with exchange margin",
                    ticker=ticker, root=root.root_symbol, configured=multiplier,
                    implied_by_margin=round(implied, 4))
            else:
                self._logger.debug("Multiplier agrees with exchange margin",
                                   root=root.root_symbol, configured=multiplier,
                                   implied_by_margin=round(implied, 4))
        except Exception as error:  # verification must never break an ingest
            self._logger.debug("Multiplier verification skipped", ticker=ticker, error=str(error))

    async def search_assets(self, query: str, **kwargs) -> pl.DataFrame:
        """Search listed instruments by ticker or name substring."""
        assets = await self._client.assets()
        needle = query.lower()
        matches = [a for a in assets
                   if needle in a.get("ticker", "").lower() or needle in a.get("name", "").lower()]
        return pl.DataFrame(matches) if matches else pl.DataFrame()
