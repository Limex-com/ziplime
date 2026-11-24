"""Grpc option chain source for grpc_chain."""

from __future__ import annotations
import asyncio
import datetime
import polars as pl
from ziplime.data.data_sources.options.source import BAR_COLUMNS, ContractSpec, OptionChainSource
from ziplime.data.data_sources.options.venues import VENUES
from ziplime.data.data_sources.options.grpc_chain.grpc_chain_error import GrpcChainError
from ziplime.data.data_sources.options.grpc_chain.support import _EXERCISE_STYLES
from ziplime.data.data_sources.options.grpc_chain.support import _OPTION_TYPES
from ziplime.data.data_sources.options.grpc_chain.support import _SETTLEMENT_TYPES
from ziplime.data.data_sources.options.grpc_chain.value_utils import _date
from ziplime.data.data_sources.options.grpc_chain.value_utils import _decimal
from ziplime.data.data_sources.options.grpc_chain.support import _logger


class GrpcOptionChainSource(OptionChainSource):
    """Option chains from the gRPC feed, on the venue's own terms.

    Args:
        data_source: A ``GrpcDataSource``. Its token and endpoint are used for every call, and
            its ``get_data`` fetches the bars -- so this class holds no credentials of its own.
        venue: Which venue's conventions the contracts follow. ``"OPRA"`` for US listed options.
        underlying_mic: Where the underlying trades, e.g. ``"ARCX"`` for SPY. Needed to resolve
            the underlying before its chain can be asked for.
        strike_window: Keep only strikes within this fraction of the underlying's last price.
            ``None`` keeps the whole chain -- 316 contracts for one SPY expiry, each needing its
            own bar request, which is minutes of wall time and rarely what anyone wants.
        expiries: Keep only these expiration dates. ``None`` keeps every listed expiry.
        max_concurrency: Parallel bar requests. The feed is the bottleneck, not this process.
    """

    is_real_market_data = True

    def __init__(self, data_source, venue: str = "OPRA", underlying_mic: str = "ARCX",
                 strike_window: float | None = 0.15,
                 expiries: list[datetime.date] | None = None,
                 max_concurrency: int = 8) -> None:
        if venue not in VENUES:
            raise GrpcChainError(
                f"Unknown venue {venue!r}. Known: {', '.join(sorted(VENUES))}.")
        self._source = data_source
        self._venue = VENUES[venue]
        self._underlying_mic = underlying_mic
        self._strike_window = strike_window
        self._expiries = set(expiries) if expiries else None
        self._semaphore = asyncio.Semaphore(max_concurrency)
        #: Vendor id per listing symbol, filled by `contracts` and read by `bars`. The feed
        #: addresses a contract by its padded OCC ticker, which is what it returned to us.
        self._tickers: dict[str, str] = {}

    @property
    def name(self) -> str:
        return f"grpc-{self._venue.name.lower()}"

    # -- chain discovery ------------------------------------------------------------------

    async def contracts(self, underlying_symbol: str, mic: str,
                        sessions: list[datetime.date]) -> list[ContractSpec]:
        """Every contract listed on ``underlying_symbol`` whose life overlaps ``sessions``.

        **``sessions`` filters; it does not query.** The feed describes the chain as it stands
        today, so a contract that expired before today is not in it at all -- and no argument here
        brings it back. That makes this source usable for a window ending near the present and
        misleading for one ending in the past: ask for 2024 and you will get today's chain
        filtered to the few contracts that were already listed then, which is survivorship
        selection of the worst kind. The window is checked and refused rather than quietly served.
        """
        if not sessions:
            return []
        first, last = min(sessions), max(sessions)
        today = datetime.date.today()
        # Warned rather than refused, and the distinction is the point. What the feed cannot show
        # is a contract that has already expired -- so a window in the past is missing exactly the
        # series that died between its end and today, and nothing else. Asking for expiries that
        # are still listed (a December chain, say) has no survivorship problem at all, and
        # refusing it outright would block the source's main legitimate use to guard against a
        # case the caller may not be in. The empty-result check after filtering is where the
        # genuinely broken window is caught, by what came back rather than by arithmetic on dates.
        if last < today and (self._expiries is None or any(e < today for e in self._expiries)):
            _logger.warning(
                "The window ends before today and this feed lists only what is listed now; any "
                "series that expired in between is missing from the chain, so the contracts you "
                "do get are the ones that outlived the window",
                window_end=str(last), today=str(today))

        import grpc
        from ziplime_grpc_data_source_private.grpc_stubs.grpc.reference import (
            securityreference_pb2 as reference,
            securityreference_pb2_grpc as reference_grpc,
        )

        endpoint = self._source._server_url
        metadata = (("authorization", self._source._authorization_token),)
        async with grpc.aio.secure_channel(endpoint, grpc.ssl_channel_credentials()) as channel:
            identifier = await self._source.get_security_identifier(
                channel=channel, symbol=f"{underlying_symbol}@{self._underlying_mic}")
            stub = reference_grpc.SecurityReferenceStub(channel)

            families = (await stub.GetOptionFamilies(
                reference.SecurityRequest(identifier=identifier), metadata=metadata)).families
            wanted = [family for family in families
                      if self._family_in_window(family, first, last)]
            if not wanted:
                raise GrpcChainError(
                    f"{underlying_symbol} has {len(families)} option families listed and none of "
                    f"them expires in {first} .. {last}.")

            reference_price = await self._underlying_price(underlying_symbol, last)
            specs: list[ContractSpec] = []
            for family in wanted:
                response = await stub.GetOptions(
                    reference.OptionFamilyRequest(family=family), metadata=metadata)
                specs.extend(self._to_specs(response.securities, underlying_symbol, mic,
                                            first, last, reference_price))

        if not specs:
            raise GrpcChainError(
                f"{len(wanted)} option families matched {first} .. {last} for "
                f"{underlying_symbol}, and no contract in them was listed during that window.\n"
                f"Either every series that traded then has since expired -- this feed lists only "
                f"what is listed now, so those are gone and a historical chain has to come from a "
                f"source that stores what was listed at the time -- or the strike window "
                f"({self._strike_window}) is too narrow for the underlying's price over it.")

        _logger.info("Fetched option chains over gRPC", underlying=underlying_symbol,
                     families=len(wanted), contracts=len(specs), venue=self._venue.name)
        return specs

    def _family_in_window(self, family, first: datetime.date, last: datetime.date) -> bool:
        expiry = _date(family.expiration_date)
        if expiry is None:
            return False
        if self._expiries is not None:
            return expiry in self._expiries
        # A contract is tradeable up to its expiry, so a family matters if it expires on or after
        # the window opens. An expiry beyond the window's end is still listed during it.
        return expiry >= first

    def _to_specs(self, securities, underlying_symbol: str, mic: str,
                  first: datetime.date, last: datetime.date,
                  reference_price: float | None) -> list[ContractSpec]:
        specs = []
        for response in securities:
            inner = response.security.security
            option, common = inner.option, inner.common
            expiry = _date(option.expiration_last_day) or _date(option.expiration_first_day)
            listed = _date(option.trade_first_day)
            if expiry is None:
                continue
            # Listed *after* the window closes means the contract did not exist during it.
            if listed is not None and listed > last:
                continue

            strike = _decimal(option.strike)
            if reference_price and self._strike_window is not None:
                if abs(strike - reference_price) > self._strike_window * reference_price:
                    continue

            option_type = _OPTION_TYPES.get(option.type)
            if option_type is None:
                raise GrpcChainError(
                    f"{common.ticker.value!r} has option type {option.type}, which is neither "
                    f"call nor put. A contract whose side is unknown cannot be priced.")
            settlement = _SETTLEMENT_TYPES.get(option.settlement_type)
            if settlement is None:
                raise GrpcChainError(
                    f"{common.ticker.value!r} reports settlement type {option.settlement_type}, "
                    f"which this source does not recognise. Settlement decides what happens at "
                    f"expiry, so guessing it is not an option.")

            multiplier = _decimal(option.multiplier) or _decimal(option.contract_size)
            if not multiplier:
                # Measured on MOEX, and documented in `venues`: the feed reports zero there.
                multiplier = self._venue.contract_size

            ticker = common.ticker.value or None
            specs.append(ContractSpec(
                underlying_symbol=underlying_symbol,
                expiration_date=expiry,
                option_type=option_type,
                strike=strike,
                # The venue the contract actually trades on, from the feed -- not the `mic`
                # argument, which names where the *underlying* trades. The synthetic source lists
                # its contracts on the underlying's venue deliberately, as a simplification it
                # documents; a real OPRA option is not listed on ARCX, and asking the feed for
                # `SPY   261218C00930000@ARCX` gets "Security not found".
                mic=common.mic.value or self._venue.mic,
                # The feed's own listing date, not a convention. A weekly lives days and a LEAPS
                # lives years, and only this field knows which this is.
                listed_date=listed or first,
                multiplier=multiplier,
                tick_size=_decimal(option.tick_value) or self._venue.tick_size,
                exercise_style=_EXERCISE_STYLES.get(option.style, self._venue.exercise_style),
                settlement_type=settlement,
                premium_style=self._venue.premium_style,
                symbol=None,            # store under the canonical compact OCC symbol
                vendor_id=str(common.security_id) if common.security_id else None,
            ))
            if ticker:
                # How the feed addresses this contract. ziplime stores the compact OCC form, the
                # feed speaks the padded 21-character one, and `bars` has to ask in the feed's.
                self._tickers[specs[-1].listing_symbol] = ticker
        return specs

    async def _underlying_price(self, underlying_symbol: str,
                                as_of: datetime.date) -> float | None:
        """The underlying's last close, for centring the strike window.

        None rather than an exception when the feed has nothing: a missing reference price means
        the whole chain is kept, which is slow and correct, rather than an empty one, which is
        fast and wrong.
        """
        if self._strike_window is None:
            return None
        frame = await self._source.get_data(
            symbols=[f"{underlying_symbol}@{self._underlying_mic}"],
            frequency=datetime.timedelta(days=1),
            date_from=datetime.datetime.combine(
                as_of - datetime.timedelta(days=10), datetime.time.min,
                tzinfo=datetime.timezone.utc),
            date_to=datetime.datetime.combine(as_of, datetime.time.max,
                                              tzinfo=datetime.timezone.utc))
        if frame.is_empty():
            _logger.warning("No underlying price to centre the strike window on; keeping the "
                            "whole chain", underlying=underlying_symbol)
            return None
        return float(frame["close"][-1])

    # -- bars -----------------------------------------------------------------------------

    async def bars(self, contracts: list[ContractSpec], timestamps: pl.Series) -> pl.DataFrame:
        """Daily bars for ``contracts``, stamped on the simulation clock's own instants.

        One request per contract, run concurrently up to ``max_concurrency``. A contract the feed
        has nothing for contributes no rows rather than an error -- an illiquid strike that never
        printed is an ordinary fact about an option chain, not a failure.
        """
        if not contracts or len(timestamps) == 0:
            return pl.DataFrame(schema=BAR_COLUMNS)

        # The candle service answers for an option by its security id and mostly not by its OCC
        # ticker, and `contracts` already has the id from GetOptions. A source that can be told
        # ids (`security_ids`, private wheel 0.3.0) is told them.
        known_ids = getattr(self._source, "security_ids", None)
        if isinstance(known_ids, dict):
            for spec in contracts:
                if spec.vendor_id and str(spec.vendor_id).isdigit():
                    symbol = self._tickers.get(spec.listing_symbol, spec.occ_symbol)
                    known_ids[f"{symbol}@{spec.mic}"] = int(spec.vendor_id)

        stamps = timestamps.to_list()
        # The clock's instants, keyed by the session they belong to. The feed stamps a daily bar
        # at midnight UTC of its own session, so the *date* is the join key and the clock's
        # instant is what the bundle has to carry.
        by_session = {stamp.date(): stamp for stamp in stamps}
        first, last = min(by_session), max(by_session)

        async def one(spec: ContractSpec) -> pl.DataFrame | None:
            symbol = self._tickers.get(spec.listing_symbol, spec.occ_symbol)
            async with self._semaphore:
                try:
                    frame = await self._source.get_data(
                        symbols=[f"{symbol}@{spec.mic}"],
                        frequency=datetime.timedelta(days=1),
                        date_from=datetime.datetime.combine(first, datetime.time.min,
                                                            tzinfo=datetime.timezone.utc),
                        date_to=datetime.datetime.combine(last, datetime.time.max,
                                                          tzinfo=datetime.timezone.utc))
                except Exception as error:  # noqa: BLE001 - one contract must not fail the chain
                    _logger.warning("No bars for one contract", symbol=symbol,
                                    error=f"{type(error).__name__}: {error}")
                    return None
            if frame.is_empty():
                return None
            return frame.with_columns(pl.lit(spec.listing_symbol).alias("_listing"))

        frames = [f for f in await asyncio.gather(*(one(spec) for spec in contracts))
                  if f is not None]
        if not frames:
            _logger.warning("The chain returned no bars at all", contracts=len(contracts))
            return pl.DataFrame(schema=BAR_COLUMNS)

        data = pl.concat(frames, how="vertical_relaxed")
        # Take the date part of the UTC stamp directly. Converting to the exchange's zone first
        # moves a midnight-UTC bar onto the previous evening, and every bar then lands one
        # session early -- the last one falling off the run entirely.
        data = data.with_columns(
            pl.col("date").dt.replace_time_zone(None).dt.date().alias("_session"))
        data = data.filter(pl.col("_session").is_in(list(by_session)))
        if data.is_empty():
            return pl.DataFrame(schema=BAR_COLUMNS)

        stamp_dtype = pl.Datetime(time_unit="us", time_zone=str(stamps[0].tzinfo))
        data = data.with_columns(
            pl.col("_session").map_elements(by_session.__getitem__,
                                            return_dtype=stamp_dtype).alias("date"),
            pl.col("_listing").alias("symbol"),
        ).drop("_session", "_listing")

        # The candle service carries trades, not quotes: no bid, no ask, no implied volatility,
        # no open interest. Present and null rather than absent, because the two mean different
        # things to a strategy -- absent is "this source does not model quotes", null is "this
        # source models them and this bar had none" -- and the first is the truth here.
        for column in BAR_COLUMNS:
            if column not in data.columns:
                data = data.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))
        return data.select(BAR_COLUMNS).sort(["symbol", "date"])
