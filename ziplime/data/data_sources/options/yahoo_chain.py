"""Option chains from Yahoo Finance, for a backtest that only needs to reach back a few days.

The other real source in this package speaks to a paid feed over gRPC. This one needs nothing but
`yfinance`, which is already a dependency, and it is the one to reach for when the question is
"does this structure behave the way I think" rather than "what is this strategy's edge".

**What Yahoo actually has, measured on 2026-09-14.** The chain itself is rich: strike, bid, ask,
last price, volume, open interest and implied volatility per contract, with the symbol already in
the compact OCC form ziplime stores listings under. The *history* behind each contract is thinner
and is the real constraint:

    daily     up to about a month, and only for contracts listed that long
    minute    the last session or two, a few hundred bars
    hourly    nothing at all -- the interval returns an empty frame

So this source is honest for a shallow window and useless for a deep one, which is the same shape
of limit the gRPC chain source has and a much shorter one.

**The chain is a snapshot of now.** A contract that has already expired is not in it, and no
argument here brings it back -- so a window ending in the past sees only the contracts that
outlived it. Using an expiry that has *not* yet passed sidesteps that completely, and that is the
intended way to use this: pick a live expiry, take a few days of its history, and trade the
structure over them.
"""
from __future__ import annotations

import asyncio
import datetime

import polars as pl
import structlog

from ziplime.assets.domain.option_type import OptionType

from .source import BAR_COLUMNS, ContractSpec, OptionChainSource
from .venues import VENUES

_logger = structlog.get_logger(__name__)

#: Yahoo's own name for a standard contract. Anything else is refused rather than assumed to be
#: a hundred shares -- an adjusted contract after a split is not, and silently treating it as one
#: misstates every position in it.
_REGULAR_CONTRACT = "REGULAR"
_REGULAR_MULTIPLIER = 100.0

#: Bar intervals Yahoo serves for an option contract. Hourly is absent from this list because it
#: comes back empty rather than erroring, which would otherwise look like an illiquid chain.
_INTERVALS = {
    datetime.timedelta(days=1): "1d",
    datetime.timedelta(minutes=1): "1m",
    datetime.timedelta(minutes=2): "2m",
    datetime.timedelta(minutes=5): "5m",
    datetime.timedelta(minutes=15): "15m",
    datetime.timedelta(minutes=30): "30m",
}


class YahooChainError(RuntimeError):
    """Yahoo answered in a way this source will not guess its way past."""


class YahooOptionChainSource(OptionChainSource):
    """Option chains and bars from Yahoo Finance.

    Args:
        venue: Whose conventions the contracts follow. ``"OPRA"`` for US listed options, which is
            what Yahoo carries.
        strike_window: Keep strikes within this fraction of the underlying's last price. A full
            chain is several hundred contracts and each one is a request; ``None`` keeps all of
            them and should be used deliberately.
        expiries: Keep only these expiration dates. ``None`` keeps every expiry Yahoo lists that
            has not passed before the window opens.
        minimum_open_interest: Drop contracts with less open interest than this. A strike nobody
            holds is a strike whose printed price is one stale trade, and a backtest that fills
            against it is measuring the quote rather than the strategy.
        batch: Contracts per ``yf.download`` call.
    """

    is_real_market_data = True

    def __init__(self, venue: str = "OPRA", strike_window: float | None = 0.05,
                 expiries: list[datetime.date] | None = None,
                 minimum_open_interest: int = 1, batch: int = 20):
        if venue not in VENUES:
            raise YahooChainError(
                f"Unknown venue {venue!r}. Known: {', '.join(sorted(VENUES))}.")
        self._venue = VENUES[venue]
        self._strike_window = strike_window
        self._expiries = set(expiries) if expiries else None
        self._minimum_open_interest = minimum_open_interest
        self._batch = batch

    @property
    def name(self) -> str:
        return "yahoo-options"

    # -- chain ----------------------------------------------------------------------------

    async def contracts(self, underlying_symbol: str, mic: str,
                        sessions: list[datetime.date]) -> list[ContractSpec]:
        """Every listed contract on ``underlying_symbol`` whose expiry is still ahead.

        Expiries that fall before the window's end are dropped rather than served: Yahoo lists
        only what is listed now, so a chain for a date already past is the survivors of it.
        """
        if not sessions:
            return []
        first, last = min(sessions), max(sessions)

        import yfinance

        ticker = yfinance.Ticker(underlying_symbol)
        listed = await asyncio.to_thread(lambda: list(ticker.options))
        if not listed:
            raise YahooChainError(
                f"Yahoo lists no option expiries for {underlying_symbol}. Either the symbol has "
                f"no options or it is not the symbol Yahoo knows it by.")

        expiries = []
        for text in listed:
            expiry = datetime.date.fromisoformat(text)
            if self._expiries is not None and expiry not in self._expiries:
                continue
            if expiry < last:
                # Expired, or expiring inside the window: either way the snapshot cannot describe
                # it as it stood, so it is left out rather than half-served.
                continue
            expiries.append((text, expiry))
        if not expiries:
            raise YahooChainError(
                f"{underlying_symbol} has {len(listed)} expiries listed and none of them is on or "
                f"after {last}. This source reads the chain as it stands now, so an expiry that "
                f"has already passed is gone from it -- pick one that has not.")

        reference = await self._reference_price(ticker)
        specs: list[ContractSpec] = []
        for text, expiry in expiries:
            chain = await asyncio.to_thread(ticker.option_chain, text)
            for frame, option_type in ((chain.calls, OptionType.CALL),
                                       (chain.puts, OptionType.PUT)):
                specs.extend(self._to_specs(frame, underlying_symbol, expiry, option_type,
                                            first, reference))

        _logger.info("Fetched option chains from Yahoo", underlying=underlying_symbol,
                     expiries=len(expiries), contracts=len(specs))
        if not specs:
            raise YahooChainError(
                f"{len(expiries)} expiries matched for {underlying_symbol} and no contract in "
                f"them survived the filters (strike window {self._strike_window}, minimum open "
                f"interest {self._minimum_open_interest}).")
        return specs

    def _to_specs(self, frame, underlying_symbol: str, expiry: datetime.date,
                  option_type: OptionType, first: datetime.date,
                  reference: float | None) -> list[ContractSpec]:
        specs = []
        for row in frame.to_dict("records"):
            strike = float(row["strike"])
            if reference and self._strike_window is not None:
                if abs(strike - reference) > self._strike_window * reference:
                    continue
            open_interest = row.get("openInterest")
            if open_interest is not None and open_interest == open_interest:
                if int(open_interest) < self._minimum_open_interest:
                    continue

            size = str(row.get("contractSize") or _REGULAR_CONTRACT).upper()
            if size != _REGULAR_CONTRACT:
                raise YahooChainError(
                    f"{row['contractSymbol']} has contract size {size!r}, which is not Yahoo's "
                    f"standard 'REGULAR'. An adjusted contract is not a hundred shares, and "
                    f"treating it as one would misstate every position in it.")

            specs.append(ContractSpec(
                underlying_symbol=underlying_symbol,
                expiration_date=expiry,
                option_type=option_type,
                strike=strike,
                mic=self._venue.mic,
                # Yahoo does not say when a contract was listed. The window's first session is
                # the honest stand-in for a shallow backtest: it claims no more than "tradeable
                # from the start of this run", and a session the contract has no bars for is one
                # the strategy sits out anyway because `data.current` returns nothing.
                listed_date=first,
                multiplier=_REGULAR_MULTIPLIER,
                tick_size=self._venue.tick_size,
                exercise_style=self._venue.exercise_style,
                settlement_type=self._venue.settlement_type,
                premium_style=self._venue.premium_style,
                symbol=None,               # Yahoo's contractSymbol is already the compact OCC form
                vendor_id=str(row["contractSymbol"]),
            ))
        return specs

    async def _reference_price(self, ticker) -> float | None:
        """The underlying's last close, for centring the strike window."""
        if self._strike_window is None:
            return None
        frame = await asyncio.to_thread(lambda: ticker.history(period="5d"))
        if frame is None or frame.empty:
            _logger.warning("No underlying price to centre the strike window on; keeping the "
                            "whole chain")
            return None
        return float(frame["Close"].iloc[-1])

    # -- bars -----------------------------------------------------------------------------

    async def bars(self, contracts: list[ContractSpec], timestamps: pl.Series) -> pl.DataFrame:
        """Bars for ``contracts`` at ``timestamps``, batched.

        A contract Yahoo has nothing for contributes no rows rather than an error -- an illiquid
        strike that never printed is an ordinary fact about an option chain, and on Yahoo it is a
        common one.
        """
        if not contracts or len(timestamps) == 0:
            return pl.DataFrame(schema={column: pl.Float64 for column in BAR_COLUMNS})

        stamps = timestamps.to_list()
        zone = str(stamps[0].tzinfo)
        interval = self._interval(stamps)
        first, last = min(stamps).date(), max(stamps).date()

        # Yahoo's `end` is exclusive for daily bars, so the last session needs a day added or it
        # is silently missing from every contract in the run.
        window = {"start": first.isoformat(),
                  "end": (last + datetime.timedelta(days=1)).isoformat(),
                  "interval": interval}

        by_symbol = {spec.vendor_id or spec.occ_symbol: spec for spec in contracts}
        symbols = list(by_symbol)
        frames = []
        for index in range(0, len(symbols), self._batch):
            chunk = symbols[index:index + self._batch]
            downloaded = await asyncio.to_thread(self._download, chunk, window)
            for symbol, frame in downloaded.items():
                spec = by_symbol.get(symbol)
                if spec is None or frame is None or frame.empty:
                    continue
                frames.append(self._to_bars(frame, spec, zone))

        if not frames:
            _logger.warning("Yahoo returned no option bars at all", contracts=len(contracts))
            return pl.DataFrame(schema={column: pl.Float64 for column in BAR_COLUMNS})

        data = pl.concat(frames, how="vertical_relaxed")
        on_grid = set(stamps)
        # Daily bars arrive stamped at midnight of their session; the clock runs to the close. So
        # a daily run matches on the session's date, and an intraday one on the instant itself.
        if interval == "1d":
            by_session = {stamp.date(): stamp for stamp in stamps}
            data = data.filter(pl.col("_session").is_in(list(by_session)))
            if data.is_empty():
                return pl.DataFrame(schema={column: pl.Float64 for column in BAR_COLUMNS})
            stamp_dtype = pl.Datetime(time_unit="us", time_zone=zone)
            data = data.with_columns(pl.col("_session").map_elements(
                by_session.__getitem__, return_dtype=stamp_dtype).alias("date"))
        else:
            data = data.filter(pl.col("date").is_in(list(on_grid)))
            if data.is_empty():
                return pl.DataFrame(schema={column: pl.Float64 for column in BAR_COLUMNS})

        data = data.drop("_session")
        for column in BAR_COLUMNS:
            if column not in data.columns:
                # Yahoo's chain carries bid, ask, implied volatility and open interest, but only
                # as they stand *now* -- they describe this moment, not the bar. Writing today's
                # spread onto last Tuesday's bar would be handing the strategy information that
                # did not exist then, so they are present and null rather than filled in.
                data = data.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))
        return data.select(BAR_COLUMNS).sort(["symbol", "date"])

    def _interval(self, stamps) -> str:
        """Yahoo's name for the bar size the clock is running at."""
        if len(stamps) < 2:
            return "1d"
        step = min(b - a for a, b in zip(stamps, stamps[1:]) if b > a)
        for delta, name in _INTERVALS.items():
            if abs(step - delta) < datetime.timedelta(seconds=1):
                return name
        raise YahooChainError(
            f"Yahoo has no bar interval matching {step}. It serves "
            f"{', '.join(sorted(_INTERVALS.values()))} for options -- and notably not hourly, "
            f"which comes back empty rather than refused.")

    @staticmethod
    def _download(symbols: list[str], window: dict) -> dict:
        """One `yf.download` for a batch, split back out per symbol."""
        import yfinance

        frame = yfinance.download(symbols, group_by="ticker", progress=False,
                                  auto_adjust=False, threads=True, **window)
        if frame is None or frame.empty:
            return {}
        out = {}
        for symbol in symbols:
            try:
                part = frame[symbol] if len(symbols) > 1 else frame
            except KeyError:
                continue
            out[symbol] = part.dropna(how="all")
        return out

    def _to_bars(self, frame, spec: ContractSpec, zone: str) -> pl.DataFrame:
        """One contract's Yahoo frame in the bundle's own columns."""
        local = frame.tz_convert(zone) if frame.index.tz is not None else frame.tz_localize(zone)
        closes = local["Close"].astype(float)
        return pl.DataFrame({
            "date": list(local.index),
            "_session": [stamp.date() for stamp in local.index],
            "symbol": [spec.listing_symbol] * len(local),
            "mic": [spec.mic] * len(local),
            "open": local["Open"].astype(float).to_list(),
            "high": local["High"].astype(float).to_list(),
            "low": local["Low"].astype(float).to_list(),
            "close": closes.to_list(),
            # `price` is what the engine marks a position at, and for an option that is its close.
            "price": closes.to_list(),
            "volume": local["Volume"].astype(float).to_list(),
        })
