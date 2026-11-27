"""Synthetic option chain source for synthetic."""

import datetime
import math
import polars as pl
from ziplime.assets.domain.option_type import OptionType
from ziplime.data.data_sources.options.source import BAR_COLUMNS, ContractSpec, OptionChainSource
from ziplime.data.data_sources.options.surface import AtmVolatilityModel, VolatilitySurface
from ziplime.data.data_sources.options.venues import OPRA
from ziplime.data.data_sources.options.option_venue import OptionVenue
from ziplime.finance.options.greeks import black_scholes_price, time_to_expiry
from ziplime.data.data_sources.options.synthetic.chain_spec import ChainSpec
from ziplime.data.data_sources.options.synthetic.liquidity_model import LiquidityModel
from ziplime.data.data_sources.options.synthetic.support import SYNTHETIC_DATA_WARNING
from ziplime.data.data_sources.options.synthetic.value_utils import _as_float
from ziplime.data.data_sources.options.synthetic.support import _logger
from ziplime.data.data_sources.options.synthetic.value_utils import _round_to_tick


class SyntheticOptionChainSource(OptionChainSource):
    """Generates 0DTE chains and their bars from an underlying's real bars.

    Args:
        underlying_symbol: Ticker the chains are written on.
        mic: Exchange the contracts are listed on. The same venue as the underlying, which keeps
            one calendar across the whole bundle.
        underlying_bars: The real bars, with ``date`` (timezone-aware), ``open``, ``high``,
            ``low``, ``close``. This is the only real data in the output and everything else is
            derived from it.
        session_closes: Closing instant of every session in the window, keyed by date. Time to
            expiry is measured to this instant, which is the single most consequential number in a
            0DTE model -- see :func:`ziplime.finance.options.greeks.time_to_expiry`.
        chain: The strike grid to list each session.
        surface: The smile.
        atm_model: Where the at-the-money level comes from.
        liquidity: Volume, open interest and spreads.
        rate: Risk-free rate used for discounting. At one day to expiry its effect on a price is
            of the order of a tenth of a cent, so it is a formality here rather than an input
            worth tuning.
    """

    is_real_market_data = False

    def __init__(self, underlying_symbol: str, mic: str, underlying_bars: pl.DataFrame,
                 session_closes: dict[datetime.date, datetime.datetime],
                 chain: ChainSpec | None = None,
                 surface: VolatilitySurface | None = None,
                 atm_model: AtmVolatilityModel | None = None,
                 liquidity: LiquidityModel | None = None,
                 venue: OptionVenue | None = None,
                 root: str | None = None,
                 life_sessions: int = 0,
                 rate: float = 0.04) -> None:
        self.underlying_symbol = underlying_symbol
        self.mic = mic
        self.chain = chain or ChainSpec()
        self.surface = surface or VolatilitySurface()
        self.atm_model = atm_model or AtmVolatilityModel()
        self.liquidity = liquidity or LiquidityModel()
        # Which venue's conventions the generated contracts carry. Defaults to OPRA, which is what
        # the 0DTE examples are about; pass MOEX to generate a margined, European, natively-named
        # chain and exercise that side of the accounting.
        self.venue = venue or OPRA
        self.root = root
        # How many sessions before its expiry a series is listed. 0 is 0DTE -- listed and expiring
        # the same day. MOEX lists weekly, so about five.
        self.life_sessions = max(0, int(life_sessions))
        self.rate = rate
        self._session_closes = dict(session_closes)

        missing = {"date", "open", "high", "low", "close"} - set(underlying_bars.columns)
        if missing:
            raise ValueError(f"Underlying bars are missing columns: {sorted(missing)}.")
        self._bars = underlying_bars.sort("date")
        self._by_session = self._index_sessions(self._bars)
        self._atm_by_session = self._atm_volatilities()

        _logger.warning(SYNTHETIC_DATA_WARNING, underlying=underlying_symbol,
                        sessions=len(self._by_session))

    @property
    def name(self) -> str:
        return f"synthetic-0dte-{self.underlying_symbol.lower()}"

    # -- session bookkeeping ----------------------------------------------------------------

    @staticmethod
    def _index_sessions(bars: pl.DataFrame) -> dict[datetime.date, pl.DataFrame]:
        """Split the underlying's bars by session, preserving order within each."""
        with_session = bars.with_columns(pl.col("date").dt.date().alias("_session"))
        return {session: frame.drop("_session")
                for (session,), frame in with_session.group_by("_session", maintain_order=True)}

    def _atm_volatilities(self) -> dict[datetime.date, float]:
        """At-the-money volatility per session, from the underlying's trailing intraday returns.

        Uses only sessions **before** the one being priced. Including the session's own bars would
        price the morning's chain off the afternoon's move, which is the most flattering look-ahead
        available in an option backtest: every straddle would be cheap on a quiet day and dear on a
        violent one, known at the open.

        Returns are taken within each session and never across the overnight gap. An overnight
        return is a different variance -- it covers sixteen hours in which the market was shut --
        and pooling it with five-minute returns inflates the estimate by a factor that depends
        only on how the bars happen to be sampled.
        """
        sessions = list(self._by_session)
        per_session: dict[datetime.date, list[float]] = {}
        bars_per_session: list[int] = []
        for session in sessions:
            closes = [float(c) for c in self._by_session[session]["close"] if c is not None]
            per_session[session] = [math.log(b / a)
                                    for a, b in zip(closes, closes[1:]) if a > 0 and b > 0]
            bars_per_session.append(max(len(closes), 1))

        typical_bars = sorted(bars_per_session)[len(bars_per_session) // 2]
        periods_per_year = typical_bars * 252.0

        levels: dict[datetime.date, float] = {}
        for index, session in enumerate(sessions):
            window = sessions[max(0, index - self.atm_model.lookback_sessions):index]
            history = [r for prior in window for r in per_session[prior]]
            levels[session] = self.atm_model.annualised(history, periods_per_year)
        return levels

    def session_reference_price(self, session: datetime.date) -> float | None:
        """The price the session's chain is centred on: its own opening print.

        Known at the first bar, so listing the chain on it is not a look-ahead. Centring on the
        session's close would be -- and would also quietly guarantee that the at-the-money strike
        is where the underlying finished, which is the one thing a 0DTE strategy is trying to
        guess.
        """
        frame = self._by_session.get(session)
        if frame is None or frame.is_empty():
            return None
        opening = frame["open"][0]
        return float(opening) if opening is not None else None

    # -- OptionChainSource ------------------------------------------------------------------

    def listing_session(self, expiration: datetime.date) -> datetime.date | None:
        """The session a series expiring on ``expiration`` was listed on.

        ``life_sessions`` back from the expiry, in sessions the underlying actually traded. Zero
        gives 0DTE -- listed and expiring the same day, which is the whole product on SPY. MOEX
        lists weekly, so its series are listed about five sessions ahead.
        """
        sessions = [session for session in self._by_session if session <= expiration]
        if not sessions:
            return None
        return sessions[max(0, len(sessions) - 1 - self.life_sessions)]

    async def contracts(self, underlying_symbol: str, mic: str,
                        sessions: list[datetime.date]) -> list[ContractSpec]:
        """One chain per expiry, centred on the price when the series was **listed**.

        Centring on the listing session's open, not the expiry's. For 0DTE the two are the same
        day; for a weekly series they are a week apart, and centring on the expiry would place the
        at-the-money strike exactly where the underlying finished -- a week of hindsight handed to
        every structure built on the chain.
        """
        if underlying_symbol != self.underlying_symbol:
            raise ValueError(
                f"This source generates chains on {self.underlying_symbol}, not {underlying_symbol}.")
        specs: list[ContractSpec] = []
        for expiration in sessions:
            listed = self.listing_session(expiration)
            reference = self.session_reference_price(listed) if listed else None
            if reference is None:
                _logger.debug("No underlying bars to list a chain against", session=expiration)
                continue
            for strike in self.chain.strikes_around(reference):
                for option_type in (OptionType.CALL, OptionType.PUT):
                    specs.append(self.venue.contract(
                        underlying_symbol=underlying_symbol,
                        expiration_date=expiration,
                        option_type=option_type,
                        strike=strike,
                        listed_date=listed,
                        root=self.root,
                        # The venue fixes the conventions; the caller says which exchange row the
                        # listing hangs off, and therefore which calendar the contracts share with
                        # their underlying.
                        mic=mic,
                    ))
        return specs

    async def bars(self, contracts: list[ContractSpec], timestamps: pl.Series) -> pl.DataFrame:
        """Price every contract on every session it is alive for.

        A contract is alive from its listing to its expiry, which for 0DTE is one session and for a
        monthly series is twenty. Emitting only the expiry session -- all 0DTE ever needs -- leaves
        a longer-dated contract tradeable but unpriced, and the engine then refuses to read it.

        The chain is priced **as a chain**, all strikes of one expiry at one instant together, and
        then forced to be arbitrage-free; see :meth:`_enforce_no_arbitrage`. Pricing each contract
        on its own cannot do that, because the property is a relation between neighbours.
        """
        alive_by_session: dict[datetime.date, list[ContractSpec]] = {}
        for spec in contracts:
            for session in self._by_session:
                if spec.listed_date <= session <= spec.expiration_date:
                    alive_by_session.setdefault(session, []).append(spec)

        stamps_by_session: dict[datetime.date, list] = {}
        for stamp in timestamps:
            stamps_by_session.setdefault(stamp.date(), []).append(stamp)

        # Open interest builds over each contract's whole life, not over each session -- which is
        # what makes a series' interest larger on its last day than on its first.
        traded_so_far: dict[str, float] = {}
        rows: list[dict] = []
        for session in sorted(alive_by_session):
            specs = alive_by_session[session]
            underlying = self._by_session.get(session)
            session_stamps = stamps_by_session.get(session)
            if underlying is None or underlying.is_empty() or not session_stamps:
                continue
            atm_volatility = self._atm_by_session.get(session, self.atm_model.seed)
            underlying_rows = {row["date"]: row for row in underlying.iter_rows(named=True)}
            session_close = self._session_closes.get(session)
            first_stamp = session_stamps[0]
            span = max(((session_close or first_stamp) - first_stamp).total_seconds(), 1.0)

            for stamp in session_stamps:
                bar = underlying_rows.get(stamp)
                if bar is None:
                    continue
                elapsed = (stamp - first_stamp).total_seconds() / span
                priced = []
                for spec in specs:
                    close_instant = self._session_closes.get(spec.expiration_date)
                    if close_instant is None:
                        continue
                    years = time_to_expiry(stamp, close_instant)
                    row = self._price_contract(spec, bar, years, atm_volatility, elapsed,
                                               traded_so_far)
                    if row is not None:
                        priced.append((spec, row))
                rows.extend(self._enforce_no_arbitrage(priced))

        if not rows:
            return pl.DataFrame(schema={column: pl.Float64 for column in BAR_COLUMNS})
        return pl.DataFrame(rows).select(BAR_COLUMNS).sort(["symbol", "date"])

    def _enforce_no_arbitrage(self, priced: list[tuple[ContractSpec, dict]]) -> list[dict]:
        """Clip a chain's prices until a call costs less the higher its strike, and a put more.

        This is the property that stops a vertical spread from being free money, and a smile can
        break it: the volatility rise in the wings can outrun the strike. That is not a corner
        case -- it happened on the first real grid this generator was pointed at, a monthly SBER
        chain reaching 20% from the money, where the 330 call priced above the 320. Every
        structure built on that chain was nonsense, and the strategy sized itself into six
        thousand lots chasing a credit that was really a guaranteed loss.

        Enforcing it by clipping rather than by tuning the smile is deliberate. Parameters that
        happen to keep one grid monotone do not keep another, and the failure is silent; this
        cannot be silent, because the invariant is checked by construction. Where a clip binds,
        the recorded ``implied_volatility`` is the model's rather than the clipped price's
        inverse -- the column says what the chain was priced from, not what it reprices to.
        """
        out = []
        for option_type, decreasing in ((OptionType.CALL, True), (OptionType.PUT, False)):
            side = sorted((item for item in priced if item[0].option_type is option_type),
                          key=lambda item: item[0].strike)
            bound = None
            for spec, row in side:
                price = row["close"]
                if bound is not None:
                    price = min(price, bound) if decreasing else max(price, bound)
                if price != row["close"]:
                    shifted = price - row["close"]
                    for field in ("open", "high", "low", "close", "price"):
                        row[field] = max(0.0, row[field] + shifted)
                    half = _round_to_tick(self.liquidity.half_spread(row["close"]),
                                          spec.tick_size)
                    row["bid"] = max(0.0, _round_to_tick(row["close"] - half, spec.tick_size))
                    row["ask"] = _round_to_tick(row["close"] + half, spec.tick_size)
                bound = row["close"]
                out.append(row)
        return out

    def _price_contract(self, spec: ContractSpec, bar: dict, years: float,
                        atm_volatility: float, session_fraction: float,
                        traded_so_far: dict[str, float]) -> dict | None:
        """One option bar from one underlying bar."""
        spot = _as_float(bar.get("close"))
        if spot is None or spot <= 0:
            return None
        option_type = spec.option_type
        volatility = self.surface.implied_volatility(atm_volatility, spot, spec.strike,
                                                     years_to_expiry=years)

        def priced(underlying_price: float | None) -> float:
            if underlying_price is None or underlying_price <= 0:
                return 0.0
            # The smile is a function of where the strike sits relative to *this* price, so it is
            # re-read at each of the four prices rather than frozen at the close. Without that a
            # bar in which the underlying moved a percent prices its own high off the wrong smile.
            iv = self.surface.implied_volatility(atm_volatility, underlying_price,
                                                 spec.strike, years_to_expiry=years)
            return black_scholes_price(option_type, underlying_price, spec.strike, years,
                                       self.rate, iv)

        high_from = _as_float(bar.get("high"))
        low_from = _as_float(bar.get("low"))
        # A put is worth most where the underlying is worth least. Taking the option's high from
        # the underlying's high inverts every stop and every high-water mark on half the chain.
        if option_type is OptionType.PUT:
            high_from, low_from = low_from, high_from

        close_price = _round_to_tick(priced(spot), spec.tick_size)
        open_price = _round_to_tick(priced(_as_float(bar.get("open"))), spec.tick_size)
        high_price = _round_to_tick(priced(high_from), spec.tick_size)
        low_price = _round_to_tick(priced(low_from), spec.tick_size)
        # Rounding to the tick can push the close outside a range built from other prices.
        high_price = max(high_price, open_price, close_price)
        low_price = min(low_price, open_price, close_price)

        volume = round(self.liquidity.volume(spot, spec.strike, session_fraction))
        symbol = spec.listing_symbol
        open_interest = traded_so_far.get(symbol, 0.0)
        traded_so_far[symbol] = open_interest + volume

        half_spread = _round_to_tick(self.liquidity.half_spread(close_price), spec.tick_size)
        return {
            "date": bar["date"],
            "symbol": symbol,
            "mic": spec.mic,
            "open": open_price,
            "high": high_price,
            "low": low_price,
            "close": close_price,
            "price": close_price,
            "volume": float(volume),
            "bid": max(0.0, _round_to_tick(close_price - half_spread, spec.tick_size)),
            "ask": _round_to_tick(close_price + half_spread, spec.tick_size),
            "implied_volatility": volatility,
            "underlying_price": spot,
            "open_interest": open_interest,
        }
