"""Black-Scholes prices, implied volatility and Greeks, with the 0DTE edges handled.

The arithmetic is ``vollib``'s -- the reference implementation of Black-Scholes and of Jäckel's
"Let Be Rational" inversion, which is worth far more than a hand-rolled Newton solver at the
moneyness extremes a 0DTE chain is mostly made of. What this module adds is the two things
``vollib`` has no opinion about and that a zero-day option makes unavoidable.

**Time is measured in minutes.** Every formula takes a year fraction, and for an option expiring
today that fraction is not "one day" but whatever is left of this session: at 15:55 it is five
minutes, about 0.00001 years. Get it wrong by using whole days and a 0DTE ATM straddle is priced
at roughly eight times its worth an hour before the close, so :func:`time_to_expiry` takes an
instant and the expiration's closing instant, not two dates.

**Expiry is a limit, not an error.** At ``t <= 0`` every Black-Scholes formula divides by
``sqrt(t)``; the honest answers there are the limits -- price goes to intrinsic, delta to a step,
gamma to infinity at the strike -- and this module returns those instead of ``nan``. That matters
because the last bar of a 0DTE run *is* ``t == 0``.

Greek conventions are ``vollib``'s, which are the practitioner ones rather than the textbook ones:

* ``delta``, ``gamma`` -- per 1.00 of underlying, unscaled.
* ``theta`` -- per **calendar day**, not per year. On a 0DTE option this is a strange number by
  construction: it is the whole remaining premium spread over a day that has hours left in it, so
  a one-day theta can exceed the option's own price. Use it as a rate, not as a prediction.
* ``vega`` -- per **one volatility point** (a move from 18% to 19%), not per 1.00 of sigma.
* ``rho`` -- per **one percentage point** of the rate.

All of them are per unit of the underlying. Multiply by the contract multiplier and the position
size to get the exposure of a book -- :func:`position_greeks` does that.
"""
import dataclasses
import datetime
import math

from ziplime.assets.domain.option_type import OptionType

#: Below this many years to expiry the closed-form formulas stop being usable -- ``sqrt(t)``
#: underflows the price into the noise -- and the expiry limits are returned instead. One second
#: expressed in years, which on a 0DTE clock is the last bar and nothing before it.
MIN_TIME_TO_EXPIRY = 1.0 / (365.0 * 24.0 * 60.0 * 60.0)

#: Years per calendar day, the unit ``theta`` is quoted in.
_DAYS_PER_YEAR = 365.0

_IMPORT_HINT = (
    "Option pricing needs the 'vollib' package, which ziplime does not install by default. "
    "Install it with `poetry install --with options` or `pip install vollib`."
)


def _vollib():
    """Import ``vollib`` on first use, with an error that says what to do about it."""
    try:
        from vollib.black_scholes import black_scholes
        from vollib.black_scholes.greeks import analytical
        from vollib.black_scholes.implied_volatility import implied_volatility as iv
    except ImportError as error:  # pragma: no cover - exercised only without the extra
        raise ImportError(_IMPORT_HINT) from error
    return black_scholes, analytical, iv


@dataclasses.dataclass(frozen=True)
class Greeks:
    """One option's price and sensitivities, per unit of the underlying.

    See the module docstring for the scaling of each field; they are ``vollib``'s conventions and
    they are not all per-year or all per-1.00.
    """

    price: float
    delta: float
    gamma: float
    #: Per calendar day.
    theta: float
    #: Per volatility point.
    vega: float
    #: Per percentage point of the interest rate.
    rho: float
    #: The volatility these were computed at. Echoed back so a caller that passed an implied
    #: volatility solved from a market price does not have to carry it alongside.
    volatility: float
    #: Years to expiry the figures were computed for. Zero means the contract has expired and the
    #: figures are the expiry limits.
    time_to_expiry: float

    @property
    def is_expired(self) -> bool:
        return self.time_to_expiry <= 0.0


def time_to_expiry(now: datetime.datetime, expires_at: datetime.datetime) -> float:
    """Years from ``now`` until ``expires_at``, never negative.

    Both must be timezone-aware and ``expires_at`` is the expiration session's **closing instant**,
    not its date. On a 0DTE contract that distinction is the whole calculation: the same contract
    at 09:31 and at 15:55 differs by a factor of eighty in time value.

    Uses a 365-day year, matching ``vollib`` and the theta convention. A trading-day year (252)
    would price the same option about 20% higher in volatility terms; the two conventions are both
    defensible and mixing them is not, so this package uses calendar years throughout.
    """
    if now.tzinfo is None or expires_at.tzinfo is None:
        raise ValueError(
            "time_to_expiry needs timezone-aware instants: a 0DTE option's remaining life is a "
            "number of minutes, and a naive datetime cannot say which minutes those are.")
    seconds = (expires_at - now).total_seconds()
    return max(seconds / (_DAYS_PER_YEAR * 24.0 * 60.0 * 60.0), 0.0)


def black_scholes_price(option_type: OptionType, spot: float, strike: float,
                        years_to_expiry: float, rate: float, volatility: float) -> float:
    """Black-Scholes price of one unit, falling back to intrinsic value at expiry."""
    if years_to_expiry <= MIN_TIME_TO_EXPIRY or volatility <= 0.0:
        return option_type.intrinsic_value(spot, strike)
    black_scholes, _, _ = _vollib()
    return float(black_scholes(option_type.vollib_flag, spot, strike, years_to_expiry,
                               rate, volatility))


def implied_volatility(price: float, option_type: OptionType, spot: float, strike: float,
                       years_to_expiry: float, rate: float) -> float | None:
    """Volatility that reprices ``price``, or ``None`` when no such volatility exists.

    ``None`` is a normal answer on a 0DTE chain rather than a failure, and it is returned instead
    of raising for the three cases that produce it constantly in the last hour of a session: a
    contract with no time left, a quote at or below intrinsic value (any volatility down to zero
    reproduces it), and a quote above the underlying itself. A caller that treats ``None`` as zero
    volatility will mark the whole far wing at intrinsic; treat it as "not measurable here".
    """
    if years_to_expiry <= MIN_TIME_TO_EXPIRY:
        return None
    if price <= option_type.intrinsic_value(spot, strike):
        return None
    _, _, iv = _vollib()
    try:
        solved = float(iv(price, spot, strike, years_to_expiry, rate, option_type.vollib_flag))
    except Exception:
        # vollib raises several different exception types for an unattainable price, all meaning
        # the same thing here.
        return None
    return solved if math.isfinite(solved) and solved > 0.0 else None


def greeks(option_type: OptionType, spot: float, strike: float, years_to_expiry: float,
           rate: float, volatility: float) -> Greeks:
    """Price and Greeks of one unit of the underlying.

    At and past expiry the expiry limits are returned rather than ``nan``: the price is intrinsic,
    delta is 1 (or -1 for an in-the-money put) on one side of the strike and 0 on the other, and
    every other sensitivity is zero. Gamma at exactly the strike is unbounded in the limit and is
    reported as 0.0, which is the only finite number available and is why a book should be flat
    before the close rather than relying on a number here.
    """
    if years_to_expiry <= MIN_TIME_TO_EXPIRY or volatility <= 0.0:
        intrinsic = option_type.intrinsic_value(spot, strike)
        in_the_money = intrinsic > 0.0
        return Greeks(
            price=intrinsic,
            delta=float(option_type.sign) if in_the_money else 0.0,
            gamma=0.0, theta=0.0, vega=0.0, rho=0.0,
            volatility=max(volatility, 0.0), time_to_expiry=0.0,
        )

    black_scholes, analytical, _ = _vollib()
    flag = option_type.vollib_flag
    args = (flag, spot, strike, years_to_expiry, rate, volatility)
    return Greeks(
        price=float(black_scholes(*args)),
        delta=float(analytical.delta(*args)),
        gamma=float(analytical.gamma(*args)),
        theta=float(analytical.theta(*args)),
        vega=float(analytical.vega(*args)),
        rho=float(analytical.rho(*args)),
        volatility=volatility,
        time_to_expiry=years_to_expiry,
    )


def position_greeks(per_unit: Greeks, amount: float, multiplier: float) -> Greeks:
    """Scale per-unit Greeks to a held position.

    ``amount`` is signed -- a short option has negative delta exposure to a call -- and
    ``multiplier`` is the contract's. The result is the position's sensitivity in money: a delta of
    -250 means the book loses 250 for every 1.00 the underlying rises.
    """
    scale = amount * multiplier
    return dataclasses.replace(
        per_unit,
        price=per_unit.price * scale,
        delta=per_unit.delta * scale,
        gamma=per_unit.gamma * scale,
        theta=per_unit.theta * scale,
        vega=per_unit.vega * scale,
        rho=per_unit.rho * scale,
    )


def aggregate_greeks(legs: list[Greeks]) -> Greeks:
    """Sum position Greeks across the legs of a strategy.

    ``volatility`` and ``time_to_expiry`` are carried from the first leg: a combination has no one
    volatility, and for the single-expiry structures this package builds every leg shares the time.
    """
    if not legs:
        raise ValueError("aggregate_greeks needs at least one leg.")
    return Greeks(
        price=math.fsum(leg.price for leg in legs),
        delta=math.fsum(leg.delta for leg in legs),
        gamma=math.fsum(leg.gamma for leg in legs),
        theta=math.fsum(leg.theta for leg in legs),
        vega=math.fsum(leg.vega for leg in legs),
        rho=math.fsum(leg.rho for leg in legs),
        volatility=legs[0].volatility,
        time_to_expiry=legs[0].time_to_expiry,
    )
