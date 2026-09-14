"""The volatility shape the synthetic chain is priced off.

This is a **shape**, not a calibration. It reproduces the three properties of a 0DTE surface that
change what a strategy does, and claims nothing beyond them:

1. **The level tracks the underlying.** At-the-money volatility is derived from the underlying's
   own trailing realised volatility, so a synthetic chain over a turbulent week is expensive and
   one over a quiet week is cheap. A fixed level would make every premium-selling strategy look
   identical in every regime, which is the one thing a development harness must not do.

2. **Puts cost more than calls.** Equity index skew is not a detail; it is most of why selling
   downside is paid for. A chain without it makes a put spread and a call spread look like mirror
   images, and every structure built on the difference between them becomes untestable.

3. **The wings turn up.** Far out of the money, implied volatility rises on both sides. Without
   the curvature term a far wing prices at nearly nothing and an iron condor's long legs come out
   free -- which quietly turns a defined-risk structure into a naked one in the arithmetic.

What it does **not** reproduce: the term structure (there is one expiry), the intraday path of
implied volatility (the level is fixed within a session while the price decays with time), jumps
around scheduled events, and any relationship between the skew and the underlying's return. Those
absences are why the output is for development and testing, not for measuring what a strategy
would have earned.
"""
import dataclasses
import math

#: Furthest from the money, in maturity-adjusted percent, that the shape is trusted. Beyond it the
#: quadratic term would dominate and produce volatilities of several hundred percent; the input is
#: clamped instead, so the wings flatten rather than exploding.
MAX_MONEYNESS = 4.0

#: Maturity the smile parameters describe, in years. One day -- they were chosen for 0DTE, where
#: the smile is at its steepest in percent-of-spot terms.
REFERENCE_MATURITY = 1.0 / 365.0

#: Shortest maturity the steepness is allowed to scale by. Without a floor the adjustment divides
#: by ``sqrt(t)`` all the way to zero and the smile becomes a vertical line in the closing minutes.
#: An hour.
MIN_MATURITY = 1.0 / (365.0 * 24.0)


@dataclasses.dataclass(frozen=True)
class VolatilitySurface:
    """A smile in one expiry, parametrised in percent away from the money and scaled by maturity.

    With ``x`` the strike's distance from spot as a percentage (``x = (K - S) / S * 100``, so
    negative below the money) and ``z`` that distance adjusted for how much time is left::

        z     = x / sqrt(t / one day)
        sigma = atm x (1 + skew x (-z) + curvature x z^2)

    clamped to ``[floor_ratio, ceiling_ratio]`` times ``atm``.

    **The maturity scaling is not decoration.** A smile is steep in percent-of-spot terms precisely
    because little time is left: five percent out of the money is many standard deviations away on
    a zero-day option and less than one on a monthly. Without the scaling, one set of parameters
    cannot describe both -- and the failure is not subtle. Parameters chosen for a 0DTE chain,
    applied to a monthly one over a grid reaching 20% from the money, priced a 330 call **above**
    a 320 call: a vertical spread whose long leg cost more than its short one, which is not a
    market, it is an arbitrage, and every structure built on it is nonsense.

    Parametrising in percent rather than in standardised log-moneyness is still deliberate -- the
    textbook normalisation divides by ``sqrt(t)`` all the way to zero -- but the time dependence
    has to be there, so it is divided by a **floored** maturity instead.

    Attributes:
        skew: How much steeper the put wing is, per adjusted percent. 0.25 puts a 1%-out put about
            55% above the at-the-money volatility on a one-day option, which is the right order.
        curvature: How fast both wings turn up, per adjusted percent squared.
        floor_ratio: Lower clamp, as a multiple of at-the-money volatility.
        ceiling_ratio: Upper clamp.
    """

    skew: float = 0.25
    curvature: float = 0.30
    floor_ratio: float = 0.60
    ceiling_ratio: float = 4.00

    def implied_volatility(self, atm_volatility: float, spot: float, strike: float,
                           years_to_expiry: float = REFERENCE_MATURITY) -> float:
        """Implied volatility for one strike, annualised."""
        if spot <= 0 or atm_volatility <= 0:
            return max(atm_volatility, 0.0)
        steepness = math.sqrt(max(years_to_expiry, MIN_MATURITY) / REFERENCE_MATURITY)
        x = (strike - spot) / spot * 100.0 / steepness
        z = max(-MAX_MONEYNESS, min(MAX_MONEYNESS, x))
        ratio = 1.0 + self.skew * (-z) + self.curvature * z * z
        ratio = max(self.floor_ratio, min(self.ceiling_ratio, ratio))
        return atm_volatility * ratio


@dataclasses.dataclass(frozen=True)
class AtmVolatilityModel:
    """Where the at-the-money level comes from: the underlying's own realised volatility.

    Measured from **intraday** returns of the preceding sessions, not from daily closes. For a
    contract that lives one session that is not a refinement, it is the difference between a
    working model and a useless one: five sessions of history give four close-to-close returns,
    whose standard error is so wide that the level it produces is noise, and on a short window it
    lands at the floor -- which prices a whole 0DTE chain at zero and makes every structure built
    on it free. The same five sessions give nearly four hundred five-minute returns.

    Implied volatility also sits above realised on average. That gap is the variance risk premium,
    and it is the reason selling options is a business rather than a coin flip. ``premium`` applies
    it as a multiplier; setting it to 1.0 removes it, which is a control worth running rather than
    a bug.

    Attributes:
        lookback_sessions: How many preceding sessions the estimate is measured over.
        premium: Multiplier applied to realised volatility.
        seed: Level used for sessions with no history in front of them -- the first session of any
            window. A guess, and deliberately a visible one: it is what the opening chain of the
            run is priced at.
        minimum: Floor on the annualised level. Well below anything SPY has sustained, so it binds
            only on degenerate input.
        maximum: Ceiling, so one gap does not price the whole chain at 200%.
    """

    lookback_sessions: int = 5
    premium: float = 1.15
    seed: float = 0.15
    minimum: float = 0.08
    maximum: float = 1.00

    def annualised(self, returns: list[float], periods_per_year: float) -> float:
        """Annualised volatility from a series of log returns sampled ``periods_per_year`` times.

        Returns :attr:`seed` when there is not enough to measure, rather than a number derived
        from two observations.
        """
        sample = [r for r in returns if r is not None and math.isfinite(r)]
        if len(sample) < 3 or periods_per_year <= 0:
            return self.seed
        mean = math.fsum(sample) / len(sample)
        variance = math.fsum((r - mean) ** 2 for r in sample) / (len(sample) - 1)
        annualised = math.sqrt(variance) * math.sqrt(periods_per_year) * self.premium
        return max(self.minimum, min(self.maximum, annualised))
