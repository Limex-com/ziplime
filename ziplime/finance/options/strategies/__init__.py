"""Multi-leg option structures: how to build them, and what they are worth.

An option strategy is a set of legs with signed sizes, and almost everything anyone wants to know
about one -- where it breaks even, the most it can make, the most it can lose, what it is worth if
the underlying finishes here -- follows from the legs by arithmetic. This module does that
arithmetic once, instead of once per strategy file.

Two numbers that are constantly confused and are kept apart here:

* **payoff** is what the structure is worth at expiry, before what was paid for it. Never negative
  for a long option, always non-positive for a short one.
* **profit** is payoff minus the net premium. This is the P&L, and it is what breakevens, maximum
  profit and maximum loss are all measured on.

Everything is quoted **per one unit of the structure** in money, with the contract multiplier
already applied: an iron condor whose maximum loss is 400 loses 400 per condor, not 4.00. Size it
by multiplying, or pass ``quantity`` to :meth:`OptionStrategy.orders`.

Payoffs are exact rather than sampled. A piecewise-linear function of the underlying bends only at
the strikes, so the breakevens are solved on the segments between them and the extremes are
evaluated at the kinks -- which is why a wide iron condor reports its true maximum loss instead of
whatever the sampling grid happened to land on.
"""

from ziplime.finance.options.strategies.option_leg import OptionLeg  # noqa: F401
from ziplime.finance.options.strategies.option_strategy import OptionStrategy  # noqa: F401
from ziplime.finance.options.strategies.utils import single  # noqa: F401
from ziplime.finance.options.strategies.utils import vertical_spread  # noqa: F401
from ziplime.finance.options.strategies.utils import straddle  # noqa: F401
from ziplime.finance.options.strategies.utils import strangle  # noqa: F401
from ziplime.finance.options.strategies.utils import butterfly  # noqa: F401
from ziplime.finance.options.strategies.utils import iron_condor  # noqa: F401
from ziplime.finance.options.strategies.utils import iron_butterfly  # noqa: F401
from ziplime.finance.options.strategies.utils import risk_reversal  # noqa: F401
from ziplime.finance.options.strategies.utils import ratio_spread  # noqa: F401
from ziplime.finance.options.strategies.utils import custom  # noqa: F401
from ziplime.finance.options.strategies.utils import condor_around  # noqa: F401


__all__ = [
    'OptionLeg',
    'OptionStrategy',
    'single',
    'vertical_spread',
    'straddle',
    'strangle',
    'butterfly',
    'iron_condor',
    'iron_butterfly',
    'risk_reversal',
    'ratio_spread',
    'custom',
    'condor_around',
]
