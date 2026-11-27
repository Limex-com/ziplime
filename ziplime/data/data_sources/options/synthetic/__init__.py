"""A synthetic 0DTE option chain built on top of a real underlying.

**These prices never traded.** They are generated from the underlying's real bars and a volatility
model, and they are for building and testing option machinery -- chain selection, multi-leg
structures, expiry settlement, Greeks, order plumbing -- not for measuring what a strategy would
have earned. The difference is not a disclaimer, it is the design: no bid-ask ever widens because
a market maker pulled back, no strike ever gaps because someone had to hedge, and the volatility
surface has no relationship to the underlying's next move. A premium-selling strategy backtested
on this will look profitable **because the model prices options with a variance risk premium and
then delivers exactly the model's own volatility**, which is a tautology, not a result. See
:data:`SYNTHETIC_DATA_WARNING` and :func:`refuse_performance_claims`.

What it does reproduce, because these are what option code has to be correct against:

* A chain listed fresh each session and expiring the same day, centred on that session's open --
  so contracts appear, are traded, expire and are settled, every session, the way 0DTE does.
* Prices that decay to exactly intrinsic value at the closing bar, with the decay accelerating
  through the afternoon rather than running linearly.
* A put-skewed smile whose wings turn up (:mod:`.surface`).
* An option's high and low taken from the *underlying's* low and high on a put, which is the sort
  of thing that is wrong in every hand-rolled fixture and silently inverts stop logic.
* Volume and open interest concentrated at the money and building through the session, and
  spreads that widen into the wings, so liquidity-aware selection has something to select on.
"""

from ziplime.data.data_sources.options.synthetic.support import (  # noqa: F401
    SYNTHETIC_DATA_WARNING,
)
from ziplime.data.data_sources.options.synthetic.synthetic_data_performance_claim import (  # noqa: F401
    SyntheticDataPerformanceClaim,
)
from ziplime.data.data_sources.options.synthetic.validation import (  # noqa: F401
    refuse_performance_claims,
)
from ziplime.data.data_sources.options.synthetic.chain_spec import ChainSpec  # noqa: F401
from ziplime.data.data_sources.options.synthetic.liquidity_model import LiquidityModel  # noqa: F401
from ziplime.data.data_sources.options.synthetic.synthetic_option_chain_source import (  # noqa: F401
    SyntheticOptionChainSource,
)


__all__ = [
    'SYNTHETIC_DATA_WARNING',
    'SyntheticDataPerformanceClaim',
    'refuse_performance_claims',
    'ChainSpec',
    'LiquidityModel',
    'SyntheticOptionChainSource',
]
