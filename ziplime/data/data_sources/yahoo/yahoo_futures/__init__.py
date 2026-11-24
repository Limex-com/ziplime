"""Futures chains on Yahoo Finance: real dated contracts, and the specifications Yahoo omits.

Yahoo carries **individual dated contracts** under ``ROOT + month code + 2-digit year + exchange
suffix`` -- ``ESZ26.CME``, ``CLX26.NYM`` -- and each one has its own price history, its own
expiration date and its own place on the curve. That is a real chain, and it is what makes rolls,
calendar spreads and term structure possible against real data.

Two limits shape what can be built on it, and both are worth knowing before planning a study:

* **Expired contracts are removed.** ``ESZ24.CME`` returns 404, and so does every contract that has
  already settled. A chain can therefore be walked *forward* from today but not reconstructed
  backwards, so a long historical backtest across many past rolls is not available here.
  Contracts that have not yet expired carry their **full** history -- ``ESU27.CME`` goes back two
  and a half years -- so the curve itself is deep even though the past rolls are gone.
* **Volume is unreliable on dated contracts.** Most sessions report zero, even for the front
  month. A volume-based roll finder has nothing to work with, so
  :class:`~ziplime.assets.domain.roll_finder.CalendarRollFinder` is the one to use with this data.
  The continuous ``ROOT=F`` series does carry real volume.

Yahoo also publishes a **continuous front-month series** per root, ``ROOT=F``, which it rolls on
its own timing with no record of when. The ingest does not use it -- a stitched series has no chain
and no curve -- but it is described here (:func:`is_continuous_ticker`, :func:`root_for_continuous`)
so a caller that asks for ``ES=F`` by name can still be told what contract specification is behind
it, and because it is the one place Yahoo reports usable futures volume.

There is also nothing at all about the **contract specification** in the payload. The multiplier --
the number that turns a quote into money -- is absent, so it comes from the table below,
transcribed from the exchanges' published terms. Each entry is checked in
:mod:`tests.test_yahoo_futures` against the notional and tick value it implies: an E-mini S&P
contract is about $296 000 of exposure at 5 916 index points and its tick is $12.50, which is a
figure anyone who trades it recognises. A wrong multiplier passes every type check and silently
misstates every position by a factor of a hundred.
"""

from ziplime.data.data_sources.yahoo.yahoo_futures.support import CONTINUOUS_SUFFIX  # noqa: F401
from ziplime.data.data_sources.yahoo.yahoo_futures.support import MONTH_CODES  # noqa: F401
from ziplime.data.data_sources.yahoo.yahoo_futures.support import MONTH_OF_CODE  # noqa: F401
from ziplime.data.data_sources.yahoo.yahoo_futures.support import QUARTERLY  # noqa: F401
from ziplime.data.data_sources.yahoo.yahoo_futures.support import MONTHLY  # noqa: F401
from ziplime.data.data_sources.yahoo.yahoo_futures.support import GRAIN  # noqa: F401
from ziplime.data.data_sources.yahoo.yahoo_futures.yahoo_futures_root import (  # noqa: F401
    YahooFuturesRoot,
)
from ziplime.data.data_sources.yahoo.yahoo_futures.roots import YAHOO_FUTURES_ROOTS  # noqa: F401
from ziplime.data.data_sources.yahoo.yahoo_futures.roots import (  # noqa: F401
    ROOTS_BY_CONTINUOUS_TICKER,
)
from ziplime.data.data_sources.yahoo.yahoo_futures.contract_ticker_value import (  # noqa: F401
    ContractTicker,
)
from ziplime.data.data_sources.yahoo.yahoo_futures.utils import contract_ticker  # noqa: F401
from ziplime.data.data_sources.yahoo.yahoo_futures.utils import (  # noqa: F401
    parse_contract_ticker,
)
from ziplime.data.data_sources.yahoo.yahoo_futures.utils import candidate_tickers  # noqa: F401
from ziplime.data.data_sources.yahoo.yahoo_futures.utils import (  # noqa: F401
    is_continuous_ticker,
)
from ziplime.data.data_sources.yahoo.yahoo_futures.utils import root_for_continuous  # noqa: F401
from ziplime.data.data_sources.yahoo.yahoo_futures.utils import root_for_contract  # noqa: F401
from ziplime.data.data_sources.yahoo.yahoo_futures.utils import parse_expiry  # noqa: F401
from ziplime.data.data_sources.yahoo.yahoo_futures.utils import fallback_expiry  # noqa: F401


__all__ = [
    'CONTINUOUS_SUFFIX',
    'MONTH_CODES',
    'MONTH_OF_CODE',
    'QUARTERLY',
    'MONTHLY',
    'GRAIN',
    'YahooFuturesRoot',
    'YAHOO_FUTURES_ROOTS',
    'ROOTS_BY_CONTINUOUS_TICKER',
    'ContractTicker',
    'contract_ticker',
    'parse_contract_ticker',
    'candidate_tickers',
    'is_continuous_ticker',
    'root_for_continuous',
    'root_for_contract',
    'parse_expiry',
    'fallback_expiry',
]
