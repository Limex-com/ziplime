"""Futures margin models.

A futures position ties up margin rather than cash, so a backtest that ignores margin lets a
strategy run at leverage no broker would grant and still look excellent. ziplime therefore always
carries a margin model: the default one explicitly models *no* margin and says so, loudly, the
first time a futures position is opened.

**Margin currency is not quote currency.** An exchange may quote a contract in dollars and collect
margin for it in its own local currency, while another collects dollars. A book spanning both posts
margin in two currencies, and those cannot be added together without an FX rate that ziplime does
not carry. Requirements are therefore reported *per currency*.

Pick a real model when leverage matters::

    from ziplime.finance.margin import PerRootFuturesMarginModel
    margin_model = PerRootFuturesMarginModel(
        initial_by_root={"Si": 13_700.0, "NG.XNYM": 3_500.0},   # in each root's margin currency
        default_per_contract=5_000.0)
"""

from ziplime.finance.margin.support import NO_MARGIN_MODEL_WARNING  # noqa: F401
from ziplime.finance.margin.support import DEFAULT_MARGIN_CURRENCY  # noqa: F401
from ziplime.finance.margin.missing_fx_rate import MissingFxRate  # noqa: F401
from ziplime.finance.margin.utils import notional  # noqa: F401
from ziplime.finance.margin.utils import quote_currency_of  # noqa: F401
from ziplime.finance.margin.utils import margin_currency_of  # noqa: F401
from ziplime.finance.margin.futures_margin_model import FuturesMarginModel  # noqa: F401
from ziplime.finance.margin.no_futures_margin_model import NoFuturesMarginModel  # noqa: F401
from ziplime.finance.margin.fixed_rate_futures_margin_model import (  # noqa: F401
    FixedRateFuturesMarginModel,
)
from ziplime.finance.margin.per_root_futures_margin_model import (  # noqa: F401
    PerRootFuturesMarginModel,
)


__all__ = [
    'NO_MARGIN_MODEL_WARNING',
    'DEFAULT_MARGIN_CURRENCY',
    'MissingFxRate',
    'notional',
    'quote_currency_of',
    'margin_currency_of',
    'FuturesMarginModel',
    'NoFuturesMarginModel',
    'FixedRateFuturesMarginModel',
    'PerRootFuturesMarginModel',
]
