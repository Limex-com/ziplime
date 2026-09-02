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
import dataclasses

import structlog

from ziplime.assets.entities.exchange_asset import ExchangeAsset

_logger = structlog.get_logger(__name__)

#: Emitted once per simulation when futures are traded without a margin model. Kept as a literal
#: so reports and tests can key on it.
NO_MARGIN_MODEL_WARNING = "NO FUTURES MARGIN MODEL"

#: Fallback when a contract does not say which currency its margin is posted in.
DEFAULT_MARGIN_CURRENCY = "RUB"


class MissingFxRate(ValueError):
    """Raised when margin would have to cross currencies without a rate to do it with."""

    def __init__(self, quote_currency: str, margin_currency: str, root_symbol: str | None = None):
        super().__init__(
            f"Margin for {root_symbol or 'this contract'} is posted in {margin_currency} but the "
            f"contract is quoted in {quote_currency}. A rate-of-notional model cannot convert "
            f"between them; pass fx_rates={{('{quote_currency}', '{margin_currency}'): rate}} or "
            f"use PerRootFuturesMarginModel, which quotes margin per contract in the currency the "
            f"exchange actually collects."
        )
        self.quote_currency = quote_currency
        self.margin_currency = margin_currency


def notional(asset: ExchangeAsset, amount: float, price: float) -> float:
    """Return the absolute notional of a futures position, in the contract's quote currency."""
    multiplier = getattr(asset.asset, "multiplier", 1.0)
    return abs(amount) * price * multiplier


def margin_currency_of(asset: ExchangeAsset) -> str:
    """Currency the exchange collects margin for ``asset`` in."""
    return getattr(asset.asset, "margin_currency", None) or DEFAULT_MARGIN_CURRENCY


def quote_currency_of(asset: ExchangeAsset) -> str:
    """Currency ``asset`` is quoted in, taken from its quote asset."""
    quote = getattr(asset, "quote", None)
    return getattr(quote, "asset_name", None) or DEFAULT_MARGIN_CURRENCY


class FuturesMarginModel:
    """Base class: how much capital a futures position ties up, and in which currency."""

    #: False for the placeholder model, so callers can tell "no margin" from "zero margin".
    models_margin = True

    def initial_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        """Margin required to open ``amount`` contracts at ``price``, in ``asset``'s margin currency."""
        raise NotImplementedError("initial_margin")

    def maintenance_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        """Margin that must remain posted while the position is held."""
        raise NotImplementedError("maintenance_margin")


class NoFuturesMarginModel(FuturesMarginModel):
    """The default: margin is not modelled, and the backtest says so.

    Results remain valid for PnL, but leverage, buying power and margin calls are not simulated,
    so a strategy can hold positions no broker would fund.
    """

    models_margin = False

    def __init__(self):
        self._warned = False

    def warn_once(self) -> None:
        """Emit the one-time realism warning."""
        if self._warned:
            return
        self._warned = True
        _logger.warning(
            f"{NO_MARGIN_MODEL_WARNING}: futures positions are being taken without a margin "
            f"model, so leverage and margin calls are not simulated. PnL is still correct; "
            f"treat position sizing and leverage figures as unconstrained. Pass a "
            f"futures_margin_model to model margin."
        )

    def initial_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        return 0.0

    def maintenance_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        return 0.0


@dataclasses.dataclass
class FixedRateFuturesMarginModel(FuturesMarginModel):
    """Margin as a fixed share of notional -- how an exchange quotes a risk rate.

    Notional is in the contract's **quote** currency, so when margin is collected in a different
    one -- a dollar-quoted contract margined in another currency -- a rate alone is not enough.
    Supply ``fx_rates`` for those pairs or the model refuses rather than silently reporting roubles
    of margin as dollars.

    Args:
        initial_rate: Share of notional required to open, e.g. ``0.15`` for 15%.
        maintenance_rate: Share required to keep the position; defaults to ``initial_rate``.
        fx_rates: ``{(quote currency, margin currency): rate}``, where ``rate`` multiplies a
            quote-currency amount to give a margin-currency one.
    """

    initial_rate: float
    maintenance_rate: float | None = None
    fx_rates: dict[tuple[str, str], float] = dataclasses.field(default_factory=dict)
    models_margin = True

    def __post_init__(self):
        if self.initial_rate <= 0:
            raise ValueError("initial_rate must be positive")
        if self.maintenance_rate is None:
            self.maintenance_rate = self.initial_rate
        if self.maintenance_rate > self.initial_rate:
            raise ValueError("maintenance_rate cannot exceed initial_rate")

    def _in_margin_currency(self, asset: ExchangeAsset, amount_in_quote: float) -> float:
        quote = quote_currency_of(asset)
        margin = margin_currency_of(asset)
        if quote == margin:
            return amount_in_quote
        rate = self.fx_rates.get((quote, margin))
        if rate is None:
            raise MissingFxRate(quote, margin, getattr(asset.asset, "root_symbol", None))
        return amount_in_quote * rate

    def initial_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        return self._in_margin_currency(asset, notional(asset, amount, price) * self.initial_rate)

    def maintenance_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        return self._in_margin_currency(asset,
                                        notional(asset, amount, price) * self.maintenance_rate)


@dataclasses.dataclass
class PerRootFuturesMarginModel(FuturesMarginModel):
    """Margin quoted per contract per root, which is how exchanges publish it.

    Amounts are already in the currency the exchange collects, so this model needs no FX rate and
    is the right choice for a book that spans venues.

    Args:
        initial_by_root: ``root symbol -> amount per contract, in that root's margin currency``.
        default_per_contract: Used for a root that is not listed.
        maintenance_ratio: Maintenance margin as a share of initial margin.
    """

    initial_by_root: dict[str, float]
    default_per_contract: float = 0.0
    maintenance_ratio: float = 1.0
    models_margin = True

    def _per_contract(self, asset: ExchangeAsset) -> float:
        root_symbol = getattr(asset.asset, "root_symbol", None)
        return self.initial_by_root.get(root_symbol, self.default_per_contract)

    def initial_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        return abs(amount) * self._per_contract(asset)

    def maintenance_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        return self.initial_margin(asset, amount, price) * self.maintenance_ratio
