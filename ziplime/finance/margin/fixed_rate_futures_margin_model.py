"""Fixed rate futures margin model for margin."""

import dataclasses
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.margin.futures_margin_model import FuturesMarginModel
from ziplime.finance.margin.missing_fx_rate import MissingFxRate
from ziplime.finance.margin.utils import margin_currency_of
from ziplime.finance.margin.utils import notional
from ziplime.finance.margin.utils import quote_currency_of


@dataclasses.dataclass
class FixedRateFuturesMarginModel(FuturesMarginModel):
    """Margin as a fixed share of notional -- how an exchange quotes a risk rate.

    Notional is in the contract's **quote** currency, so when margin is collected in a different
    one -- a dollar-quoted contract margined in another currency -- a rate alone is not enough.
    Supply ``fx_rates`` for those pairs or the model refuses rather than silently reporting euros
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

    def __post_init__(self) -> None:
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
