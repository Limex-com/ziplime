import datetime
from dataclasses import dataclass

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.data.domain.fx_observation import FXObservation


@dataclass(frozen=True)
class MonetaryValuation:
    """Immutable current monetary estimate with price and FX provenance.

    Attributes
    ----------
    asset : ExchangeAsset
        Listing whose quote currency and instrument conventions determine value.
    quantity : float
        Signed shares, contracts, or currency units; this is not a money amount.
    native_value : float
        Signed ``quantity * instrument_unit_value`` in the listing's quote
        currency, including option multipliers or bond accrued interest.
    native_currency : str
        ISO code of the listing's quote currency.
    value : float
        ``native_value * fx.rate`` in the requested currency.
    currency : str
        ISO code of the requested reporting currency.
    valuation_at : datetime.datetime
        Aware time at which the estimate was requested.
    reference_price : float
        Input market or limit quote, before instrument-specific monetary scaling.
    price_at : datetime.datetime
        Aware availability time of the input quote, no later than ``valuation_at``.
    fx : FXObservation
        As-of conversion rate and source-listing/timestamp provenance.

    Notes
    -----
    Fields are constructor arguments. This is a current estimate, not historical
    acquisition cost, a fill, a fee-inclusive reservation, or buying power.
    """

    asset: ExchangeAsset
    quantity: float
    native_value: float
    native_currency: str
    value: float
    currency: str
    valuation_at: datetime.datetime
    reference_price: float
    price_at: datetime.datetime
    fx: FXObservation
