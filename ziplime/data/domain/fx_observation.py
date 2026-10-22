import datetime
from dataclasses import dataclass


@dataclass(frozen=True)
class FXObservation:
    """Immutable as-of FX quote and its provenance.

    Attributes
    ----------
    base_currency : str
        ISO code of the source currency being converted.
    currency : str
        ISO code of the destination currency.
    rate : float
        Destination-currency units per source-currency unit. Multiply by this
        rate to convert a source amount; inverse listings are already inverted.
    observed_at : datetime.datetime
        Aware timestamp when the underlying quote became available.
    listing_sid : int or None
        Persisted currency-listing SID. Same-currency conversions use ``None``,
        a rate of one, and the valuation time as ``observed_at``.
    """

    base_currency: str
    currency: str
    rate: float
    observed_at: datetime.datetime
    listing_sid: int | None
