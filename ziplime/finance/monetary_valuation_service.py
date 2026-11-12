import datetime
import math

from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.equity import Equity
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.entities.option_contract import OptionContract
from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.finance.bonds import BondBook
from ziplime.finance.domain.monetary_valuation import MonetaryValuation


class MonetaryValuationService:
    """Value instrument quantities before comparing or aggregating currencies.

    Parameters
    ----------
    conversion : CurrencyConversionService
        Shared as-of FX service for native quote currencies and reporting currencies.
    bond_book : BondBook or None, optional
        Existing bond schedule/face-value book. A new empty book is used when omitted;
        callers must load schedules needed for accurate bond accrued interest.

    Attributes
    ----------
    conversion : CurrencyConversionService
        FX observations and the freshness bound applied to prices.
    bond_book : BondBook
        Bond monetary-value conventions shared with execution accounting.
    """

    def __init__(self, conversion: CurrencyConversionService, bond_book: BondBook | None = None):
        """Bind FX conversion and the optional shared bond schedule book."""
        self.conversion = conversion
        self.bond_book = bond_book if bond_book is not None else BondBook()

    def unit_value(self, asset: ExchangeAsset, price: float, dt: datetime.datetime) -> float:
        """Translate a quote into native money for one instrument unit.

        Parameters
        ----------
        asset : ExchangeAsset
            Listing identifying the instrument and its quote currency.
        price : float
            Finite, nonnegative quote: money per share/currency unit, option
            premium per underlying unit, or the bond's configured quote convention.
        dt : datetime.datetime
            Valuation time used for outstanding bond face value and accrued interest.

        Returns
        -------
        float
            Native value per unit: price for equities/currencies, premium times
            multiplier for premium-paid options, or dirty monetary value for bonds.

        Raises
        ------
        ValueError
            If the quote is invalid or the instrument is unsupported. Margined
            futures/options require settlement accounting instead of principal cost.
        """
        if not math.isfinite(price) or price < 0:
            raise ValueError("An instrument price must be finite and nonnegative.")
        instrument = asset.asset
        if isinstance(instrument, FuturesContract) or (
                isinstance(instrument, OptionContract) and instrument.premium_style.is_margined):
            raise ValueError("Margined instruments require settlement accounting, not investment cost-basis PNL.")
        if isinstance(instrument, Bond):
            return self.bond_book.dirty_value(bond=instrument, quoted_price=price, dt=dt)
        if isinstance(instrument, OptionContract):
            return price * instrument.multiplier
        if isinstance(instrument, (Equity, Currency)):
            return price
        raise ValueError(f"No investment monetary valuation for {type(instrument).__name__}.")

    def value(
            self, asset: ExchangeAsset, quantity: float, price: float,
            price_at: datetime.datetime, dt: datetime.datetime, currency: str,
    ) -> MonetaryValuation:
        """Estimate a signed quantity's current value and retain quote/FX provenance.

        Parameters
        ----------
        asset : ExchangeAsset
            Instrument listing; its quote currency determines the native amount.
        quantity : float
            Finite signed shares, contracts, or currency units.
        price : float
            Finite, nonnegative quote to scale with ``unit_value``.
        price_at : datetime.datetime
            Aware quote availability time, no later than ``dt`` and no older than
            ``conversion.max_rate_age``.
        dt : datetime.datetime
            Aware current valuation time, also used for bond accrual and as-of FX.
        currency : str
            Destination ISO currency code.

        Returns
        -------
        MonetaryValuation
            Immutable signed native/converted values with the input quote and FX
            observation. Fees, slippage, and historical acquisition cost are excluded.

        Raises
        ------
        ValueError
            If inputs are invalid, the price is stale/future-dated, the instrument
            is unsupported, or a required fresh FX observation is unavailable.
        """
        if not math.isfinite(quantity):
            raise ValueError("Quantity must be finite.")
        if price_at.tzinfo is None or dt.tzinfo is None:
            raise ValueError("Price and valuation timestamps must be timezone-aware.")
        if price_at > dt:
            raise ValueError("Cannot value an investment using a future price.")
        if dt - price_at > self.conversion.max_rate_age:
            raise ValueError("The instrument price is stale.")
        native_currency = asset.quote.asset_name
        native_value = quantity * self.unit_value(asset=asset, price=price, dt=dt)
        fx = self.conversion.observation(base=native_currency, quote=currency, dt=dt)
        return MonetaryValuation(
            asset=asset,
            quantity=quantity,
            native_value=native_value,
            native_currency=native_currency,
            value=native_value * fx.rate,
            currency=currency,
            valuation_at=dt,
            reference_price=price,
            price_at=price_at,
            fx=fx,
        )
