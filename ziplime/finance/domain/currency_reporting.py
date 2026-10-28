from dataclasses import dataclass
from collections.abc import Mapping

from iso4217 import Currency

from ziplime.data.services.currency_conversion_service import CurrencyConversionService
from ziplime.finance.domain.investment_history import InvestmentHistory


@dataclass(frozen=True)
class CurrencyReporting:
    """Configure independent currency reports and optional investment accounting.

    Attributes
    ----------
    base_currency : str
        ISO currency code for execution portfolio/account monetary totals.
    currencies : tuple[str, ...]
        Nonempty, unique reporting ISO codes in output order. Each receives
        independent metric instances, histories, and translated benchmark returns.
    conversion : CurrencyConversionService
        Shared as-of FX service containing the required direct/inverse pairs and
        event-time history; cross-rates are not synthesized.
    benchmark_currency : str or None, optional
        Currency of supplied benchmark returns; required for ``benchmark_returns``.
        For a listing benchmark it must match the listing's quote currency.
    investment_pnl : bool, optional
        Enable native multi-currency cash and historical FIFO costs. Default
        ``False`` retains single-base-currency reporting without investment lots.
    opening_cash : Mapping[tuple[str, str, str], float] or None, optional
        Native balances keyed by ``(exchange_name, trading_account_id, currency)``.
        In investment mode these replace scalar opening cash; otherwise a
        single-exchange run seeds scalar cash in ``base_currency``. Multi-exchange
        investment runs require explicit balances. Opening balances must be
        nonnegative; orders require projected funding in the payment currency.
    investment_history : InvestmentHistory or None, optional
        Native posted events, cash, and unpaid income entitlements to restore in
        investment mode. History takes precedence over ``opening_cash``. Existing
        execution positions must reconcile to its lots and have timestamped marks.

    Raises
    ------
    ValueError
        If reporting codes are empty/duplicated or any configured ISO code is invalid.

    Notes
    -----
    All attributes are constructor arguments. This frozen configuration does not
    execute FX trades or serialize investment history. Order admission rejects
    unfunded projected costs unless a simulation enables automatic currency exchange.
    Executed trades and fees retain actual amounts if they exceed the projection.
    """

    base_currency: str
    currencies: tuple[str, ...]
    conversion: CurrencyConversionService
    benchmark_currency: str | None = None
    investment_pnl: bool = False
    opening_cash: Mapping[tuple[str, str, str], float] | None = None
    investment_history: InvestmentHistory | None = None

    def __post_init__(self) -> None:
        """Validate reporting-currency uniqueness and configured ISO currency codes."""
        if not self.currencies or len(set(self.currencies)) != len(self.currencies):
            raise ValueError("Reporting currencies must be nonempty and unique.")
        for code in (self.base_currency, *self.currencies):
            Currency(value=code)
        if self.benchmark_currency is not None:
            Currency(value=self.benchmark_currency)
