import datetime
from copy import copy
from dataclasses import fields, replace

from ziplime.finance.domain.currency_reporting import CurrencyReporting
from ziplime.finance.domain.ledger import Ledger
from ziplime.finance.domain.investment_snapshot import InvestmentSnapshot


class ReportingLedger(Ledger):
    """Currency-specific performance view with independent return/metric history.

    Parameters
    ----------
    source : Ledger
        Execution ledger whose portfolio/account have been updated by the caller.
    reporting : CurrencyReporting
        Accounting base, FX conversion service, and investment-mode configuration.
    currency : str
        Destination ISO reporting code.
    initial_dt : datetime.datetime
        Aware simulation-start time for initial equity, FX, and lifetime-PNL baselines.

    Attributes
    ----------
    source : Ledger
        Native execution source; this view never processes strategy fills itself.
    reporting : CurrencyReporting
        Shared reporting configuration.
    currency : str
        Currency of book-level monetary aggregates and independent performance history.
    investment : InvestmentSnapshot or None
        Most recently refreshed lifetime security accounting in investment mode.
    initial_investment_pnl : float
        Lifetime security PNL already present at simulation start, subtracted
        when attributing simulation-period portfolio PNL to cash FX.
    _initial_rate : float
        Fixed base-to-reporting FX multiplier at simulation start.
    _event_ix : int
        Index of the next unconsumed source reporting cash-flow event.
    _external_capital : float
        Translated external flow total consumed from reporting events.
    _cash_flow_total : float
        Internal cash flows translated at each event's FX rate.
    _initial_external : float
        Historical external capital already present at the simulation start;
        excluded from the simulation-period contribution adjustment.

    Notes
    -----
    Balances use valuation-time FX; cash flows/contributions use posting-time
    FX. Native orders, fills, positions, and their quote/cost-basis fields are
    not converted. ``refresh`` updates values, not execution or return-history rows.
    """

    def __init__(
            self, source: Ledger, reporting: CurrencyReporting, currency: str,
            initial_dt: datetime.datetime,
    ):
        """Capture initial currency/equity baselines and refresh the reporting view."""
        super().__init__(
            trading_sessions=source.daily_returns_series.index,
            data_frequency=source.position_tracker.data_frequency,
        )
        self.source = source
        self.reporting = reporting
        self.currency = currency
        self._initial_rate = self._rate(dt=initial_dt)
        self._event_ix = 0
        self._external_capital = 0.0
        self._cash_flow_total = source.portfolio.cash_flow * self._initial_rate
        self.investment: InvestmentSnapshot | None = None
        self.initial_investment_pnl = 0.0
        self._initial_external = (
            source.investments.external_value(
                currency=currency,
                dt=initial_dt,
            ) if source.investments is not None else 0.0)
        self.refresh(dt=initial_dt)
        if self.investment is not None:
            self.initial_investment_pnl = self.investment.total_pnl

    def _rate(self, dt: datetime.datetime) -> float:
        """Return accounting-base to reporting-currency FX at aware time ``dt``."""
        return self.reporting.conversion.rate(base=self.reporting.base_currency, quote=self.currency, dt=dt)

    def refresh(self, dt: datetime.datetime) -> None:
        """Refresh the view and consume new source cash-flow reporting events.

        Parameters
        ----------
        dt : datetime.datetime
            Aware current valuation time. The caller must first update native
            portfolio/account values and any desired position market marks.

        Notes
        -----
        Keeps initial equity fixed, excludes external capital at contribution-time
        FX, and preserves source execution records in native units. Investment
        mode also refreshes historical FIFO PNL. Missing/stale FX, invalid assets,
        or unreconciled investment positions raise ``ValueError``.
        """
        rate = self._rate(dt=dt)
        native = self.source.portfolio
        for position in self.source.position_tracker.get_position_list():
            self.source.validate_reporting_asset(asset=position.asset)
        external_capital = self._external_capital
        cash_flow_total = self._cash_flow_total
        for stamp, amount, external in self.source.reporting_events[self._event_ix:]:
            converted = amount * self._rate(dt=stamp)
            if external:
                external_capital += converted
            else:
                cash_flow_total += converted
        self._external_capital = external_capital
        self._cash_flow_total = cash_flow_total
        self._event_ix = len(self.source.reporting_events)
        value = native.portfolio_value * rate
        initial_value = native.starting_cash * self._initial_rate
        self._portfolio = replace(
            native,
            starting_cash=initial_value,
            portfolio_value=value,
            cash=native.cash * rate,
            cash_flow=self._cash_flow_total,
            pnl=value - initial_value - self._external_capital,
            returns=(1 + native.returns) * rate / self._initial_rate - 1,
            positions_value=native.positions_value * rate,
            positions_exposure=native.positions_exposure * rate,
            positions={key: replace(position) for key, position in native.positions.items()},
        )
        nonmonetary = {"cushion", "day_trades_remaining", "leverage", "net_leverage", "gross_leverage"}
        self._account = copy(x=self.source.account)
        for field in fields(class_or_instance=self.source.account):
            if field.name not in nonmonetary:
                setattr(self._account, field.name, getattr(self.source.account, field.name) * rate)
        # Freeze stats rather than revalue converted quotes (bond and derivative quotes have
        # instrument-specific units). All book-level money shares one declared base currency.
        self.position_tracker = copy(x=self.source.position_tracker)
        stats = copy(x=self.source.position_tracker.stats)
        for name in ("gross_exposure", "gross_value", "long_exposure", "long_value",
                     "net_exposure", "net_value", "short_exposure", "short_value"):
            setattr(stats, name, getattr(stats, name) * rate)
        if stats.position_exposure_array is not None:
            stats.position_exposure_array = stats.position_exposure_array.copy() * rate
        if stats.position_exposure_series is not None:
            stats.position_exposure_series = stats.position_exposure_series.copy() * rate
        self.position_tracker._stats = stats
        self.position_tracker._dirty_stats = False
        if self.source.investments is not None:
            self.investment = self.source.investment_snapshot(currency=self.currency, dt=dt)
            self._portfolio.pnl = (
                value - initial_value -
                (self.source.investments.external_value(currency=self.currency, dt=dt) - self._initial_external))

    @property
    def positions(self):
        """Return copied source positions, with native prices and average cost bases."""
        return self.source.positions

    def transactions(self, dt=None):
        """Return copied native fills for aware timestamp ``dt``, or all current records if omitted."""
        return [replace(transaction) for transaction in self.source.transactions(dt=dt)]

    def orders(self, dt=None):
        """Return shallow copies of native orders at ``dt``, or all current records if omitted."""
        return [copy(x=order) for order in self.source.orders(dt=dt)]
