"""The seam between "where option chains come from" and everything that consumes them.

There is exactly one interface here, and it exists so that the synthetic generator in this package
and a real quote feed are interchangeable. A chain source answers two questions and nothing else:

1. **What contracts existed?** -- :meth:`OptionChainSource.contracts`, per session. This is what
   gets written to the asset database and minted into sids.
2. **What were they worth?** -- :meth:`OptionChainSource.bars`, per contract per timestamp.

Where the answers come from is the implementation's business. :class:`SyntheticOptionChainSource`
derives both from the underlying's own bars and a volatility model; a gRPC source would open a
channel in its constructor and stream them. Neither appears in the other's signature, which is the
point: nothing downstream -- the bundle, the chain selectors, the strategies -- can tell which one
it is holding.

A real feed differs from the synthetic one in three ways that the interface deliberately allows
for rather than papering over:

* **Strikes are listed, not computed.** A real 0DTE chain is not a tidy window around the money;
  it is whatever the exchange listed, it is wider than anyone trades, and it is asymmetric. The
  interface returns the contracts a source knows about rather than a rule for generating them.
* **Bars have gaps.** A far wing stops being quoted for minutes at a time. :meth:`bars` may return
  no row for a contract at a timestamp, and everything downstream already handles that -- it is
  the same "the data stopped" path a delisting takes.
* **Prices are quotes, not a model.** ``implied_volatility`` is a column a real source fills from
  its own quotes and the synthetic one fills from the model it priced with, so a strategy reading
  it works either way.
"""
import abc
import dataclasses
import datetime

import polars as pl

from ziplime.assets.domain.exercise_style import ExerciseStyle
from ziplime.assets.domain.option_type import OptionType
from ziplime.assets.domain.premium_style import PremiumStyle
from ziplime.assets.domain.settlement_type import SettlementType
from ziplime.assets.entities.option_contract import format_occ_symbol

#: Columns :meth:`OptionChainSource.bars` must produce. The first eight are the bundle's own
#: schema; the rest are what makes an option bar usable and are specific to this asset class.
BAR_COLUMNS: tuple[str, ...] = (
    "date", "symbol", "mic", "open", "high", "low", "close", "price", "volume",
    # Option-specific, and worth carrying rather than recomputing: a strategy that wants to select
    # by implied volatility should read what the contract was priced at, not re-invert the price.
    "bid", "ask", "implied_volatility", "underlying_price", "open_interest",
)


@dataclasses.dataclass(frozen=True)
class ContractSpec:
    """One contract a source says exists, before the asset database has given it a sid.

    Deliberately not an :class:`~ziplime.assets.entities.option_contract.OptionContract`: that
    entity carries database identity and a resolved underlying, and a source knows neither. This
    is the vendor's view -- what a chain endpoint returns -- and
    :func:`ziplime.data.data_sources.options.ingest.ingest_option_chains` is what turns a list of
    these into stored contracts and listings.
    """

    underlying_symbol: str
    expiration_date: datetime.date
    option_type: OptionType
    strike: float
    mic: str
    #: First session the contract trades. For 0DTE this equals ``expiration_date``, and that
    #: equality is the whole definition of the product rather than an accident of the generator.
    listed_date: datetime.date
    multiplier: float = 100.0
    tick_size: float = 0.01
    exercise_style: ExerciseStyle = ExerciseStyle.AMERICAN
    settlement_type: SettlementType = SettlementType.CASH
    #: Whether the premium is paid at the trade or margined daily. ``UPFRONT`` for OPRA,
    #: ``MARGINED`` for MOEX/FORTS. The accounting differs completely between the two, so a source
    #: that gets this wrong produces a book that is silently free or silently double-counted -- see
    #: :class:`~ziplime.assets.domain.premium_style.PremiumStyle`.
    premium_style: PremiumStyle = PremiumStyle.UPFRONT
    #: What the **venue** calls this contract, when that is not its OCC symbol.
    #:
    #: OCC is a US convention. MOEX names the same kind of instrument ``SR310CI6`` -- root, strike,
    #: a letter that encodes both month and side, year -- and a strategy on that venue expects to
    #: see that, not a synthesised ``SBER260916C00310000``. The listing is stored under this name
    #: when it is given; :attr:`occ_symbol` still describes the contract canonically either way.
    symbol: str | None = None
    #: How the *source* names this contract, when that is not its OCC symbol.
    #:
    #: Real feeds do not speak OCC. A contract is addressed by a vendor's own identifier -- a
    #: numeric security id, a Bloomberg symbol, a RIC -- and every later request for its bars uses
    #: that, not the symbol ziplime stores it under. Kept here so the round trip survives: it is
    #: written to the listing's ``external_id``, which exists for exactly this, and a source can
    #: read it back to ask for quotes without rebuilding its own mapping.
    vendor_id: str | None = None

    @property
    def occ_symbol(self) -> str:
        """The canonical OCC description of this contract, whatever its venue calls it."""
        return format_occ_symbol(self.underlying_symbol, self.expiration_date,
                                 self.option_type, self.strike)

    @property
    def listing_symbol(self) -> str:
        """The name the listing is stored under: the venue's own, or OCC when it has none."""
        return self.symbol or self.occ_symbol

    @property
    def is_zero_dte(self) -> bool:
        """Whether this contract is listed and expires on the same session."""
        return self.listed_date == self.expiration_date

    def __hash__(self):
        return hash((self.listing_symbol, self.mic))


class OptionChainSource(abc.ABC):
    """Supplies option chains and their bars over a window."""

    #: Whether the prices this source produces describe a market that existed. ``False`` marks a
    #: model, and everything that reports performance is expected to check it -- see
    #: :func:`ziplime.data.data_sources.options.synthetic.refuse_performance_claims`.
    is_real_market_data: bool = True

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """Short identifier, used in bundle names and log lines."""

    @abc.abstractmethod
    async def contracts(self, underlying_symbol: str, mic: str,
                        sessions: list[datetime.date]) -> list[ContractSpec]:
        """Every contract that existed on any of ``sessions`` for this underlying.

        For a 0DTE source this is one chain per session, each expiring on the session it was
        listed on, so the result grows linearly with the window -- a month is twenty chains and
        several hundred contracts, all of which are real and none of which outlive their day.
        """

    @abc.abstractmethod
    async def bars(self, contracts: list[ContractSpec],
                   timestamps: pl.Series) -> pl.DataFrame:
        """Bars for ``contracts`` at ``timestamps``, with :data:`BAR_COLUMNS`.

        ``timestamps`` is the simulation clock's own grid, already timezone-aware, so a source
        must stamp its bars on those instants rather than on its vendor's. A contract with nothing
        to report at a timestamp simply has no row.
        """
