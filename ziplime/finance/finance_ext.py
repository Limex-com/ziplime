from math import sqrt
from collections.abc import Iterable

import numpy as np
import pandas as pd
import structlog
import polars as pl

from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.futures_contract import FuturesContract
from ziplime.assets.entities.option_contract import OptionContract
from ziplime.finance.bonds import BondBook
from ziplime.finance.domain.position import Position
from ziplime.finance.position_stats import PositionStats


logger = structlog.get_logger(__name__)


def _bond_unit_value(bond_book: BondBook | None, bond: Bond, position: Position) -> float:
    """Money one bond of ``position`` is worth at its current mark.

    Falls back to the bare quote when no schedule is loaded and the bond quotes in money; for a
    percent-quoted bond the face value alone is enough to get the magnitude right, so the
    conversion still happens without a schedule.
    """
    as_of = position.last_sale_date
    if bond_book is None:
        from ziplime.finance.bonds import BondBook
        bond_book = BondBook()
    return bond_book.dirty_value(bond, position.last_sale_price, as_of)


def calculate_position_tracker_stats(
    positions: Iterable[Position],
    position_count: int,
    stats: PositionStats,
    bond_book: BondBook | None = None,
):
    """Calculate various stats about the current positions.

    Parameters
    ----------
    positions : list[Position]
        The positions with a non-zero amount.
    position_count : int
        How many positions ``positions`` holds, in total, across exchanges and accounts.
    stats : PositionStats
        The object to write the results into, reusing its arrays where they already fit.
    bond_book : ziplime.finance.bonds.BondBook, optional
        Coupon and amortization schedules. Bonds are quoted as a percentage of face value and
        settle at the dirty price, so without it a bond position is valued at its bare quote --
        roughly a tenth of its worth for a standard 1000-unit nominal.

    Returns
    -------
    position_stats : PositionStats
        The computed statistics.
    """
    npos = position_count

    old_index = stats.underlying_index_array
    old_position_exposure = (
        stats.underlying_value_array
    )

    long_value = 0.0
    short_value = 0.0

    long_exposure = 0.0
    short_exposure = 0.0

    longs_count = 0
    shorts_count = 0

    # attempt to reuse the memory of the old exposure series
    if len(old_index) < npos:
        # we don't have enough space in the cached buffer, allocate a new
        # array
        stats.underlying_index_array = index = np.empty(npos, dtype='int64')
        stats.underlying_value_array = position_exposure = np.empty(
            npos,
            dtype='float64',
        )

        stats.position_exposure_array = position_exposure
        # create a new series to expose the arrays
        stats.position_exposure_series = pd.Series(
            position_exposure,
            index=index,
        )
    elif len(old_index) > npos:
        # we have more space than needed, slice off the extra but leave it
        # available
        index = old_index[:npos]
        position_exposure = old_position_exposure[:npos]

        stats.position_exposure_array = position_exposure
        # create a new series with the sliced arrays
        stats.position_exposure_series = pd.Series(
            position_exposure,
            index=index,
        )
    else:
        # we have exactly the right amount of space, no slicing or allocation
        # needed
        index = old_index
        position_exposure = old_position_exposure

        stats.position_exposure_array = position_exposure
        stats.position_exposure_series = pd.Series(
            position_exposure,
            index=index,
        )

    ix = 0

    for position in positions:
        # NOTE: this loop does a lot of stuff!
        # we call this function every time the portfolio value is needed,
        # which is at least once per simulation day, so let's not iterate
        # through every single position multiple times.
        exposure = position.amount * position.last_sale_price
        instrument = position.asset.asset
        if type(instrument) is FuturesContract:
            # Futures don't have an inherent position value.
            value = 0

            # unchecked cast, this is safe because we do a type check above
            exposure *= instrument.multiplier
        elif type(instrument) is OptionContract:
            exposure *= instrument.multiplier
            if instrument.premium_style.is_margined:
                # Nothing was paid for it, so there is nothing to be worth. A margined option is
                # carried exactly like a futures position: the P&L has already reached cash
                # through variation margin, and counting a value here as well would book the same
                # money twice.
                value = 0
            else:
                # A premium-paid option *is* worth something: it was bought and paid for, and a
                # short one is a liability that has to be bought back. Which is why a short option
                # shows negative value here, exactly like a short equity.
                value = exposure
        elif type(instrument) is Bond:
            # A bond quote is a percentage of face value, and a holding is worth the
            # dirty price: what it would fetch is the clean value plus the coupon
            # accrued so far, which the buyer would have to hand over.
            per_bond = _bond_unit_value(bond_book, instrument, position)
            value = exposure = position.amount * per_bond
        else:
            value = exposure
        if exposure > 0:
            longs_count += 1
            long_value += value
            long_exposure += exposure
        elif exposure < 0:
            shorts_count += 1
            short_value += value
            short_exposure += exposure

        index[ix] = position.asset.sid
        position_exposure[ix] = exposure

        ix += 1

    net_value = long_value + short_value
    gross_value = long_value - short_value

    net_exposure = long_exposure + short_exposure
    gross_exposure = long_exposure - short_exposure

    stats.gross_exposure = gross_exposure
    stats.gross_value = gross_value
    stats.long_exposure = long_exposure
    stats.long_value = long_value
    stats.longs_count = longs_count
    stats.net_exposure = net_exposure
    stats.net_value = net_value
    stats.short_exposure = short_exposure
    stats.short_value = short_value
    stats.shorts_count = shorts_count


def minute_annual_volatility(date_labels: pl.Series | np.ndarray,
                             minute_returns: pl.Series | np.ndarray,
                             daily_returns: pl.Series | np.ndarray):
    """Pre-compute the minute cumulative volatility field.
    """
    out = np.empty_like(minute_returns)
    previous_date = date_labels[0]
    day_ix = 0
    daily_sum = 0
    todays_prod = 1
    annualization_factor = sqrt(252.0)

    for ix in range(len(minute_returns)):
        date = date_labels[ix]
        this_minute_returns = minute_returns[ix]

        if date != previous_date:
            previous_date = date
            daily_sum += daily_returns[day_ix]
            day_ix += 1
            todays_prod = 1

        if day_ix < 1:
            variance = np.nan
        else:
            todays_prod *= 1 + this_minute_returns

            intermediate_sum = daily_sum + todays_prod - 1
            mean = intermediate_sum / (day_ix + 1)

            variance = todays_prod - 1 - mean
            variance *= variance  # squared

            demeaned_old = daily_returns[:day_ix] - mean
            variance += demeaned_old.dot(demeaned_old)

            # variance /= day_ix  # day_count - 1 for ddof=1
            variance = variance/day_ix
        # print(ix, sqrt(variance) * annualization_factor)
        res = sqrt(variance) * annualization_factor
        if np.isnan(res):
            out[ix] = 0
        else:
            out[ix] = res

    return out
