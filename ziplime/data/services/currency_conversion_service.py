import datetime
import math
from bisect import bisect_right
from collections.abc import Sequence
from typing import Self

import polars as pl

from ziplime.assets.entities.currency import Currency
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.data.domain.fx_observation import FXObservation
from ziplime.data.services.bundle_storage import BundleStorage


class CurrencyConversionService:
    """As-of FX conversion using asset metadata and unadjusted currency bars.

    Parameters
    ----------
    listings : Sequence[ExchangeAsset]
        Persisted currency/currency listings, one per unordered pair. Pair
        direction comes from ``listing.asset`` and ``listing.quote``, not symbols.
    data : polars.DataFrame
        Bars with ``sid``, ``price_column``, and ``timestamp_column``. Rates must
        be positive and finite; timestamps must be aware, non-null, and unique
        per listing. Prices express quote-currency units per asset-currency unit.
    max_rate_age : datetime.timedelta, optional
        Positive maximum elapsed quote age, default seven days. Quotes older
        than this are rejected, never forward-filled indefinitely.
    price_column : str, optional
        Unadjusted FX quote column, default ``"close"``.
    timestamp_column : str, optional
        Quote availability-time column, default ``"date"``. A daily bar's label
        must not be used as its close's availability time if that would look ahead.

    Attributes
    ----------
    max_rate_age : datetime.timedelta
        Freshness bound also used by ``MonetaryValuationService`` for input prices.
    _rates : dict[tuple[str, str], tuple[list[datetime.datetime], list[float]]]
        Selected directed pairs mapped to sorted UTC availability times and rates.
    _sids : dict[tuple[str, str], int]
        Source listing SID for each stored pair, retained for FX provenance.

    Notes
    -----
    Observations are stored in UTC. Direct and inverse conversions are supported;
    cross-currency routing is not inferred. This service values amounts but does
    not execute trades or mutate native cash.
    """

    def __init__(
            self,
            listings: Sequence[ExchangeAsset],
            data: pl.DataFrame,
            max_rate_age: datetime.timedelta = datetime.timedelta(days=7),
            price_column: str = "close",
            timestamp_column: str = "date",
    ):
        """Validate selected listings and index their timestamped FX observations."""
        if max_rate_age <= datetime.timedelta(0):
            raise ValueError("max_rate_age must be positive.")
        self.max_rate_age = max_rate_age
        self._rates: dict[tuple[str, str], tuple[list[datetime.datetime], list[float]]] = {}
        self._sids: dict[tuple[str, str], int] = {}
        dtype = data.schema.get(timestamp_column)
        if not isinstance(dtype, pl.Datetime) or dtype.time_zone is None:
            raise ValueError("FX observation timestamps must be timezone-aware Datetime values.")
        for listing in listings:
            if not isinstance(listing.asset, Currency) or not isinstance(listing.quote, Currency):
                raise ValueError(f"{listing} is not a currency/currency listing.")
            if listing.sid is None:
                raise ValueError("FX listings must have persisted SIDs.")
            pair = (listing.asset.asset_name, listing.quote.asset_name)
            if pair in self._rates or pair[::-1] in self._rates:
                raise ValueError(f"Select exactly one FX listing for {pair}.")
            frame = (data.filter(pl.col("sid") == listing.sid)
                     .select(pl.col(timestamp_column).dt.convert_time_zone("UTC").alias("date"),
                             pl.col(price_column).cast(pl.Float64).alias("rate"))
                     .sort("date"))
            if frame.is_empty():
                raise ValueError(f"No FX observations for {listing}.")
            if frame["date"].null_count() or frame["date"].n_unique() != frame.height:
                raise ValueError(f"FX timestamps must be non-null and unique for {listing}.")
            values = frame["rate"].to_list()
            if any(value is None or not math.isfinite(value) or value <= 0 for value in values):
                raise ValueError(f"FX rates must be finite and positive for {listing}.")
            self._rates[pair] = (frame["date"].to_list(), values)
            self._sids[pair] = listing.sid

    @classmethod
    async def from_bundle(
            cls,
            storage: BundleStorage,
            bundle: DataBundle,
            listings: Sequence[ExchangeAsset],
            start: datetime.datetime,
            end: datetime.datetime,
            max_rate_age: datetime.timedelta = datetime.timedelta(days=7),
            price_column: str = "close",
            timestamp_column: str = "date",
    ) -> Self:
        """Load FX bars asynchronously, including the initial as-of lookback.

        Parameters
        ----------
        storage : BundleStorage
            Existing bundle reader, such as Delta Lake storage.
        bundle : DataBundle
            Currency market-data bundle and its calendar.
        listings : Sequence[ExchangeAsset]
            Explicit persisted FX listings to load and index.
        start, end : datetime.datetime
            Aware simulation bounds, with ``start <= end``. Loading begins at
            ``start - max_rate_age`` so the initial valuation can use prior quotes.
        max_rate_age : datetime.timedelta, optional
            Maximum quote age and lookback, default seven days.
        price_column, timestamp_column : str, optional
            Quote and availability-time columns, as in the constructor.

        Returns
        -------
        Self
            Service initialized with the loaded observations; no storage remains open here.

        Raises
        ------
        ValueError
            If bounds are naive/reversed or loaded observations fail constructor validation.
        """
        if start.tzinfo is None or end.tzinfo is None:
            raise ValueError("FX loading bounds must be timezone-aware.")
        if start > end:
            raise ValueError("FX loading start must not follow end.")
        data = await storage.load_data_bundle(
            data_bundle=bundle, assets=list(listings),
            start_date=(start - max_rate_age).astimezone(tz=bundle.trading_calendar.tz),
            end_date=end.astimezone(tz=bundle.trading_calendar.tz),
        )
        return cls(
            listings=listings,
            data=data,
            max_rate_age=max_rate_age,
            price_column=price_column,
            timestamp_column=timestamp_column,
        )

    def rate(self, base: str, quote: str, dt: datetime.datetime) -> float:
        """Return the as-of multiplier converting ``base`` amounts to ``quote``.

        Parameters
        ----------
        base, quote : str
            Source and destination ISO currency codes.
        dt : datetime.datetime
            Aware valuation time; future quotes are never used.

        Returns
        -------
        float
            Destination units per source unit, or one for identical currencies.

        Raises
        ------
        ValueError
            If time is naive, the pair is unavailable, or no fresh prior quote exists.

        See Also
        --------
        observation : Obtain the same rate together with timestamp and listing provenance.
        """
        return self.observation(base=base, quote=quote, dt=dt).rate

    def observation(self, base: str, quote: str, dt: datetime.datetime) -> FXObservation:
        """Select the latest available quote at or before an aware valuation time.

        Parameters
        ----------
        base, quote : str
            Source and destination ISO currency codes. A selected inverse pair
            is reciprocated automatically.
        dt : datetime.datetime
            Aware valuation time, compared by instant against UTC observations.

        Returns
        -------
        FXObservation
            Rate and original availability time/listing SID. Identity conversion
            uses rate one, ``dt`` as its timestamp, and no listing SID.

        Raises
        ------
        ValueError
            If time is naive, neither pair direction is selected, or the latest
            prior quote is missing or older than ``max_rate_age``.
        """
        if dt.tzinfo is None or dt.utcoffset() is None:
            raise ValueError("FX valuation timestamps must be timezone-aware.")
        if base == quote:
            return FXObservation(base_currency=base, currency=quote, rate=1.0, observed_at=dt, listing_sid=None)
        inverse = (base, quote) not in self._rates
        pair = (quote, base) if inverse else (base, quote)
        if pair not in self._rates:
            raise ValueError(f"No direct or inverse FX listing selected for {base}->{quote}.")
        dates, values = self._rates[pair]
        ix = bisect_right(a=dates, x=dt) - 1
        if ix < 0 or dt - dates[ix] > self.max_rate_age:
            raise ValueError(f"No fresh FX observation for {base}->{quote} at {dt.isoformat()}.")
        return FXObservation(
            base_currency=base,
            currency=quote,
            rate=1.0 / values[ix] if inverse else values[ix],
            observed_at=dates[ix],
            listing_sid=self._sids[pair],
        )

    def rates_at(self, base: str, quote: str, timestamps: Sequence[datetime.datetime]) -> pl.Series:
        """Return as-of multipliers in the supplied timestamp order.

        Parameters
        ----------
        base, quote : str
            Source and destination ISO codes.
        timestamps : Sequence[datetime.datetime]
            Aware valuation times; ordering and repeated timestamps are preserved.

        Returns
        -------
        polars.Series
            Series named ``"fx_rate"``, one value per timestamp.

        Raises
        ------
        ValueError
            If any lookup fails the validation described by ``observation``.
        """
        return pl.Series(
            name="fx_rate",
            values=[self.rate(base=base, quote=quote, dt=dt) for dt in timestamps],
            dtype=pl.Float64,
        )
