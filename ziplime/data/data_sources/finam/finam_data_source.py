"""Market-data source that fetches OHLCV bars for MOEX FORTS instruments from Finam."""
import asyncio
import datetime
from typing import Self

import polars as pl
import structlog

from ziplime.constants.period import Period
from ziplime.data.data_sources.finam.finam_client import FinamClient, parse_decimal, to_time_frame
from ziplime.data.data_sources.finam.moex_futures import RTSX_MIC
from ziplime.data.services.data_bundle_source import DataBundleSource
from ziplime.utils.date_utils import period_to_timedelta


class FinamDataSource(DataBundleSource):
    """Fetches bars for a list of FORTS tickers and shapes them for :class:`BundleService`.

    Symbols are accepted either bare (``SiZ6``) or fully qualified (``SiZ6@RTSX``); the frame always
    comes back with a bare ``symbol`` and a separate ``mic``, which is the pair the bundle ingest
    resolves to a ``sid``.

    Pass ``symbol_mics`` when the symbols span more than one venue -- a bare ``NGZ25`` is a NYMEX
    contract while a bare ``NGZ5`` is a MOEX one, and ``default_mic`` cannot tell them apart.

    Pass ``symbol_windows`` when ingesting a futures chain. A contract only trades for about two
    years, so asking for a six-year window per contract spends most of its requests on periods the
    contract did not exist -- which is both slow and the fastest way to get rate limited.

    Daily and coarser bars are stamped at the start of their session. Finam timestamps a daily bar
    at 04:00 UTC (07:00 Moscow), while the bundle ingest buckets sessions from midnight in the
    calendar's timezone, so without normalising, every session would look like it was missing.
    """

    def __init__(self, client: FinamClient, default_mic: str = RTSX_MIC,
                 symbol_windows: dict[str, tuple[datetime.date, datetime.date]] | None = None,
                 symbol_mics: dict[str, str] | None = None,
                 logger=None):
        super().__init__()
        self._client = client
        self._default_mic = default_mic
        self._symbol_windows = symbol_windows or {}
        self._symbol_mics = symbol_mics or {}
        self._logger = logger or structlog.get_logger(__name__)

    @classmethod
    def for_assets(cls, client: FinamClient, assets, **kwargs) -> Self:
        """Build a source that knows each listing's venue and fetches only over its lifetime."""
        return cls(client=client,
                   symbol_windows={a.symbol: (a.start_date, a.end_date) for a in assets},
                   symbol_mics={a.symbol: a.mic for a in assets},
                   **kwargs)

    @classmethod
    def from_env(cls, **kwargs) -> Self:
        return cls(client=FinamClient.from_env(), **kwargs)

    async def get_data(self, symbols: list[str],
                       frequency: datetime.timedelta | Period,
                       date_from: datetime.datetime,
                       date_to: datetime.datetime,
                       **kwargs) -> pl.DataFrame:
        """Fetch bars for ``symbols`` and return them as one frame.

        Returns:
            A frame with ``date, symbol, mic, open, high, low, close, volume, price``. ``date`` is
            localised to ``date_from``'s timezone, matching what the trading calendar expects.
        """
        frequency_td = period_to_timedelta(frequency)
        time_frame = to_time_frame(frequency_td)
        pairs = [self._split_symbol(symbol) for symbol in symbols]

        frames = await asyncio.gather(*(
            self._get_symbol_data(ticker, mic, time_frame,
                                  *self._window_for(ticker, date_from, date_to),
                                  session_aligned=frequency_td >= datetime.timedelta(days=1))
            for ticker, mic in pairs
        ))
        frames = [frame for frame in frames if not frame.is_empty()]
        if not frames:
            self._logger.warning("No Finam data for requested symbols", symbols=symbols,
                                 date_from=date_from, date_to=date_to)
            return pl.DataFrame()
        return pl.concat(frames)

    def _window_for(self, ticker: str, date_from: datetime.datetime,
                    date_to: datetime.datetime) -> tuple[datetime.datetime, datetime.datetime]:
        """Clip the requested range to the listing's own lifetime, when it is known."""
        window = self._symbol_windows.get(ticker)
        if window is None:
            return date_from, date_to
        start, end = window
        listed_from = datetime.datetime.combine(start, datetime.time.min, tzinfo=date_from.tzinfo)
        # +1 day so the final trading session is inside the half-open interval the API expects.
        listed_to = datetime.datetime.combine(end + datetime.timedelta(days=1), datetime.time.min,
                                              tzinfo=date_to.tzinfo)
        return max(date_from, listed_from), min(date_to, listed_to)

    def _split_symbol(self, symbol: str) -> tuple[str, str]:
        """Resolve ``TICKER`` or ``TICKER@MIC`` to ``(ticker, mic)``.

        A bare ticker is looked up in the known listings first: the same bare name can belong to a
        different venue depending on the contract.
        """
        ticker, _, mic = symbol.partition("@")
        return ticker, mic or self._symbol_mics.get(ticker) or self._default_mic

    async def _get_symbol_data(self, ticker: str, mic: str, time_frame: str,
                               date_from: datetime.datetime,
                               date_to: datetime.datetime,
                               session_aligned: bool) -> pl.DataFrame:
        if date_to <= date_from:
            return pl.DataFrame()
        bars = await self._client.bars(f"{ticker}@{mic}", time_frame, date_from, date_to)
        if not bars:
            self._logger.warning("No bars returned", symbol=f"{ticker}@{mic}",
                                 date_from=date_from, date_to=date_to)
            return pl.DataFrame()

        rows = {
            "date": [], "open": [], "high": [], "low": [], "close": [], "volume": [],
        }
        for bar in bars:
            rows["date"].append(
                datetime.datetime.fromisoformat(bar["timestamp"].replace("Z", "+00:00"))
            )
            for field in ("open", "high", "low", "close", "volume"):
                rows[field].append(parse_decimal(bar[field]))

        df = pl.DataFrame(rows, schema_overrides={
            "open": pl.Float64, "high": pl.Float64, "low": pl.Float64,
            "close": pl.Float64, "volume": pl.Float64,
        })
        df = df.with_columns(
            pl.col("date").dt.convert_time_zone(str(date_from.tzinfo)),
            pl.lit(ticker).alias("symbol"),
            pl.lit(mic).alias("mic"),
            pl.col("close").alias("price"),
        )
        if session_aligned:
            df = df.with_columns(pl.col("date").dt.truncate("1d"))
        return df.filter(pl.col("date") >= date_from, pl.col("date") <= date_to)
