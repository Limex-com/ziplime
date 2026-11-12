"""Bars for a sweep, read the way run_simulation will read them.

The same :class:`DataBundle` feeds the sweep and the verification run, so a difference between the
two can only come from the simulation, never from the data.
"""
import contextlib
import datetime
import hashlib
import importlib
import io
import json
import logging
from pathlib import Path
from typing import Callable

import numpy as np
import pandas as pd
import polars as pl
import structlog

from ziplime.constants.data_type import DataType
from ziplime.data.domain.data_bundle import DataBundle
from ziplime.sweep.params import SweepError
from ziplime.utils.bundle_utils import get_bundle_service, get_market_data_source
from ziplime.utils.calendar_utils import get_calendar
from ziplime.vectorized.signals import PricePanel

ONE_DAY = datetime.timedelta(days=1)


async def load_market_data(*, bundle: str | None, version: str | None, storage: Path,
                           cache_dir: Path | None, universe: dict, calendar,
                           first: datetime.date, last: datetime.date,
                           say: Callable[[str], None],
                           rate: datetime.timedelta = ONE_DAY) -> DataBundle:
    """Bars for the sweep and for the verification run: the named bundle, or Yahoo."""
    if not bundle:
        if rate != ONE_DAY:
            raise SweepError("An intraday sweep needs a bundle of that frequency; Yahoo is daily.")
        return await load_yahoo(universe, calendar, first, last, cache_dir, say)

    service = get_bundle_service(str(storage))
    try:
        metadata = await service.load_bundle_metadata(bundle_name=bundle, bundle_version=version)
    except KeyError:
        metadata = "old"  # no dates in the registry entry: written by an older ziplime
    if metadata == "old" or (metadata and not _storage_class_exists(metadata)):
        loaded = read_bundle_files(storage, bundle, version, first, last, say)
        present = set(loaded.data["sid"].unique().to_list())
        if not all(listing.sid in present for listing in universe.values()):
            loaded = rekeyed(loaded, universe, bundle, say)
        say(f"Data: bundle {loaded.name} version {loaded.version} (older format, read from its "
            f"files)")
        return loaded
    if metadata is None:
        known = sorted({b["name"] for b in await service.list_bundles()})
        raise SweepError(f'No bundle "{bundle}"' + (f" version {version}" if version else "")
                         + f" in {storage}. There are: {', '.join(known)}.")

    # No wider than the bundle itself: the warm-up history may start before it does
    covered_from, covered_to = metadata["start_date"].date(), metadata["end_date"].date()
    first = max(first, covered_from)
    if last > covered_to:
        say(f"The bundle ends on {covered_to}; the run stops there.")
        last = covered_to

    async def load(assets):
        # load_bundle warns once per missing bar; the sweep reports gaps itself
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.ERROR))
        try:
            loaded, _ = await service.load_bundle(
                bundle_name=bundle, bundle_version=version, assets=assets, frequency=rate,
                start_date=datetime.datetime.combine(first, datetime.time.min, tzinfo=calendar.tz),
                end_date=datetime.datetime.combine(last, datetime.time.max, tzinfo=calendar.tz))
        except ValueError as error:
            raise SweepError(f'Bundle "{bundle}" did not load: {error}') from None
        finally:
            structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.WARNING))
        return loaded

    loaded = await load(list(universe.values()))
    present = set(loaded.data["sid"].unique().to_list())
    if not all(listing.sid in present for listing in universe.values()):
        loaded = rekeyed(await load(None), universe, bundle, say)
    say(f"Data: bundle {loaded.name} version {loaded.version}")
    return loaded


def _storage_class_exists(metadata: dict) -> bool:
    module, _, name = metadata["bundle_storage_class"].rpartition(".")
    try:
        return hasattr(importlib.import_module(module), name)
    except ModuleNotFoundError:
        return False


def _with_indexes(data: pl.DataFrame, **fields) -> DataBundle:
    spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
        [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
    return DataBundle(
        data=data, data_type=DataType.MARKET_DATA,
        sid_indexes={r["sid"]: (r["first"], r["last"] + 1) for r in spans.iter_rows(named=True)},
        **fields)


def read_bundle_files(storage: Path, name: str, version: str | None, first: datetime.date,
                      last: datetime.date, say: Callable[[str], None]) -> DataBundle:
    """A bundle the current ziplime does not open -- its registry entry has no dates, or its
    storage class is gone -- read straight from its files, on the calendar its entry names."""
    registry = storage / "bundle_registry"
    versions = sorted(path.stem[len(name) + 1:] for path in registry.glob(f"{name}_*.json")
                      if path.stem[len(name) + 1:].isdigit())
    if not versions or (version and version not in versions):
        raise SweepError(f'No bundle "{name}"' + (f" version {version}" if version else "")
                         + f" in {registry}.")
    version = version or versions[-1]
    metadata = json.loads((registry / f"{name}_{version}.json").read_text())
    folder = storage / "data_bundle" / name / version
    if (folder / "data.parquet").exists():
        data = pl.read_parquet(folder / "data.parquet")
    elif (folder / "data.delta").exists():
        data = pl.read_delta(str(folder / "data.delta"))
    else:
        raise SweepError(f"Bundle {name} version {version} has no bar file in {folder}.")

    calendar = get_calendar(metadata["trading_calendar_name"])
    data = (data.with_columns(pl.col("date").dt.convert_time_zone(str(calendar.tz)))
            .filter(pl.col("date").dt.date().is_between(first, last))
            .sort(["sid", "date"]))
    # The engine walks the calendar's sessions. Bars outside them (weekend trading on MOEX, say)
    # it never sees, so the sweep must not see them either.
    sessions = set(calendar.sessions_in_range(first, last).date)
    in_calendar = data["date"].dt.date().is_in(list(sessions))
    if not in_calendar.all():
        dropped = data.filter(~in_calendar)["date"].dt.date().unique().sort()
        say(f"{len(dropped)} days with bars outside the {calendar.name} sessions dropped (e.g. "
            f"{', '.join(map(str, dropped[:3]))}) -- the engine does not see them either.")
        data = data.filter(in_calendar)
    if data.is_empty():
        raise SweepError(f"Bundle {name} version {version} has no bars between {first} and {last}.")
    frequency = datetime.timedelta(seconds=metadata.get("frequency_seconds") or 86400)
    return _with_indexes(
        data, name=name, version=version, start_date=data["date"].min(),
        end_date=data["date"].max(), trading_calendar=calendar, frequency=frequency,
        original_frequency=frequency, timestamp=data["date"].max())


def rekeyed(bundle: DataBundle, universe: dict, name: str,
            say: Callable[[str], None]) -> DataBundle:
    """A bundle ingested against another asset database: its sids are foreign, its tickers and
    MICs are not. The bars stay the bundle's; only the numbering moves to the current database."""
    by_listing = {(listing.symbol, listing.mic): listing.sid for listing in universe.values()}
    pairs = bundle.data.select("sid", "symbol", "mic").unique()
    remap = {sid: by_listing[(symbol, mic)] for sid, symbol, mic in pairs.iter_rows()
             if (symbol, mic) in by_listing}
    absent = [f"{label} ({listing.symbol}@{listing.mic})" for label, listing in universe.items()
              if listing.sid not in set(remap.values())]
    if absent:
        raise SweepError(
            f'Bundle "{name}" has no bars for {", ".join(absent)} -- neither by sid nor by ticker '
            f'and MIC. A ticker listed on several venues needs its MIC: context.symbol("META@XNGS").')
    data = (bundle.data.filter(pl.col("sid").is_in(list(remap)))
            .with_columns(pl.col("sid").replace_strict(remap, return_dtype=pl.Int64))
            .sort(["sid", "date"]))
    say("The bundle's sids are not this asset database's (it was ingested against another one); "
        "instruments matched by ticker and MIC: "
        + ", ".join(f"{symbol}@{mic} {old}->{remap[old]}" for old, symbol, mic in pairs.iter_rows()
                    if old in remap))
    return _with_indexes(
        data, name=bundle.name, version=bundle.version, start_date=bundle.start_date,
        end_date=bundle.end_date, trading_calendar=bundle.trading_calendar,
        frequency=bundle.frequency, original_frequency=bundle.original_frequency,
        timestamp=bundle.timestamp)


async def load_yahoo(universe: dict, calendar, first: datetime.date, last: datetime.date,
                     cache_dir: Path | None, say: Callable[[str], None]) -> DataBundle:
    """Daily bars from Yahoo as a bundle. Cached in ``cache_dir``: Yahoo's adjusted prices move
    slightly from one download to the next, and a sweep has to be reproducible."""
    key = hashlib.sha1(json.dumps(
        [sorted(f"{l.symbol}@{l.mic}" for l in universe.values()), str(first), str(last)]
    ).encode()).hexdigest()[:16]
    cached = cache_dir / f"bars_{key}.parquet" if cache_dir else None
    if cached and cached.exists():
        data = pl.read_parquet(cached)
        say(f"Data: cached {cached.parent.name}/{cached.name}")
    else:
        data = await download_bars(universe, calendar, first, last)
        if cached:
            cache_dir.mkdir(parents=True, exist_ok=True)
            data.write_parquet(cached)
        say("Data: downloaded from Yahoo" + (f", cached as {cache_dir.name}/{cached.name}"
                                             if cached else ""))
    return _with_indexes(
        data, name="sweep", version="1", start_date=data["date"].min(),
        end_date=data["date"].max(), trading_calendar=calendar, frequency=ONE_DAY,
        original_frequency=ONE_DAY, timestamp=data["date"].max())


async def download_bars(universe: dict, calendar, first: datetime.date,
                        last: datetime.date) -> pl.DataFrame:
    """Bundle rows from Yahoo, stamped at the calendar's session closes."""
    by_symbol = {listing.symbol: listing for listing in universe.values()}
    source = get_market_data_source("yahoo", assets=list(universe.values()))
    # Yahoo sometimes answers partially and the source fails stitching the tickers together; a
    # second attempt usually goes through
    for attempt in range(1, 4):
        try:
            with contextlib.redirect_stdout(io.StringIO()), \
                    contextlib.redirect_stderr(io.StringIO()):  # yfinance's progress bar
                frame = await source.get_data(
                    symbols=list(by_symbol), frequency=ONE_DAY,
                    date_from=datetime.datetime.combine(first, datetime.time.min, tzinfo=calendar.tz),
                    date_to=datetime.datetime.combine(last, datetime.time.max, tzinfo=calendar.tz))
            break
        except Exception as error:
            if attempt == 3:
                raise SweepError(f"Yahoo failed three times: {error}") from None
    if frame.is_empty():
        raise SweepError("Yahoo returned no data -- check the dates and the connection.")

    sessions = calendar.sessions_in_range(first, last)
    closes = {stamp.date(): stamp.to_pydatetime() for stamp in
              calendar.schedule.loc[sessions, "close"].dt.tz_convert(calendar.tz)}
    wide = frame.to_pandas()
    wide["_d"] = pd.to_datetime(wide["date"]).dt.tz_convert(calendar.tz).dt.date
    wide = wide[wide["_d"].isin(closes)]
    rows = [{"date": closes[r["_d"]], "sid": by_symbol[r["symbol"]].sid, "symbol": r["symbol"],
             "mic": by_symbol[r["symbol"]].mic, "open": r["open"], "high": r["high"],
             "low": r["low"], "close": r["close"], "price": r["close"], "volume": r["volume"]}
            for r in wide.to_dict("records")]
    return pl.DataFrame(rows).sort(["sid", "date"])


def price_panel(bundle: DataBundle, universe: dict, start: datetime.date, warmup: int,
                say: Callable[[str], None]):
    """The panel compute_signals and the indicators see: the run plus exactly ``WARMUP`` bars
    before it, as TradingAlgorithm._trim_to_warmup cuts it."""
    names = {listing.sid: name for name, listing in universe.items()}
    panel = PricePanel.from_bundle_rows(bundle.data, names)
    positions = [ix for ix, stamp in enumerate(panel.index) if stamp.date() >= start]
    if not positions:
        raise SweepError(f"No bars from {start} on.")
    if positions[0] < warmup:
        say(f"Warning: {positions[0]} bars of history before the start, WARMUP is {warmup}.")
    first = max(0, positions[0] - warmup)
    panel = PricePanel({field: getattr(panel, field).iloc[first:] for field in panel.fields},
                       panel.index[first:])
    in_run = np.array([stamp.date() >= start for stamp in panel.index])
    return panel, in_run


async def attach_datasets(panel: PricePanel, declared: dict, universe: dict, calendar,
                          start: datetime.date, end: datetime.date, asset_service,
                          registered: dict) -> PricePanel:
    """``context.datasets`` resolved as of every bar, as TradingAlgorithm._attach_datasets does:
    the same sources, read over everything they hold up to the run's end, through the same
    as_of_panel -- so ``prices.dataset(...)`` in compute_signals sees what the engine shows it."""
    from ziplime.data.services.data_source import DataSource
    from ziplime.vectorized.signals import as_of_panel

    if not isinstance(declared, dict):
        raise SweepError(f"context.datasets has to be a dict of name -> source, got "
                         f"{type(declared).__name__}.")
    end_stamp = calendar.session_close(end).tz_convert(calendar.tz).to_pydatetime()
    names = {listing.sid: name for name, listing in universe.items()}
    datasets = {}
    for label, address in declared.items():
        if isinstance(address, DataSource):
            source = address
        elif address in registered:
            source = registered[address]
        else:
            source = await _mount(address, asset_service, calendar, start, end)
        begins = getattr(source, "start_date", None) or datetime.date(1900, 1, 1)
        if isinstance(begins, datetime.datetime):
            begins = begins if begins.tzinfo else begins.replace(tzinfo=calendar.tz)
        else:
            begins = datetime.datetime.combine(begins, datetime.time.min, tzinfo=calendar.tz)
        rows = await source.get_data_by_window(
            fields=None, since=end_stamp - begins + datetime.timedelta(days=1), end_date=end_stamp,
            frequency=ONE_DAY, assets=frozenset(universe.values()), include_end_date=True)
        coalesce = str(getattr(getattr(source, "resolution", None), "value", "")) == "coalesce"
        datasets[label] = as_of_panel(rows, names=names, index=panel.index, coalesce=coalesce)
    return PricePanel({field: getattr(panel, field) for field in panel.fields}, panel.index, datasets)


async def _mount(address: str, asset_service, calendar, start, end):
    """A dataset named by address -- hf://owner/name/config, or a scheme an extension registered."""
    import inspect

    from ziplime.data.data_sources import address_schemes
    from ziplime.data.data_sources.huggingface.huggingface_data_source import (
        HuggingFaceDataSource, is_address,
    )
    if is_address(address):
        return await HuggingFaceDataSource.mount(address, asset_service=asset_service,
                                                 start_date=start, end_date=end,
                                                 trading_calendar=calendar)
    mount = address_schemes.mount_for(address)
    if mount is None:
        raise SweepError(f"No data source named {address!r}: pass it to run_sweep("
                         f"custom_data_sources=[...]) or name a dataset by address (hf://...).")
    source = mount(address, asset_service=asset_service, start_date=start, end_date=end,
                   trading_calendar=calendar)
    return await source if inspect.isawaitable(source) else source
