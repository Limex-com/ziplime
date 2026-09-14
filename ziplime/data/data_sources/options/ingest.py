"""Turning whatever a chain source reports into stored contracts, listings and a bundle.

One function does the whole thing -- :func:`build_option_bundle` -- and it is the same for a
synthetic source and for a real one, which is the point of the seam in :mod:`.source`.

The order matters and is not obvious:

1. Ask the source which contracts existed on each session.
2. Write them to the asset database, then write a **listing** for each. The listing is what mints
   the ``sid`` that bars are keyed by, and it has to exist before any bar can be filed under it.
3. Read the chain back to learn those sids. Not from the write: re-ingesting an already-stored
   chain legitimately writes nothing, and taking the sids from the return value would silently
   produce an empty bundle on the second run.
4. Ask the source for bars, map symbols to sids, and put the option bars in one frame with the
   **underlying's** -- one bundle, because a strategy that trades an option and reads its
   underlying's price is doing one thing, not two.
"""
import datetime

import polars as pl
import structlog

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.entities.option_contract import OptionContract
from ziplime.assets.services.asset_service import AssetService
from ziplime.constants.data_type import DataType
from ziplime.data.data_sources.options.source import ContractSpec, OptionChainSource
from ziplime.data.domain.data_bundle import DataBundle

_logger = structlog.get_logger(__name__)

#: Columns the bundle carries for every instrument in it. The option-specific ones are null on the
#: underlying's own rows, which is what `how="diagonal"` leaves behind and is the honest answer:
#: an ETF has no strike and no implied volatility.
BUNDLE_COLUMNS: tuple[str, ...] = (
    "date", "sid", "symbol", "mic", "open", "high", "low", "close", "price", "volume",
    "bid", "ask", "implied_volatility", "underlying_price", "open_interest",
)


async def register_contracts(specs: list[ContractSpec], asset_service: AssetService,
                             underlying: ExchangeAsset) -> list[OptionContract]:
    """Write ``specs`` to the asset database as contracts and listings.

    Idempotent: a contract already stored under its OCC symbol is left alone, so re-running a
    window costs a lookup rather than a duplicate chain.
    """
    if not specs:
        return []
    contracts = [
        OptionContract(
            id=None,
            isin=None,
            asset_name=spec.listing_symbol,
            # A 0DTE contract's whole life is one session, and these four dates say so. The
            # auto-close date being the expiration date is what makes the engine settle it at the
            # end of that session rather than carrying it forward -- see
            # `TradingAlgorithm._settlement_price`.
            start_date=spec.listed_date,
            first_traded=spec.listed_date,
            end_date=spec.expiration_date,
            auto_close_date=spec.expiration_date,
            underlying_exchange_asset=underlying,
            underlying_asset=underlying.asset,
            underlying_symbol=spec.underlying_symbol,
            option_type=spec.option_type,
            strike=spec.strike,
            expiration_date=spec.expiration_date,
            multiplier=spec.multiplier,
            tick_size=spec.tick_size,
            exercise_style=spec.exercise_style,
            settlement_type=spec.settlement_type,
            premium_style=spec.premium_style,
        )
        for spec in specs
    ]
    stored = await asset_service.save_option_contracts(contracts)
    # The source's own identifier for each contract, so a later request for its bars can be made
    # in the vendor's terms rather than in OCC's. Falls back to the OCC symbol for a source that
    # has no separate id -- the synthetic one does not.
    vendor_ids = {spec.listing_symbol: spec.vendor_id for spec in specs}
    await asset_service.save_exchange_assets(exchange_assets=[
        ExchangeAsset(
            sid=None,
            symbol=contract.asset_name,
            start_date=contract.start_date,
            end_date=contract.end_date,
            first_traded=contract.first_traded,
            auto_close_date=contract.auto_close_date,
            external_id=vendor_ids.get(contract.asset_name) or contract.asset_name,
            exchange=underlying.exchange,
            asset=contract,
            quote=underlying.quote,
        )
        for contract in stored
    ])
    return stored


async def build_option_bundle(
        source: OptionChainSource,
        asset_service: AssetService,
        underlying: ExchangeAsset,
        underlying_bars: pl.DataFrame,
        sessions: list[datetime.date],
        timestamps: pl.Series,
        trading_calendar,
        emission_rate: datetime.timedelta,
        bundle_name: str | None = None,
) -> tuple[DataBundle, list[ExchangeAsset]]:
    """Assemble one bundle holding the underlying's bars and its option chains'.

    Args:
        source: Where the chains come from. Synthetic or real; this function cannot tell.
        asset_service: Where contracts and listings are written.
        underlying: The listing the chains are written on, already stored.
        underlying_bars: Its bars, carrying ``date``, ``sid`` and the OHLCV columns.
        sessions: Sessions to list chains for.
        timestamps: The simulation clock's grid. Option bars are stamped on these instants.
        trading_calendar: Calendar the bundle and the run share.
        emission_rate: Bar interval.
        bundle_name: Defaults to the source's name, which for a synthetic source begins with
            ``synthetic-`` -- so what the bundle is made of is visible wherever it is named.

    Returns:
        The bundle, and the option listings in it.
    """
    specs = await source.contracts(underlying_symbol=underlying.symbol, mic=underlying.mic,
                                   sessions=sessions)
    await register_contracts(specs, asset_service=asset_service, underlying=underlying)

    # Read back rather than trusting the write: an already-stored chain writes nothing.
    #
    # Looked up by the expiries the chain actually has, not by the run's sessions. Iterating
    # sessions assumed every contract expires inside the window, which is true of a 0DTE chain and
    # of nothing else: a December call held through a summer window expires months after the last
    # session, so it was registered and then never found, and the failure read as "no listings"
    # rather than as "the query only asks for same-window expiries". For a 0DTE chain the two are
    # the same set, so this changes nothing there.
    expiries = sorted({spec.expiration_date for spec in specs})
    listings: list[ExchangeAsset] = []
    for expiration_date in expiries:
        listings.extend(await asset_service.get_exchange_option_contracts(
            underlying_symbol=underlying.symbol, expiration_date=expiration_date,
            mic=underlying.mic))
    if not listings:
        raise ValueError(
            f"No option listings for {underlying.symbol} over {sessions[0]}..{sessions[-1]} after "
            f"registering {len(specs)} contracts expiring {expiries[0] if expiries else '-'}.."
            f"{expiries[-1] if expiries else '-'}. Nothing can be traded.")

    option_bars = await source.bars(contracts=specs, timestamps=timestamps)
    sid_by_symbol = {listing.symbol: listing.sid for listing in listings}
    unknown = set(option_bars["symbol"].unique()) - set(sid_by_symbol) if not option_bars.is_empty() else set()
    if unknown:
        raise ValueError(
            f"{len(unknown)} contracts have bars but no listing, e.g. {sorted(unknown)[:3]}. The "
            f"source reported bars for contracts it did not report in `contracts`.")

    if option_bars.is_empty():
        raise ValueError(
            f"{source.name} produced no option bars over {sessions[0]}..{sessions[-1]}. A 0DTE "
            f"chain only has bars inside its own session -- check that the clock's timestamps fall "
            f"on the sessions the chains were listed for.")
    option_bars = option_bars.with_columns(
        pl.col("symbol").replace_strict(sid_by_symbol, return_dtype=pl.Int64).alias("sid"))

    # Both frames carry `sid`, and they arrive with different integer widths -- the underlying's
    # from a literal, the options' from a symbol-to-sid mapping. A diagonal concat refuses to
    # reconcile Int32 with Int64, so they are made to agree before they meet.
    underlying_bars = underlying_bars.with_columns(pl.col("sid").cast(pl.Int64))
    option_bars = option_bars.with_columns(pl.col("sid").cast(pl.Int64))
    data = pl.concat([underlying_bars, option_bars], how="diagonal").select(
        [column for column in BUNDLE_COLUMNS if column in
         set(underlying_bars.columns) | set(option_bars.columns)]
    ).sort(["sid", "date"])

    spans = data.with_row_index().group_by("sid", maintain_order=True).agg(
        [pl.col("index").first().alias("first"), pl.col("index").last().alias("last")])
    bundle = DataBundle(
        name=bundle_name or source.name,
        version="1",
        start_date=data["date"].min(),
        end_date=data["date"].max(),
        trading_calendar=trading_calendar,
        frequency=emission_rate,
        original_frequency=emission_rate,
        data_type=DataType.MARKET_DATA,
        timestamp=data["date"].max(),
        data=data,
        sid_indexes={row["sid"]: (row["first"], row["last"] + 1)
                     for row in spans.iter_rows(named=True)},
        asset_service=asset_service,
    )
    _logger.info("Built option bundle", name=bundle.name, contracts=len(listings),
                 option_bars=option_bars.height, sessions=len(sessions),
                 real_market_data=source.is_real_market_data)
    return bundle, listings
