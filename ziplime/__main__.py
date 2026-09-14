"""The ``ziplime`` command line.

Four things, in the order anyone actually does them:

1. ``ziplime providers`` -- which data connectors this install has, and which are configured.
2. ``ziplime ingest-assets`` -- reference data. Nothing else works before this: a bundle keys its
   bars by ``sid``, and a sid only exists once the instrument is in the asset database.
3. ``ziplime ingest`` -- the bars themselves, into a named bundle.
4. ``ziplime run`` -- a backtest over that bundle.

Every command drives the same entry points the Python API and ``examples/`` use --
:func:`ziplime.core.ingest_data.ingest_market_data` and
:func:`ziplime.core.run_simulation.run_simulation` -- rather than reaching into the engine itself.
That is deliberate: the previous version of this file called four internal constructors directly
and every one of them had moved on without it, so ``run`` and ``ingest`` had been raising
``TypeError`` on their first line for some time while ``--help`` kept working.
"""
import asyncio

import importlib.metadata
import logging
import sys
from pathlib import Path

import asyncclick as click

from ziplime.assets.domain.asset_type import AssetType
from ziplime.core.ingest_data import get_asset_service, ingest_assets, ingest_market_data
from ziplime.core.run_simulation import run_simulation
from ziplime.data.data_sources.registry import (
    MissingProviderCredentials, UnknownDataProvider, list_providers, provider_names,
)
from ziplime.domain.data_frequency import DataFrequency
from ziplime.finance.commission import PerShare
from ziplime.finance.slippage.fixed_basis_points_slippage import FixedBasisPointsSlippage
from ziplime.utils.bundle_utils import (
    get_asset_data_source, get_bundle_service, get_market_data_source,
)
from ziplime.utils.calendar_utils import get_calendar

#: Where bundles and the asset database live unless told otherwise.
DEFAULT_STORAGE = Path(Path.home(), ".ziplime", "data")
DEFAULT_ASSET_DB = Path(Path.home(), ".ziplime", "assets.sqlite")

#: Date formats every date option accepts.
DATE_FORMATS = ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S")

FREQUENCIES = [frequency.value for frequency in DataFrequency]


def storage_option(function):
    return click.option(
        "--bundle-storage-path", default=str(DEFAULT_STORAGE), show_default=True,
        help="Where bundles are stored.")(function)


def asset_db_option(function):
    return click.option(
        "--asset-db", default=str(DEFAULT_ASSET_DB), show_default=True,
        help="Asset database. Instruments have to be ingested into it before any bundle can "
             "reference them.")(function)


async def _resolve(asset_service, symbols: str, asset_type: AssetType) -> list:
    """Turn ``TICKER`` or ``TICKER@MIC`` into stored listings, or fail saying which one is missing.

    Resolving up front matters for more than error messages. A ticker is unique only within an
    asset class and often not even then -- ``T`` is a New York listing and a Moscow one -- so the
    listings, not the strings, are what the rest of the pipeline should be handed. It also keeps
    the MIC out of the download: ``@MIC`` disambiguates a row in the asset database, and a vendor
    asked for ``AAPL@XNGS`` looks for a ticker by that name and finds nothing.
    """
    from ziplime.assets.entities.asset_symbol import AssetSymbol

    listings = []
    for symbol in [s.strip() for s in symbols.split(",") if s.strip()]:
        ticker, _, mic = symbol.partition("@")
        listing = await asset_service.get_exchange_asset_by_symbol(
            symbol=AssetSymbol(symbol=ticker, mic=mic or None), asset_type=asset_type)
        if listing is None:
            _fail(f"{symbol} is not in the asset database as {asset_type.value}.",
                  "Run `ziplime ingest-assets` first, or check --asset-type.")
        listings.append(listing)
    return listings


def _fail(message: str, hint: str | None = None) -> None:
    """Report a problem the way a command line should: on stderr, with what to do next."""
    click.secho(f"error: {message}", fg="red", err=True)
    if hint:
        click.secho(f"hint: {hint}", fg="yellow", err=True)
    raise SystemExit(1)


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(importlib.metadata.version("ziplime"), "-V", "--version",
                      prog_name="ziplime")
@click.option("-v", "--verbose", is_flag=True, help="Log at DEBUG rather than INFO.")
@click.pass_context
async def main(ctx, verbose):
    """Ingest market data and run backtests.

    Start with `ziplime providers` to see what this install can read, then `ziplime ingest-assets`
    to populate the asset database -- everything else depends on it.
    """
    logging.basicConfig(
        format="[%(asctime)s %(levelname)s] %(message)s",
        level=logging.DEBUG if verbose else logging.INFO,
        datefmt="%Y-%m-%dT%H:%M:%S%z",
    )


# -- providers ---------------------------------------------------------------------------------


@main.command()
@click.option("--configured-only", is_flag=True,
              help="Show only connectors whose credentials are set.")
async def providers(configured_only):
    """List the data connectors this install can use.

    A connector is skipped rather than reported broken when its package is absent, so this is also
    how you find out whether an optional one is installed. ``env`` names the environment variables
    it needs; a connector that needs none is always ready.
    """
    found = list_providers(configured_only=configured_only)
    if not found:
        click.echo("No data providers registered.")
        return
    click.echo(f"{'name':<14}{'assets':<8}{'bars':<7}{'ready':<7}{'env':<28}description")
    click.echo("-" * 110)
    for provider in found:
        missing = provider.missing_env()
        ready = "yes" if provider.is_configured else "no"
        env = ", ".join(provider.required_env) or "-"
        click.echo(f"{provider.name:<14}{'yes' if provider.supports_assets else 'no':<8}"
                   f"{'yes' if provider.supports_market_data else 'no':<7}{ready:<7}{env:<28}"
                   f"{provider.description[:52]}")
        if missing:
            click.secho(f"{'':<36}not set: {', '.join(missing)}", fg="yellow")


# -- reference data ----------------------------------------------------------------------------


@main.command("ingest-assets")
@click.option("-p", "--provider", default="yahoo", show_default=True,
              type=click.Choice(provider_names()),
              help="Connector to read instrument definitions from.")
@asset_db_option
@click.option("--clear", is_flag=True,
              help="Delete the asset database first. Every stored sid is lost, so any bundle "
                   "already keyed by one becomes unreadable.")
async def ingest_assets_command(provider, asset_db, clear):
    """Load instrument definitions into the asset database.

    This is the prerequisite for everything else. A bundle stores bars against a ``sid``, and a
    sid is minted here; ingesting bars for a symbol the database has never heard of fails with
    "Symbols are missing in asset database".
    """
    try:
        source = get_asset_data_source(provider)
    except MissingProviderCredentials as error:
        _fail(str(error), "See `ziplime providers` for what each connector needs.")
    except UnknownDataProvider as error:
        _fail(str(error))

    asset_service = get_asset_service(db_path=asset_db, clear_asset_db=clear)
    try:
        await ingest_assets(asset_service=asset_service, asset_data_source=source)
        click.secho(f"Assets ingested into {asset_db}", fg="green")
    finally:
        await asset_service._asset_repository.engine.dispose()


# -- market data -------------------------------------------------------------------------------


@main.command()
@click.option("-b", "--bundle", required=True, help="Name to store the bundle under.")
@click.option("-s", "--symbols", required=True,
              help="Comma-separated symbols. A symbol may name its exchange as TICKER@MIC, which "
                   "is required wherever a ticker is not unique.")
@click.option("-c", "--trading-calendar", default="XNYS", show_default=True,
              help="Calendar the sessions come from.")
@click.option("--start-date", required=True, type=click.DateTime(formats=DATE_FORMATS))
@click.option("--end-date", required=True, type=click.DateTime(formats=DATE_FORMATS))
@click.option("-f", "--frequency", default="1d", show_default=True,
              type=click.Choice(FREQUENCIES), help="Bar interval.")
@click.option("-p", "--provider", default="yahoo", show_default=True,
              type=click.Choice(provider_names()), help="Connector to read bars from.")
@click.option("--asset-type", default=AssetType.EQUITY.value, show_default=True,
              type=click.Choice([t.value for t in AssetType]),
              help="Which kind of instrument the symbols name. A ticker is unique only within a "
                   "kind, so this is what stops futures bars being filed as an equity's.")
@click.option("--forward-fill/--no-forward-fill", default=True, show_default=True,
              help="Fill sessions an instrument did not trade with its previous close.")
@click.option("--merge", is_flag=True, help="Merge into an existing bundle of this name.")
@storage_option
@asset_db_option
async def ingest(bundle, symbols, trading_calendar, start_date, end_date, frequency, provider,
                 asset_type, forward_fill, merge, bundle_storage_path, asset_db):
    """Download bars for SYMBOLS and store them as a named bundle.

    Market data only. Fundamentals and other non-bar datasets go through
    :func:`ziplime.core.ingest_data.ingest_custom_data`, which takes a source object rather than a
    connector name and so has no useful command-line form.

    Run `ziplime ingest-assets` first: the symbols must already be in the asset database.
    """
    calendar = get_calendar(trading_calendar)
    asset_service = get_asset_service(db_path=asset_db)
    try:
        listings = await _resolve(asset_service, symbols, AssetType(asset_type))
        try:
            # The listings go to the connector too: it then knows each instrument's exchange
            # without a metadata lookup per symbol, which on some vendors is rate limited.
            source = get_market_data_source(provider, assets=listings)
        except MissingProviderCredentials as error:
            _fail(str(error), "See `ziplime providers` for what each connector needs.")
        except UnknownDataProvider as error:
            _fail(str(error))

        ingested = await ingest_market_data(
            start_date=start_date.replace(tzinfo=calendar.tz),
            end_date=end_date.replace(tzinfo=calendar.tz),
            trading_calendar=trading_calendar,
            bundle_name=bundle,
            # Plain tickers: the MIC was for resolving the listing, and a vendor asked for
            # "AAPL@XNGS" returns nothing at all.
            symbols=[listing.symbol for listing in listings],
            data_frequency=DataFrequency(frequency).to_timedelta(),
            data_bundle_source=source,
            asset_service=asset_service,
            merge=merge,
            forward_fill_missing_ohlcv_data=forward_fill,
            bundle_storage_path=bundle_storage_path,
            asset_type=AssetType(asset_type),
            assets=listings,
        )
        if ingested is None:
            _fail(f"{provider} returned no bars for {symbols} between "
                  f"{start_date:%Y-%m-%d} and {end_date:%Y-%m-%d}, so nothing was stored.",
                  "Check the symbols and the window against what the connector covers.")
        click.secho(f"Ingested {bundle} from {provider}: "
                    f"{len(listings)} instrument(s), {ingested.data.height:,} bars", fg="green")
    except ValueError as error:
        _fail(str(error))
    finally:
        await asset_service._asset_repository.engine.dispose()


# -- bundles -----------------------------------------------------------------------------------


@main.command()
@storage_option
async def bundles(bundle_storage_path):
    """List stored bundles, newest version of each first."""
    registry = get_bundle_service(bundle_storage_path=bundle_storage_path)._bundle_registry
    stored = await registry.list_bundles()
    if not stored:
        click.echo(f"No bundles in {bundle_storage_path}.")
        return

    by_name: dict[str, list] = {}
    for entry in stored:
        by_name.setdefault(entry["name"], []).append(entry)
    click.echo(f"{'bundle':<32}{'versions':>9}  latest")
    click.echo("-" * 70)
    for name in sorted(by_name):
        versions = sorted(by_name[name], key=lambda item: item["timestamp"], reverse=True)
        click.echo(f"{name:<32}{len(versions):>9}  {versions[0]['timestamp']}")
        for entry in versions[1:]:
            click.secho(f"{'':<41}{entry['timestamp']}", fg="bright_black")


@main.command()
@click.option("-b", "--bundle", required=True, help="Bundle to clean.")
@click.option("-e", "--before", type=click.DateTime(formats=DATE_FORMATS),
              help="Delete versions created before this. Not with --keep-last.")
@click.option("-a", "--after", type=click.DateTime(formats=DATE_FORMATS),
              help="Delete versions created after this. Not with --keep-last.")
@click.option("-k", "--keep-last", type=int, metavar="N",
              help="Keep the newest N versions and delete the rest. Not with --before/--after.")
@storage_option
async def clean(bundle, before, after, keep_last, bundle_storage_path):
    """Delete stored versions of a bundle.

    The three selectors are mutually exclusive, and this refuses rather than picking one. The help
    has said so since the command was written but nothing enforced it, so passing both silently
    applied whichever the service happened to check first -- on a command whose whole job is
    deleting data.
    """
    if keep_last is not None and (before is not None or after is not None):
        _fail("--keep-last cannot be combined with --before or --after.",
              "They select versions in different ways; pick one.")
    if before is None and after is None and keep_last is None:
        _fail("Nothing selected, so nothing would be deleted.",
              "Pass --before, --after or --keep-last.")

    service = get_bundle_service(bundle_storage_path=bundle_storage_path)
    try:
        deleted = await service.clean(bundle_name=bundle, before=before, after=after,
                                      keep_last=keep_last)
    except ValueError as error:
        _fail(str(error), "See `ziplime bundles` for what is stored.")

    # Saying which versions went, rather than "Cleaned": this deletes data, and a command that
    # deleted nothing looked exactly like one that deleted everything.
    if not deleted:
        click.secho(f"No version of {bundle} matched; nothing was deleted.", fg="yellow")
        return
    click.secho(f"Deleted {len(deleted)} version(s) of {bundle}:", fg="green")
    for version in deleted:
        click.echo(f"  {version}")


# -- backtest ----------------------------------------------------------------------------------


@main.command()
@click.option("-f", "--algofile", required=True, type=click.Path(exists=True, dir_okay=False),
              help="Python file with `initialize` and `handle_data`.")
@click.option("-b", "--bundle", required=True, help="Bundle to run over.")
@click.option("--bundle-version", default=None, help="Version to load. Defaults to the newest.")
@click.option("-s", "--symbols", required=True,
              help="Comma-separated symbols to load from the bundle, as TICKER or TICKER@MIC.")
@click.option("--start-date", required=True, type=click.DateTime(formats=DATE_FORMATS))
@click.option("--end-date", required=True, type=click.DateTime(formats=DATE_FORMATS))
@click.option("-c", "--trading-calendar", default="XNYS", show_default=True)
@click.option("--emission-rate", default="1d", show_default=True, type=click.Choice(FREQUENCIES),
              help="How often the strategy is called.")
@click.option("--capital-base", default=100_000.0, show_default=True, type=float)
@click.option("--asset-type", default=AssetType.EQUITY.value, show_default=True,
              type=click.Choice([t.value for t in AssetType]))
@click.option("--benchmark-symbol", default=None,
              help="Instrument to measure against, as TICKER@MIC. Omitted means a zero-return "
                   "benchmark, which is a statement about the run, not a missing value.")
@click.option("--max-leverage", default=1.0, show_default=True, type=float)
@click.option("--same-bar-execution/--next-bar-execution", default=False, show_default=True,
              help="Same-bar execution fills an order at the close of the bar the decision was "
                   "taken on -- a price the market had not printed yet. Off by default here "
                   "because a look-ahead should be asked for, not inherited.")
@click.option("--fill-price", default="close", show_default=True,
              type=click.Choice(["open", "close", "high", "low"]),
              help="Which price of the bar an order fills at.")
@click.option("--commission-per-share", default=0.0, show_default=True, type=float)
@click.option("--slippage-bps", default=5.0, show_default=True, type=float,
              help="Fixed slippage in basis points. Fills are also capped at a tenth of the "
                   "bar's volume. Pass 0 for no price impact -- a control run, not a realistic "
                   "one.")
@click.option("--stop-on-error", is_flag=True,
              help="Raise on the first error instead of collecting them.")
@click.option("--print-algo", is_flag=True, help="Print the strategy source before running.")
@click.option("-o", "--output", default=None, type=click.Path(dir_okay=False),
              help="Write the performance table here. `.csv` writes CSV, anything else a pickle. "
                   "Without it, a summary is printed.")
@storage_option
@asset_db_option
async def run(algofile, bundle, bundle_version, symbols, start_date, end_date, trading_calendar,
              emission_rate, capital_base, asset_type, benchmark_symbol, max_leverage,
              same_bar_execution, fill_price, commission_per_share, slippage_bps, stop_on_error,
              print_algo, output, bundle_storage_path, asset_db):
    """Run a backtest over an ingested bundle.

    The symbols are named explicitly rather than taken from the bundle: loading a bundle checks
    each instrument for missing sessions, and it can only do that against the instruments you say
    you expect. A bundle holding more than you name is fine -- the extra is simply not loaded.
    """
    calendar = get_calendar(trading_calendar)
    start = start_date.replace(tzinfo=calendar.tz)
    end = end_date.replace(tzinfo=calendar.tz)
    if start >= end:
        _fail(f"--start-date {start_date:%Y-%m-%d} is not before --end-date {end_date:%Y-%m-%d}.")

    asset_service = get_asset_service(db_path=asset_db)
    try:
        listings = await _resolve(asset_service, symbols, AssetType(asset_type))
        service = get_bundle_service(bundle_storage_path=bundle_storage_path)
        try:
            market_data, missing = await service.load_bundle(
                bundle_name=bundle, bundle_version=bundle_version, assets=listings,
                start_date=start, end_date=end,
                frequency=DataFrequency(emission_rate).to_timedelta(),
                asset_service=asset_service)
        except ValueError as error:
            _fail(str(error), "See `ziplime bundles` for what is stored.")
        if missing:
            for listing, (first, last) in missing.items():
                click.secho(f"warning: {listing.symbol}@{listing.mic} has no data "
                            f"{first} .. {last}", fg="yellow", err=True)

        result = await run_simulation(
            start_date=start, end_date=end, trading_calendar=trading_calendar,
            emission_rate=DataFrequency(emission_rate).to_timedelta(),
            total_cash=capital_base, market_data_source=market_data, custom_data_sources=[],
            algorithm_file=algofile, stop_on_error=stop_on_error, asset_service=asset_service,
            benchmark_asset_symbol=benchmark_symbol, benchmark_returns=None,
            equity_commission=PerShare(cost=commission_per_share, min_trade_cost=0.0),
            equity_slippage=FixedBasisPointsSlippage(basis_points=slippage_bps),
            max_leverage=max_leverage, same_bar_execution=same_bar_execution,
            price_used_in_order_execution=fill_price, print_algo=print_algo)
    finally:
        await asset_service._asset_repository.engine.dispose()

    _report(result, output)
    return result


# -- mcp ---------------------------------------------------------------------------------------


@main.command()
async def mcp():
    """Run the local MCP server, speaking MCP over stdin and stdout.

    For a client that launches a server as a subprocess -- Claude Code, Claude Desktop, Cursor.
    It exposes the same ingest and backtest path the commands above do, so a model drives this
    install rather than a hosted one. Nothing it exposes places a real order.

    Not a command to run by hand: with no client on the other end it waits on stdin forever.
    """
    try:
        from ziplime.mcp import serve_stdio
    except ImportError as error:
        _fail(f"The MCP server needs the `mcp` package, which is not installed ({error}).",
              "pip install mcp, or poetry install --with mcp.")
    # Awaited, not `ziplime.mcp.main()`: that one calls `anyio.run()` and this command is
    # already inside asyncclick's loop, so it would raise "Already running asyncio in this
    # thread" -- which reaches the client as a server that closed the connection.
    await serve_stdio()


def _report(result, output: str | None) -> None:
    """Print what the run did, and write the table if asked.

    ``str(result)`` used to be the whole output, which is a dataclass repr -- several screens of
    nested frames and no answer to "what happened".
    """
    perf = result.perf
    errors = list(result.errors or [])
    if output:
        path = Path(output)
        if path.suffix == ".csv":
            perf.to_csv(path)
        else:
            perf.to_pickle(path)
        click.secho(f"Wrote {len(perf)} rows to {path}", fg="green")

    if perf.empty:
        click.secho("The run recorded no sessions.", fg="red", err=True)
    else:
        total = float(perf["algorithm_period_return"].iloc[-1])
        drawdown = float(perf["max_drawdown"].iloc[-1])
        trades = sum(len(row) for row in perf["transactions"])
        click.echo()
        click.echo(f"  sessions      {len(perf)}  ({perf.index[0].date()} .. {perf.index[-1].date()})")
        click.echo(f"  return        {total:+.2%}")
        click.echo(f"  max drawdown  {drawdown:.2%}")
        click.echo(f"  transactions  {trades}")
        click.echo(f"  final value   {float(perf['portfolio_value'].iloc[-1]):,.2f}")

    if errors:
        click.echo()
        click.secho(f"{len(errors)} error(s) during the run:", fg="red", err=True)
        for error in errors[:5]:
            click.secho(f"  {error.message[:160]}", fg="red", err=True)
        if len(errors) > 5:
            click.secho(f"  ... and {len(errors) - 5} more", fg="red", err=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:  # pragma: no cover - interactive only
        sys.exit(130)
