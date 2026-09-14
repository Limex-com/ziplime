"""A local MCP server over the ziplime engine — no account, no cloud.

Everything runs on this machine: data you ingested, strategies in a directory
you own, backtests written to disk beside them. Nothing leaves the process
except what the data connector fetches.

**Why the official `mcp` SDK and not `fastmcp`.** They cannot be installed
together: `lime-trader-sdk` pins `attrs<23.0.0`, `fastmcp` pulls `cyclopts`
which needs `attrs>=23.1.0`, and pip refuses. The `mcp` SDK does not depend on
attrs at all, so `pip install ziplime mcp` resolves. Measured, not assumed —
if `lime-trader-sdk` ever relaxes that pin this can be revisited, and until
then swapping the import back is a broken install for everyone.
"""
from __future__ import annotations

import json
import sys
from typing import Any

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

from . import code_rules, docs, engine, formatting, workspace
from .errors import InvalidArguments, NotFound, ZiplimeMcpError

VERSION = "0.1.0"

INSTRUCTIONS = f"""\
ziplime, running locally. Backtesting on your own machine, with your own data.

THE LOOP, IN THE ORDER IT HAPPENS

1. Data — nothing works until bars are on disk. `list_bundles` shows what is
   already ingested; `ingest_instruments` then `ingest_market_data` fetches
   more. A backtest can only see symbols and dates that were ingested.
2. Write — `write_strategy` saves a `.py` file; `check_strategy_code` reads it
   for the mistakes that cost a run. Check before running, always: the engine
   validates by executing, so a broken hook is found after the data is loaded.
3. Run — `run_backtest` simulates and records the result to disk.
4. Read — `get_backtest` for one run, `compare_backtests` for several.

THIS IS NOT THE HOSTED PLATFORM, AND THE CODE CONTRACT DIFFERS.
Imports are allowed here and forbidden there; `before_trading_start` and
`analyze` exist here and not there; parameters come from a config class here.
`get_strategy_language_reference` has the rules that apply to *this* engine.
Do not carry the cloud rules over — they produce worse code than the engine
accepts.

NOTHING HERE PLACES A REAL ORDER. Live trading exists in the engine and is
deliberately not exposed as a tool: see `describe_live_trading`.

Ids and tool names are for calling, not for reading aloud. A strategy has a
name and a run has a date; use those when talking to a person.
"""

server = MCPServer(name="ziplime-local", version=VERSION, instructions=INSTRUCTIONS)


def _handled(func):
    """The only path out. Every tool needs it; a traceback must never be the answer.

    `ToolError` is the SDK's "a failure you saw coming" — its message reaches
    the client intact, which is the whole point of the error contract. Anything
    else the SDK treats as a crash and replaces with a generic line naming only
    the tool, so an unexpected exception is caught here too: its traceback goes
    to stderr for whoever runs the server, and the model gets a code it can act
    on rather than "Error executing tool".
    """
    import functools
    import traceback

    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except ZiplimeMcpError as exc:
            raise ToolError(exc.render()) from None
        except Exception as exc:  # noqa: BLE001 — deliberate catch-all
            traceback.print_exc(file=sys.stderr)
            raise ToolError(
                ZiplimeMcpError(
                    f"{func.__name__} failed unexpectedly",
                    code="internal_error",
                    hint=(
                        "This is a bug in the local MCP server or in the engine "
                        "beneath it. The server's stderr has the traceback; the "
                        "detail line below is what to report."
                    ),
                    detail=f"{type(exc).__name__}: {exc}",
                ).render()
            ) from None

    return wrapper


def _read(title: str) -> dict:
    """A read: safe to call, safe to repeat, and it says so.

    `title` is set in two places on purpose — the tool's own field and the
    annotations block — because clients read one or the other and a tool
    without a title shows up as its function name.
    """
    return {
        "title": title,
        "annotations": ToolAnnotations(
            title=title, readOnlyHint=True, destructiveHint=False, idempotentHint=True
        ),
    }


def _write(title: str) -> dict:
    """A write that can be undone. MCP treats an unannotated tool as
    destructive, so every one of these has to say otherwise — a client that
    prompts on every call teaches people to click through the prompts."""
    return {
        "title": title,
        "annotations": ToolAnnotations(
            title=title, readOnlyHint=False, destructiveHint=False
        ),
    }


# ===========================================================================
# DATA — nothing else works until this is done once
# ===========================================================================

@server.tool(**_read("List data connectors"))
@_handled
async def list_data_providers() -> dict:
    """Lists the data connectors this installation has, and what each needs.

    `yahoo` ships with the engine and needs no credentials, which makes it the
    one to reach for first. Others register themselves when installed.

    Returns:
        JSON with each connector's name, the credentials it needs, and what
        data it supplies.
    """
    found = engine.providers()
    return {
        "providers": found,
        "count": len(found),
        "nothing_here_yet": (
            "No connector is registered, which usually means an incomplete "
            "install rather than an empty machine."
        ) if not found else None,
    }


@server.tool(**_read("List ingested data bundles"))
@_handled
async def list_bundles() -> dict:
    """Lists the market-data bundles on this machine.

    A bundle is bars already fetched and stored — a backtest reads one and
    cannot see past its dates or its symbols. Start here when a run complains
    it has no data.

    Returns:
        JSON with each bundle's name, version, bar frequency, calendar and when
        it was ingested. The registry records no date range, so none is
        reported -- run a backtest to find out what a bundle covers.
    """
    rows = await engine.bundles()
    result: dict[str, Any] = {"bundles": rows, "count": len(rows),
                              "storage": workspace.bundle_storage_path()}
    if not rows:
        result["nothing_here_yet"] = (
            "No data is ingested yet. This is the normal state of a fresh "
            "install, not a failure — ingest_instruments then "
            "ingest_market_data fills it."
        )
    return result


@server.tool(**_write("Ingest the instrument catalogue"))
@_handled
async def ingest_instruments(provider: str = "yahoo") -> dict:
    """Fetches the list of tradable instruments into the local asset database.

    Run this **once per connector, before ingesting any bars**. Bars are
    resolved against this catalogue, so an empty catalogue matches nothing and
    an ingest silently fetches no symbols.

    Takes a while — it is thousands of listings — and is safe to repeat.

    Args:
        provider: Connector to read from. `yahoo` needs no credentials.

    Returns:
        JSON naming the provider and where the database was written.
    """
    return await engine.ingest_instruments(provider)


@server.tool(**_write("Ingest market data"))
@_handled
async def ingest_market_data(
    bundle: str,
    symbols: list[str],
    start: str,
    end: str,
    provider: str = "yahoo",
    frequency: str = "1d",
    calendar: str = engine.DEFAULT_CALENDAR,
    merge: bool = False,
) -> dict:
    """Fetches bars for these symbols into a named bundle on disk.

    The bundle is what a backtest reads. Ingest a window wider than the one you
    intend to test: an indicator with a 50-bar lookback produces nothing until
    50 bars have passed, so a backtest starting on the bundle's first day
    trades nothing for its first fifty.

    Args:
        bundle: Name to store under. Reuse it with merge=True to extend.
        symbols: Tickers, e.g. ["AAPL", "MSFT"].
        start: First date, YYYY-MM-DD.
        end: Last date, YYYY-MM-DD.
        provider: Connector to fetch from.
        frequency: Bar size — 1m, 5m, 15m, 1h, 1d or 1w.
        calendar: Exchange calendar, e.g. NYSE.
        merge: Add to an existing bundle instead of replacing it.

    Returns:
        JSON with what was ingested, and any symbol that is not in the
        catalogue and was therefore skipped.
    """
    if not symbols:
        raise InvalidArguments("Give at least one symbol to ingest")
    return await engine.ingest_bars(
        provider=provider, bundle=bundle, symbols=symbols, start=start, end=end,
        freq=frequency, calendar=calendar, merge=merge,
    )


# ===========================================================================
# STRATEGIES — files in a directory you own
# ===========================================================================

@server.tool(**_read("Strategy language reference"))
@_handled
async def get_strategy_language_reference() -> dict:
    """Returns the contract this engine enforces on strategy code.

    Read this before writing a strategy. It is **not** the hosted platform's
    contract: imports are allowed here, there are more hooks, and parameters
    come from a config class.

    Returns:
        JSON with the rules, a complete worked example, and what differs from
        the cloud platform.
    """
    return {
        "engine_rules": docs.STRATEGY_RULES,
        "example": docs.EXAMPLE_STRATEGY,
        "how_this_differs_from_the_hosted_platform": docs.LOCAL_VS_CLOUD,
    }


@server.tool(**_read("Check strategy code"))
@_handled
async def check_strategy_code(code: str) -> dict:
    """Reads strategy code for the mistakes that would waste a run.

    Milliseconds, and no data is touched. The engine validates by executing,
    so a hook that is not `async` or takes the wrong arguments is otherwise
    discovered after the bundle is loaded and the clock has started.

    Blocking problems mean the run cannot work. Advisory ones are matched on
    names and can be wrong — they are reported, never enforced.

    Args:
        code: The strategy source to check.

    Returns:
        JSON with the problems found, whether any of them blocks, and the
        engine rules when there is something to fix.
    """
    return code_rules.check(code)


@server.tool(**_write("Save a strategy"))
@_handled
async def write_strategy(name: str, code: str, overwrite: bool = False) -> dict:
    """Saves strategy code as a file in the local workspace.

    The code is checked first, and a **blocking** problem refuses the write —
    saving a strategy that cannot run only defers the error to a point where it
    costs a simulation. Advisory findings come back with the result and do not
    stop anything.

    Args:
        name: Short name, letters/digits/dash/underscore. Becomes the filename.
        code: The strategy source.
        overwrite: Required to replace a strategy that already exists.

    Returns:
        JSON with the path written and anything the check found.
    """
    path = workspace.strategy_path(name)
    if path.exists() and not overwrite:
        raise InvalidArguments(
            f"A strategy named {name!r} already exists",
            hint="Pass overwrite=true to replace it, or choose another name.",
        )
    checked = code_rules.check(code)
    if checked["blocking"]:
        raise InvalidArguments(
            f"That code cannot run, so it was not saved as {name!r}",
            hint="Fix the blocking problems and call write_strategy again.",
            detail=json.dumps(
                [p for p in checked["problems"] if p["severity"] == "blocking"]
            ),
        )
    workspace.save_strategy(name, code)
    return {
        "name": name,
        "path": str(path),
        "replaced": path.exists() and overwrite,
        "check": checked,
        "next_tool": "run_backtest",
    }


@server.tool(**_read("List strategies"))
@_handled
async def list_strategies() -> dict:
    """Lists the strategies saved in the local workspace.

    Returns:
        JSON with each strategy's name, size and when it last changed.
    """
    rows = workspace.list_strategies()
    result: dict[str, Any] = {"strategies": rows, "count": len(rows),
                              "workspace": str(workspace.strategies_dir())}
    if not rows:
        result["nothing_here_yet"] = (
            "No strategies saved yet — normal on a fresh install. "
            "get_strategy_language_reference has the contract and a worked "
            "example; write_strategy saves one."
        )
    return result


@server.tool(**_read("Read a strategy"))
@_handled
async def read_strategy(name: str) -> dict:
    """Returns the source of a saved strategy.

    Args:
        name: The strategy's name.

    Returns:
        JSON with the code and its path.
    """
    return {
        "name": name,
        "code": workspace.load_strategy(name),
        "path": str(workspace.strategy_path(name)),
    }


# ===========================================================================
# BACKTESTS
# ===========================================================================

@server.tool(**_write("Run a backtest"))
@_handled
async def run_backtest(
    strategy: str,
    bundle: str,
    symbols: list[str],
    start: str,
    end: str,
    capital: float = 100000.0,
    benchmark: str | None = None,
    frequency: str = "1d",
    calendar: str = engine.DEFAULT_CALENDAR,
    config_file: str | None = None,
    commission_per_share: float | None = None,
    slippage_bps: float = 5.0,
    max_leverage: float = 1.0,
    same_bar_execution: bool = False,
    fill_price: str = "close",
    stop_on_error: bool = True,
) -> dict:
    """Runs a saved strategy against a local bundle and records the result.

    Everything happens on this machine and the result is written to the
    workspace, so it can be read back and compared later.

    The window has to sit inside the bundle's, and every symbol has to be in
    it. `list_bundles` shows both.

    Args:
        strategy: Name of a saved strategy.
        bundle: Which ingested bundle to read bars from.
        symbols: The instruments the strategy trades.
        start: First date of the simulation, YYYY-MM-DD.
        end: Last date, YYYY-MM-DD.
        capital: Starting cash.
        benchmark: Ticker to measure against. Omitted means no benchmark, and
            the benchmark columns come back empty rather than wrong.
        frequency: Bar size — must match what the bundle holds.
        calendar: Exchange calendar.
        config_file: Path to a JSON file for the strategy's AlgorithmConfig.
        commission_per_share: Overrides the engine's default commission model.
        slippage_bps: Price impact in basis points. 0 is a control run, not a
            realistic one.
        max_leverage: 1.0 means no leverage.
        same_bar_execution: Fill at the close of the bar the decision was taken
            on. That is a price the market had not printed yet, so it is
            look-ahead and off by default; turning it on makes the run
            optimistic by roughly a bar of edge.
        fill_price: Which price of the bar an order fills at -- open, close,
            high or low.
        stop_on_error: Stop at the first error in strategy code rather than
            carrying on with a strategy that is throwing every bar.

    Returns:
        JSON with the run's id, its parameters, a summary of the result and any
        errors the strategy raised.
    """
    if not symbols:
        raise InvalidArguments("Give at least one symbol for the strategy to trade")
    if fill_price not in ("open", "close", "high", "low"):
        raise InvalidArguments(
            f"fill_price must be open, close, high or low, got {fill_price!r}")
    meta = await engine.run_backtest(
        strategy=strategy, bundle=bundle, start=start, end=end, capital=capital,
        symbols=symbols, benchmark=benchmark, freq=frequency, calendar=calendar,
        config_file=config_file, commission_per_share=commission_per_share,
        slippage_bps=slippage_bps, max_leverage=max_leverage,
        same_bar_execution=same_bar_execution, fill_price=fill_price,
        stop_on_error=stop_on_error,
    )
    if same_bar_execution:
        meta["look_ahead_was_on"] = (
            "same_bar_execution filled orders at the close of the bar each "
            "decision was taken on -- a price that had not printed yet. These "
            "numbers are optimistic by roughly a bar of edge and are not a "
            "forecast of live trading."
        )
    if meta["errors"]:
        meta["the_strategy_raised"] = (
            "The run completed but the strategy raised. Those errors are the "
            "result, not a footnote — the numbers below describe a strategy "
            "that was failing."
        )
    elif not meta["summary"].get("transactions"):
        meta["it_placed_no_orders"] = (
            "The run finished without trading. Usually the lookback never "
            "filled, the condition never became true, or an engine call was "
            "not awaited — check_strategy_code finds the third."
        )
    return meta


@server.tool(**_read("List backtests"))
@_handled
async def list_backtests(strategy: str | None = None) -> dict:
    """Lists backtests recorded on this machine, newest first.

    Args:
        strategy: Only runs of this strategy.

    Returns:
        JSON with each run's id, strategy, parameters and headline numbers.
    """
    rows = workspace.list_backtests(strategy)
    trimmed = [
        {
            "backtest_id": row.get("backtest_id"),
            "strategy": row.get("strategy"),
            "ran_at": row.get("ran_at"),
            "window": f"{row.get('parameters', {}).get('start')} → "
                      f"{row.get('parameters', {}).get('end')}",
            "summary": row.get("summary"),
            "errors": len(row.get("errors") or []),
        }
        for row in rows
    ]
    result: dict[str, Any] = {"backtests": trimmed, "count": len(trimmed)}
    if not trimmed:
        result["nothing_here_yet"] = (
            "No backtests recorded yet." if not strategy
            else f"No runs of {strategy!r} yet. Other strategies may have some."
        )
    return result


@server.tool(**_read("Get a backtest result"))
@_handled
async def get_backtest(backtest_id: str, equity_curve_points: int = 100) -> dict:
    """Returns one recorded run: its parameters, its numbers, its equity curve.

    The curve is thinned evenly rather than truncated, so its shape survives,
    and the response says how many rows there were and how many came back.

    Args:
        backtest_id: The run's id, from list_backtests.
        equity_curve_points: How many points of the curve to return, up to 500.
            Zero omits it.

    Returns:
        JSON with the run's metadata, summary and equity curve.
    """
    meta = workspace.load_backtest(backtest_id)
    if equity_curve_points:
        perf = workspace.load_backtest_perf(backtest_id)
        meta["equity_curve"] = formatting.equity_curve(
            perf, limit=max(2, min(int(equity_curve_points), 500))
        )
    return meta


@server.tool(**_read("Compare backtests"))
@_handled
async def compare_backtests(backtest_ids: list[str]) -> dict:
    """Puts several recorded runs side by side.

    Args:
        backtest_ids: Two or more run ids.

    Returns:
        JSON with one row per run and which had the highest Sharpe — with the
        caveat that runs over different windows are not comparable.
    """
    if len(backtest_ids) < 2:
        raise InvalidArguments("Give at least two backtest ids to compare")
    return formatting.compare([workspace.load_backtest(i) for i in backtest_ids])


@server.tool(**_read("About live trading"))
@_handled
async def describe_live_trading() -> dict:
    """Explains how this engine trades live, and why no tool here does it.

    Returns:
        JSON with the route to live trading and what it requires.
    """
    return {
        "no_tool_here_places_an_order": (
            "Deliberate. An MCP tool that submits real orders sits one "
            "misunderstanding away from a model deciding to use it, and the "
            "local server has no consent step to put in front of it. Live "
            "trading is run explicitly, by you, from the command line."
        ),
        "how_the_engine_does_it": (
            "Execution sits behind one interface, ziplime/exchanges/exchange.py. "
            "SimulationExchange is what a backtest uses; LimeTraderSdkExchange "
            "is the live one, built on the lime-trader-sdk package. Point "
            "run_live_trading at the latter and the same strategy file trades "
            "for real."
        ),
        "what_it_needs": {
            "credentials": "LIME_SDK_CREDENTIALS_FILE, pointing at your broker credentials.",
            "example": "examples/run_live_trading.py in the ziplime repository.",
        },
        "another_broker": (
            "Subclass the same exchange interface. The Lime adapter is one "
            "file and is the reference implementation; strategy code does not "
            "change when the exchange does."
        ),
    }


async def serve_stdio() -> None:
    """Speak MCP over stdio from inside an event loop that is already running.

    `server.run()` calls `anyio.run()`, which refuses to nest -- so a caller that is already
    async (the `ziplime mcp` command, which asyncclick runs in its own loop) cannot use it and
    gets `RuntimeError: Already running asyncio in this thread`. The client sees only a server
    that closed the connection.
    """
    await server.run_stdio_async()


def main() -> None:
    """Entry point for `python -m ziplime.mcp`: speak MCP over stdio."""
    server.run("stdio")


if __name__ == "__main__":
    main()
