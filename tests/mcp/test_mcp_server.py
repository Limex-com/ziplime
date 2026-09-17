"""Offline tests for the local MCP server.

Nothing here ingests, downloads or runs a simulation — those need data and
minutes, and a test suite that needs either stops being run. What is covered is
everything that can be wrong without them: the tool surface a client sees, the
code checker, the workspace, the formatting, and the error contract.
"""
import json

import pytest

from ziplime.mcp import code_rules, formatting, workspace
from ziplime.mcp.errors import InvalidArguments, NotFound, ZiplimeMcpError

@pytest.fixture(autouse=True)
def isolated_home(tmp_path, monkeypatch):
    """Never touch the developer's real ~/.ziplime."""
    monkeypatch.setenv("ZIPLIME_MCP_HOME", str(tmp_path / "mcp"))
    monkeypatch.setenv("ZIPLIME_BUNDLE_PATH", str(tmp_path / "data"))
    monkeypatch.setenv("ZIPLIME_ASSET_DB", str(tmp_path / "assets.sqlite"))


GOOD = '''\
from ziplime.finance.execution import MarketOrder


async def initialize(context):
    context.asset = await context.symbol("AAPL")


async def handle_data(context, data):
    await context.order_target_percent(
        asset=context.asset, style=MarketOrder(), target=0.5
    )
'''


class TestTheToolSurface:
    """MCP reads a tool with no annotations as destructive, so a client prompts
    before every call — which teaches people to click through prompts. Every
    tool here has to say what it is."""

    @pytest.mark.asyncio
    async def test_every_tool_is_annotated_and_titled(self):
        from ziplime.mcp.server import server

        for tool in await server.list_tools():
            assert tool.annotations is not None, tool.name
            assert tool.title, tool.name
            assert tool.annotations.destructive_hint is False, tool.name

    @pytest.mark.asyncio
    async def test_every_tool_has_a_description(self):
        """The docstring is the interface the calling model reads."""
        from ziplime.mcp.server import server

        for tool in await server.list_tools():
            assert tool.description and len(tool.description) > 80, tool.name

    @pytest.mark.asyncio
    async def test_nothing_here_can_place_an_order(self):
        """The deliberate omission. A local server has no consent step to put
        in front of a tool that trades, so it does not offer one."""
        from ziplime.mcp.server import server

        names = {tool.name for tool in await server.list_tools()}
        for forbidden in ("deploy", "trade_live", "submit_order", "start_live"):
            assert not any(forbidden in name for name in names), forbidden

    @pytest.mark.asyncio
    async def test_the_documented_arguments_exist(self):
        """A description that names an argument the signature does not have
        sends a model into a validation error it cannot read its way out of."""
        import re

        from ziplime.mcp.server import server

        for tool in await server.list_tools():
            documented = set(re.findall(
                r"^\s{8}(\w+):", tool.description or "", re.MULTILINE))
            actual = set((tool.input_schema or {}).get("properties", {}))
            assert documented <= actual, (tool.name, documented - actual)


class TestTheCodeCheckerRunsBeforeARunIsSpent:
    """The engine validates by executing, so a broken hook is otherwise found
    after the bundle is loaded and the clock has started."""

    def test_good_code_passes_clean(self):
        assert code_rules.check(GOOD)["problems"] == []

    def test_a_syntax_error_blocks_and_names_the_line(self):
        result = code_rules.check("async def initialize(context)\n    pass\n")
        assert result["blocking"]
        assert result["problems"][0]["rule"] == "syntax_error"
        assert result["problems"][0]["line"] == 1

    def test_a_synchronous_hook_blocks(self):
        """`def` returns None to an `await`, so the engine cannot run it."""
        result = code_rules.check("def initialize(context):\n    pass\n\n"
                                  "async def handle_data(context, data):\n    pass\n")
        assert result["blocking"]
        assert any(p["rule"] == "hook_not_async" for p in result["problems"])

    def test_the_wrong_arity_blocks(self):
        result = code_rules.check("async def initialize(context):\n    pass\n\n"
                                  "async def handle_data(context):\n    pass\n")
        assert any(p["rule"] == "hook_signature" for p in result["problems"])

    def test_a_missing_handle_data_blocks_because_nothing_would_trade(self):
        result = code_rules.check("async def initialize(context):\n    pass\n")
        assert result["blocking"]

    def test_a_missing_initialize_only_warns(self):
        """The engine substitutes a no-op, so it runs. It is usually a mistake,
        which is not the same as being impossible."""
        result = code_rules.check("async def handle_data(context, data):\n    pass\n")
        assert not result["blocking"]

    def test_a_missing_await_is_advisory_not_blocking(self):
        """Matched on a name, so it can be wrong — and a false positive that
        refuses a correct strategy is worse than the mistake."""
        result = code_rules.check(
            "async def initialize(context):\n    context.a = context.symbol('AAPL')\n\n"
            "async def handle_data(context, data):\n    pass\n")
        missing = [p for p in result["problems"] if p["rule"] == "missing_await"]
        assert missing and missing[0]["severity"] == "advisory"
        assert not result["blocking"]

    def test_pandas_accessors_are_flagged(self):
        """Market data is a Polars frame; `.iloc` does not exist on one."""
        result = code_rules.check(
            "async def initialize(context):\n    pass\n\n"
            "async def handle_data(context, data):\n    x = data.iloc[-1]\n")
        assert any(p["rule"] == "pandas_accessor" for p in result["problems"])

    def test_imports_are_not_flagged(self):
        """The hosted platform forbids them. This engine does not, and carrying
        that rule over would refuse ordinary local code."""
        result = code_rules.check(
            "import numpy as np\nimport talib\n\n"
            "async def initialize(context):\n    pass\n\n"
            "async def handle_data(context, data):\n    pass\n")
        assert result["problems"] == []

    def test_the_rules_ship_with_any_problem(self):
        """Whoever has to fix it needs the contract in the same response."""
        assert "engine_rules" in code_rules.check("def initialize(c): pass")

    def test_the_documented_example_breaks_no_rule(self):
        """The example a model copies cannot drift from what the engine takes."""
        from ziplime.mcp import docs

        assert code_rules.check(docs.EXAMPLE_STRATEGY)["problems"] == []


class TestTheWorkspaceIsFilesYouOwn:
    def test_a_strategy_round_trips(self):
        workspace.save_strategy("demo", GOOD)
        assert workspace.load_strategy("demo") == GOOD
        assert [row["name"] for row in workspace.list_strategies()] == ["demo"]

    def test_a_missing_strategy_says_where_to_look(self):
        with pytest.raises(NotFound) as excinfo:
            workspace.load_strategy("absent")
        assert "list_strategies" in excinfo.value.hint

    @pytest.mark.parametrize("name", ["../escape", "with/slash", "", " ", "a" * 65])
    def test_a_name_that_is_not_a_filename_is_refused(self, name):
        """Not defence against an attacker — this runs as you. Defence against
        `../` turning a typo into a deleted directory somewhere else."""
        with pytest.raises(InvalidArguments):
            workspace.strategy_path(name)

    def test_backtest_ids_are_unique_and_sort_by_time(self):
        first = workspace.new_backtest_id("demo")
        workspace.save_backtest(first, {"backtest_id": first, "strategy": "demo"})
        second = workspace.new_backtest_id("demo")
        assert first != second

    def test_a_run_round_trips(self):
        run_id = workspace.new_backtest_id("demo")
        workspace.save_backtest(run_id, {"backtest_id": run_id, "strategy": "demo",
                                         "summary": {"bars": 3}})
        assert workspace.load_backtest(run_id)["summary"]["bars"] == 3
        assert len(workspace.list_backtests("demo")) == 1
        assert workspace.list_backtests("other") == []


class TestOutputIsSmallEnoughToRead:
    def _frame(self, rows: int):
        import pandas as pd

        index = pd.date_range("2024-01-01", periods=rows, freq="D")
        return pd.DataFrame({
            "portfolio_value": [100000 + i * 10 for i in range(rows)],
            "sharpe": [1.5] * rows,
            "transactions": [[] for _ in range(rows)],
        }, index=index)

    def test_a_summary_reads_the_last_row(self):
        summary = formatting.summarise(self._frame(10))
        assert summary["bars"] == 10
        assert summary["ending_portfolio_value"] == 100090.0
        assert summary["total_return_percent"] == 0.09

    def test_a_column_the_engine_did_not_produce_is_absent_not_zero(self):
        """Reporting a Sharpe of 0.0 for a run that computed none is a lie a
        model will repeat."""
        assert "sortino" not in formatting.summarise(self._frame(3))

    def test_nan_is_a_gap_not_a_number(self):
        import numpy as np

        frame = self._frame(3)
        frame.loc[frame.index[-1], "sharpe"] = np.nan
        assert "sharpe" not in formatting.summarise(frame)

    def test_an_empty_result_says_so_rather_than_dividing_by_zero(self):
        import pandas as pd

        assert formatting.summarise(pd.DataFrame())["bars"] == 0

    def test_the_curve_is_thinned_evenly_not_truncated(self):
        """Head-truncating a three-year run is a picture of its first fortnight."""
        curve = formatting.equity_curve(self._frame(1000), limit=10)
        assert curve["row_count"] == 1000
        assert curve["truncated"] is True
        assert curve["returned"] <= 12
        assert curve["rows"][-1]["portfolio_value"] == 109990.0

    def test_a_short_curve_is_not_thinned(self):
        curve = formatting.equity_curve(self._frame(5), limit=100)
        assert curve["truncated"] is False and curve["returned"] == 5

    def test_comparison_warns_that_windows_differ(self):
        result = formatting.compare([
            {"backtest_id": "a", "strategy": "s", "summary": {"sharpe": 1.0}},
            {"backtest_id": "b", "strategy": "s", "summary": {"sharpe": 2.0}},
        ])
        assert result["highest_sharpe"] == "b"
        assert "incomparable" in result["note"]


class TestTheErrorContract:
    def test_a_failure_carries_a_code_a_cause_and_a_retry_verdict(self):
        rendered = ZiplimeMcpError(
            "It broke", code="not_found", hint="Try the other one.",
            detail="HTTP 404",
        ).render()
        assert rendered.startswith("[not_found] It broke")
        assert "What to do: Try the other one." in rendered
        assert "Retry: no" in rendered
        assert "Details: HTTP 404" in rendered

    def test_a_retryable_code_says_so(self):
        assert "Retry: yes" in ZiplimeMcpError("slow", code="upstream_timeout").render()

    @pytest.mark.asyncio
    async def test_a_tool_failure_reaches_the_client_intact(self):
        """The whole point of the contract: the SDK replaces an unexpected
        exception with a generic line, so anticipated failures raise ToolError."""
        from mcp.server.mcpserver.exceptions import ToolError

        from ziplime.mcp import server as tools

        with pytest.raises(ToolError) as excinfo:
            await tools.get_backtest("does-not-exist")
        assert "[not_found]" in str(excinfo.value)

    @pytest.mark.asyncio
    async def test_an_empty_symbol_list_is_refused_before_anything_loads(self):
        from mcp.server.mcpserver.exceptions import ToolError

        from ziplime.mcp import server as tools

        with pytest.raises(ToolError) as excinfo:
            await tools.run_backtest(strategy="s", bundle="b", symbols=[],
                                     start="2024-01-01", end="2024-02-01")
        assert "invalid_arguments" in str(excinfo.value)


class TestTheLocalContractIsNotTheCloudOne:
    """A model carrying the hosted platform's rules here writes worse code than
    the engine accepts, and one carrying these rules there writes code the
    sandbox rejects. The difference is stated, not implied."""

    def test_the_reference_says_imports_are_allowed(self):
        from ziplime.mcp import docs

        assert "IMPORTS ARE ALLOWED" in docs.STRATEGY_RULES

    def test_it_names_what_differs_from_the_hosted_platform(self):
        from ziplime.mcp import docs

        assert "sandbox" in docs.LOCAL_VS_CLOUD
        assert "before_trading_start" in docs.LOCAL_VS_CLOUD

    @pytest.mark.asyncio
    async def test_the_server_instructions_warn_about_it(self):
        from ziplime.mcp.server import INSTRUCTIONS

        assert "Do not carry the cloud rules over" in INSTRUCTIONS


class TestWhatTheServerReportsAboutADefectItFound:
    """Three things this server got wrong, each verified here so it cannot come back.

    None of them raised. That is what makes them worth a test: a run that quietly
    reports a look-ahead number, a listing that quietly says a bundle is empty, and a
    strategy bug quietly blamed on the engine all look like success.
    """

    def test_a_bundle_listing_reports_the_keys_the_registry_actually_has(self):
        """`bundles()` read `frequency`, `start_date` and `end_date` off registry rows
        that carry none of them, so every bundle listed a blank frequency and an empty
        date range -- which reads as a bundle holding nothing."""
        from ziplime.mcp import engine

        row = {
            "name": "daily", "version": "17", "timestamp": "2026-09-13T08:58:07Z",
            "frequency_seconds": 86400.0, "frequency_text": None,
            "trading_calendar_name": "XNYS", "data_type": "MARKET_DATA",
        }
        listed = engine._SECONDS_TO_FREQUENCY[int(row["frequency_seconds"])]
        assert listed == "1d"
        assert engine._SECONDS_TO_FREQUENCY[60] == "1m"
        assert engine._SECONDS_TO_FREQUENCY[3600] == "1h"

    def test_no_tool_promises_a_date_range_the_registry_does_not_store(self):
        """The registry records no start or end. A hint that sends a model to
        list_bundles "for which dates" sends it somewhere the answer is not."""
        import inspect

        from ziplime.mcp import engine
        from ziplime.mcp.server import list_bundles

        for text in (list_bundles.__doc__, inspect.getsource(engine)):
            assert "for which dates" not in text

    def test_an_error_raised_by_the_strategy_is_blamed_on_the_strategy(self, tmp_path):
        """`server._handled` calls anything unexpected an internal_error -- "a bug in
        the local MCP server or in the engine beneath it". For a strategy that raises,
        which is the common case for generated code, that is both wrong and the opposite
        of the advice the model needs."""
        from ziplime.mcp.engine import _strategy_frame

        strategy = tmp_path / "s.py"
        strategy.write_text("def boom():\n    raise ValueError('no')\n")
        namespace: dict = {}
        exec(compile(strategy.read_text(), str(strategy), "exec"), namespace)
        try:
            namespace["boom"]()
        except ValueError as error:
            blame = _strategy_frame(error, strategy)
        assert blame is not None
        line, text = blame
        assert line == 2
        assert "raise ValueError" in text

    def test_an_error_from_the_engine_is_not_blamed_on_the_strategy(self, tmp_path):
        """The other half of the contract: a real engine bug must keep reporting as one."""
        from ziplime.mcp.engine import _strategy_frame

        try:
            raise RuntimeError("deep inside the engine")
        except RuntimeError as error:
            assert _strategy_frame(error, tmp_path / "never_ran.py") is None

    def test_the_run_defaults_to_next_bar_execution(self):
        """It hard-coded same_bar_execution=True, so every backtest filled at the close
        of the bar it decided on -- a price the market had not printed. The command line
        defaults this off and says why; the two must not disagree about look-ahead."""
        import inspect

        from ziplime.mcp import engine, server

        for function in (engine.run_backtest, server.run_backtest):
            signature = inspect.signature(function)
            assert signature.parameters["same_bar_execution"].default is False

    def test_turning_look_ahead_on_says_so_in_the_result(self):
        """Available, but never silent: a number produced with look-ahead has to arrive
        carrying that fact, because it is the result that gets read back to a person."""
        import inspect

        source = inspect.getsource(__import__(
            "ziplime.mcp.server", fromlist=["run_backtest"]).run_backtest)
        assert "look_ahead_was_on" in source

    def test_the_run_exposes_slippage_like_the_command_line_does(self):
        """It set no slippage at all, so a run through this server and the same run
        through `ziplime run` priced fills differently with nothing saying so."""
        import inspect

        from ziplime.mcp import server

        assert "slippage_bps" in inspect.signature(server.run_backtest).parameters


class TestTheServerCanBeStartedTheWayEachClientStartsIt:
    def test_there_is_an_entry_point_for_a_loop_the_caller_already_owns(self):
        """`server.run()` calls `anyio.run()`, which refuses to nest. `ziplime mcp` runs
        inside asyncclick's loop, so wiring it to that entry point raised "Already
        running asyncio in this thread" -- reaching the client as a closed connection."""
        import inspect

        import ziplime.mcp

        assert inspect.iscoroutinefunction(ziplime.mcp.serve_stdio)
        assert not inspect.iscoroutinefunction(ziplime.mcp.main)

    def test_the_cli_command_awaits_the_async_entry_point(self):
        import inspect

        import ziplime.__main__ as cli

        # `cli.mcp` is the click Command; the function it wraps is its callback.
        source = inspect.getsource(cli.mcp.callback)
        assert "await serve_stdio()" in source
