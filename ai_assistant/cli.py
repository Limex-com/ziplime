"""
Ziplime AI Backtesting Assistant — interactive CLI.

Usage:
    python -m ai_assistant
    python ai_assistant/cli.py

Environment variables (optional):
    OPENROUTER_API_KEY   Your OpenRouter API key
    OPENROUTER_MODEL     LLM model (default: x-ai/grok-4.1-fast)
"""
from __future__ import annotations

import asyncio
import os
import sys
import datetime

# Ensure parent directory is on the path so ziplime is importable
sys.path.insert(0, str(__import__("pathlib").Path(__file__).parent.parent))

from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich import box
from rich.prompt import Prompt

from .agent import ZiplimeAgent
from .data_manager import DataManager
from .executor import BacktestExecutor, BacktestResult
from .prompts import HELP_TEXT

console = Console()


# ------------------------------------------------------------------ #
# Display helpers                                                      #
# ------------------------------------------------------------------ #

def display_welcome():
    console.print(
        Panel(
            "[bold green]Ziplime AI Backtesting Assistant[/bold green]\n\n"
            "Describe any trading strategy in plain English.\n"
            "The AI will generate and run the backtest automatically.\n\n"
            "[dim]Data source: Yahoo Finance (free, no API key needed)\n"
            "Type [bold]help[/bold] for examples, [bold]quit[/bold] to exit.[/dim]",
            border_style="green",
            padding=(1, 2),
        )
    )


def display_results(result: BacktestResult):
    """Print a rich table with the key backtest metrics."""
    # Determine colour coding
    ret_color    = "green" if result.total_return_pct >= 0 else "red"
    sharpe_color = "green" if result.sharpe_ratio >= 1.0 else ("yellow" if result.sharpe_ratio >= 0 else "red")
    dd_color     = "green" if result.max_drawdown_pct > -10 else ("yellow" if result.max_drawdown_pct > -20 else "red")

    table = Table(
        title="[bold]Backtest Results[/bold]",
        box=box.ROUNDED,
        border_style="bright_blue",
        show_header=False,
        padding=(0, 2),
    )
    table.add_column("Metric", style="dim", width=28)
    table.add_column("Value", justify="right", width=16)

    table.add_row("Strategy", ", ".join(result.symbols))
    table.add_row("Period", f"{result.start_date}  →  {result.end_date}")
    table.add_row("Trading Days", str(result.num_trading_days))
    table.add_row("─" * 28, "─" * 16)
    table.add_row("Starting Capital",    f"${result.starting_capital:>14,.0f}")
    table.add_row("Final Portfolio Value", f"${result.final_portfolio_value:>14,.2f}")
    table.add_row(
        "Total Return",
        f"[{ret_color}]{result.total_return_pct:>+13.2f}%[/{ret_color}]",
    )
    table.add_row(
        "Annualized Return",
        f"[{ret_color}]{result.annualized_return_pct:>+13.2f}%[/{ret_color}]",
    )
    table.add_row(
        "Sharpe Ratio",
        f"[{sharpe_color}]{result.sharpe_ratio:>15.3f}[/{sharpe_color}]",
    )
    table.add_row(
        "Max Drawdown",
        f"[{dd_color}]{result.max_drawdown_pct:>13.2f}%[/{dd_color}]",
    )

    if result.alpha is not None or result.beta is not None:
        table.add_row("─" * 28, "─" * 16)
        if result.alpha is not None:
            alpha_color = "green" if result.alpha >= 0 else "red"
            table.add_row(
                "Alpha (vs SPY)",
                f"[{alpha_color}]{result.alpha:>+15.4f}[/{alpha_color}]",
            )
        if result.beta is not None:
            beta_color = "green" if 0.5 <= result.beta <= 1.2 else "yellow"
            table.add_row(
                "Beta (vs SPY)",
                f"[{beta_color}]{result.beta:>15.4f}[/{beta_color}]",
            )

    console.print()
    console.print(table)

    if result.html_report_path:
        console.print(
            f"  [dim]QuantStats report → [link=file://{result.html_report_path}]"
            f"{result.html_report_path}[/link][/dim]"
        )
    if result.strategy_file_path:
        console.print(
            f"  [dim]Strategy code  → [link=file://{result.strategy_file_path}]"
            f"{result.strategy_file_path}[/link][/dim]"
        )
    console.print()


def display_code(code: str):
    """Show the generated algorithm code (optional, for --show-code mode)."""
    console.print(
        Panel(
            f"[dim]{code}[/dim]",
            title="[bold dim]Generated Algorithm Code[/bold dim]",
            border_style="dim",
            padding=(1, 2),
        )
    )


def display_ai_message(text: str):
    if text.strip():
        console.print(
            Panel(
                Markdown(text),
                title="[bold blue]AI Analysis[/bold blue]",
                border_style="blue",
                padding=(1, 2),
            )
        )


def display_run_params(config, model: str):
    """Show a summary of what is about to be run before execution starts."""
    table = Table(
        title="[bold]Backtest Parameters[/bold]",
        box=box.SIMPLE_HEAVY,
        border_style="cyan",
        show_header=False,
        padding=(0, 2),
    )
    table.add_column("Key", style="dim", width=22)
    table.add_column("Value", width=40)

    table.add_row("Symbols",    "[bold]" + ", ".join(config.symbols) + "[/bold]")
    table.add_row("Start date", config.start_date)
    table.add_row("End date",   config.end_date)
    table.add_row("Capital",    f"${config.capital:,.0f}")
    table.add_row("Benchmark",  config.benchmark or "none")
    table.add_row("Model",      f"[dim]{model}[/dim]")

    console.print()
    console.print(table)
    console.print()


def display_errors(errors: list[str], max_shown: int = 5):
    """Show the first few simulation errors so the user can debug."""
    if not errors:
        return
    lines = [f"[yellow]{len(errors)} non-fatal simulation error(s).[/yellow]"]
    shown = errors[:max_shown]
    for i, err in enumerate(shown, 1):
        # Trim very long error strings for readability
        short = err if len(err) <= 300 else err[:297] + "…"
        lines.append(f"  [dim]{i}.[/dim] {short}")
    if len(errors) > max_shown:
        lines.append(f"  [dim]… and {len(errors) - max_shown} more[/dim]")
    console.print(
        Panel(
            "\n".join(lines),
            title="[bold yellow]Simulation Warnings[/bold yellow]",
            border_style="yellow",
            padding=(0, 2),
        )
    )


# ------------------------------------------------------------------ #
# Main REPL                                                            #
# ------------------------------------------------------------------ #

async def run_assistant(
    api_key: str,
    model: str = "x-ai/grok-4.1-fast",
    show_code: bool = False,
):
    """Main interactive loop."""
    agent        = ZiplimeAgent(api_key=api_key, model=model)
    data_manager = DataManager(on_progress=lambda msg: console.print(f"  [dim]{msg}[/dim]"))
    executor     = BacktestExecutor(data_manager=data_manager)

    display_welcome()
    console.print(f"[dim]Model: {model}[/dim]\n")

    while True:
        try:
            user_input = console.input("[bold green]You:[/bold green] ").strip()
        except (EOFError, KeyboardInterrupt):
            console.print("\n[dim]Goodbye![/dim]")
            break

        if not user_input:
            continue

        lower = user_input.lower()
        if lower in {"quit", "exit", "q", "bye"}:
            console.print("[dim]Goodbye![/dim]")
            break
        if lower in {"help", "h", "?"}:
            console.print(Panel(HELP_TEXT, border_style="dim"))
            continue
        if lower in {"clear", "reset", "new"}:
            agent.clear_history()
            console.print("[dim]Conversation history cleared. Starting fresh.[/dim]\n")
            continue

        # --- Ask the LLM ------------------------------------------------
        with console.status("[bold blue]Thinking…[/bold blue]", spinner="dots"):
            try:
                response = await agent.chat(user_input)
            except Exception as exc:
                console.print(f"[red]LLM error:[/red] {exc}")
                continue

        # --- Show explanation text first (if any) -----------------------
        if response.text:
            display_ai_message(response.text)

        # --- Run backtest if the LLM requested one ----------------------
        if response.has_backtest:
            # Show parameters before doing anything
            display_run_params(response.backtest_config, model)

            if show_code and response.algorithm_code:
                display_code(response.algorithm_code)

            # 1. Ensure data is available
            try:
                start_dt = datetime.datetime.strptime(
                    response.backtest_config.start_date, "%Y-%m-%d"
                ).replace(tzinfo=datetime.timezone.utc)
                end_dt = datetime.datetime.strptime(
                    response.backtest_config.end_date, "%Y-%m-%d"
                ).replace(tzinfo=datetime.timezone.utc)

                # Include the benchmark symbol so BenchmarkSource can find it
                all_symbols = list(response.backtest_config.symbols)
                bm = response.backtest_config.benchmark
                if bm and bm not in all_symbols:
                    all_symbols.append(bm)

                await data_manager.ensure_data(
                    all_symbols,
                    start_dt,
                    end_dt,
                )
            except Exception as exc:
                console.print(f"[red]Data ingestion error:[/red] {exc}")
                continue

            # 2. Run the backtest
            with console.status("[bold blue]Running backtest…[/bold blue]", spinner="dots"):
                try:
                    result = await executor.run(
                        algorithm_code=response.algorithm_code,
                        config=response.backtest_config,
                    )
                except Exception as exc:
                    console.print(f"[red]Backtest error:[/red] {exc}")
                    import traceback
                    console.print(f"[dim]{traceback.format_exc()}[/dim]")
                    continue

            # 3. Display simulation warnings (actual error messages)
            if result.errors:
                display_errors(result.errors)

            # 4. Display results table
            display_results(result)

            # 5. Ask LLM to interpret results (include full QuantStats metrics)
            agent.add_result_context(result.to_summary_text())
            with console.status("[bold blue]Analysing results…[/bold blue]", spinner="dots"):
                try:
                    interpretation = await agent.chat(
                        "Please interpret these backtest results for me in plain language. "
                        "Use all available metrics (Sharpe, Sortino, Calmar, drawdown, "
                        "win rate, etc.) to give a thorough assessment."
                    )
                except Exception as exc:
                    console.print(f"[red]Interpretation error:[/red] {exc}")
                    continue

            if interpretation.text:
                display_ai_message(interpretation.text)

        console.print()


# ------------------------------------------------------------------ #
# Entry point                                                          #
# ------------------------------------------------------------------ #

def main():
    """CLI entry point. Called by `python -m ai_assistant` or directly."""
    # --- Resolve API key -----------------------------------------------
    api_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        console.print(
            "[yellow]No OPENROUTER_API_KEY environment variable found.[/yellow]\n"
            "Get a free API key at [link=https://openrouter.ai]https://openrouter.ai[/link]\n"
        )
        api_key = Prompt.ask("Enter your OpenRouter API key", password=True)
        if not api_key:
            console.print("[red]API key required. Exiting.[/red]")
            sys.exit(1)

    # --- Resolve model -------------------------------------------------
    model = os.environ.get(
        "OPENROUTER_MODEL",
        "x-ai/grok-4.1-fast",
    )

    # --- Parse simple flags -------------------------------------------
    show_code = "--show-code" in sys.argv or "-v" in sys.argv

    # --- Run -----------------------------------------------------------
    try:
        asyncio.run(run_assistant(api_key=api_key, model=model, show_code=show_code))
    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted. Goodbye![/dim]")


if __name__ == "__main__":
    main()
