"""Run every Hugging Face dataset example and report what each one did.

Each strategy is checked for two things: that it ran without errors, and that it actually traded.
A silent zero usually means the dataset mounted but matched nothing -- a window outside its
coverage, or a universe whose tickers it does not carry.

The two examples run on different windows because their datasets cover different decades:
congressional disclosures run to 2026, insider filings stop in March 2016. The ``sess`` column
shows it.

The first run downloads; later runs read the Hugging Face cache and start immediately.
"""
import argparse
import asyncio
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from _harness import list_strategies, run_strategy, summarise  # noqa: E402

from ziplime.utils.logging_utils import configure_logging  # noqa: E402


async def main(only: list[str] | None = None, verbose: bool = False):
    strategies = list_strategies()
    if only:
        strategies = [s for s in strategies if any(key in s["name"] for key in only)]
    if not strategies:
        raise SystemExit("No strategies matched.")

    rows, failures = [], []
    for info in strategies:
        try:
            result = await run_strategy(info)
            row = summarise(info, result)
        except Exception as error:  # a broken example must not hide the others
            failures.append((info["name"], f"{type(error).__name__}: {error}"))
            if verbose:
                raise
            continue
        rows.append(row)
        if row["errors"]:
            failures.append((row["name"], f"algorithm errors: {row['errors'][:1]}"))
        elif row["transactions"] == 0:
            failures.append((row["name"], "placed no trades"))

    width = max((len(r["name"]) for r in rows), default=10)
    print()
    print(f"{'strategy'.ljust(width)}  {'trades':>6s} {'return':>9s} {'cagr':>7s} "
          f"{'vol':>6s} {'sharpe':>7s} {'max dd':>8s} {'ret/dd':>7s}  description")
    print("-" * (width + 66))
    def cell(value, spec: str, width: int) -> str:
        """A blank rather than `nan`. A run too short to annualise has no annual rate, and
        printing one that reads `+nan%` invites someone to fix the formatting instead."""
        return "--".rjust(width) if value != value else format(value, spec)

    for row in rows:
        nan = float("nan")
        print(f"{row['name'].ljust(width)}  {row['transactions']:>6d} {row['return']:>+8.2%} "
              f"{cell(row.get('cagr', nan), '>+6.2%', 7)} "
              f"{cell(row.get('volatility', nan), '>6.1%', 6)} "
              f"{cell(row.get('sharpe', nan), '>7.2f', 7)} {row['max_drawdown']:>+8.2%} "
              f"{cell(row.get('return_to_drawdown', nan), '>7.2f', 7)}  {row['description'][:52]}")

    print()
    if failures:
        print(f"PROBLEMS ({len(failures)}):")
        for name, reason in failures:
            print(f"  {name}: {reason}")
        return 1
    print(f"All {len(rows)} strategies ran and traded.")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", nargs="*", help="substring match on strategy names")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    configure_logging(level=logging.CRITICAL, file_name="mylog.log")
    sys.exit(asyncio.run(main(only=args.only, verbose=args.verbose)))
