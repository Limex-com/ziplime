"""Run every futures strategy example and report what each one did.

Each strategy is checked for two things: that it ran without errors, and that it actually traded.
An example that places no trades is reported as a problem, because every one of these is written
to take a position -- a silent zero usually means the chain or the bundle is missing rather than
that the strategy decided to stay flat.

Note that the strategies do not all run on the same window: the roll examples use the recent one,
where the front of the stored chain is the market's real front month. The ``sess`` column shows it.
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
    print(f"{'strategy'.ljust(width)}  {'sess':>5s} {'trades':>6s} {'final value':>14s} "
          f"{'return':>9s} {'max dd':>8s}  description")
    print("-" * (width + 60))
    for row in rows:
        print(f"{row['name'].ljust(width)}  {row['sessions']:>5d} {row['transactions']:>6d} "
              f"{row['final_value']:>14,.2f} {row['return']:>+8.2%} {row['max_drawdown']:>+8.2%}"
              f"  {row['description']}")

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
