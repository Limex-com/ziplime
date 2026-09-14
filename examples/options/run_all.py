"""Run the 0DTE option examples and report what each one did.

Not what it earned. The option prices are synthetic and a return computed on them describes the
generator rather than a market -- ``_harness.summarise`` refuses to call them performance, and the
table below has no return column for that reason.
"""
import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from _harness import list_strategies, run_strategy, summarise  # noqa: E402

from ziplime.data.data_sources.options.synthetic import SYNTHETIC_DATA_WARNING  # noqa: E402
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
            result, context = await run_strategy(info)
            rows.append(summarise(info, result, context))
        except Exception as error:  # a broken example must not hide the others
            failures.append((info["name"], f"{type(error).__name__}: {error}"))
            if verbose:
                import traceback
                traceback.print_exc()

    print()
    print("=" * 100)
    print(SYNTHETIC_DATA_WARNING)
    print("=" * 100)
    header = f"{'strategy':<22} {'sess':>4} {'bars':>5} {'listed':>7} {'trades':>7} {'lots':>6} {'names':>6}  description"
    print(header)
    print("-" * len(header))
    for row in rows:
        print(f"{row['name']:<22} {row['sessions']:>4} {row['bars']:>5} "
              f"{row['contracts_listed']:>7} {row['option_trades']:>7} "
              f"{row['contracts_traded']:>6} {row['distinct_contracts']:>6}  "
              f"{row['description'][:44]}")
        for error in row["errors"]:
            print(f"    ! {error.message[:140]}")
    for name, error in failures:
        print(f"{name:<22} FAILED  {error[:120]}")
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--only", nargs="*", help="Run only strategies whose name contains these")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()
    configure_logging()
    asyncio.run(main(only=args.only, verbose=args.verbose))
