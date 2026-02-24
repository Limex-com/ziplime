import datetime
from pathlib import Path

from ziplime.core.run_live_trading import run_live_trading

if __name__ == "__main__":
    res = run_live_trading(
        start_date=None,
        end_date=None,
        trading_calendar="NYSE",
        algorithm_file=str(Path("algorithms/test_algo/test_algo.py").absolute()),
        total_cash=100000.0,
        # bundle_name="limex_us_polars_minute",
        bundle_name=None,
        config_file=str(Path("algorithms/test_algo/test_algo_config.json").absolute()),
        emission_rate=datetime.timedelta(seconds=5),
    )
    print(res.head(n=10))
