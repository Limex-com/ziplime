# ZipLime. The iconic backtester. Reinvented <a href='https://ziplime.limex.com'><img src='img/logo.png' align="left" height="35" /></a>

 <a target="new" href="https://pypi.python.org/pypi/ziplime"><img border=0 src="https://img.shields.io/badge/python-3.7+-blue.svg?style=flat" alt="Python version"></a>
 <a target="new" href="https://pypi.python.org/pypi/ziplime"><img border=0 src="https://img.shields.io/pypi/v/ziplime?maxAge=60%" alt="PyPi version"></a>
 <a target="new" href="https://pypi.python.org/pypi/ziplime"><img border=0 src="https://img.shields.io/pypi/dm/ziplime.svg?maxAge=2592000&label=installs&color=%2327B1FF" alt="PyPi downloads"></a>
 <a target="new" href="https://github.com/Limex-com/ziplime"><img border=0 src="https://img.shields.io/github/stars/Limex-com/ziplime.svg?style=social&label=Star&maxAge=60" alt="Star this repo"></a>

## Elevate Your Backtesting with Speed and Flexibility

`Ziplime` is a powerful reimagining of the classic Zipline library, now turbocharged with the speed of Polars and enhanced with a versatile new data storage architecture. Designed for quant traders and data scientists, Ziplime retains the familiar backtesting syntax of Quantopian's Zipline while introducing groundbreaking features:

- `Unmatched Performance`: Experience lightning-fast backtesting with Polars, enabling you to iterate faster and optimize strategies with ease.
- `Flexible Data Intervals`: Seamlessly integrate any data interval, from minutes to months, to match your strategy's unique requirements.
- `Comprehensive Data Support`: Go beyond price data. Incorporate fundamental data into your analyses for a more holistic view of the markets.
- `Portable`: OS independent, runs on Linux, macOS, and Windows. Deploy using Docker.
- `Live`: Use identical strategy implementations between backtesting and live deployments.

We evaluated the speed of various event-driven backtesters using a simple strategy applied to a single instrument over five years of daily data. The strategy involved entering positions based on one technical indicator and exiting based on another.

![Alt text](img/compare_small.png "Compare small universe")

As a result, we found that ZipLime outperforms major benchmarks in terms of speed. This advantage is largely due to the internal parallelism built into Polars, which allows ZipLime to demonstrate significant performance benefits on multi-core tests, particularly on the Apple Silicon M3
