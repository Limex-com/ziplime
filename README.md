<p align="center">
  <img src="img/logo.png" width="320" alt="Ziplime">
</p>

<h1 align="center">The backtesting engine built for AI agents</h1>

<p align="center">
  <b>Vibe-code trading strategies. Verify them like a quant. Trade them live.</b>
</p>

<p align="center">
  Zipline, rebuilt from scratch on Polars — with a native <b>MCP server</b>, so Claude, Cursor, Codex or any agent can research, backtest and deploy strategies on stocks, ETFs and futures.<br>
  One file for backtest and live. No glue code. No copy-paste.
</p>

<p align="center">
  <a href="https://pypi.python.org/pypi/ziplime"><img src="https://img.shields.io/badge/python-3.12+-blue.svg?style=flat" alt="Python"></a>
  <a href="https://pypi.python.org/pypi/ziplime"><img src="https://img.shields.io/pypi/v/ziplime?maxAge=60" alt="PyPI"></a>
  <a href="https://pypi.python.org/pypi/ziplime"><img src="https://img.shields.io/pypi/dm/ziplime.svg?maxAge=2592000&label=installs&color=%2327B1FF" alt="Installs"></a>
  <a href="https://github.com/Limex-com/ziplime"><img src="https://img.shields.io/github/stars/Limex-com/ziplime.svg?style=social&label=Star&maxAge=60" alt="Stars"></a>
  <!-- TODO: add MCP badge once listed in a directory, e.g. https://img.shields.io/badge/MCP-server-8A2BE2 -->
</p>

<p align="center">
  <a href="#-connect-your-ai-in-30-seconds">Connect your AI</a> ·
  <a href="https://ziplime.limex.com">Try in browser</a> ·
  <a href="#-python-quick-start">Python quick start</a> ·
  <a href="https://limex-com.github.io/ziplime/">Docs</a> ·
  <a href="#-community">Community</a>
</p>

<!-- TODO: replace with a 20–30s GIF of Claude/Cursor running the full loop through MCP: prompt → strategy → backtest → metrics → "deploy?" -->

---

## Why this exists

LLMs write trading strategies in seconds. Most of them are wrong in ways that *look* right: lookahead bias, survivorship bias, fills that never happen, a Sharpe of 4 that evaporates on the first day of live trading.

And every tool makes **you** the glue: copy code out of the chat, fix imports, wrangle data, run, paste results back, repeat.

Ziplime flips it. **The agent proposes. The engine proves.**

Your AI gets a real backtester as a tool — point-in-time data, realistic execution, versioned results, a live-readiness check — and you get strategies that survived contact with reality instead of a chat transcript.

> Ziplime is not a wrapper around an LLM.
> It is a production-grade backtesting and live-trading engine that any LLM can drive.

---

## ⚡ Connect your AI in 30 seconds

Works with Claude Desktop, Claude Code, Cursor, Codex, Windsurf — anything that speaks MCP.

```json
{
  "mcpServers": {
    "ziplime": {
      "url": "https://ziplime.limex.com/mcp"
    }
  }
}
```
<!-- TODO: confirm the public MCP URL (/mcp vs /mcp-zipline/mcp) and whether first connect needs OAuth; if it does, add one line: "Sign in once, then the agent works on your behalf." -->

Then just talk to your agent:

> **You:** Backtest a 20/50 SMA crossover on NVDA for 2020–2025 and tell me if it beats buy-and-hold.
>
> **Agent:** *searches the ticker → writes the strategy → runs the backtest → pulls metrics → compares to benchmark*
>
> "CAGR 31% vs 68% for buy-and-hold, max drawdown 24% vs 66%. Lower return, much smoother ride. Want me to add a volatility filter and rerun?"

Prefer everything on your own machine?

```bash
pip install "ziplime[mcp]"
ziplime mcp        # local MCP server over stdio — no account, no cloud
```

```json
{
  "mcpServers": {
    "ziplime": { "command": "ziplime", "args": ["mcp"] }
  }
}
```

> ⚠️ The local server lets the agent write a strategy file and run it — that is arbitrary Python
> executing on your machine with your permissions. `check_strategy_code` catches mistakes, it is
> not a sandbox. Review what the agent writes, or run the server in a container.

---

## 🛠 What your agent can do

The hosted MCP server exposes the whole quant workflow — not just "run backtest".

| Stage | Tools the agent gets |
| --- | --- |
| **Research** | `search_tickers` · `screen_market` · `analyze_signal` · `get_stock_factors` · `list_themes` |
| **Build** | `create_strategy` · `check_strategy_code` · `get_strategy_language_reference` · `copy_strategy` |
| **Test** | `run_backtest` · `get_backtest_metrics` · `get_backtest_trades` · `compare_backtests` · `compare_execution_costs` |
| **Ship** | `assess_live_readiness` · `prepare_deployment_plan` · `create_live_deployment` · `get_live_deployment_activity` |

40+ tools. The agent can run the full loop on its own: **idea → screen → strategy → backtest → iterate → live** — and explain every step.

The local `ziplime mcp` server is the engine on your machine: 14 tools to ingest data, write and check strategies, and run backtests. It places no orders.

---

## 🍋 Zipline, rebuilt for 2026

Zipline was the gold standard of backtesting — until it was abandoned on pandas 0.x and Python 3.6. Dozens of forks patched it. None rebuilt it.

Ziplime keeps the API you know and replaces everything underneath:

- **Polars instead of pandas/NumPy** — Parquet-native, 2–5× faster, screaming on Apple Silicon
- **Full asyncio** — the engine is async end to end
- **Any frequency** — 1-minute, hourly, daily, weekly, monthly, or your own custom bars
- **OHLCV + point-in-time fundamentals** — P/E, revenue, margins, earnings, as they were known *at the time*
- **Python 3.12+**, modern packaging, active releases

```
Benchmark: 5 years daily data, 500 assets, SMA crossover strategy

Ziplime (Polars)  ████████░░░░░░░░░░░░░░░░░░  12.4s
Zipline (pandas)  █████████████████████████░  38.7s
Backtrader        ████████████████████████████ 52.1s
```
*Apple Silicon M3, Python 3.12. Reproduce it: `python benchmarks/sma_500.py`*
<!-- TODO: add the benchmarks/ folder with the script. An unreproducible benchmark is a liability on HN. Consider adding zipline-reloaded and vectorbt rows. -->

---

## 📈 Stocks, ETFs, futures — one portfolio, one file

- **Equities & ETFs** — US markets out of the box
- **Futures** — contract specs, multipliers, roll handling <!-- TODO: verify exact futures support wording (continuous contracts? roll rules?) -->
- **Mixed portfolios** — hold SPY, ES and a basket of single names in the same algorithm
- **Multiple data sources:**

| Source | Type | Cost |
| --- | --- | --- |
| Yahoo Finance | Historical OHLCV | Free |
| CSV files | Any custom data | Free |
| Lime Trader SDK | Real-time & historical | Broker account |
| LimexHub | Professional-grade data + fundamentals | Subscription |

<!-- TODO: if crypto / forex are supported, add them here; if not, don't. -->

---

## 🔴 Backtest → live in zero lines

The same algorithm file. The same logic. Just change the runner.

```bash
# Backtest (see the quick start below for the one-time data setup)
ziplime run -f my_strategy.py -b quickstart -s AAPL --start-date 2023-01-03 --end-date 2023-12-29
```

```python
# Live, via Lime Trader SDK (needs a Lime brokerage account)
import datetime
from ziplime.core.run_live_trading import run_live_trading

run_live_trading(algorithm_file="my_strategy.py", trading_calendar="XNYS",
                 total_cash=100_000, emission_rate=datetime.timedelta(minutes=1))
```

No adapter classes. No "paper trading wrapper." No rewrite.
**Your backtest *is* your live strategy** — and on the hosted server your agent can deploy it (`create_live_deployment`) after `assess_live_readiness` says it's safe.

---

## 🐍 Python quick start

```bash
pip install ziplime
```

**1. Write a strategy.**

```python
# my_strategy.py
from ziplime.finance.execution import MarketOrder

async def initialize(context):
    context.asset = await context.symbol("AAPL")
    context.invested = False

async def handle_data(context, data):
    if not context.invested:
        await context.order_target_percent(
            asset=context.asset, target=1.0, style=MarketOrder()
        )
        context.invested = True
```

**2. Load data once.** Instruments first — every bundle refers to them — then daily bars from Yahoo Finance (free, no key):

```bash
ziplime ingest-assets      # instrument catalogue into ~/.ziplime/assets.sqlite, about a minute
ziplime ingest -b quickstart -s AAPL --start-date 2023-01-01 --end-date 2023-12-31
```

**3. Run it.**

```bash
ziplime run -f my_strategy.py -b quickstart -s AAPL --start-date 2023-01-03 --end-date 2023-12-29
```

```
  sessions      250  (2023-01-03 .. 2023-12-29)
  return        +53.66%
  max drawdown  -15.03%
```

Orders fill on the next bar by default, so a strategy cannot trade on a price it has not seen yet.

<details>
<summary>The same run from Python</summary>

```python
import asyncio
import datetime

from ziplime.assets.domain.asset_type import AssetType
from ziplime.assets.entities.asset_symbol import AssetSymbol
from ziplime.core.ingest_data import get_asset_service
from ziplime.core.run_simulation import run_simulation
from ziplime.utils.bundle_utils import get_bundle_service
from ziplime.utils.calendar_utils import get_calendar
from ziplime.utils.date_utils import normalize_datetime


async def main():
    calendar = get_calendar("XNYS")
    start = normalize_datetime(datetime.datetime(2023, 1, 3), calendar.tz)
    end = normalize_datetime(datetime.datetime(2023, 12, 29), calendar.tz)

    asset_service = get_asset_service()  # ~/.ziplime/assets.sqlite, filled by `ziplime ingest-assets`
    [aapl] = await asset_service.get_exchange_assets_by_symbols(
        symbols=[AssetSymbol(symbol="AAPL", mic=None)], asset_type=AssetType.EQUITY)
    market_data, _ = await get_bundle_service().load_bundle(
        bundle_name="quickstart", bundle_version=None, assets=[aapl],
        start_date=start, end_date=end, frequency=datetime.timedelta(days=1),
        asset_service=asset_service)

    result = await run_simulation(
        algorithm_file="my_strategy.py",
        start_date=start,
        end_date=end,
        trading_calendar="XNYS",
        emission_rate=datetime.timedelta(days=1),
        total_cash=100_000,
        market_data_source=market_data,
        custom_data_sources=[],
        asset_service=asset_service,
        stop_on_error=True,
        same_bar_execution=False,  # fill on the next bar: no lookahead
    )
    print(result.perf[["portfolio_value", "algorithm_period_return"]].tail())


asyncio.run(main())
```

</details>

Optional features install as extras: `ziplime[mcp]` (local MCP server), `ziplime[huggingface]` (`hf://` point-in-time datasets), `ziplime[options]` (option pricing), `ziplime[numba]` (compiled vector kernel), or `ziplime[all]`.

Coming from Zipline? Most algorithms port by adding `async`/`await`. See the [migration guide](https://limex-com.github.io/ziplime/). <!-- TODO: write the migration guide page; this is the #1 question from zipline-reloaded users -->

---

## 🧭 Pick your door

| You are… | Start here |
| --- | --- |
| A trader who doesn't want to code | **[ziplime.limex.com](https://ziplime.limex.com)** — chat, backtest, deploy in the browser. Nothing to install. |
| A quant or Python developer | `pip install ziplime` — the engine, local, yours. |
| Building agents or AI products | **[MCP server](#-connect-your-ai-in-30-seconds)** — give your agent a real backtester in one config line. |

---

## ⚔️ How it compares

<!-- TODO: verify EVERY cell before publishing. A wrong competitor claim is the fastest way to get roasted. -->

| | **Ziplime** | zipline-reloaded | backtrader | vectorbt | nautilus_trader |
| --- | :---: | :---: | :---: | :---: | :---: |
| Native MCP server (agent-driven) | ✅ | — | — | — | — |
| Data engine | Polars / Parquet | pandas / HDF5 | pandas | NumPy / Numba | Rust |
| Live trading built in | ✅ | — | ✅ | — | ✅ |
| Same file: backtest → live | ✅ | — | ✅ | — | ✅ |
| Futures | ✅ | partial | ✅ | — | ✅ |
| Point-in-time fundamentals | ✅ | — | — | — | — |
| Hosted, no-install version | ✅ | — | — | — | — |
| Actively maintained | ✅ | ✅ | ⚠️ | ✅ | ✅ |

---

## 🤝 Community

- ⭐ **Star the repo** — it's how new people find us
- 💬 [Discord](#) / [Telegram](#) <!-- TODO: pick one, create it, link it -->
- 📖 [Documentation](https://limex-com.github.io/ziplime/)
- 🐛 [Issues](https://github.com/Limex-com/ziplime/issues) · [Good first issues](https://github.com/Limex-com/ziplime/labels/good%20first%20issue)
- 🏠 Built by [Limex](https://limex.com)

<p align="center">
  <a href="https://star-history.com/#Limex-com/ziplime&Date">
    <img src="https://api.star-history.com/svg?repos=Limex-com/ziplime&type=Date" width="600" alt="Star history">
  </a>
</p>

---

## License

<!-- TODO: resolve GPL-3.0 vs END_USER_LICENSE_AGREEMENT before the promo push. Cleanest story: engine = Apache-2.0 or GPL-3.0 (pick one, remove the EULA from the repo); hosted platform terms live on the website, not in the repo. -->
Ziplime engine is open source under GPL-3.0. The hosted platform at ziplime.limex.com has its own [terms](https://ziplime.limex.com/terms).

---

<p align="center"><i>Zipline is dead. Long live Ziplime.</i> 🍋</p>
