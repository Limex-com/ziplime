# 🍋 Ziplime — The First Open-Source Backtester with Native AI Support

<a target="new" href="https://pypi.python.org/pypi/ziplime"><img border=0 src="https://img.shields.io/badge/python-3.12+-blue.svg?style=flat" alt="Python version"></a>
<a target="new" href="https://pypi.python.org/pypi/ziplime"><img border=0 src="https://img.shields.io/pypi/v/ziplime?maxAge=60%" alt="PyPi version"></a>
<a target="new" href="https://pypi.python.org/pypi/ziplime"><img border=0 src="https://img.shields.io/pypi/dm/ziplime.svg?maxAge=2592000&label=installs&color=%2327B1FF" alt="PyPi downloads"></a>
<a target="new" href="https://github.com/Limex-com/ziplime"><img border=0 src="https://img.shields.io/github/stars/Limex-com/ziplime.svg?style=social&label=Star&maxAge=60" alt="Star this repo"></a>

**Write trading strategies in plain English. Backtest them in seconds.**

*Built on the legacy of Zipline. Rebuilt for the age of AI.*

---

## The Problem

[Zipline](https://github.com/quantopian/zipline) was the gold standard of open-source backtesting — until it was abandoned. Pinned to legacy pandas, Python 3.6, and deprecated dependencies, it became unusable for modern projects. Dozens of forks tried to patch it. None truly modernized it.

Meanwhile, AI can now write trading strategies — but every tool forces you to copy-paste between ChatGPT and your terminal, manually fixing imports, data formats, and library quirks.

There had to be a better way.

---

## The Solution

Ziplime is Zipline reborn from scratch for 2025:

- 🧠 **AI generates strategies natively** — describe what you want in plain English, Ziplime writes the code, runs the backtest, and returns the results. No copy-paste. No glue code. Everything runs locally and privately.
- ⚡ **Polars replaces pandas & NumPy** — dramatically faster data pipelines, especially on Apple Silicon.
- 🔴 **Live trading built in** — the same algorithm file works for backtesting and live execution via Lime Trader SDK. No rewrite required.
- 📊 **Any data, any frequency** — 1-minute, hourly, daily, weekly, monthly, or any custom period. OHLCV + fundamentals (P/E, revenue, margins, earnings).

> Ziplime is not a wrapper around an LLM.
> It is a full-featured, production-grade backtesting engine that also understands natural language.

---

## Key Features

| Feature | Description |
|---|---|
| **MCP server** | Claude, Cursor or any agent drives the engine as a tool |
| **Polars engine** | 2–5× faster than pandas-based backtesters |
| **Any frequency** | 1-minute, hourly, daily, weekly, monthly, or custom |
| **Fundamental data** | P/E, revenue, margins, earnings alongside OHLCV |
| **Full async** | `asyncio`-native throughout — algorithms, ingestion, exchange |
| **Live trading** | Same file for backtest and live via Lime Trader SDK |
| **Free data** | Yahoo Finance built in, no API key needed |
| **Portable** | Linux, macOS, Windows, Docker |

---

## Performance

Ziplime replaces the entire pandas/NumPy data layer with [Polars](https://pola.rs/) — a DataFrame library written in Rust with built-in multi-core parallelism.

```
Benchmark: 5 years daily data, 500 assets, SMA crossover strategy

Ziplime (Polars)  ████████░░░░░░░░░░░░░░░░░░  12.4s
Zipline (pandas)  █████████████████████████░  38.7s
Backtrader        ████████████████████████████ 52.1s
```

*Tested on Apple Silicon M3, Python 3.12*

---

## Two Ways to Use Ziplime

### 1. AI Mode — Let Your Agent Drive

Give Claude, Cursor or any MCP client the engine as a tool. It ingests data, writes the strategy,
runs the backtest and reads the results back to you.

```bash
pip install "ziplime[mcp]"
```

```json
{
  "mcpServers": {
    "ziplime": { "command": "ziplime", "args": ["mcp"] }
  }
}
```

```
You: Backtest a 20/50 SMA crossover on AAPL for 2023 and compare it to buy-and-hold.
```

!!! warning
    The local server lets the agent write a strategy file and run it: arbitrary Python executing
    on your machine with your permissions. `check_strategy_code` catches mistakes; it is not a
    sandbox. Review what the agent writes, or run the server in a container.

### 2. Code Mode — Full Control

Write strategies in Python using the familiar Zipline lifecycle:

```python
# my_strategy.py
import datetime

from ziplime.finance.execution import MarketOrder

async def initialize(context):
    context.asset = await context.symbol("AAPL")

async def handle_data(context, data):
    df = await data.history(assets=[context.asset], fields=["close"], bar_count=50,
                            frequency=datetime.timedelta(days=1))
    closes = df["close"].to_list()
    if len(closes) < 50:
        return

    sma20 = sum(closes[-20:]) / 20
    sma50 = sum(closes) / 50

    if sma20 > sma50:
        await context.order_target_percent(
            asset=context.asset, target=1.0, style=MarketOrder()
        )
    else:
        await context.order_target_percent(
            asset=context.asset, target=0.0, style=MarketOrder()
        )
```

Load the data once, then run it:

```bash
ziplime ingest-assets
ziplime ingest -b quickstart -s AAPL --start-date 2023-01-01 --end-date 2023-12-31
ziplime run -f my_strategy.py -b quickstart -s AAPL --start-date 2023-01-03 --end-date 2023-12-29
```

The [README quick start](https://github.com/Limex-com/ziplime#-python-quick-start) shows the same
run from Python with `run_simulation`.

---

## Comparison with Original Zipline

| Feature | Zipline | **Ziplime** |
|---|---|---|
| Data engine | NumPy / HDF5 | **Polars / Parquet** |
| Performance | Baseline | **2–5× faster** |
| Data frequencies | Daily only | **Any frequency** |
| Fundamental data | — | **Yes** |
| Async support | — | **Full asyncio** |
| Live trading | — | **Lime Trader SDK** |
| Multiple data sources | Limited | **LimexHub, Yahoo Finance, CSV, SDK** |
| **AI strategy generation** | — | **✅ Native** |
| Python requirement | 3.6 | **3.12+** |
| Maintained | ❌ Abandoned | **✅ Active** |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                    Natural Language Input                     │
│             "RSI strategy on AAPL, 2023"                     │
└─────────────────────────┬────────────────────────────────────┘
                          │ Your agent, over MCP
┌─────────────────────────▼────────────────────────────────────┐
│                    Your Algorithm                             │
│   async initialize()  ·  async handle_data()                 │
└─────────────────────────┬────────────────────────────────────┘
                          │
┌─────────────────────────▼────────────────────────────────────┐
│                   TradingAlgorithm                            │
│  order_target_percent() · symbol() · portfolio · ...         │
└──────┬───────────────────────────────────────────┬───────────┘
       │                                           │
┌──────▼──────┐   ┌──────────────────┐   ┌────────▼───────────┐
│   Blotter   │   │   Simulation     │   │   Data Bundles     │
│ (orders /   │◄─►│   Exchange       │   │ (Parquet, Polars)  │
│  fills)     │   │ (slippage /      │   │                    │
└─────────────┘   │  commission)     │   │ Yahoo · LimexHub   │
                  └──────────────────┘   │ CSV · SDK          │
                                         └────────────────────┘
```

---

## Data Sources

| Source | Type | Cost |
|---|---|---|
| Yahoo Finance | Historical OHLCV | Free |
| CSV files | Any custom data | Free |
| Lime Trader SDK | Real-time & historical | Broker account |
| LimexHub | Professional-grade data + fundamentals | Subscription |

---

## Getting Started

1. **[Install Ziplime](installation.md)**
2. **[Run your first backtest](../tutorial/first-steps.md)**
3. **[Connect your agent over MCP](#1-ai-mode-let-your-agent-drive)**
4. **[Learn the algorithm API](../tutorial/algorithm-file-structure.md)**

---

*Zipline is dead. Long live Ziplime.* 🍋
