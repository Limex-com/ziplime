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
| **AI assistant** | Describe strategies in plain English — no code required |
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

### 1. AI Mode — No Code Required

Describe your strategy. Ziplime handles everything else.

```bash
pip install -r ai_assistant/requirements.txt
export OPENROUTER_API_KEY=your_key_here
python -m ai_assistant
```

```
You: RSI mean-reversion on AAPL and MSFT for 2023, starting with $50,000

AI: Running backtest...
    ╭─ Backtest Results ──────────────────────────╮
    │ Total Return         +18.4%                 │
    │ Annualized Return    +18.4%                 │
    │ Sharpe Ratio          1.42                  │
    │ Max Drawdown         -8.1%                  │
    │ Final Value         $59,200                 │
    ╰─────────────────────────────────────────────╯
```

The AI downloads data from Yahoo Finance, generates production-ready algorithm code, runs the simulation, and interprets the results — all in a single conversation.

### 2. Code Mode — Full Control

Write strategies in Python using the familiar Zipline lifecycle:

```python
# my_strategy.py
from ziplime.finance.execution import MarketOrder

async def initialize(context):
    context.asset = await context.symbol("AAPL")

async def handle_data(context, data):
    df = data.history(assets=[context.asset], fields=["close"], bar_count=50)
    closes = df["close"].to_numpy()

    sma20 = talib.SMA(closes, timeperiod=20)[-1]
    sma50 = talib.SMA(closes, timeperiod=50)[-1]

    if sma20 > sma50:
        await context.order_target_percent(
            asset=context.asset, target=1.0, style=MarketOrder()
        )
    else:
        await context.order_target_percent(
            asset=context.asset, target=0.0, style=MarketOrder()
        )
```

```python
import asyncio, datetime
from ziplime.core.run_simulation import run_simulation

asyncio.run(run_simulation(
    algorithm_file="my_strategy.py",
    start_date=datetime.datetime(2023, 1, 1, tzinfo=datetime.timezone.utc),
    end_date=datetime.datetime(2023, 12, 31, tzinfo=datetime.timezone.utc),
    total_cash=100_000,
    trading_calendar="NYSE",
))
```

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
                          │ AI Assistant (OpenRouter LLM)
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
3. **[Try the AI assistant](../ai-assistant/index.md)** — no code needed
4. **[Learn the algorithm API](../tutorial/algorithm-file-structure.md)**

---

*Zipline is dead. Long live Ziplime.* 🍋
