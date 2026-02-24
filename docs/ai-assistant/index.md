# Ziplime AI Assistant

The Ziplime AI Assistant lets you run backtests using plain English — no programming experience required.

You describe a trading strategy in your own words, and the AI:

1. Translates it into a working Ziplime algorithm
2. Downloads the necessary historical data (Yahoo Finance, free)
3. Runs the backtest automatically
4. Explains the results in plain language

---

## How it works

```
You: "Backtest a moving average crossover on Apple stock for 2024"
       │
       ▼
  OpenRouter LLM (GPT-4o or any other model)
  → understands your strategy
  → generates Ziplime algorithm code
       │
       ▼
  Data Manager
  → downloads AAPL daily data from Yahoo Finance
       │
       ▼
  Ziplime Backtesting Engine
  → runs event-driven simulation
       │
       ▼
┌─────────────────────────────────┐
│ Total Return:      +14.3%       │
│ Sharpe Ratio:       1.21        │
│ Max Drawdown:      -8.4%        │
│ Final Value:    $114,300        │
└─────────────────────────────────┘
       │
       ▼
  AI Analysis
  "The strategy outperformed buy-and-hold in this period...
   The Sharpe ratio of 1.21 indicates good risk-adjusted returns..."
```

---

## Setup

### Step 1 — Install Ziplime

```bash
pip install ziplime
```

### Step 2 — Install AI Assistant dependencies

```bash
pip install -r ai_assistant/requirements.txt
```

This installs:
- `openai` — the API client (works with OpenRouter's OpenAI-compatible API)
- `rich` — beautiful terminal output

### Step 3 — Get an OpenRouter API key

1. Go to [openrouter.ai](https://openrouter.ai) and create a free account
2. Go to **API Keys** and create a new key
3. OpenRouter gives you access to many LLMs including GPT-4o, Claude, Gemini, and free models

### Step 4 — Set your API key

=== "Environment variable (recommended)"

    ```bash
    export OPENROUTER_API_KEY="your-key-here"
    ```

    Add this to your `~/.bashrc` or `~/.zshrc` to make it permanent.

=== "Enter when prompted"

    Just run the assistant and enter the key when asked. It won't be saved to disk.

### Step 5 — Run the assistant

```bash
# From the ziplime root directory:
python -m ai_assistant
```

Or directly:

```bash
python ai_assistant/cli.py
```

---

## Usage

Once started, you'll see an interactive prompt:

```
╔══════════════════════════════════════════════════════╗
║          Ziplime AI Backtesting Assistant            ║
║                                                      ║
║  Describe any trading strategy in plain language.    ║
║  The AI will generate and run the backtest for you.  ║
╚══════════════════════════════════════════════════════╝

You: _
```

Type your question or strategy description and press Enter.

### Example conversations

**Simple buy-and-hold:**
```
You: Backtest holding Apple stock for all of 2024 with $50,000 capital

AI: I'll test a buy-and-hold strategy on AAPL for 2024...
    [Downloading data... Running backtest...]

    Total Return:   +28.4%
    Sharpe Ratio:    1.45
    Max Drawdown:   -11.2%
    Final Value:   $64,200

AI Analysis: Apple had an excellent 2024, returning 28.4%. The Sharpe ratio of 1.45
    is strong, meaning good risk-adjusted returns. The worst drawdown was -11.2%
    which occurred in [month]. Overall this was a solid investment for the period.
```

**Strategy comparison:**
```
You: Compare buy-and-hold vs a 20/50 day moving average crossover on SPY for 2022-2024

AI: I'll test both strategies. Let me start with the moving average crossover...
    [Runs backtest]
    ...
    Now for buy-and-hold...
    [Runs backtest]
    ...
```

**Follow-up questions:**
```
You: What was the worst month?
You: How would this perform with 2x leverage?
You: Try the same strategy on NASDAQ (QQQ) instead
You: What's the Sharpe ratio telling me?
```

---

## Commands

| Command | Description |
|---------|-------------|
| `help` | Show example strategies |
| `clear` or `reset` | Start a new conversation |
| `quit` or `exit` | Exit the assistant |

---

## Configuration

### Choosing a model

The model is controlled by the `OPENROUTER_MODEL` environment variable.
The default is **`x-ai/grok-4.1-fast`** — a fast, cost-effective model that handles strategy generation well.

```bash
# Default — fast and cost-effective (recommended)
export OPENROUTER_MODEL="x-ai/grok-4.1-fast"

# Claude Sonnet — excellent code generation quality
export OPENROUTER_MODEL="anthropic/claude-sonnet-4-5"

# GPT-4o — strong all-round reasoning
export OPENROUTER_MODEL="openai/gpt-4o"

# GPT-4o Mini — cheapest OpenAI option
export OPENROUTER_MODEL="openai/gpt-4o-mini"

# Free tier (rate limits apply)
export OPENROUTER_MODEL="meta-llama/llama-3.1-8b-instruct:free"
```

Set it before launching the assistant, or export it permanently in your shell profile (`~/.zshrc`, `~/.bashrc`):

```bash
# Add to ~/.zshrc or ~/.bashrc
export OPENROUTER_API_KEY="your_key_here"
export OPENROUTER_MODEL="x-ai/grok-4.1-fast"   # change this line to switch models
```

Browse all 300+ available models at [openrouter.ai/models](https://openrouter.ai/models).

### Show generated code

Run with `--show-code` to see the Python algorithm code the AI generates:

```bash
python -m ai_assistant --show-code
```

This is useful if you want to understand what the AI is doing or copy the code for further customization.

### Data storage

Downloaded market data is stored in `~/.ziplime/data/` as Parquet files. The AI assistant uses the bundle named `ai_assistant_yahoo_daily`.

The assistant tracks which symbols and date ranges have been downloaded in `~/.ziplime/ai_assistant_state.json`. Data is only re-downloaded when new symbols or date ranges are requested.

---

## Supported strategies

The AI assistant can implement any strategy that works with daily OHLCV data:

- Buy-and-hold
- Moving average crossovers (SMA, EMA)
- Momentum strategies
- Mean reversion
- Equal-weight portfolios
- Threshold-based rebalancing
- Multi-asset ranking
- Sector rotation

For strategies requiring tick data, intraday bars, options, or futures, use the full [programmatic API](../tutorial/first-steps.md) instead.

---

## Limitations

- **Daily bars only** — Yahoo Finance provides daily OHLCV data. Minute-level backtesting requires LimexHub (see [programmatic API](../tutorial/first-steps.md)).
- **US stocks only** — Yahoo Finance data covers NYSE and NASDAQ-listed equities and ETFs.
- **AI-generated code** — The assistant executes AI-generated Python code locally. Always review the output if used for real trading decisions.
- **Historical data range** — Yahoo Finance provides data from approximately 2010 onwards for most symbols.

---

## For developers

The AI assistant is a standalone module in the `ai_assistant/` directory. It does not modify any existing Ziplime code and can be added to the repository without breaking anything.

### Module structure

```
ai_assistant/
├── __init__.py          # Package metadata
├── __main__.py          # python -m ai_assistant entry point
├── cli.py               # Interactive REPL (rich terminal UI)
├── agent.py             # OpenRouter LLM client + response parser
├── executor.py          # Wraps algorithm code, runs run_simulation()
├── data_manager.py      # Yahoo Finance ingestion + state caching
├── prompts.py           # System prompt + display text
└── requirements.txt     # openai, rich, numpy
```

### Integrating into your own application

```python
import asyncio
import datetime
import pytz

from ai_assistant.agent import ZiplimeAgent
from ai_assistant.data_manager import DataManager
from ai_assistant.executor import BacktestExecutor

async def main():
    agent        = ZiplimeAgent(api_key="your-openrouter-key")
    data_manager = DataManager()
    executor     = BacktestExecutor(data_manager)

    # Get a strategy from the LLM
    response = await agent.chat("Buy AAPL when it's up 3 days in a row, sell after 5 days")

    if response.has_backtest:
        await data_manager.ensure_data(
            response.backtest_config.symbols,
            datetime.datetime(2024, 1, 3, tzinfo=pytz.utc),
            datetime.datetime(2025, 1, 1, tzinfo=pytz.utc),
        )
        result = await executor.run(response.algorithm_code, response.backtest_config)
        print(result.to_summary_text())

asyncio.run(main())
```
