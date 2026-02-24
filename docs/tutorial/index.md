# Tutorial — User Guide

This guide walks you through the complete workflow of using Ziplime: from installing the library and ingesting historical data to writing a trading algorithm and running a backtest.

## What you will learn

1. **[First steps](first-steps.md)** — install Ziplime, ingest your first dataset, and run a minimal backtest end-to-end.
2. **[Algorithm file structure](algorithm-file-structure.md)** — understand the lifecycle functions (`initialize`, `handle_data`, `before_trading_start`, `analyze`) and how to structure your strategy code.
3. **[Algorithm parameters](algorithm-parameters.md)** — configure commission models, slippage, position controls, benchmarks, and more.

## Ziplime workflow at a glance

```
1. Ingest data          2. Write algorithm        3. Run backtest
─────────────────       ──────────────────────    ───────────────────────
LimexHub API     ──►   initialize(context)  ──►  run_simulation(...)
Yahoo Finance    ──►   handle_data(context, data)
CSV file         ──►   before_trading_start(...)   4. Analyse results
                        analyze(context, results)  ───────────────────────
                                                   result.perf  (Polars DF)
```

## Prerequisites

- Python 3.12 or newer
- Basic knowledge of async/await in Python
- (Optional) A LimexHub API key for professional-grade data
