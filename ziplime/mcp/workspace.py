"""Where a local run keeps its work: plain files under a directory you own.

The hosted platform has a database, an account and version history. A local
install has a filesystem, and pretending otherwise would mean inventing a
schema nobody asked for. So a strategy is a ``.py`` file and a backtest is a
directory holding its parameters and its output — both readable, diffable, and
deletable with ``rm``.

Everything lives under ``ZIPLIME_MCP_HOME`` (default ``~/.ziplime/mcp``),
beside the engine's own ``~/.ziplime/data`` bundles rather than inside them:
bundles are a cache that can be re-ingested, and this is work that cannot.
"""
from __future__ import annotations

import json
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path

from .errors import InvalidArguments, NotFound

_SAFE_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


def home() -> Path:
    """The workspace root, created on first use."""
    root = Path(os.environ.get("ZIPLIME_MCP_HOME") or Path.home() / ".ziplime" / "mcp")
    root.mkdir(parents=True, exist_ok=True)
    return root


def bundle_storage_path() -> str:
    """Where the engine keeps ingested bundles. Its default, not ours."""
    return os.environ.get("ZIPLIME_BUNDLE_PATH") or str(Path.home() / ".ziplime" / "data")


def asset_db_path() -> str:
    """The instrument database ingestion writes and a backtest reads."""
    return os.environ.get("ZIPLIME_ASSET_DB") or str(Path.home() / ".ziplime" / "assets.sqlite")


def _checked(name: str, kind: str) -> str:
    """A name that is safe as a single path segment, or a refusal that says why.

    Not defence against an attacker — this server runs as the user who started
    it. It is defence against `../` and empty strings turning a mistake into a
    deleted directory somewhere else.
    """
    name = (name or "").strip()
    if not _SAFE_NAME.match(name):
        raise InvalidArguments(
            f"{kind} name {name!r} is not usable as a filename",
            hint=(
                "Use letters, digits, dot, dash or underscore, up to 64 characters, "
                "starting with a letter or digit — for example 'sma_crossover'."
            ),
        )
    return name


# ---------------------------------------------------------------- strategies

def strategies_dir() -> Path:
    path = home() / "strategies"
    path.mkdir(parents=True, exist_ok=True)
    return path


def strategy_path(name: str) -> Path:
    return strategies_dir() / f"{_checked(name, 'Strategy')}.py"


def save_strategy(name: str, code: str) -> Path:
    path = strategy_path(name)
    path.write_text(code, encoding="utf-8")
    return path


def load_strategy(name: str) -> str:
    path = strategy_path(name)
    if not path.exists():
        raise NotFound(
            f"No strategy named {name!r}",
            hint="list_strategies shows what is here. write_strategy creates one.",
        )
    return path.read_text(encoding="utf-8")


def list_strategies() -> list[dict]:
    rows = []
    for path in sorted(strategies_dir().glob("*.py")):
        stat = path.stat()
        rows.append({
            "name": path.stem,
            "modified": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
            "lines": len(path.read_text(encoding="utf-8").splitlines()),
            "path": str(path),
        })
    return rows


# ----------------------------------------------------------------- backtests

def backtests_dir() -> Path:
    path = home() / "backtests"
    path.mkdir(parents=True, exist_ok=True)
    return path


def new_backtest_id(strategy: str) -> str:
    """Sortable, readable, and unique without a counter that needs locking."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    base = f"{stamp}-{_checked(strategy, 'Strategy')}"
    candidate, suffix = base, 2
    while (backtests_dir() / candidate).exists():
        candidate, suffix = f"{base}-{suffix}", suffix + 1
    return candidate


def backtest_dir(backtest_id: str) -> Path:
    return backtests_dir() / _checked(backtest_id, "Backtest")


def save_backtest(backtest_id: str, meta: dict, perf=None) -> Path:
    """Parameters and result together, so a run can be read back whole."""
    path = backtest_dir(backtest_id)
    path.mkdir(parents=True, exist_ok=True)
    (path / "meta.json").write_text(
        json.dumps(meta, indent=2, default=str), encoding="utf-8"
    )
    if perf is not None and not perf.empty:
        # CSV rather than parquet: it is the format that needs no extra
        # dependency to open, and a person poking at ~/.ziplime is the point.
        perf.to_csv(path / "perf.csv")
    return path


def load_backtest(backtest_id: str) -> dict:
    path = backtest_dir(backtest_id) / "meta.json"
    if not path.exists():
        raise NotFound(
            f"No backtest {backtest_id!r}",
            hint="list_backtests shows the runs on this machine.",
        )
    return json.loads(path.read_text(encoding="utf-8"))


def load_backtest_perf(backtest_id: str):
    """The performance frame of a finished run, or None when it recorded none."""
    import pandas as pd

    path = backtest_dir(backtest_id) / "perf.csv"
    if not path.exists():
        return None
    return pd.read_csv(path, index_col=0, parse_dates=True)


def list_backtests(strategy: str | None = None) -> list[dict]:
    rows = []
    for path in sorted(backtests_dir().iterdir(), reverse=True):
        meta_file = path / "meta.json"
        if not meta_file.is_file():
            continue
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
        except ValueError:
            continue
        if strategy and meta.get("strategy") != strategy:
            continue
        rows.append(meta)
    return rows


def delete_backtest(backtest_id: str) -> None:
    shutil.rmtree(backtest_dir(backtest_id), ignore_errors=True)
