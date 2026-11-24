"""Extracted sweep costs and windows remain wired into execution and rediscovery."""

import dataclasses
import pickle

import numpy as np
import pandas as pd
import pytest

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.sweep import api, engine, lazy
from ziplime.sweep.backends import Backend
from ziplime.sweep.convert import convert
from ziplime.sweep.costs import Costs
from ziplime.sweep.market import Market
from ziplime.sweep.windows import Windows


@pytest.fixture
def market() -> Market:
    prices = np.array([[10.0, 20.0], [12.0, 18.0], [14.0, 16.0]])
    spot = {"close": prices, "volume": np.full(prices.shape, 1_000.0)}
    history_end = np.broadcast_to(np.arange(1, 4)[:, None], prices.shape).copy()
    return Market(
        index=pd.date_range("2024-01-02", periods=3, tz="UTC"),
        columns=["A", "B"], listings=[10, 20],
        spot=spot, spot_before=spot, marks=prices,
        history_end=history_end, history_end_before=history_end - 1, rows=spot,
        stopped=np.zeros(prices.shape, dtype=bool), auto_close=np.zeros(prices.shape, dtype=bool),
        multiplier=np.ones(2), is_future=np.zeros(2, dtype=bool),
        splits=np.ones(prices.shape), dividends=[],
        session_start=np.ones(3, dtype=bool), session_end=np.ones(3, dtype=bool),
        row_days=history_end - 1,
    )


@pytest.fixture
def costs() -> Costs:
    return Costs(
        per_unit=np.array([0.1, 0.2]), min_trade=np.zeros(2), initial=np.zeros(2),
        unit_model=np.ones(2), per_dollar=np.zeros(2), per_trade=np.zeros(2),
        percentage=np.zeros(2), spread=np.zeros(2), volume_limit=np.ones(2),
        slippage_kind=np.zeros(2),
    )


def test_costs_copy_to_backend_and_round_trip_for_workers(costs: Costs) -> None:
    device = costs.on(Backend("numpy"))
    restored = pickle.loads(pickle.dumps(costs))

    assert type(device) is type(restored) is Costs
    assert device is not costs
    for field in dataclasses.fields(Costs):
        host_values = getattr(costs, field.name)
        np.testing.assert_array_equal(getattr(device, field.name), host_values[None, :])
        np.testing.assert_array_equal(getattr(restored, field.name), host_values)
    np.testing.assert_allclose(device.projected(np, np.array([[2.0, 3.0]]), 100.0), [[0.2, 0.6]])


@pytest.mark.parametrize("daily", [False, True])
def test_windows_rediscover_prepare_and_select_history(market: Market, daily: bool) -> None:
    from ziplime.sweep.errors import Rediscover
    windows = Windows(market, Backend("numpy"))

    with pytest.raises(Rediscover):
        windows.value("close", "mean", 2, False, daily)
    windows.request("close", "mean", False, [1], daily)
    windows.prepare()
    windows.now = windows.at(2)

    np.testing.assert_array_equal(windows.value("close", "mean", 2, False, daily), [[13.0, 17.0]])
    np.testing.assert_array_equal(
        windows.value("close", "mean", np.array([[1], [2]]), False, daily),
        [[14.0, 16.0], [13.0, 17.0]],
    )


def test_lazy_history_raises_the_legacy_rediscovery_exception(market: Market) -> None:
    from ziplime.sweep.errors import Rediscover
    windows = Windows(market, Backend("numpy"))
    rows = lazy.leaf(None, 0, "close", 2, False, False).to_numpy()
    value = lazy.record(np.mean, (rows,))

    with pytest.raises(Rediscover):
        windows.lazy.value(value)


def test_engine_runs_history_strategy_with_extracted_costs_and_windows(
    market: Market, costs: Costs, monkeypatch: pytest.MonkeyPatch,
) -> None:
    from ziplime.sweep.all_assets import ALL

    monkeypatch.setattr(type(ALL), "_columns", None)

    async def handle_data(context: api.Context, data: api.Data) -> None:
        for name, asset in context.universe.items():
            history = await data.history(assets=[asset], bar_count=2, fields=["close"])
            mean = history["close"].mean()
            current = await data.current(assets=[asset], fields=["close"])
            if current["close"][0] > mean:
                await context.order_target(asset, 1)

    windows = Windows(market, Backend("numpy"))
    result = engine.run(
        market=market, hooks=engine.Hooks(convert(handle_data), None, []),
        configs=[BaseAlgorithmConfig()], states=[{}], signals={}, signal_index=None,
        timing="same_close", initial_cash=100.0, costs=costs, backend="numpy",
        compile=False, record=True, windows=windows,
    )

    np.testing.assert_allclose(result.value[:, 0], [100.0, 99.9, 101.9])
    np.testing.assert_allclose(result.cash, [87.9])
    np.testing.assert_array_equal(result.positions, [[1.0, 0.0]])
    np.testing.assert_allclose(result.fees, [0.1])
    np.testing.assert_array_equal(result.fills, [1])
    assert not result.unsupported.any()
