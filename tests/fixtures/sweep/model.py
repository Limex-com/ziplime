"""Sweep parity: a model fitted in initialize -- with a parameter of the config, so each value
of it is its own model -- predicting from features computed on history rows in handle_data."""
import numpy as np
from sklearn.linear_model import LogisticRegression

from ziplime.config.base_algorithm_config import BaseAlgorithmConfig
from ziplime.finance.execution import MarketOrder

WARMUP = 0


class AlgorithmConfig(BaseAlgorithmConfig):
    strength: float = 1.0
    threshold: float = 0.5
    symbols: list[str] = ["JNJ@XNYS", "KO@XNYS"]


async def initialize(context):
    context.universe = {s.split("@")[0]: await context.symbol(s)
                        for s in context.algorithm.config.symbols}
    # A model of next-day direction from two momentum features, on a fixed sample
    rng = np.random.default_rng(0)
    features = rng.normal(0, 0.03, (400, 2))
    target = (features @ np.array([1.5, -0.7]) + rng.normal(0, 0.02, 400)) > 0
    context.model = LogisticRegression(C=context.algorithm.config.strength).fit(features, target)


async def handle_data(context, data):
    config = context.algorithm.config
    for name, asset in context.universe.items():
        p = (await data.history(assets=[asset], bar_count=30, fields=["close"]))["close"].to_numpy()
        features = np.array([[p[-1] / p[-5] - 1, p[-1] / p[-20] - 1]])
        up = context.model.predict_proba(features)[0, 1]
        held = await context.portfolio.get_asset_positions_amount(asset)
        if up > config.threshold and held == 0:
            await context.order_target_percent(asset, 0.45, style=MarketOrder())
        elif up < config.threshold and held > 0:
            await context.order_target_percent(asset, 0.0, style=MarketOrder())
