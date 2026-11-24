"""No futures margin model for margin."""

from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.margin.futures_margin_model import FuturesMarginModel
from ziplime.finance.margin.support import NO_MARGIN_MODEL_WARNING
from ziplime.finance.margin.support import _logger


class NoFuturesMarginModel(FuturesMarginModel):
    """The default: margin is not modelled, and the backtest says so.

    Results remain valid for PnL, but leverage, buying power and margin calls are not simulated,
    so a strategy can hold positions no broker would fund.
    """

    models_margin = False

    def __init__(self) -> None:
        self._warned = False

    def warn_once(self) -> None:
        """Emit the one-time realism warning."""
        if self._warned:
            return
        self._warned = True
        _logger.warning(
            f"{NO_MARGIN_MODEL_WARNING}: futures positions are being taken without a margin "
            f"model, so leverage and margin calls are not simulated. PnL is still correct; "
            f"treat position sizing and leverage figures as unconstrained. Pass a "
            f"futures_margin_model to model margin."
        )

    def initial_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        return 0.0

    def maintenance_margin(self, asset: ExchangeAsset, amount: float, price: float) -> float:
        return 0.0
