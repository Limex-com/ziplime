from ziplime.finance.shared import AllowedAssetMarker

from ziplime.assets.entities.equity import Equity
from ziplime.finance.slippage.slippage_model import SlippageModel


class EquitySlippageModel(SlippageModel, metaclass=AllowedAssetMarker):
    """Base class for slippage models which only support equities."""

    allowed_asset_types = (Equity,)
