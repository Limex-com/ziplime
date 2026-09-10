from ziplime.assets.entities.bond import Bond
from ziplime.finance.shared import AllowedAssetMarker
from ziplime.finance.slippage.slippage_model import SlippageModel


class BondSlippageModel(SlippageModel, metaclass=AllowedAssetMarker):
    """Base class for slippage models which only support bonds."""

    allowed_asset_types = (Bond,)
