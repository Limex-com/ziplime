from ziplime.assets.entities.option_contract import OptionContract
from ziplime.finance.shared import AllowedAssetMarker
from ziplime.finance.slippage.slippage_model import SlippageModel


class OptionSlippageModel(SlippageModel, metaclass=AllowedAssetMarker):
    """Base class for slippage models which only support options."""

    allowed_asset_types = (OptionContract,)
