from ziplime.assets.entities.option_contract import OptionContract
from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.shared import AllowedAssetMarker


class OptionCommissionModel(CommissionModel, metaclass=AllowedAssetMarker):
    """
    Base class for commission models which only support options.
    """

    allowed_asset_types = (OptionContract,)
