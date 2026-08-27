from ziplime.assets.entities.bond import Bond
from ziplime.finance.commission.commission_model import CommissionModel
from ziplime.finance.shared import AllowedAssetMarker


class BondCommissionModel(CommissionModel, metaclass=AllowedAssetMarker):
    """
    Base class for commission models which only support bonds.
    """

    allowed_asset_types = (Bond,)
