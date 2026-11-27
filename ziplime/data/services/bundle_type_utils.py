"""Asset type normalization for bundle selection."""

from typing import Sequence
from ziplime.assets.domain.asset_type import AssetType


def _asset_type_names(asset_type: AssetType | Sequence[AssetType]) -> str:
    """Render one or several asset types for an error message."""
    if isinstance(asset_type, AssetType):
        return asset_type.value
    return " / ".join(candidate.value for candidate in asset_type)
