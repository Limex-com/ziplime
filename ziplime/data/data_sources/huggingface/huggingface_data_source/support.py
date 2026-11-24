"""Support for huggingface_data_source."""

import re
import structlog
from ziplime.assets.domain.asset_type import AssetType


_logger = structlog.get_logger(__name__)


ADDRESS_SCHEME = "hf://"


_YEAR_PARTITION = re.compile(r"(?:^|/)(?:knowledge_year|year)=(\d{4})(?:/|$)")


_ENTITY_DOMAIN_ASSET_TYPES = {"us_equities": AssetType.EQUITY}
