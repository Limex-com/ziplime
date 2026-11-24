"""Grouping for chain."""

import datetime
from collections.abc import Sequence
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.finance.options.chain.option_chain import OptionChain
from ziplime.finance.options.chain.utils import contract_of


def group_by_expiry(listings: Sequence[ExchangeAsset]) -> dict[datetime.date, OptionChain]:
    """Split a flat list of option listings into one chain per expiry."""
    expiries = sorted({contract_of(listing).expiration_date for listing in listings})
    return {expiry: OptionChain.from_listings(listings, expiration_date=expiry)
            for expiry in expiries}
