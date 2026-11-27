"""Identity keys used by the SQLAlchemy asset repository."""

from ziplime.assets.entities.asset import Asset


def _asset_identity(asset: Asset) -> tuple:
    """What tells two stored assets apart before either has an id: type, ISIN and name -- and, for
    a contract, its expiry.

    Exchanges reuse contract tickers (the Moscow Exchange's ``SiH4`` is the March 2014 and the
    March 2024 delivery) and few contracts carry an ISIN, so without the expiry both deliveries
    resolved to one asset and the second listing was filed under the first contract.
    """
    return type(asset), asset.isin, asset.asset_name, getattr(asset, "expiration_date", None)
