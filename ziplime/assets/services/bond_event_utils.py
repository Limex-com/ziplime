"""Bond event rebinding during asset ingestion."""

import dataclasses
from ziplime.assets.entities.bond import Bond
from ziplime.assets.entities.bond_event import BondEvent


def _rebind_bond_events(bond_events: list[BondEvent], bonds: list[Bond]) -> list[BondEvent]:
    """Point each event at the stored bond of the same identity, so it has a real asset id."""
    stored = {(bond.isin, bond.asset_name): bond for bond in bonds}
    rebound = []
    for event in bond_events:
        bond = stored.get((event.asset.isin, event.asset.asset_name))
        rebound.append(event if bond is None else dataclasses.replace(event, asset=bond))
    return rebound
