"""Roll finder for roll_finder."""

import datetime
import structlog
from ziplime.assets.domain.ordered_contracts import OrderedContracts
from ziplime.assets.entities.exchange_asset import ExchangeAsset
from ziplime.assets.services.asset_service import AssetService
from ziplime.assets.domain.roll_date_utils import _as_date


class RollFinder:
    """Base class: maps ``(root_symbol, date, offset)`` onto a contract.

    Args:
        asset_service: Used to load the contract chain of a root symbol.
        mic: Restrict chains to a single exchange, if given.
    """

    def __init__(self, asset_service: AssetService, mic: str | None = None, logger=None) -> None:
        self._asset_service = asset_service
        self._mic = mic
        self._ordered_contracts: dict[str, OrderedContracts] = {}
        self._logger = logger or structlog.get_logger(__name__)

    async def get_ordered_contracts(self, root_symbol: str) -> OrderedContracts:
        """Return (and memoise) the chain of ``root_symbol``."""
        if root_symbol not in self._ordered_contracts:
            self._ordered_contracts[root_symbol] = await self._asset_service.get_ordered_contracts(
                root_symbol=root_symbol, mic=self._mic)
        return self._ordered_contracts[root_symbol]

    async def get_contract_center(self, root_symbol: str,
                                  dt: datetime.date | datetime.datetime,
                                  offset: int = 0) -> ExchangeAsset | None:
        """Return the contract a continuous future holds on ``dt`` at ``offset`` from the front."""
        raise NotImplementedError("get_contract_center")

    async def get_rolls(self, root_symbol: str,
                        start: datetime.date | datetime.datetime,
                        end: datetime.date | datetime.datetime,
                        offset: int = 0) -> list[tuple[ExchangeAsset, datetime.date | None]]:
        """Return ``[(contract, roll_date), ...]`` covering ``[start, end]``.

        ``roll_date`` is the first date the *next* contract is held; it is ``None`` for the last
        segment, which runs to ``end``. Segments are returned oldest first, which is the order
        history has to be spliced in.
        """
        start, end = _as_date(start), _as_date(end)
        ordered = await self.get_ordered_contracts(root_symbol)
        if not len(ordered):
            return []

        rolls: list[tuple[ExchangeAsset, datetime.date | None]] = []
        current: ExchangeAsset | None = None
        # Walking sessions is exact and cheap enough: a chain covers a few thousand days at most,
        # and both roll rules are pure lookups against pre-loaded data.
        day = start
        while day <= end:
            contract = await self.get_contract_center(root_symbol, day, offset)
            if contract is not None and (current is None or contract.sid != current.sid):
                if current is not None:
                    rolls[-1] = (rolls[-1][0], day)
                rolls.append((contract, None))
                current = contract
            day += datetime.timedelta(days=1)
        return rolls
