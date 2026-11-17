import logging

import aiocache
import structlog
from aiocache import Cache

from ziplime.exchanges.exchange import Exchange
from ziplime.exchanges.repositories.exchange_repository import ExchangeRepository


class InMemoryExchangeRepository(ExchangeRepository):
    def __init__(self, logger: logging.Logger = structlog.getLogger(__name__)) -> None:
        super().__init__()
        self._logger = logger
        self._exchanges = {}

    @aiocache.cached(cache=Cache.MEMORY)
    async def get_default_exchange(self) -> Exchange:
        return next(filter(lambda x: x.is_default, list(self._exchanges.values())))

    async def get_all_exchanges(self) -> list[Exchange]:
        return list(self._exchanges.values())

    def get_all_exchanges_sync(self) -> list[Exchange]:
        return list(self._exchanges.values())

    async def get_exchange_by_mic(self, mic: str) -> Exchange:
        return self._exchanges[mic]

    async def add_exchange(self, exchange: Exchange) -> None:
        self._exchanges[exchange.name] = exchange
