from abc import abstractmethod, ABC

from ziplime.exchanges.exchange import Exchange


class ExchangeRepository(ABC):
    @abstractmethod
    async def get_exchange_by_mic(self, mic: str) -> Exchange:
        raise NotImplementedError

    @abstractmethod
    async def add_exchange(self, exchange: Exchange) -> Exchange:
        raise NotImplementedError

    @abstractmethod
    async def get_default_exchange(self) -> Exchange:
        raise NotImplementedError

    @abstractmethod
    async def get_all_exchanges(self) -> list[Exchange]:
        raise NotImplementedError

    @abstractmethod
    def get_all_exchanges_sync(self) -> list[Exchange]:
        raise NotImplementedError
