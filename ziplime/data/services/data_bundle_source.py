import datetime
from abc import ABC, abstractmethod

import polars as pl

from ziplime.constants.period import Period


class DataBundleSource(ABC):
    """Contract for providers used by bundle ingestion.

    Providers must implement the asynchronous API.  The synchronous method is
    deliberately optional because most providers are network-backed; callers
    that need it receive an explicit error instead of silently getting no data.
    """

    @abstractmethod
    async def get_data(self, symbols: list[str],
                       frequency: datetime.timedelta | Period,
                       date_from: datetime.datetime,
                       date_to: datetime.datetime,
                       **kwargs
                       ) -> pl.DataFrame:
        raise NotImplementedError

    def get_data_sync(self, symbols: list[str],
                      frequency: datetime.timedelta | Period,
                      date_from: datetime.datetime,
                      date_to: datetime.datetime,
                      ) -> pl.DataFrame:
        raise NotImplementedError(
            f"{type(self).__name__} does not provide a synchronous data API"
        )
