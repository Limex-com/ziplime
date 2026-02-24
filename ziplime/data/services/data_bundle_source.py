import datetime

import polars as pl

from ziplime.constants.period import Period


class DataBundleSource:

    def __init__(self):
        pass

    async def get_data(self, symbols: list[str],
                       frequency: datetime.timedelta | Period,
                       date_from: datetime.datetime,
                       date_to: datetime.datetime,
                       **kwargs
                       ) -> pl.DataFrame:
        pass

    def get_data_sync(self, symbols: list[str],
                      frequency: datetime.timedelta | Period,
                      date_from: datetime.datetime,
                      date_to: datetime.datetime,
                      ) -> pl.DataFrame:
        pass
