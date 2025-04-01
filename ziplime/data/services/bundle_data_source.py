import datetime

import polars as pl


class BundleDataSource:

    def __init__(self):
        pass

    async def get_data(self, symbols: list[str],
                       frequency: datetime.timedelta,
                       date_from: datetime.datetime,
                       date_to: datetime.datetime,
                       ) -> pl.DataFrame:
        pass
