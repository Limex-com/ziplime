from functools import lru_cache

import pandas as pd
from exchange_calendars import get_calendar as ec_get_calendar  # get_calendar,


@lru_cache
def get_calendar(*args, start=None, **kwargs):
    if not start:
        if args and args[0] in ["us_futures", "CMES", "XNYS", "NYSE"]:
            start = pd.Timestamp("1886-01-01")
    # if args[0] in ["us_futures", "CMES", "XNYS", "NYSE"]:
    #     return ec_get_calendar(*args, side="right", start=pd.Timestamp("1886-01-01"))
    return ec_get_calendar(*args, start=start, side="right")
