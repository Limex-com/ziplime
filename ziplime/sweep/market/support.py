"""Support for market."""

import datetime


#: Fields a bundle row may carry that the sweep reads.
FIELDS = ("open", "high", "low", "close", "price", "volume")


BEFORE_TRADING_START = datetime.timedelta(minutes=46)


WINDOW_OPS = ("mean", "sum", "std", "var", "min", "max", "median", "first", "last", "count")


DAILY_AGGREGATE = {"open": "first", "high": "max", "low": "min", "volume": "sum"}
