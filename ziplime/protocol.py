import enum
from contextlib import contextmanager


class DataSourceType(enum.Enum):
    AS_TRADED_EQUITY = 0
    MERGER = 1
    SPLIT = 2
    DIVIDEND = 3
    TRADE = 4
    TRANSACTION = 5
    ORDER = 6
    EMPTY = 7
    DONE = 8
    CUSTOM = 9
    BENCHMARK = 10
    COMMISSION = 11
    CLOSE_POSITION = 12


@contextmanager
def handle_non_market_minutes(bar_data):
    try:
        bar_data._handle_non_market_minutes = True
        yield
    finally:
        bar_data._handle_non_market_minutes = False
