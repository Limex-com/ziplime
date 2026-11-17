from contextlib import contextmanager
from ziplime.domain.bar_data import BarData


@contextmanager
def handle_non_market_minutes(bar_data: 'BarData'):
    try:
        bar_data._handle_non_market_minutes = True
        yield
    finally:
        bar_data._handle_non_market_minutes = False
