"""Contract ticker for yahoo_futures."""

from dataclasses import dataclass
from ziplime.data.data_sources.yahoo.yahoo_futures.support import MONTH_OF_CODE


@dataclass(frozen=True)
class ContractTicker:
    """A dated contract ticker, decomposed."""

    ticker: str
    root_symbol: str
    month_code: str
    year: int

    @property
    def delivery_month(self) -> int:
        return MONTH_OF_CODE[self.month_code]
