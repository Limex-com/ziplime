import enum


class TradingSignalType(enum.Enum):
    POSITION = "POSITION"
    DCA = "DCA"
    TWAP = "TWAP"
    ARBITRAGE = "ARBITRAGE"
