import datetime
import enum


class DataFrequency(enum.Enum):
    SECOND = "1s"
    MINUTE = "1m"
    MINUTE_3 = "3m"
    MINUTE_5 = "5m"
    MINUTE_15 = "15m"
    MINUTE_30 = "30m"

    HOUR = "1h"
    HOUR_2 = "1h"
    HOUR_4 = "4h"
    HOUR_6 = "6h"
    HOUR_8 = "8h"
    HOUR_12 = "12h"

    DAY = "1d"
    DAY_3 = "1d"

    WEEK = "1w"

    MONTH = "1M"

    def to_timedelta(self) -> datetime.timedelta:
        time_unit, value = self.value[-1:], int(self.value[:-1])
        match time_unit:
            case "s":
                return datetime.timedelta(seconds=value)
            case "m":
                return datetime.timedelta(minutes=value)
            case "h":
                return datetime.timedelta(hours=value)
            case "d":
                return datetime.timedelta(days=value)
            case "w":
                return datetime.timedelta(weeks=value)
            case "M":
                return datetime.timedelta(days=30)
            case _:
                raise ValueError(f"Invalid time unit: {time_unit}")
