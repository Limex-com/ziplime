from typing import Literal

# define a type that can only be one of these three strings
Period = Literal[
    # "1ns",  # (1 nanosecond)
    "1us",  # (1 microsecond)
    "1ms",  # (1 millisecond)
    "1s",  # (1 second)
    "1m",  # (1 minute)
    "1h",  # (1 hour)
    "1d",  # (1 calendar day)
    "1w",  # (1 calendar week)
    "1mo",  # (1 calendar month)
    "1q",  # (1 calendar quarter)
    "1y",  # (1 calendar year)
    # "1i",  # (1 index count)
]
