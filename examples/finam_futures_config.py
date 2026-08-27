"""Shared settings for the Finam MOEX FORTS futures examples."""
import datetime
import os
from pathlib import Path

#: The asset database the futures examples read and write.
ASSET_DB_PATH = str(Path(Path(__file__).parent.parent, "data", "assets.sqlite").absolute())

#: MOEX derivatives market.
MIC = "RTSX"

#: exchange_calendars name for the Moscow Exchange.
TRADING_CALENDAR = "XMOS"

#: Futures chains to ingest. Roots come from
#: ziplime.data.data_sources.finam.moex_futures.MOEX_FUTURES_ROOTS.
# RUB-quoted roots only: mixing a USD-quoted root (RI, BR, GD, SV) into one
# portfolio would add up P&L in two currencies.
# RUB-quoted MOEX roots for the general strategy examples. The USD-quoted natural gas roots live
# in their own bundle -- see ingest_natgas_arbitrage_data.py -- because mixing currencies in one
# portfolio would add up P&L in two of them.
ROOT_SYMBOLS = ["Si", "SR", "GZ", "MX", "GL"]

#: Natural gas on both venues: MOEX FORTS and the NYMEX Henry Hub contract Finam mirrors. Both are
#: quoted in USD, which is what makes a cross-venue spread coherent.
NATGAS_ROOT_SYMBOLS = ["NG", "NG.XNYM"]
NATGAS_BUNDLE_NAME = "finam_natgas_arbitrage"
NATGAS_START = datetime.date(2023, 6, 1)
NATGAS_END = datetime.date(2026, 8, 1)

BUNDLE_NAME = "finam_futures_daily"

INGEST_START = datetime.date(2021, 1, 1)
INGEST_END = datetime.date(2026, 8, 1)


#: Optional. Leave unset: the client reads the account the token owns from TokenDetails, which
#: unlocks contract specifications and expiration dates (they work for archived contracts too).
ACCOUNT_ID_ENV = "FINAM_ACCOUNT_ID"


def require_finam_secret() -> str:
    """Return the Finam API secret, with a pointer to how to set it if missing."""
    secret = os.environ.get("FINAM_API_SECRET")
    if not secret:
        raise SystemExit(
            "FINAM_API_SECRET is not set.\n"
            "Get an API token in the Finam cabinet and run, for example:\n"
            "  export FINAM_API_SECRET=tapi_sk_...\n"
            "Optionally also set FINAM_ACCOUNT_ID to let ziplime read contract specifications "
            "(size, tick, expiry) from the API instead of the built-in MOEX table."
        )
    return secret
