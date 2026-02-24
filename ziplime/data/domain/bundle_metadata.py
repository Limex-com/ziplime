import datetime
from dataclasses import dataclass

@dataclass
class BundleMetadata:
    name: str
    version: str
    calendar_name: str
    start_date: datetime.datetime
    end_date: datetime.datetime
    frequency: datetime.timedelta
