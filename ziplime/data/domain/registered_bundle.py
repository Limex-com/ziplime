import dataclasses
import datetime
from typing import Callable


@dataclasses.dataclass
class RegisteredBundle:
    calendar_name:str
    start_session:datetime.datetime
    end_session: datetime.datetime
    ingest: Callable
