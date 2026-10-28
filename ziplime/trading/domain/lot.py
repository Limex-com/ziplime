import datetime
from dataclasses import dataclass, field


@dataclass
class Lot:
    """One native FIFO holding; converted costs are derived from dated money components."""

    quantity: float
    price: float | None
    commission: float = 0.0
    transaction_id: str = ""
    costs: list[tuple[datetime.datetime, str, float]] = field(default_factory=list)
    cost_by_currency: dict[str, float] = field(default_factory=dict)
