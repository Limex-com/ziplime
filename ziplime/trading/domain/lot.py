from dataclasses import dataclass

@dataclass
class Lot:
    quantity: int
    price: float
    commission: float
