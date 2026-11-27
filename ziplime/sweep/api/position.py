"""Position for api."""

from ziplime.sweep.errors import NotConvertible


class Position:
    """``portfolio.positions.get(asset)``: the position's amount, for every combination."""

    def __init__(self, amount) -> None:
        self.amount = amount

    def sweep_truth(self):
        """The engine keeps only open positions: a missing one is None, an open one is truthy."""
        return self.amount != 0

    def __getattr__(self, name: str):
        if name.startswith("__") or name.startswith("sweep_") or name in ("shape", "dtype"):
            raise AttributeError(name)
        raise NotConvertible(f"position.{name} is not simulated in a sweep; position.amount is.")
