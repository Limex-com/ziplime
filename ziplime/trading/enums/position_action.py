from enum import Enum


class PositionAction(Enum):
    """
    Enum representing position actions for derivatives trading.
    """
    OPEN = "OPEN"  # Open a new position or add to existing
    CLOSE = "CLOSE"  # Close an existing position
    NIL = "NIL"  # No specific position action (e.g., for spot trading)
    REDUCE = "REDUCE"  # Reduce size of an existing position
    FLIP = "FLIP"  # Change position from long to short or vice versa
